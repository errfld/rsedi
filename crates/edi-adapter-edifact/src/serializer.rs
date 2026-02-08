//! EDIFACT serializer

use std::collections::BTreeMap;

use edi_ir::{Document, Node, NodeType, Value};

use crate::{Error, Result};

/// Serializer for EDIFACT documents.
pub struct EdifactSerializer;

impl EdifactSerializer {
    /// Create a new EDIFACT serializer.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Serialize a document to EDIFACT text.
    ///
    /// Supports two IR shapes:
    /// 1. Native EDIFACT IR (`NodeType::Segment` with `NodeType::Element` children).
    /// 2. Mapping output field notation (`SEG.e1`, `SEG.e1.c2`, or contextual `e1` under a
    ///    segment-like parent node such as `LIN`).
    ///
    /// # Errors
    ///
    /// Returns an error if the document does not contain any serializable EDIFACT segments.
    pub fn serialize_document(&self, document: &Document) -> Result<String> {
        let mut native_segments = Vec::new();
        collect_native_segment_strings(&document.root, &mut native_segments);
        if !native_segments.is_empty() {
            return Ok(native_segments.join("\n"));
        }

        let mut mapped_fields = Vec::new();
        collect_mapped_fields(&document.root, None, &mut mapped_fields);
        if mapped_fields.is_empty() {
            return Err(Error::Serialize(
                "No serializable EDIFACT segments found. Expected Segment nodes with Element \
                 children, or mapping fields named like SEG.e1 / SEG.e1.c1."
                    .to_string(),
            ));
        }

        let segments = build_segments_from_fields(&mapped_fields)?;
        if segments.is_empty() {
            return Err(Error::Serialize(
                "Unable to build EDIFACT segments from mapped fields".to_string(),
            ));
        }

        Ok(segments
            .iter()
            .map(SegmentAccumulator::render)
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

impl Default for EdifactSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MappedField {
    descriptor: SegmentDescriptor,
    element_index: usize,
    component_index: Option<usize>,
    value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentDescriptor {
    tag: String,
    qualifier: Option<Qualifier>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Qualifier {
    /// Qualifier is placed in element 1, e.g. `NAD_BY` -> `NAD+BY`.
    Element(String),
    /// Qualifier is placed in component 1 of element 1, e.g. `QTY21` -> `QTY+21:...`.
    Component(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ElementValue {
    Simple(String),
    Composite(BTreeMap<usize, String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentAccumulator {
    descriptor: SegmentDescriptor,
    elements: BTreeMap<usize, ElementValue>,
}

impl SegmentDescriptor {
    fn from_token(token: &str) -> Option<Self> {
        if let Some((tag, qualifier)) = token.split_once('_') {
            if is_segment_tag(tag) && !qualifier.is_empty() && is_segment_qualifier(qualifier) {
                return Some(Self {
                    tag: tag.to_string(),
                    qualifier: Some(Qualifier::Element(qualifier.to_string())),
                });
            }
            return None;
        }

        if token.len() > 3 {
            let (prefix, suffix) = token.split_at(3);
            if is_segment_tag(prefix) && !suffix.is_empty() && is_segment_qualifier(suffix) {
                return Some(Self {
                    tag: prefix.to_string(),
                    qualifier: Some(Qualifier::Component(suffix.to_string())),
                });
            }
        }

        if is_segment_tag(token) {
            return Some(Self {
                tag: token.to_string(),
                qualifier: None,
            });
        }

        None
    }

    fn remap_position(
        &self,
        element_index: usize,
        component_index: Option<usize>,
    ) -> (usize, Option<usize>) {
        match &self.qualifier {
            Some(Qualifier::Component(_)) if element_index == 1 => match component_index {
                Some(index) => (1, Some(index + 1)),
                None => (1, Some(2)),
            },
            _ => (element_index, component_index),
        }
    }
}

impl SegmentAccumulator {
    fn new(descriptor: SegmentDescriptor) -> Self {
        let mut accumulator = Self {
            descriptor,
            elements: BTreeMap::new(),
        };
        accumulator.seed_qualifier();
        accumulator
    }

    fn seed_qualifier(&mut self) {
        match &self.descriptor.qualifier {
            Some(Qualifier::Element(value)) => {
                self.elements.insert(1, ElementValue::Simple(value.clone()));
            }
            Some(Qualifier::Component(value)) => {
                let mut components = BTreeMap::new();
                components.insert(1, value.clone());
                self.elements.insert(1, ElementValue::Composite(components));
            }
            None => {}
        }
    }

    fn can_accept(&self, field: &MappedField) -> bool {
        if self.descriptor != field.descriptor {
            return false;
        }

        let (element_index, component_index) = self
            .descriptor
            .remap_position(field.element_index, field.component_index);

        match (self.elements.get(&element_index), component_index) {
            (None, _) => true,
            (Some(ElementValue::Simple(_)), None) => false,
            (Some(ElementValue::Simple(_)), Some(_)) => false,
            (Some(ElementValue::Composite(_)), None) => false,
            (Some(ElementValue::Composite(existing)), Some(index)) => {
                !existing.contains_key(&index)
            }
        }
    }

    fn insert(&mut self, field: &MappedField) -> Result<()> {
        let (element_index, component_index) = self
            .descriptor
            .remap_position(field.element_index, field.component_index);

        match component_index {
            None => {
                if self.elements.contains_key(&element_index) {
                    return Err(Error::Serialize(format!(
                        "Duplicate value for segment '{}' element e{}",
                        self.descriptor.tag, element_index
                    )));
                }
                self.elements
                    .insert(element_index, ElementValue::Simple(field.value.clone()));
            }
            Some(component) => match self.elements.get_mut(&element_index) {
                None => {
                    let mut components = BTreeMap::new();
                    components.insert(component, field.value.clone());
                    self.elements
                        .insert(element_index, ElementValue::Composite(components));
                }
                Some(ElementValue::Simple(_)) => {
                    return Err(Error::Serialize(format!(
                        "Cannot map component c{} into simple element e{} for segment '{}'",
                        component, element_index, self.descriptor.tag
                    )));
                }
                Some(ElementValue::Composite(existing)) => {
                    if existing.insert(component, field.value.clone()).is_some() {
                        return Err(Error::Serialize(format!(
                            "Duplicate value for segment '{}' element e{} component c{}",
                            self.descriptor.tag, element_index, component
                        )));
                    }
                }
            },
        }

        Ok(())
    }

    fn render(&self) -> String {
        let max_element = self.elements.keys().copied().max().unwrap_or(0);
        let mut elements = Vec::with_capacity(max_element);

        for index in 1..=max_element {
            let element = match self.elements.get(&index) {
                None => String::new(),
                Some(ElementValue::Simple(value)) => escape_edifact_value(value),
                Some(ElementValue::Composite(components)) => {
                    let max_component = components.keys().copied().max().unwrap_or(0);
                    let mut component_values = Vec::with_capacity(max_component);
                    for component_index in 1..=max_component {
                        let value = components
                            .get(&component_index)
                            .map_or_else(String::new, |value| escape_edifact_value(value));
                        component_values.push(value);
                    }
                    while component_values.last().is_some_and(String::is_empty) {
                        component_values.pop();
                    }
                    component_values.join(":")
                }
            };
            elements.push(element);
        }

        while elements.last().is_some_and(String::is_empty) {
            elements.pop();
        }

        if elements.is_empty() {
            format!("{}'", self.descriptor.tag)
        } else {
            format!("{}+{}'", self.descriptor.tag, elements.join("+"))
        }
    }
}

fn collect_native_segment_strings(node: &Node, segments: &mut Vec<String>) {
    if matches!(node.node_type, NodeType::Segment) {
        segments.push(serialize_native_segment(node));
    }

    for child in &node.children {
        collect_native_segment_strings(child, segments);
    }
}

fn serialize_native_segment(segment: &Node) -> String {
    let mut rendered = segment.name.clone();
    let mut elements = Vec::new();

    for element in &segment.children {
        if !matches!(element.node_type, NodeType::Element) {
            continue;
        }

        if element.children.is_empty() {
            let value = element
                .value
                .as_ref()
                .and_then(Value::as_string)
                .unwrap_or_default();
            elements.push(escape_edifact_value(&value));
            continue;
        }

        let components = element
            .children
            .iter()
            .map(|component| {
                component
                    .value
                    .as_ref()
                    .and_then(Value::as_string)
                    .map_or_else(String::new, |value| escape_edifact_value(&value))
            })
            .collect::<Vec<_>>()
            .join(":");
        elements.push(components);
    }

    while elements.last().is_some_and(String::is_empty) {
        elements.pop();
    }

    if !elements.is_empty() {
        rendered.push('+');
        rendered.push_str(&elements.join("+"));
    }

    rendered.push('\'');
    rendered
}

fn collect_mapped_fields(
    node: &Node,
    context_segment: Option<&str>,
    fields: &mut Vec<MappedField>,
) {
    let next_context = if is_segment_context_name(&node.name) {
        Some(node.name.as_str())
    } else {
        context_segment
    };

    if let Some(value) = node.value.as_ref().and_then(Value::as_string) {
        if let Some((descriptor, element_index, component_index)) =
            parse_mapped_field_name(&node.name, next_context)
        {
            fields.push(MappedField {
                descriptor,
                element_index,
                component_index,
                value,
            });
        }
    }

    for child in &node.children {
        collect_mapped_fields(child, next_context, fields);
    }
}

fn parse_mapped_field_name(
    name: &str,
    context_segment: Option<&str>,
) -> Option<(SegmentDescriptor, usize, Option<usize>)> {
    if let Some((segment_token, element_part)) = name.split_once('.') {
        let descriptor = SegmentDescriptor::from_token(segment_token)?;
        let (element_index, component_index) = parse_element_reference(element_part)?;
        return Some((descriptor, element_index, component_index));
    }

    let descriptor = SegmentDescriptor::from_token(context_segment?)?;
    let (element_index, component_index) = parse_element_reference(name)?;
    Some((descriptor, element_index, component_index))
}

fn parse_element_reference(name: &str) -> Option<(usize, Option<usize>)> {
    let mut parts = name.split('.');
    let element = parts.next()?;
    let element_index = parse_ref_index(element, 'e')?;
    let component_index = match parts.next() {
        None => None,
        Some(component) => Some(parse_ref_index(component, 'c')?),
    };
    if parts.next().is_some() {
        return None;
    }
    Some((element_index, component_index))
}

fn parse_ref_index(value: &str, prefix: char) -> Option<usize> {
    let remainder = value.strip_prefix(prefix)?;
    if remainder.is_empty() || !remainder.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let parsed = remainder.parse::<usize>().ok()?;
    if parsed == 0 { None } else { Some(parsed) }
}

fn build_segments_from_fields(fields: &[MappedField]) -> Result<Vec<SegmentAccumulator>> {
    let mut segments: Vec<SegmentAccumulator> = Vec::new();

    for field in fields {
        if let Some(last) = segments.last_mut() {
            if last.can_accept(field) {
                last.insert(field)?;
                continue;
            }
        }

        let mut new_segment = SegmentAccumulator::new(field.descriptor.clone());
        new_segment.insert(field)?;
        segments.push(new_segment);
    }

    Ok(segments)
}

fn escape_edifact_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '?' | '+' | ':' | '\'' => {
                escaped.push('?');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn is_segment_context_name(name: &str) -> bool {
    SegmentDescriptor::from_token(name).is_some()
}

fn is_segment_tag(token: &str) -> bool {
    (2..=10).contains(&token.len()) && token.chars().all(|ch| ch.is_ascii_uppercase())
}

fn is_segment_qualifier(token: &str) -> bool {
    token
        .chars()
        .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use edi_ir::{Node, NodeType, Value};

    #[test]
    fn serializes_native_segment_nodes() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut segment = Node::new("BGM", NodeType::Segment);
        segment.add_child(Node::with_value(
            "1001",
            NodeType::Element,
            Value::String("220".to_string()),
        ));
        segment.add_child(Node::with_value(
            "1004",
            NodeType::Element,
            Value::String("PO1".to_string()),
        ));
        root.add_child(segment);

        let rendered = EdifactSerializer::new()
            .serialize_document(&Document::new(root))
            .expect("serialize");

        assert_eq!(rendered, "BGM+220+PO1'");
    }

    #[test]
    fn serializes_mapped_segment_fields() {
        let mut root = Node::new("EANCOM_D96A_ORDERS", NodeType::Root);
        root.add_child(Node::with_value(
            "BGM.e1",
            NodeType::Field,
            Value::String("220".to_string()),
        ));
        root.add_child(Node::with_value(
            "BGM.e2",
            NodeType::Field,
            Value::String("PO1".to_string()),
        ));

        let rendered = EdifactSerializer::new()
            .serialize_document(&Document::new(root))
            .expect("serialize");

        assert_eq!(rendered, "BGM+220+PO1'");
    }

    #[test]
    fn serializes_contextual_fields_from_segment_group() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut lin_group = Node::new("LIN", NodeType::SegmentGroup);

        let mut line1 = Node::with_value("e1", NodeType::Field, Value::String("1".to_string()));
        line1.add_child(Node::with_value(
            "QTY21.e1",
            NodeType::Field,
            Value::String("12".to_string()),
        ));
        lin_group.add_child(line1);

        let mut line2 = Node::with_value("e1", NodeType::Field, Value::String("2".to_string()));
        line2.add_child(Node::with_value(
            "QTY21.e1",
            NodeType::Field,
            Value::String("7".to_string()),
        ));
        lin_group.add_child(line2);
        root.add_child(lin_group);

        let rendered = EdifactSerializer::new()
            .serialize_document(&Document::new(root))
            .expect("serialize");

        let lines = rendered.lines().collect::<Vec<_>>();
        assert_eq!(lines, vec!["LIN+1'", "QTY+21:12'", "LIN+2'", "QTY+21:7'"]);
    }

    #[test]
    fn returns_error_without_serializable_shape() {
        let mut root = Node::new("JSON_ORDERS", NodeType::Root);
        root.add_child(Node::with_value(
            "order_number",
            NodeType::Field,
            Value::String("PO1".to_string()),
        ));

        let error = EdifactSerializer::new()
            .serialize_document(&Document::new(root))
            .expect_err("expected shape error");

        assert!(
            error
                .to_string()
                .contains("No serializable EDIFACT segments found"),
            "unexpected error: {error}"
        );
    }
}

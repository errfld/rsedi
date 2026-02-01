//! EDIFACT streaming parser
//!
//! This module provides a streaming parser for EDIFACT documents that yields
//! messages one at a time, supporting large batch files without loading
//! everything into memory.

use crate::syntax::{Separators, SyntaxBuffer};
use crate::{Error, Result};
use edi_ir::document::DocumentMetadata;
use edi_ir::{Document, Node, NodeType, Position, Value};

/// A parsed EDIFACT segment
#[derive(Debug, Clone)]
pub struct Segment {
    /// Segment tag (3 characters)
    pub tag: String,
    /// Data elements (simple or composite)
    pub elements: Vec<Element>,
    /// Position in source
    pub position: Position,
}

/// A data element (simple or composite)
#[derive(Debug, Clone)]
pub enum Element {
    /// Simple element (single value)
    Simple(Vec<u8>),
    /// Composite element (multiple components)
    Composite(Vec<Vec<u8>>),
}

impl Segment {
    /// Convert this segment to an IR Node
    pub fn to_node(&self) -> Node {
        let mut node = Node::new(&self.tag, NodeType::Segment);
        node.set_attribute("source_line", self.position.line.to_string());
        node.set_attribute("source_column", self.position.column.to_string());

        for (i, element) in self.elements.iter().enumerate() {
            let elem_node = match element {
                Element::Simple(value) => Node::with_value(
                    format!("e{}", i + 1),
                    NodeType::Element,
                    Value::String(String::from_utf8_lossy(value).to_string()),
                ),
                Element::Composite(components) => {
                    let mut n = Node::new(format!("e{}", i + 1), NodeType::Element);
                    for (j, comp) in components.iter().enumerate() {
                        let comp_node = Node::with_value(
                            format!("c{}", j + 1),
                            NodeType::Component,
                            Value::String(String::from_utf8_lossy(comp).to_string()),
                        );
                        n.add_child(comp_node);
                    }
                    n
                }
            };
            node.add_child(elem_node);
        }

        node
    }
}

/// Parser for individual segments
pub struct SegmentParser<'a> {
    buffer: SyntaxBuffer<'a>,
    _source_name: String,
}

impl<'a> SegmentParser<'a> {
    /// Create a new segment parser from bytes
    pub fn new(data: &'a [u8], source_name: impl Into<String>) -> Self {
        Self {
            buffer: SyntaxBuffer::new(data),
            _source_name: source_name.into(),
        }
    }

    /// Check if input starts with UNA and parse separators
    pub fn parse_una(&mut self) -> Option<Separators> {
        // Check for UNA at the start (must be at position 0)
        if self.buffer.position() == 0 && self.buffer.data.starts_with(b"UNA") {
            // Read the full UNA segment (9 bytes for UNA + separators)
            if self.buffer.data.len() >= 9 {
                let una_buf = &self.buffer.data[0..9];
                if let Some(sep) = Separators::from_una(una_buf) {
                    self.buffer.set_separators(sep);
                    // UNA is fixed format - position at byte 9 (after the 9 UNA chars)
                    self.buffer.pos = 9;
                    return Some(sep);
                }
            }
        }
        None
    }

    /// Parse the next segment
    pub fn next_segment(&mut self) -> Option<Result<Segment>> {
        // Skip any whitespace/newlines between segments
        self.skip_whitespace();

        if self.buffer.is_empty() {
            return None;
        }

        let (tag_line, tag_column) = self.buffer.line_column();

        // Read segment tag
        let tag_bytes = match self.buffer.read_tag() {
            Some(t) => t,
            None => {
                return Some(Err(Error::Parse {
                    line: tag_line,
                    column: tag_column,
                    message: "Expected segment tag (3 characters)".to_string(),
                }));
            }
        };

        let (post_tag_line, post_tag_column) = self.buffer.line_column();
        let position = Position::new(post_tag_line, post_tag_column, self.buffer.position(), 0);

        if self.buffer.is_empty() {
            return Some(Err(Error::Parse {
                line: post_tag_line,
                column: post_tag_column,
                message: "Unexpected end of input after segment tag".to_string(),
            }));
        }

        if let Some(next) = self.buffer.peek() {
            if next != self.buffer.separators.element && next != self.buffer.separators.segment {
                return Some(Err(Error::Parse {
                    line: post_tag_line,
                    column: post_tag_column,
                    message: "Expected element separator or segment terminator after segment tag"
                        .to_string(),
                }));
            }
        }

        let tag = String::from_utf8_lossy(&tag_bytes).to_string();

        // UNA is special - no elements
        if tag == "UNA" {
            return Some(Ok(Segment {
                tag,
                elements: vec![],
                position,
            }));
        }

        // Parse elements until segment terminator
        let mut elements = Vec::new();
        let mut components = Vec::new();

        // Skip the element separator immediately after the tag (if present)
        if self.buffer.peek() == Some(self.buffer.separators.element) {
            self.buffer.next_byte();
        }

        loop {
            let (value, delimiter) = self.buffer.read_until_delimiter();

            // Handle empty values - when we get an empty value with a delimiter,
            // we need to determine if it's an empty element or empty component
            if value.is_empty() && components.is_empty() {
                if let Some(d) = delimiter {
                    if d == self.buffer.separators.element {
                        // Empty element followed by element separator
                        elements.push(Element::Simple(Vec::new()));
                        continue;
                    } else if d == self.buffer.separators.component {
                        // Empty element followed by component separator
                        // This means we have an empty simple element, then a composite starts
                        // Add the empty simple element first
                        elements.push(Element::Simple(Vec::new()));
                        // Now start the composite with an empty first component
                        components.push(Vec::new());
                        continue;
                    } else if d == self.buffer.separators.segment {
                        // Empty segment or trailing empty element
                        if elements.is_empty() {
                            // Empty segment - nothing to add
                            break;
                        }
                        // Trailing empty element before terminator
                        elements.push(Element::Simple(Vec::new()));
                        break;
                    }
                }
            }

            match delimiter {
                Some(d) if d == self.buffer.separators.component => {
                    // Component separator - add current value as a component
                    components.push(value);
                }
                Some(d) if d == self.buffer.separators.element => {
                    // Element separator - finish current element
                    components.push(value);
                    if components.len() == 1 {
                        // Simple element (single component)
                        elements.push(Element::Simple(components.pop().unwrap()));
                    } else {
                        // Composite element (multiple components)
                        elements.push(Element::Composite(std::mem::take(&mut components)));
                    }
                    components.clear();
                }
                Some(d) if d == self.buffer.separators.segment => {
                    // Segment terminator - finish last element
                    components.push(value);
                    if components.len() == 1 {
                        elements.push(Element::Simple(components.pop().unwrap()));
                    } else {
                        elements.push(Element::Composite(components));
                    }
                    break;
                }
                None => {
                    // End of input without segment terminator
                    components.push(value);
                    if components.len() == 1 {
                        elements.push(Element::Simple(components.pop().unwrap()));
                    } else {
                        elements.push(Element::Composite(components));
                    }
                    break;
                }
                _ => {
                    return Some(Err(Error::Parse {
                        line: post_tag_line,
                        column: post_tag_column,
                        message: "Unexpected delimiter".to_string(),
                    }));
                }
            }
        }

        Some(Ok(Segment {
            tag,
            elements,
            position,
        }))
    }

    fn skip_whitespace(&mut self) {
        while let Some(b) = self.buffer.peek() {
            if b == b' ' || b == b'\n' || b == b'\r' || b == b'\t' {
                self.buffer.next_byte();
            } else {
                break;
            }
        }
    }

    /// Get the remaining unparsed data
    pub fn remaining(&self) -> &[u8] {
        &self.buffer.data[self.buffer.position()..]
    }
}

/// Streaming EDIFACT parser that yields messages one at a time
pub struct EdifactParser;

impl EdifactParser {
    /// Create a new EDIFACT parser
    pub fn new() -> Self {
        Self
    }

    /// Parse a complete EDIFACT document and return all messages
    pub fn parse(&self, data: &[u8], source_name: impl Into<String>) -> Result<Vec<Document>> {
        let source_name = source_name.into();
        let mut documents = Vec::new();

        // Parse segments
        let mut parser = SegmentParser::new(data, &source_name);

        // Check for UNA
        let _una = parser.parse_una();

        let mut current_segments: Vec<Segment> = Vec::new();
        let mut _interchange_ref: Option<String> = None;
        let mut _message_count = 0;

        while let Some(result) = parser.next_segment() {
            let segment = result?;

            match segment.tag.as_str() {
                "UNB" => {
                    // Interchange header
                    if let Some(Element::Simple(ref _sender)) = segment.elements.get(1) {
                        if let Some(Element::Simple(ref id)) = segment.elements.get(4) {
                            _interchange_ref = Some(String::from_utf8_lossy(id).to_string());
                        }
                    }
                    current_segments.push(segment);
                }
                "UNH" => {
                    // Message header - start new message
                    _message_count += 1;
                    current_segments.push(segment);
                }
                "UNT" => {
                    // Message trailer - complete message
                    current_segments.push(segment);
                    // Create document from collected segments
                    if let Some(doc) = self.build_message(&current_segments) {
                        documents.push(doc);
                    }
                    // Keep UNB for next message if in batch
                    let unb = current_segments.drain(..).find(|s| s.tag == "UNB");
                    if let Some(unb) = unb {
                        current_segments.push(unb);
                    }
                }
                "UNZ" => {
                    // Interchange trailer - end of interchange
                    current_segments.push(segment);
                    // Clear for potential next interchange
                    current_segments.clear();
                    _interchange_ref = None;
                }
                _ => {
                    current_segments.push(segment);
                }
            }
        }

        // Handle case where file doesn't end with UNT (malformed)
        if !current_segments.is_empty() {
            if let Some(doc) = self.build_message(&current_segments) {
                documents.push(doc);
            }
        }

        Ok(documents)
    }

    /// Parse a single message from a byte slice
    pub fn parse_message(&self, data: &[u8], source_name: impl Into<String>) -> Result<Document> {
        let mut docs = self.parse(data, source_name)?;
        if docs.is_empty() {
            return Err(Error::Parse {
                line: 1,
                column: 1,
                message: "No message found in data".to_string(),
            });
        }
        if docs.len() > 1 {
            return Err(Error::Parse {
                line: 1,
                column: 1,
                message: "Multiple messages found, use parse() instead".to_string(),
            });
        }
        Ok(docs.remove(0))
    }

    fn build_message(&self, segments: &[Segment]) -> Option<Document> {
        // Find UNH and UNT to identify message boundaries
        let unh_pos = segments.iter().position(|s| s.tag == "UNH")?;
        let unt_pos = segments.iter().position(|s| s.tag == "UNT")?;

        let message_segments = &segments[unh_pos..=unt_pos];

        let (message_type, version, message_ref) = Self::message_info(message_segments);

        // Build document root
        let mut root = Node::new("MESSAGE", NodeType::Message);

        let children = if message_type.as_deref() == Some("ORDERS") {
            Self::group_orders(message_segments)
        } else {
            message_segments
                .iter()
                .map(Segment::to_node)
                .collect::<Vec<_>>()
        };

        for child in children {
            root.add_child(child);
        }

        let mut metadata = DocumentMetadata::default();

        metadata.doc_type = message_type;
        metadata.version = version;
        if let Some(message_ref) = message_ref {
            metadata.message_refs.push(message_ref);
        }

        Some(Document::with_metadata(root, metadata))
    }

    fn message_info(segments: &[Segment]) -> (Option<String>, Option<String>, Option<String>) {
        let mut message_type = None;
        let mut version = None;
        let mut message_ref = None;

        if let Some(unh) = segments.iter().find(|s| s.tag == "UNH") {
            // UNH structure: +message_reference+message_type:version:release:agency'
            if let Some(Element::Composite(ref msg_id)) = unh.elements.get(1) {
                if let Some(type_component) = msg_id.first() {
                    message_type = Some(String::from_utf8_lossy(type_component).to_string());
                }

                let version_str = msg_id
                    .get(1)
                    .map(|v| String::from_utf8_lossy(v).to_string());
                let release_str = msg_id
                    .get(2)
                    .map(|r| String::from_utf8_lossy(r).to_string());

                version = match (version_str, release_str) {
                    (Some(v), Some(r)) => Some(format!("{}_{}", v, r)),
                    (Some(v), None) => Some(v),
                    _ => None,
                };
            }

            if let Some(Element::Simple(ref msg_ref)) = unh.elements.first() {
                message_ref = Some(String::from_utf8_lossy(msg_ref).to_string());
            }
        }

        (message_type, version, message_ref)
    }

    fn group_orders(segments: &[Segment]) -> Vec<Node> {
        let mut children = Vec::new();
        let mut current_group: Option<Node> = None;

        for segment in segments {
            match segment.tag.as_str() {
                "LIN" => {
                    if let Some(group) = current_group.take() {
                        children.push(group);
                    }

                    let mut group = Node::new("LINE_ITEM", NodeType::SegmentGroup);
                    group.add_child(segment.to_node());
                    current_group = Some(group);
                }
                "UNS" | "UNT" => {
                    if let Some(group) = current_group.take() {
                        children.push(group);
                    }
                    children.push(segment.to_node());
                }
                _ => {
                    if let Some(group) = current_group.as_mut() {
                        group.add_child(segment.to_node());
                    } else {
                        children.push(segment.to_node());
                    }
                }
            }
        }

        if let Some(group) = current_group.take() {
            children.push(group);
        }

        children
    }
}

impl Default for EdifactParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_segment() {
        let data = b"UNB+UNOA:3+SENDER+RECEIVER+200101:1200+12345'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "UNB");
        // UNB has 5 data elements: syntax, sender, receiver, datetime, control_ref
        assert_eq!(segment.elements.len(), 5);
    }

    #[test]
    fn test_parse_segment_with_composite() {
        let data = b"UNH+12345+ORDERS:D:96A:UN'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "UNH");
        assert_eq!(segment.elements.len(), 2);

        // Second element is composite
        match &segment.elements[1] {
            Element::Composite(comps) => {
                assert_eq!(comps.len(), 4);
                assert_eq!(String::from_utf8_lossy(&comps[0]), "ORDERS");
            }
            _ => panic!("Expected composite element"),
        }
    }

    #[test]
    fn test_parse_with_release_char() {
        let data = b"NAD+BY+1234567890123::9'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "NAD");
    }

    #[test]
    fn test_parse_una() {
        // UNA sets custom separators, then UNB follows
        // Note: No + after UNA - the ' is the segment terminator
        let data = b"UNA:+.? 'UNB+UNOA:3+SENDER+RECEIVER'";
        let mut parser = SegmentParser::new(data, "test");

        let una = parser.parse_una();
        assert!(una.is_some());

        // After UNA, position should be ready for UNB
        let unb = parser.next_segment().unwrap().unwrap();
        assert_eq!(unb.tag, "UNB");
        assert_eq!(unb.elements.len(), 3); // UNOA:3, SENDER, RECEIVER
    }

    #[test]
    fn test_full_message() {
        let data = b"UNH+1+ORDERS:D:96A:UN'\
BGM+220+12345+9'\
DTM+137:20200101:102'\
NAD+BY+1234567890123::9'\
UNT+5+1'";

        let parser = EdifactParser::new();
        let docs = parser.parse(data, "test").unwrap();

        assert_eq!(docs.len(), 1);
        // doc_type should be ORDERS (from UNH segment), not the message reference
        assert_eq!(docs[0].metadata.doc_type, Some("ORDERS".to_string()));
    }

    #[test]
    fn test_segment_to_node() {
        let segment = Segment {
            tag: "BGM".to_string(),
            elements: vec![
                Element::Simple(b"220".to_vec()),
                Element::Simple(b"12345".to_vec()),
                Element::Simple(b"9".to_vec()),
            ],
            position: Position::new(1, 1, 0, 0),
        };

        let node = segment.to_node();
        assert_eq!(node.name, "BGM");
        assert_eq!(node.children.len(), 3);
    }

    #[test]
    fn test_orders_grouping_with_multiple_lin_loops() {
        let data = b"UNH+1+ORDERS:D:96A:UN'\
BGM+220+PO123+9'\
LIN+1++123456789:EN'\
QTY+21:10'\
LIN+2++987654321:EN'\
QTY+21:5'\
UNT+7+1'";

        let parser = EdifactParser::new();
        let docs = parser.parse(data, "test").unwrap();

        assert_eq!(docs.len(), 1);
        let root = &docs[0].root;

        let group_nodes: Vec<&Node> = root
            .children
            .iter()
            .filter(|node| node.node_type == NodeType::SegmentGroup)
            .collect();

        assert_eq!(group_nodes.len(), 2);
        assert_eq!(group_nodes[0].name, "LINE_ITEM");
        assert_eq!(group_nodes[1].name, "LINE_ITEM");

        let first_group_first_child = group_nodes[0].children.first();
        let second_group_first_child = group_nodes[1].children.first();

        assert!(matches!(first_group_first_child, Some(child) if child.name == "LIN"));
        assert!(matches!(second_group_first_child, Some(child) if child.name == "LIN"));
    }

    #[test]
    fn test_empty_element_handling() {
        // Test NAD+BY++12345 - empty qualifier between party code and GLN
        let data = b"NAD+BY++12345::9'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "NAD");
        assert_eq!(segment.elements.len(), 3); // Should have 3 elements

        // Check empty element (C082 is empty - represented as empty Simple element)
        match &segment.elements[1] {
            Element::Simple(val) => {
                assert!(val.is_empty(), "Expected empty element at position 2");
            }
            _ => panic!("Expected simple element, not composite"),
        }

        // Check the GLN element (third element)
        match &segment.elements[2] {
            Element::Composite(comps) => {
                assert_eq!(comps.len(), 3);
                assert_eq!(String::from_utf8_lossy(&comps[0]), "12345");
            }
            _ => panic!("Expected composite element at position 3"),
        }
    }

    #[test]
    fn test_multiple_empty_elements() {
        // Test TAG+VAL1++VAL3+ - trailing empty element
        let data = b"TAG+VAL1++VAL3+'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "TAG");
        assert_eq!(segment.elements.len(), 4); // Should have 4 elements: VAL1, "", VAL3, ""

        // Check each element
        match &segment.elements[0] {
            Element::Simple(val) => assert_eq!(String::from_utf8_lossy(val), "VAL1"),
            _ => panic!("Expected simple element"),
        }
        match &segment.elements[1] {
            Element::Simple(val) => assert!(val.is_empty(), "Expected empty element at position 2"),
            _ => panic!("Expected simple element"),
        }
        match &segment.elements[2] {
            Element::Simple(val) => assert_eq!(String::from_utf8_lossy(val), "VAL3"),
            _ => panic!("Expected simple element"),
        }
        match &segment.elements[3] {
            Element::Simple(val) => assert!(val.is_empty(), "Expected empty trailing element"),
            _ => panic!("Expected simple element"),
        }
    }

    #[test]
    fn test_composite_with_empty_middle_component() {
        // Test NAD+BY+1234567890123::9' - empty middle component in composite
        // Structure: NAD+BY+1234567890123::9'
        // - Tag: NAD
        // - Element 0: BY (simple)
        // - Element 1: 1234567890123::9 (composite with 3 components)
        let data = b"NAD+BY+1234567890123::9'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "NAD");
        assert_eq!(segment.elements.len(), 2);

        // First element should be BY
        match &segment.elements[0] {
            Element::Simple(val) => assert_eq!(String::from_utf8_lossy(val), "BY"),
            _ => panic!("Expected simple element at position 0"),
        }

        // Second element should be composite with 3 components
        match &segment.elements[1] {
            Element::Composite(comps) => {
                assert_eq!(
                    comps.len(),
                    3,
                    "Should have 3 components: [1234567890123, '', 9]"
                );
                assert_eq!(String::from_utf8_lossy(&comps[0]), "1234567890123");
                assert_eq!(String::from_utf8_lossy(&comps[1]), "");
                assert_eq!(String::from_utf8_lossy(&comps[2]), "9");
            }
            _ => panic!(
                "Expected composite element at position 1, got {:?}",
                segment.elements[1]
            ),
        }
    }

    #[test]
    fn test_composite_with_empty_trailing_component() {
        // Test TST+ABC+XYZ:' - trailing empty component in composite
        // Structure: TST+ABC+XYZ:'
        // - Tag: TST
        // - Element 0: ABC (simple)
        // - Element 1: XYZ: (composite with trailing empty - 2 components)
        let data = b"TST+ABC+XYZ:'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "TST");
        assert_eq!(segment.elements.len(), 2);

        // First element should be ABC
        match &segment.elements[0] {
            Element::Simple(val) => assert_eq!(String::from_utf8_lossy(val), "ABC"),
            _ => panic!("Expected simple element at position 0"),
        }

        // Second element should be composite with 2 components: XYZ and empty
        match &segment.elements[1] {
            Element::Composite(comps) => {
                assert_eq!(comps.len(), 2, "Should have 2 components: [XYZ, '']");
                assert_eq!(String::from_utf8_lossy(&comps[0]), "XYZ");
                assert_eq!(String::from_utf8_lossy(&comps[1]), "");
            }
            _ => panic!("Expected composite element at position 1"),
        }
    }

    #[test]
    fn test_composite_with_multiple_empty_components() {
        // Test TST+::XYZ' - leading empty components
        // Structure: TST+::XYZ'
        // - Tag: TST
        // - Element 0: (empty simple)
        // - Element 1: ::XYZ (composite with 3 components: '', '', XYZ)
        let data = b"TST+::XYZ'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "TST");
        assert_eq!(segment.elements.len(), 2);

        // First element should be empty simple
        match &segment.elements[0] {
            Element::Simple(val) => assert!(
                val.is_empty(),
                "Expected empty simple element at position 0"
            ),
            _ => panic!(
                "Expected simple element at position 0, got {:?}",
                segment.elements[0]
            ),
        }

        // Second element should be composite with 3 components
        match &segment.elements[1] {
            Element::Composite(comps) => {
                assert_eq!(comps.len(), 3, "Should have 3 components: ['', '', XYZ]");
                assert_eq!(String::from_utf8_lossy(&comps[0]), "");
                assert_eq!(String::from_utf8_lossy(&comps[1]), "");
                assert_eq!(String::from_utf8_lossy(&comps[2]), "XYZ");
            }
            _ => panic!("Expected composite element at position 1"),
        }
    }

    #[test]
    fn test_composite_all_empty_components() {
        // Test TST+:::' - all empty components
        // Structure: TST+:::'
        // - Tag: TST
        // - Element 0: (empty simple)
        // - Element 1: ::: (composite with 4 components, all empty)
        let data = b"TST+:::'";
        let mut parser = SegmentParser::new(data, "test");

        let segment = parser.next_segment().unwrap().unwrap();
        assert_eq!(segment.tag, "TST");
        assert_eq!(segment.elements.len(), 2);

        // First element should be empty simple
        match &segment.elements[0] {
            Element::Simple(val) => assert!(
                val.is_empty(),
                "Expected empty simple element at position 0"
            ),
            _ => panic!(
                "Expected simple element at position 0, got {:?}",
                segment.elements[0]
            ),
        }

        // Second element should be composite with 4 components (all empty)
        match &segment.elements[1] {
            Element::Composite(comps) => {
                assert_eq!(comps.len(), 4, "Should have 4 empty components");
                assert!(comps[0].is_empty());
                assert!(comps[1].is_empty());
                assert!(comps[2].is_empty());
                assert!(comps[3].is_empty());
            }
            _ => panic!("Expected composite element at position 1"),
        }
    }

    #[test]
    fn test_error_position_after_tag() {
        let data = b"UNH?";
        let mut parser = SegmentParser::new(data, "test");

        let err = parser.next_segment().unwrap().unwrap_err();
        match err {
            Error::Parse { line, column, .. } => {
                assert_eq!(line, 1);
                assert_eq!(column, 4);
            }
            _ => panic!("Expected parse error"),
        }
    }
}

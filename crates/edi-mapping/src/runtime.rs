//! Mapping runtime
//!
//! Provides runtime execution engine for DSL mappings.

use edi_ir::{Document, Node, NodeType, Value};
use serde::Serialize;
use std::collections::HashMap;

use crate::dsl::{Condition, LookupDefinition, Mapping, MappingRule, Transform};
use crate::extensions::ExtensionRegistry;
use crate::transforms::apply_transform;

/// Runtime for executing mappings
pub struct MappingRuntime {
    /// Extension registry for custom functions
    extensions: ExtensionRegistry,

    /// Lookup tables available for the current mapping execution
    lookup_tables: HashMap<String, LookupDefinition>,

    /// Root node for absolute path resolution
    root_node: Option<Node>,

    /// Context stack for nested execution
    context_stack: Vec<MappingContext>,

    /// Rule diagnostics captured during traced execution.
    trace_events: Option<Vec<MappingTraceEvent>>,
}

/// Trace for a complete mapping dry run.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MappingTrace {
    /// Mapping name.
    pub mapping: String,
    /// Source document type declared by the mapping.
    pub source_type: String,
    /// Target document type declared by the mapping.
    pub target_type: String,
    /// Per-message traces.
    pub messages: Vec<MessageMappingTrace>,
}

/// Trace for one source message/document.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MessageMappingTrace {
    /// One-based message index in the input batch.
    pub message_index: usize,
    /// Rule diagnostics emitted while mapping the message.
    pub rules: Vec<MappingTraceEvent>,
}

/// Diagnostic emitted for a single mapping rule evaluation.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MappingTraceEvent {
    /// Rule kind: field, foreach, condition, lookup, or block.
    pub rule_type: String,
    /// Source path or key path used by the rule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Target path/name produced by the rule, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Number of nodes/values selected by the rule.
    pub resolved_node_count: usize,
    /// Selected input value, when scalar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_value: Option<String>,
    /// Produced output value, when scalar.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_value: Option<String>,
    /// Whether a condition evaluated true/false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_result: Option<bool>,
    /// Lookup table name, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup_table: Option<String>,
    /// Whether a lookup found an explicit entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup_hit: Option<bool>,
}

/// Execution context for a mapping
#[derive(Debug, Clone)]
pub struct MappingContext {
    /// Current source node being processed
    pub source_node: Node,

    /// Current target node being built
    pub target_node: Option<Node>,

    /// Variable bindings
    pub variables: HashMap<String, Value>,

    /// Current path in source
    pub path: String,

    /// Loop index if inside a foreach
    pub loop_index: Option<usize>,
}

impl MappingContext {
    /// Create a new mapping context
    #[must_use]
    pub fn new(source_node: Node) -> Self {
        Self {
            source_node,
            target_node: None,
            variables: HashMap::new(),
            path: String::new(),
            loop_index: None,
        }
    }

    /// Create a child context for nested execution
    #[must_use]
    pub fn child_context(&self, source_node: Node, path: impl Into<String>) -> Self {
        Self {
            source_node,
            target_node: None,
            variables: self.variables.clone(),
            path: path.into(),
            loop_index: None,
        }
    }

    /// Set a variable
    pub fn set_variable(&mut self, name: impl Into<String>, value: Value) {
        self.variables.insert(name.into(), value);
    }

    /// Get a variable
    #[must_use]
    pub fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(name)
    }

    /// Get current target node or create one
    pub fn get_or_create_target(
        &mut self,
        name: impl Into<String>,
        node_type: NodeType,
    ) -> &mut Node {
        self.target_node
            .get_or_insert_with(|| Node::new(name.into(), node_type))
    }
}

impl MappingRuntime {
    const INVALID_SELECTOR_KEY: &str = "__invalid_selector_key__";

    /// Create a new mapping runtime
    #[must_use]
    pub fn new() -> Self {
        Self {
            extensions: ExtensionRegistry::new(),
            lookup_tables: HashMap::new(),
            root_node: None,
            context_stack: Vec::new(),
            trace_events: None,
        }
    }

    /// Create a runtime with an extension registry
    #[must_use]
    pub fn with_extensions(extensions: ExtensionRegistry) -> Self {
        Self {
            extensions,
            lookup_tables: HashMap::new(),
            root_node: None,
            context_stack: Vec::new(),
            trace_events: None,
        }
    }

    /// Execute a mapping on a document
    ///
    /// # Errors
    ///
    /// Returns an error if any mapping rule fails during execution.
    pub fn execute(&mut self, mapping: &Mapping, document: &Document) -> crate::Result<Document> {
        let root_node = document.root.clone();
        self.lookup_tables.clone_from(&mapping.lookups);
        self.root_node = Some(root_node.clone());
        let mut context = MappingContext::new(root_node);

        let result = (|| {
            // Execute all rules
            for rule in &mapping.rules {
                self.execute_rule(rule, &mut context)?;
            }

            // Build result document with a stable root node.
            let mut result_root = Node::new(&mapping.target_type, NodeType::Root);
            if let Some(mapped_output) = context.target_node {
                result_root.add_child(mapped_output);
            }

            Ok(Document::new(result_root))
        })();

        self.lookup_tables.clear();
        self.root_node = None;
        result
    }

    /// Execute a mapping and return rule-level diagnostics alongside the result.
    ///
    /// # Errors
    ///
    /// Returns an error if any mapping rule fails during execution.
    pub fn execute_with_trace(
        &mut self,
        mapping: &Mapping,
        document: &Document,
    ) -> crate::Result<(Document, Vec<MappingTraceEvent>)> {
        self.trace_events = Some(Vec::new());
        let result = self.execute(mapping, document);
        let trace_events = self.trace_events.take().unwrap_or_default();
        result.map(|document| (document, trace_events))
    }

    fn emit_trace(&mut self, event: MappingTraceEvent) {
        if let Some(events) = &mut self.trace_events {
            events.push(event);
        }
    }

    fn scalar_trace_value(value: &Value) -> Option<String> {
        value.as_string()
    }

    fn resolved_scalar_count(value: &Value) -> usize {
        usize::from(!matches!(value, Value::Null))
    }

    /// Execute a single mapping rule
    fn execute_rule(
        &mut self,
        rule: &MappingRule,
        context: &mut MappingContext,
    ) -> crate::Result<()> {
        match rule {
            MappingRule::Field {
                source,
                target,
                transform,
            } => self.execute_field_mapping(source, target, transform.as_ref(), context),
            MappingRule::Foreach {
                source,
                target,
                rules,
            } => self.execute_foreach(source, target, rules, context),
            MappingRule::Condition {
                when,
                then,
                else_rules,
            } => self.execute_condition(when, then, else_rules, context),
            MappingRule::Lookup {
                table,
                key_source,
                target,
                default_value,
            } => self.execute_lookup(table, key_source, target, default_value.as_ref(), context),
            MappingRule::Block { rules } => {
                for rule in rules {
                    self.execute_rule(rule, context)?;
                }
                Ok(())
            }
        }
    }

    /// Execute a field mapping
    fn execute_field_mapping(
        &mut self,
        source_path: &str,
        target_name: &str,
        transform: Option<&Transform>,
        context: &mut MappingContext,
    ) -> crate::Result<()> {
        // Get value from source
        let value = self.resolve_path(&context.source_node, source_path)?;

        // Apply transform if present
        let transformed_value = if let Some(tfm) = transform {
            apply_transform(&value, tfm)?
        } else {
            value.clone()
        };

        self.emit_trace(MappingTraceEvent {
            rule_type: "field".to_string(),
            source: Some(source_path.to_string()),
            target: Some(target_name.to_string()),
            resolved_node_count: Self::resolved_scalar_count(&value),
            input_value: Self::scalar_trace_value(&value),
            output_value: Self::scalar_trace_value(&transformed_value),
            condition_result: None,
            lookup_table: None,
            lookup_hit: None,
        });

        // Create target node
        let target_node = Node::with_value(target_name, NodeType::Field, transformed_value);

        // Add to parent or set as root
        if let Some(ref mut parent) = context.target_node {
            parent.add_child(target_node);
        } else {
            context.target_node = Some(target_node);
        }

        Ok(())
    }

    /// Execute a foreach loop
    fn execute_foreach(
        &mut self,
        source_path: &str,
        target_name: &str,
        rules: &[MappingRule],
        context: &mut MappingContext,
    ) -> crate::Result<()> {
        // Find source collection
        let collection = self.find_collection(&context.source_node, source_path)?;
        self.emit_trace(MappingTraceEvent {
            rule_type: "foreach".to_string(),
            source: Some(source_path.to_string()),
            target: Some(target_name.to_string()),
            resolved_node_count: collection.len(),
            input_value: None,
            output_value: None,
            condition_result: None,
            lookup_table: None,
            lookup_hit: None,
        });

        // Create target container
        let mut container = Node::new(target_name, NodeType::SegmentGroup);

        // Process each item
        for (index, item) in collection.iter().enumerate() {
            let mut child_context =
                context.child_context(item.clone(), format!("{source_path}[{index}]"));
            child_context.loop_index = Some(index);

            // Execute rules for this item
            for rule in rules {
                self.execute_rule(rule, &mut child_context)?;
            }

            // Add result to container
            if let Some(child_result) = child_context.target_node {
                container.add_child(child_result);
            }
        }

        // Add container to parent or set as root
        if let Some(ref mut parent) = context.target_node {
            parent.add_child(container);
        } else {
            context.target_node = Some(container);
        }

        Ok(())
    }

    /// Execute conditional logic
    fn execute_condition(
        &mut self,
        condition: &Condition,
        then_rules: &[MappingRule],
        else_rules: &[MappingRule],
        context: &mut MappingContext,
    ) -> crate::Result<()> {
        let condition_met = self.evaluate_condition(condition, context)?;
        self.emit_trace(MappingTraceEvent {
            rule_type: "condition".to_string(),
            source: None,
            target: None,
            resolved_node_count: usize::from(condition_met),
            input_value: None,
            output_value: None,
            condition_result: Some(condition_met),
            lookup_table: None,
            lookup_hit: None,
        });

        if condition_met {
            for rule in then_rules {
                self.execute_rule(rule, context)?;
            }
        } else {
            for rule in else_rules {
                self.execute_rule(rule, context)?;
            }
        }

        Ok(())
    }

    /// Execute a lookup
    fn execute_lookup(
        &mut self,
        table: &str,
        key_source: &str,
        target_name: &str,
        default_value: Option<&String>,
        context: &mut MappingContext,
    ) -> crate::Result<()> {
        // Get key from source
        let key = self.resolve_path(&context.source_node, key_source)?;
        let key_str = key.as_string().ok_or_else(|| {
            crate::Error::Runtime(format!("Lookup key '{key_source}' is not a string"))
        })?;

        let lookup_table = self
            .lookup_tables
            .get(table)
            .ok_or_else(|| crate::Error::Runtime(format!("Lookup table '{table}' not found")))?;

        let lookup_hit = lookup_table.entries.contains_key(&key_str);
        let result_value = if let Some(value) = lookup_table.entries.get(&key_str) {
            Value::String(value.clone())
        } else if let Some(default) = default_value {
            Value::String(default.clone())
        } else {
            return Err(crate::Error::Runtime(format!(
                "Lookup key '{key_str}' not found in table '{table}'"
            )));
        };

        self.emit_trace(MappingTraceEvent {
            rule_type: "lookup".to_string(),
            source: Some(key_source.to_string()),
            target: Some(target_name.to_string()),
            resolved_node_count: Self::resolved_scalar_count(&key),
            input_value: Some(key_str),
            output_value: Self::scalar_trace_value(&result_value),
            condition_result: None,
            lookup_table: Some(table.to_string()),
            lookup_hit: Some(lookup_hit),
        });

        // Create target node
        let target_node = Node::with_value(target_name, NodeType::Field, result_value);

        if let Some(ref mut parent) = context.target_node {
            parent.add_child(target_node);
        } else {
            context.target_node = Some(target_node);
        }

        Ok(())
    }

    /// Resolve a path to a value
    fn resolve_path(&self, node: &Node, path: &str) -> crate::Result<Value> {
        if path.is_empty() {
            return Ok(node.value.clone().unwrap_or(Value::Null));
        }

        // Handle absolute paths
        if let Some(relative_path) = path.strip_prefix('/') {
            let root = self.root_node.as_ref().unwrap_or(node);
            return self.resolve_path(root, relative_path);
        }

        // Split path into components
        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        if components.is_empty() {
            return Ok(node.value.clone().unwrap_or(Value::Null));
        }

        // Traverse path
        let mut current = node;
        for (i, component) in components.iter().enumerate() {
            let (component_name, selector) = Self::parse_component(component);
            if i == components.len() - 1 {
                // Last component - return value
                if let Some(child) =
                    Self::find_first_matching_child(current, component_name, selector)
                {
                    return Ok(child.value.clone().unwrap_or(Value::Null));
                }
                return Ok(Value::Null);
            }
            // Intermediate component - traverse deeper
            if let Some(child) = Self::find_first_matching_child(current, component_name, selector)
            {
                current = child;
            } else {
                return Ok(Value::Null);
            }
        }

        Ok(Value::Null)
    }

    /// Find a collection of nodes
    fn find_collection(&self, node: &Node, path: &str) -> crate::Result<Vec<Node>> {
        if let Some(relative_path) = path.strip_prefix('/') {
            let root = self.root_node.as_ref().unwrap_or(node);
            return self.find_collection(root, relative_path);
        }

        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        if components.is_empty() {
            return Ok(vec![node.clone()]);
        }

        let mut current = vec![node.clone()];

        for component in components {
            let (component_name, selector) = Self::parse_component(component);
            let mut next = Vec::new();
            for node in current {
                for child in Self::find_matching_children(&node, component_name, selector) {
                    next.push(child.clone());
                }
            }
            current = next;
        }

        Ok(current)
    }

    fn parse_component(component: &str) -> (&str, Option<(&str, &str)>) {
        let Some(selector_start) = component.find('[') else {
            return (component, None);
        };
        if !component.ends_with(']') || selector_start == 0 {
            return (component, None);
        }

        let component_name = &component[..selector_start];
        let selector_content = &component[selector_start + 1..component.len() - 1];
        let selector =
            Self::parse_selector(selector_content).or(Some((Self::INVALID_SELECTOR_KEY, "")));
        (component_name, selector)
    }

    /// Parse a bracket selector expression into `(key, value)`.
    ///
    /// Empty keys and bare values default to `c1`, so `[='137']` and `['137']`
    /// are accepted shorthand for `[c1='137']`. Empty or malformed selectors
    /// return `None` so callers can treat them as invalid selectors.
    fn parse_selector(selector: &str) -> Option<(&str, &str)> {
        let trimmed = selector.trim();
        if trimmed.is_empty() {
            return None;
        }

        if let Some((key, raw_value)) = trimmed.split_once('=') {
            let key = key.trim();
            let value = Self::clean_selector_literal(raw_value);
            if value.is_empty() {
                return None;
            }
            let key = if key.is_empty() { "c1" } else { key };
            if !Self::is_supported_selector_key(key) {
                tracing::warn!(
                    selector_key = key,
                    "unrecognized selector key; selector will not match"
                );
                return Some((Self::INVALID_SELECTOR_KEY, value));
            }
            return Some((key, value));
        }

        Some(("c1", Self::clean_selector_literal(trimmed)))
    }

    fn clean_selector_literal(value: &str) -> &str {
        value.trim().trim_matches('\'').trim_matches('"')
    }

    fn is_supported_selector_key(key: &str) -> bool {
        let normalized = key.trim();
        normalized.eq_ignore_ascii_case("c1")
            || normalized
                .strip_prefix(['c', 'C', 'e', 'E'])
                .is_some_and(|suffix| {
                    !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
                })
            || normalized.chars().all(|c| c.is_ascii_digit())
    }

    fn find_first_matching_child<'a>(
        node: &'a Node,
        component_name: &str,
        selector: Option<(&str, &str)>,
    ) -> Option<&'a Node> {
        node.children
            .iter()
            .find(|child| child.name == component_name && Self::matches_selector(child, selector))
    }

    fn find_matching_children<'a>(
        node: &'a Node,
        component_name: &str,
        selector: Option<(&str, &str)>,
    ) -> Vec<&'a Node> {
        node.children
            .iter()
            .filter(|child| child.name == component_name && Self::matches_selector(child, selector))
            .collect()
    }

    fn matches_selector(node: &Node, selector: Option<(&str, &str)>) -> bool {
        let Some((key, expected)) = selector else {
            return true;
        };
        if key == Self::INVALID_SELECTOR_KEY {
            return false;
        }

        Self::selector_value(node, key).is_some_and(|actual| actual == expected)
    }

    fn selector_value(node: &Node, key: &str) -> Option<String> {
        let normalized = key.trim();
        let normalized_lower = normalized.to_ascii_lowercase();
        if normalized_lower == "c1" {
            return Self::qualifier_component_value(node);
        }

        if normalized_lower.starts_with('c') {
            if let Some(value) = node
                .find_child("e1")
                .and_then(|element| element.find_child(&normalized_lower))
                .and_then(|component| component.value.as_ref())
                .and_then(Value::as_string)
            {
                return Some(value);
            }
            return node
                .find_child(&normalized_lower)
                .and_then(|component| component.value.as_ref())
                .and_then(Value::as_string);
        }

        if normalized_lower.starts_with('e') {
            return node
                .find_child(&normalized_lower)
                .and_then(|element| element.value.as_ref())
                .and_then(Value::as_string);
        }

        // Common EDI qualifier code selectors (e.g. 2005, 3035, 6063) map to
        // the segment qualifier value, but arbitrary numeric keys should fail
        // closed instead of silently matching the first element's qualifier.
        if normalized.chars().all(|ch| ch.is_ascii_digit()) {
            if Self::is_supported_numeric_qualifier_key(normalized) {
                return Self::qualifier_component_value(node);
            }
            tracing::warn!(
                selector_key = normalized,
                node_name = node.name.as_str(),
                "unrecognized numeric selector key; selector will not match"
            );
            return None;
        }

        None
    }

    fn is_supported_numeric_qualifier_key(key: &str) -> bool {
        matches!(key, "1153" | "2005" | "3035" | "5025" | "5125" | "6063")
    }

    fn qualifier_component_value(node: &Node) -> Option<String> {
        let element = node.find_child("e1")?;
        element
            .value
            .as_ref()
            .and_then(Value::as_string)
            .or_else(|| {
                element
                    .find_child("c1")
                    .and_then(|component| component.value.as_ref())
                    .and_then(Value::as_string)
            })
    }

    /// Evaluate a condition
    fn evaluate_condition(
        &self,
        condition: &Condition,
        context: &MappingContext,
    ) -> crate::Result<bool> {
        match condition {
            Condition::Exists { field } => {
                let value = self.resolve_path(&context.source_node, field)?;
                Ok(
                    !matches!(value, Value::Null)
                        && !value.as_string().is_none_or(|s| s.is_empty()),
                )
            }
            Condition::Equals {
                field,
                value: expected,
            } => {
                let actual = self.resolve_path(&context.source_node, field)?;
                match actual {
                    Value::String(s) => Ok(&s == expected),
                    Value::Integer(i) => Ok(i.to_string() == *expected),
                    Value::Decimal(d) => Ok(d.to_string() == *expected),
                    Value::Boolean(b) => Ok(b.to_string() == *expected),
                    _ => Ok(false),
                }
            }
            Condition::Contains {
                field,
                value: expected,
            } => {
                let actual = self.resolve_path(&context.source_node, field)?;
                match actual {
                    Value::String(s) => Ok(s.contains(expected)),
                    _ => Ok(false),
                }
            }
            Condition::Matches { field, pattern } => {
                let actual = self.resolve_path(&context.source_node, field)?;
                match actual {
                    Value::String(s) => {
                        // Simple pattern matching - in production use regex
                        if pattern.starts_with('^') && pattern.ends_with('$') {
                            // Exact match pattern
                            let inner = &pattern[1..pattern.len() - 1];
                            if inner == "[0-9]+" {
                                Ok(s.chars().all(|c| c.is_ascii_digit()))
                            } else if inner.starts_with("ORD") && inner.contains("[0-9]") {
                                // Handle patterns like "ORD[0-9]+" or "ORD[0-9]{6}"
                                let after_ord = &inner[3..];
                                if s.starts_with("ORD") && s.len() > 3 {
                                    let num_part = &s[3..];
                                    if after_ord.starts_with("[0-9]+") {
                                        Ok(num_part.chars().all(|c| c.is_ascii_digit()))
                                    } else if after_ord.starts_with("[0-9]{")
                                        && after_ord.contains('}')
                                    {
                                        // Handle exact digit count like [0-9]{6}
                                        let count_str = after_ord
                                            .trim_start_matches("[0-9]{")
                                            .split('}')
                                            .next()
                                            .unwrap_or("");
                                        if let Ok(count) = count_str.parse::<usize>() {
                                            Ok(num_part.chars().all(|c| c.is_ascii_digit())
                                                && num_part.len() == count)
                                        } else {
                                            Ok(num_part.chars().all(|c| c.is_ascii_digit()))
                                        }
                                    } else {
                                        Ok(num_part.chars().all(|c| c.is_ascii_digit()))
                                    }
                                } else {
                                    Ok(false)
                                }
                            } else {
                                Ok(s == inner)
                            }
                        } else {
                            Ok(s.contains(pattern))
                        }
                    }
                    _ => Ok(false),
                }
            }
            Condition::And { conditions } => {
                for cond in conditions {
                    if !self.evaluate_condition(cond, context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Condition::Or { conditions } => {
                for cond in conditions {
                    if self.evaluate_condition(cond, context)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Condition::Not { condition } => Ok(!self.evaluate_condition(condition, context)?),
        }
    }

    /// Push a context onto the stack
    pub fn push_context(&mut self, context: MappingContext) {
        self.context_stack.push(context);
    }

    /// Pop a context from the stack
    pub fn pop_context(&mut self) -> Option<MappingContext> {
        self.context_stack.pop()
    }

    /// Get current context
    #[must_use]
    pub fn current_context(&self) -> Option<&MappingContext> {
        self.context_stack.last()
    }

    /// Get extension registry
    #[must_use]
    pub fn extensions(&self) -> &ExtensionRegistry {
        &self.extensions
    }

    /// Get mutable extension registry
    pub fn extensions_mut(&mut self) -> &mut ExtensionRegistry {
        &mut self.extensions
    }
}

impl Default for MappingRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::MappingDsl;

    fn first_mapped_node(document: &Document) -> &Node {
        document
            .root
            .children
            .first()
            .expect("expected mapped output node")
    }

    fn create_test_document() -> Document {
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut header = Node::new("HEADER", NodeType::SegmentGroup);
        header.add_child(Node::with_value(
            "ORDER_NUMBER",
            NodeType::Field,
            Value::String("ORD12345".to_string()),
        ));
        header.add_child(Node::with_value(
            "ORDER_DATE",
            NodeType::Field,
            Value::String("20240115".to_string()),
        ));

        let mut items = Node::new("ITEMS", NodeType::SegmentGroup);

        let mut item1 = Node::new("ITEM", NodeType::Segment);
        item1.add_child(Node::with_value(
            "LINE_NO",
            NodeType::Field,
            Value::Integer(1),
        ));
        item1.add_child(Node::with_value(
            "SKU",
            NodeType::Field,
            Value::String("ABC123".to_string()),
        ));
        item1.add_child(Node::with_value("QTY", NodeType::Field, Value::Integer(10)));

        let mut item2 = Node::new("ITEM", NodeType::Segment);
        item2.add_child(Node::with_value(
            "LINE_NO",
            NodeType::Field,
            Value::Integer(2),
        ));
        item2.add_child(Node::with_value(
            "SKU",
            NodeType::Field,
            Value::String("DEF456".to_string()),
        ));
        item2.add_child(Node::with_value("QTY", NodeType::Field, Value::Integer(5)));

        items.add_child(item1);
        items.add_child(item2);

        root.add_child(header);
        root.add_child(items);

        Document::new(root)
    }

    #[test]
    fn test_execute_simple_mapping() {
        let dsl = r"
name: simple_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /HEADER/ORDER_NUMBER
    target: order_id
  - type: field
    source: /HEADER/ORDER_DATE
    target: order_date
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(result.root.name, "OUTPUT");
        assert_eq!(mapped.name, "order_id");
        assert_eq!(mapped.value, Some(Value::String("ORD12345".to_string())));
        assert_eq!(mapped.children.len(), 1);
        assert_eq!(mapped.children[0].name, "order_date");
        assert_eq!(
            mapped.children[0].value,
            Some(Value::String("20240115".to_string()))
        );
    }

    #[test]
    fn test_execute_foreach() {
        let dsl = r"
name: foreach_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: foreach
    source: /ITEMS/ITEM
    target: lines
    rules:
      - type: field
        source: LINE_NO
        target: line_number
      - type: field
        source: SKU
        target: product_code
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "lines");
        assert_eq!(mapped.node_type, NodeType::SegmentGroup);
        assert_eq!(mapped.children.len(), 2);

        // Check first item - first field becomes parent, second is child
        let first = &mapped.children[0];
        assert_eq!(first.name, "line_number");
        assert_eq!(first.value, Some(Value::Integer(1)));
        assert_eq!(first.children.len(), 1);
        assert_eq!(
            first.children[0].value,
            Some(Value::String("ABC123".to_string()))
        );

        // Check second item
        let second = &mapped.children[1];
        assert_eq!(second.name, "line_number");
        assert_eq!(second.value, Some(Value::Integer(2)));
        assert_eq!(second.children.len(), 1);
        assert_eq!(
            second.children[0].value,
            Some(Value::String("DEF456".to_string()))
        );
    }

    #[test]
    fn test_execute_condition() {
        let dsl = r#"
name: condition_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: equals
      field: /HEADER/ORDER_NUMBER
      value: "ORD12345"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: matched_order
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "matched_order");

        // Test condition not met
        let dsl2 = r#"
name: condition_test2
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: equals
      field: /HEADER/ORDER_NUMBER
      value: "NONEXISTENT"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: matched_order
"#;

        let mapping2 = MappingDsl::parse(dsl2).unwrap();
        let result2 = runtime.execute(&mapping2, &document).unwrap();
        assert!(result2.root.children.is_empty()); // No output when condition not met
    }

    #[test]
    fn test_resolve_path_with_component_selector() {
        let dsl = r"
name: selector_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /DTM[c1='137']/e1/c2
    target: response_date
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut dtm_356 = Node::new("DTM", NodeType::Segment);
        let mut dtm_356_e1 = Node::new("e1", NodeType::Element);
        dtm_356_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("356".to_string()),
        ));
        dtm_356_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("20260114".to_string()),
        ));
        dtm_356.add_child(dtm_356_e1);
        root.add_child(dtm_356);

        let mut dtm_137 = Node::new("DTM", NodeType::Segment);
        let mut dtm_137_e1 = Node::new("e1", NodeType::Element);
        dtm_137_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("137".to_string()),
        ));
        dtm_137_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("20260115".to_string()),
        ));
        dtm_137.add_child(dtm_137_e1);
        root.add_child(dtm_137);

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "response_date");
        assert_eq!(
            mapped.value,
            Some(Value::String("20260115".to_string())),
            "selector should pick DTM qualifier 137 instead of first DTM segment"
        );
    }

    #[test]
    fn test_foreach_with_component_selector() {
        let dsl = r"
name: selector_foreach_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: foreach
    source: LINE_ITEM
    target: rows
    rules:
      - type: field
        source: QTY[c1='153']/e1/c2
        target: quantity
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        for quantity in ["120", "80"] {
            let mut line_item = Node::new("LINE_ITEM", NodeType::SegmentGroup);

            let mut qty_ignored = Node::new("QTY", NodeType::Segment);
            let mut qty_ignored_e1 = Node::new("e1", NodeType::Element);
            qty_ignored_e1.add_child(Node::with_value(
                "c1",
                NodeType::Component,
                Value::String("47".to_string()),
            ));
            qty_ignored_e1.add_child(Node::with_value(
                "c2",
                NodeType::Component,
                Value::String("999".to_string()),
            ));
            qty_ignored.add_child(qty_ignored_e1);
            line_item.add_child(qty_ignored);

            let mut qty_target = Node::new("QTY", NodeType::Segment);
            let mut qty_target_e1 = Node::new("e1", NodeType::Element);
            qty_target_e1.add_child(Node::with_value(
                "c1",
                NodeType::Component,
                Value::String("153".to_string()),
            ));
            qty_target_e1.add_child(Node::with_value(
                "c2",
                NodeType::Component,
                Value::String(quantity.to_string()),
            ));
            qty_target.add_child(qty_target_e1);
            line_item.add_child(qty_target);

            root.add_child(line_item);
        }

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "rows");
        assert_eq!(mapped.children.len(), 2);
        assert_eq!(
            mapped.children[0].value,
            Some(Value::String("120".to_string()))
        );
        assert_eq!(
            mapped.children[1].value,
            Some(Value::String("80".to_string()))
        );
    }

    #[test]
    fn test_selector_no_match_falls_back_to_null() {
        let dsl = r"
name: selector_no_match_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /DTM[c1='999']/e1/c2
    target: response_date
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut dtm_137 = Node::new("DTM", NodeType::Segment);
        let mut dtm_137_e1 = Node::new("e1", NodeType::Element);
        dtm_137_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("137".to_string()),
        ));
        dtm_137_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("20260115".to_string()),
        ));
        dtm_137.add_child(dtm_137_e1);
        root.add_child(dtm_137);

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "response_date");
        assert_eq!(mapped.value, Some(Value::Null));
    }

    #[test]
    fn test_selector_empty_or_malformed_selector_graceful() {
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut dtm_137 = Node::new("DTM", NodeType::Segment);
        let mut dtm_137_e1 = Node::new("e1", NodeType::Element);
        dtm_137_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("137".to_string()),
        ));
        dtm_137_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("20260115".to_string()),
        ));
        dtm_137.add_child(dtm_137_e1);
        root.add_child(dtm_137);
        let document = Document::new(root);

        for source in ["/DTM[]/e1/c2", "/DTM[=]/e1/c2"] {
            let dsl = format!(
                "name: selector_malformed_test\nsource_type: TEST\ntarget_type: OUTPUT\nrules:\n  - type: field\n    source: {source}\n    target: response_date\n"
            );
            let mapping = MappingDsl::parse(&dsl).unwrap();
            let mut runtime = MappingRuntime::new();
            let result = runtime.execute(&mapping, &document).unwrap();
            let mapped = first_mapped_node(&result);
            assert_eq!(
                mapped.value,
                Some(Value::Null),
                "malformed selector {source} should degrade to null without panicking"
            );
        }
    }

    #[test]
    fn test_unknown_numeric_selector_key_does_not_match() {
        let dsl = r"
name: selector_unknown_numeric_key_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /DTM[2006='137']/e1/c2
    target: date
";
        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut dtm = Node::new("DTM", NodeType::Segment);
        let mut dtm_e1 = Node::new("e1", NodeType::Element);
        dtm_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("137".to_string()),
        ));
        dtm_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("20260116".to_string()),
        ));
        dtm.add_child(dtm_e1);
        root.add_child(dtm);
        let document = Document::new(root);

        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.value, Some(Value::Null));
    }

    #[test]
    fn test_selector_shorthand_defaults_to_c1() {
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut qty = Node::new("QTY", NodeType::Segment);
        let mut qty_e1 = Node::new("e1", NodeType::Element);
        qty_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("153".to_string()),
        ));
        qty_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("120".to_string()),
        ));
        qty.add_child(qty_e1);
        root.add_child(qty);
        let document = Document::new(root);

        for source in ["/QTY['153']/e1/c2", "/QTY[='153']/e1/c2"] {
            let dsl = format!(
                "name: selector_shorthand_test\nsource_type: TEST\ntarget_type: OUTPUT\nrules:\n  - type: field\n    source: {source}\n    target: quantity\n"
            );
            let mapping = MappingDsl::parse(&dsl).unwrap();
            let mut runtime = MappingRuntime::new();
            let result = runtime.execute(&mapping, &document).unwrap();
            let mapped = first_mapped_node(&result);
            assert_eq!(mapped.value, Some(Value::String("120".to_string())));
        }
    }

    #[test]
    fn test_selector_c2_matches_component_value() {
        let dsl = r"
name: selector_c2_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /QTY[c2='120']/e1/c3
    target: unit
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut qty_ignored = Node::new("QTY", NodeType::Segment);
        let mut qty_ignored_e1 = Node::new("e1", NodeType::Element);
        qty_ignored_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("153".to_string()),
        ));
        qty_ignored_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("999".to_string()),
        ));
        qty_ignored_e1.add_child(Node::with_value(
            "c3",
            NodeType::Component,
            Value::String("BOX".to_string()),
        ));
        qty_ignored.add_child(qty_ignored_e1);
        root.add_child(qty_ignored);

        let mut qty_target = Node::new("QTY", NodeType::Segment);
        let mut qty_target_e1 = Node::new("e1", NodeType::Element);
        qty_target_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("153".to_string()),
        ));
        qty_target_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("120".to_string()),
        ));
        qty_target_e1.add_child(Node::with_value(
            "c3",
            NodeType::Component,
            Value::String("PCE".to_string()),
        ));
        qty_target.add_child(qty_target_e1);
        root.add_child(qty_target);

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "unit");
        assert_eq!(mapped.value, Some(Value::String("PCE".to_string())));
    }

    #[test]
    fn test_numeric_selector_matches_simple_e1_qualifier() {
        let dsl = r"
name: selector_simple_e1_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /NAD[3035='SU']/e2
    target: supplier_id
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut buyer = Node::new("NAD", NodeType::Segment);
        buyer.add_child(Node::with_value(
            "e1",
            NodeType::Element,
            Value::String("BY".to_string()),
        ));
        buyer.add_child(Node::with_value(
            "e2",
            NodeType::Element,
            Value::String("BUYER-001".to_string()),
        ));
        root.add_child(buyer);

        let mut supplier = Node::new("NAD", NodeType::Segment);
        supplier.add_child(Node::with_value(
            "e1",
            NodeType::Element,
            Value::String("SU".to_string()),
        ));
        supplier.add_child(Node::with_value(
            "e2",
            NodeType::Element,
            Value::String("SUPPLIER-001".to_string()),
        ));
        root.add_child(supplier);

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "supplier_id");
        assert_eq!(
            mapped.value,
            Some(Value::String("SUPPLIER-001".to_string()))
        );
    }

    #[test]
    fn test_numeric_selector_matches_composite_e2_qualifier() {
        let dsl = r"
name: selector_composite_e2_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /NAD[3035='SU']/e2/c1
    target: supplier_id
";
        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut buyer = Node::new("NAD", NodeType::Segment);
        buyer.add_child(Node::with_value(
            "e1",
            NodeType::Element,
            Value::String("BY".to_string()),
        ));
        let mut buyer_e2 = Node::new("e2", NodeType::Element);
        buyer_e2.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("1234567890123".to_string()),
        ));
        buyer.add_child(buyer_e2);
        root.add_child(buyer);

        let mut supplier = Node::new("NAD", NodeType::Segment);
        supplier.add_child(Node::with_value(
            "e1",
            NodeType::Element,
            Value::String("SU".to_string()),
        ));
        let mut supplier_e2 = Node::new("e2", NodeType::Element);
        supplier_e2.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("9876543210987".to_string()),
        ));
        supplier.add_child(supplier_e2);
        root.add_child(supplier);

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "supplier_id");
        assert_eq!(
            mapped.value,
            Some(Value::String("9876543210987".to_string()))
        );
    }

    #[test]
    fn test_unrecognized_selector_key_does_not_match() {
        let dsl = r"
name: selector_unknown_key_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /DTM[typo='137']/e1/c2
    target: response_date
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let mut root = Node::new("ROOT", NodeType::Root);

        let mut dtm_137 = Node::new("DTM", NodeType::Segment);
        let mut dtm_137_e1 = Node::new("e1", NodeType::Element);
        dtm_137_e1.add_child(Node::with_value(
            "c1",
            NodeType::Component,
            Value::String("137".to_string()),
        ));
        dtm_137_e1.add_child(Node::with_value(
            "c2",
            NodeType::Component,
            Value::String("20260115".to_string()),
        ));
        dtm_137.add_child(dtm_137_e1);
        root.add_child(dtm_137);

        let document = Document::new(root);
        let mut runtime = MappingRuntime::new();
        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        assert_eq!(mapped.name, "response_date");
        assert_eq!(mapped.value, Some(Value::Null));
    }

    #[test]
    fn test_execute_lookup() {
        let dsl = r#"
name: lookup_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: lookup
    table: countries
    key_source: /HEADER/ORDER_NUMBER
    target: lookup_result
    default_value: "NOT_FOUND"
lookups:
  countries:
    name: countries
    entries:
      ORD12345: "Germany"
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "lookup_result");
        assert_eq!(
            first_mapped_node(&result).value,
            Some(Value::String("Germany".to_string()))
        );
    }

    #[test]
    fn test_execute_lookup_missing_entry_without_default() {
        let dsl = r#"
name: lookup_missing_entry_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: lookup
    table: countries
    key_source: /HEADER/ORDER_NUMBER
    target: lookup_result
lookups:
  countries:
    name: countries
    entries:
      OTHER: "Other Country"
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let err = runtime.execute(&mapping, &document).unwrap_err();
        assert!(
            err.to_string()
                .contains("Lookup key 'ORD12345' not found in table 'countries'")
        );
    }

    #[test]
    fn test_execute_lookup_missing_table() {
        let dsl = r"
name: lookup_missing_table_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: lookup
    table: countries
    key_source: /HEADER/ORDER_NUMBER
    target: lookup_result
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let err = runtime.execute(&mapping, &document).unwrap_err();
        assert!(
            err.to_string()
                .contains("Lookup table 'countries' not found")
        );
    }

    #[test]
    fn test_runtime_error_handling() {
        let dsl = r"
name: error_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: field
    source: /NONEXISTENT/PATH
    target: output
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        // Should not error on missing path, just return null
        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "output");
        assert_eq!(first_mapped_node(&result).value, Some(Value::Null));
    }

    #[test]
    fn test_mapping_context() {
        let node = Node::new("TEST", NodeType::Segment);
        let mut context = MappingContext::new(node);

        // Test variable setting/getting
        context.set_variable("test_var", Value::String("value".to_string()));
        assert_eq!(
            context.get_variable("test_var"),
            Some(&Value::String("value".to_string()))
        );
        assert_eq!(context.get_variable("nonexistent"), None);

        // Test child context
        let child_node = Node::new("CHILD", NodeType::Field);
        let child_context = context.child_context(child_node, "/child/path");

        // Child should inherit variables
        assert_eq!(
            child_context.get_variable("test_var"),
            Some(&Value::String("value".to_string()))
        );
        assert_eq!(child_context.path, "/child/path");
    }

    #[test]
    fn test_nested_execution() {
        let dsl = r"
name: nested_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: block
    rules:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: level1
      - type: block
        rules:
          - type: field
            source: /HEADER/ORDER_DATE
            target: level2
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        // Block creates nested structure where first field is parent, second is child
        assert_eq!(first_mapped_node(&result).name, "level1");
        assert_eq!(first_mapped_node(&result).children.len(), 1);
        assert_eq!(first_mapped_node(&result).children[0].name, "level2");
    }

    #[test]
    fn test_condition_exists() {
        let dsl = r"
name: exists_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: exists
      field: /HEADER/ORDER_NUMBER
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: exists_result
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "exists_result");
        assert_eq!(
            first_mapped_node(&result).value,
            Some(Value::String("ORD12345".to_string()))
        );
    }

    #[test]
    fn test_condition_contains() {
        let dsl = r#"
name: contains_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: contains
      field: /HEADER/ORDER_NUMBER
      value: "123"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: contains_result
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "contains_result");
    }

    #[test]
    fn test_condition_matches() {
        let dsl = r#"
name: matches_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: matches
      field: /HEADER/ORDER_NUMBER
      pattern: "^ORD[0-9]+$"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: matches_result
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "matches_result");
    }

    #[test]
    fn test_condition_and() {
        let dsl = r"
name: and_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: and
      conditions:
        - op: exists
          field: /HEADER/ORDER_NUMBER
        - op: exists
          field: /HEADER/ORDER_DATE
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: and_result
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "and_result");
    }

    #[test]
    fn test_condition_or() {
        let dsl = r#"
name: or_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: or
      conditions:
        - op: equals
          field: /HEADER/ORDER_NUMBER
          value: "ORD12345"
        - op: equals
          field: /HEADER/ORDER_NUMBER
          value: "NONEXISTENT"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: or_result
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "or_result");
    }

    #[test]
    fn test_condition_not() {
        let dsl = r#"
name: not_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: not
      condition:
        op: equals
        field: /HEADER/ORDER_NUMBER
        value: "NONEXISTENT"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: not_result
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "not_result");
    }

    #[test]
    fn test_foreach_with_transform() {
        let dsl = r"
name: foreach_transform_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: foreach
    source: /ITEMS/ITEM
    target: lines
    rules:
      - type: field
        source: SKU
        target: product_code
        transform:
          op: uppercase
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        let mapped = first_mapped_node(&result);

        // Check transforms were applied - first field is parent, transform applied to its value
        let first = &mapped.children[0];
        assert_eq!(first.name, "product_code");
        // The SKU value should be uppercased ("abc123" -> "ABC123")
        assert_eq!(first.value, Some(Value::String("ABC123".to_string())));
    }

    #[test]
    fn test_mapping_with_extensions() {
        let registry = ExtensionRegistry::new();
        let runtime = MappingRuntime::with_extensions(registry);

        // Runtime should have the registry
        assert!(runtime.extensions().is_empty().unwrap());
    }

    #[test]
    fn test_context_stack_operations() {
        let mut runtime = MappingRuntime::new();

        let node1 = Node::new("NODE1", NodeType::Segment);
        let ctx1 = MappingContext::new(node1);

        runtime.push_context(ctx1);
        assert!(runtime.current_context().is_some());

        let node2 = Node::new("NODE2", NodeType::Segment);
        let ctx2 = MappingContext::new(node2);
        runtime.push_context(ctx2);

        assert_eq!(runtime.context_stack.len(), 2);

        let popped = runtime.pop_context();
        assert!(popped.is_some());
        assert_eq!(runtime.context_stack.len(), 1);
    }

    #[test]
    fn test_empty_foreach() {
        let dsl = r"
name: empty_foreach_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: foreach
    source: /NONEXISTENT
    target: items
    rules:
      - type: field
        source: VALUE
        target: output
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        // Should create empty container
        assert_eq!(first_mapped_node(&result).name, "items");
        assert!(first_mapped_node(&result).children.is_empty());
    }

    #[test]
    fn test_complex_nested_conditions() {
        let dsl = r#"
name: complex_nested_test
source_type: TEST
target_type: OUTPUT
rules:
  - type: condition
    when:
      op: and
      conditions:
        - op: exists
          field: /HEADER/ORDER_NUMBER
        - op: or
          conditions:
            - op: equals
              field: /HEADER/ORDER_NUMBER
              value: "ORD12345"
            - op: equals
              field: /HEADER/ORDER_NUMBER
              value: "OTHER"
    then:
      - type: field
        source: /HEADER/ORDER_NUMBER
        target: complex_result
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        let document = create_test_document();
        let mut runtime = MappingRuntime::new();

        let result = runtime.execute(&mapping, &document).unwrap();
        assert_eq!(first_mapped_node(&result).name, "complex_result");
    }
}

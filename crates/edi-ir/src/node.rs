//! Node types for the Intermediate Representation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A node in the IR tree
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Node name (e.g., segment tag, field name)
    pub name: String,

    /// Node type
    pub node_type: NodeType,

    /// Node value (if applicable)
    pub value: Option<Value>,

    /// Child nodes
    pub children: Vec<Node>,

    /// Node attributes (metadata, flags, etc.)
    pub attributes: HashMap<String, String>,

    /// Schema type reference
    pub schema_type: Option<String>,
}

/// Types of nodes in the IR
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    /// Root of the document
    Root,

    /// Interchange envelope (e.g., UNB/UNZ)
    Interchange,

    /// Message envelope (e.g., UNH/UNT)
    Message,

    /// Group of related segments
    SegmentGroup,

    /// Individual segment
    Segment,

    /// Data element (simple or composite)
    Element,

    /// Component within a composite element
    Component,

    /// Field in a custom format
    Field,

    /// Record in CSV/DB format
    Record,
}

/// Values that can be stored in nodes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// String value
    String(String),

    /// Integer value
    Integer(i64),

    /// Decimal value
    Decimal(f64),

    /// Boolean value
    Boolean(bool),

    /// Date value
    Date(String), // ISO 8601 format

    /// Time value
    Time(String), // ISO 8601 format

    /// DateTime value
    DateTime(String), // ISO 8601 format

    /// Raw bytes
    Binary(Vec<u8>),

    /// Null/empty value
    Null,
}

impl Node {
    /// Create a new node
    pub fn new(name: impl Into<String>, node_type: NodeType) -> Self {
        Self {
            name: name.into(),
            node_type,
            value: None,
            children: Vec::new(),
            attributes: HashMap::new(),
            schema_type: None,
        }
    }

    /// Create a node with a value
    pub fn with_value(name: impl Into<String>, node_type: NodeType, value: Value) -> Self {
        Self {
            name: name.into(),
            node_type,
            value: Some(value),
            children: Vec::new(),
            attributes: HashMap::new(),
            schema_type: None,
        }
    }

    /// Add a child node
    pub fn add_child(&mut self, child: Node) -> &mut Self {
        self.children.push(child);
        self
    }

    /// Set an attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Set the schema type
    pub fn set_schema_type(&mut self, schema_type: impl Into<String>) -> &mut Self {
        self.schema_type = Some(schema_type.into());
        self
    }

    /// Find a child by name
    pub fn find_child(&self, name: &str) -> Option<&Node> {
        self.children.iter().find(|c| c.name == name)
    }

    /// Find all children by name
    pub fn find_children(&self, name: &str) -> Vec<&Node> {
        self.children.iter().filter(|c| c.name == name).collect()
    }
}

impl Value {
    /// Convert value to string
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            Value::Integer(i) => Some(i.to_string()),
            Value::Decimal(d) => Some(d.to_string()),
            Value::Boolean(b) => Some(b.to_string()),
            Value::Date(d) => Some(d.clone()),
            Value::Time(t) => Some(t.clone()),
            Value::DateTime(dt) => Some(dt.clone()),
            Value::Binary(_) | Value::Null => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node = Node::new("TEST", NodeType::Segment);
        assert_eq!(node.name, "TEST");
        assert_eq!(node.node_type, NodeType::Segment);
        assert!(node.children.is_empty());
        assert!(node.attributes.is_empty());
        assert!(node.value.is_none());
        assert!(node.schema_type.is_none());
    }

    #[test]
    fn test_node_with_value() {
        let value = Value::String("test value".to_string());
        let node = Node::with_value("FIELD", NodeType::Field, value.clone());
        assert_eq!(node.name, "FIELD");
        assert_eq!(node.node_type, NodeType::Field);
        assert_eq!(node.value, Some(value));
    }

    #[test]
    fn test_add_child() {
        let mut parent = Node::new("PARENT", NodeType::SegmentGroup);
        let child1 = Node::new("CHILD1", NodeType::Segment);
        let child2 = Node::new("CHILD2", NodeType::Segment);

        parent.add_child(child1);
        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].name, "CHILD1");

        parent.add_child(child2);
        assert_eq!(parent.children.len(), 2);
        assert_eq!(parent.children[1].name, "CHILD2");
    }

    #[test]
    fn test_set_attribute() {
        let mut node = Node::new("TEST", NodeType::Element);
        node.set_attribute("key1", "value1")
            .set_attribute("key2", "value2");

        assert_eq!(node.attributes.get("key1"), Some(&"value1".to_string()));
        assert_eq!(node.attributes.get("key2"), Some(&"value2".to_string()));
        assert_eq!(node.attributes.len(), 2);
    }

    #[test]
    fn test_set_schema_type() {
        let mut node = Node::new("TEST", NodeType::Segment);
        node.set_schema_type("EANCOM_D96A_ORDERS");
        assert_eq!(node.schema_type, Some("EANCOM_D96A_ORDERS".to_string()));
    }

    #[test]
    fn test_find_child() {
        let mut parent = Node::new("PARENT", NodeType::SegmentGroup);
        let child1 = Node::new("CHILD1", NodeType::Segment);
        let child2 = Node::new("CHILD2", NodeType::Segment);

        parent.add_child(child1);
        parent.add_child(child2);

        assert!(parent.find_child("CHILD1").is_some());
        assert!(parent.find_child("CHILD2").is_some());
        assert!(parent.find_child("NONEXISTENT").is_none());
    }

    #[test]
    fn test_find_children() {
        let mut parent = Node::new("PARENT", NodeType::SegmentGroup);
        let item1 = Node::new("ITEM", NodeType::Segment);
        let item2 = Node::new("ITEM", NodeType::Segment);
        let other = Node::new("OTHER", NodeType::Segment);

        parent.add_child(item1);
        parent.add_child(other);
        parent.add_child(item2);

        let items = parent.find_children("ITEM");
        assert_eq!(items.len(), 2);

        let others = parent.find_children("OTHER");
        assert_eq!(others.len(), 1);

        let none = parent.find_children("NONEXISTENT");
        assert!(none.is_empty());
    }

    #[test]
    fn test_nested_children() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut level1 = Node::new("LEVEL1", NodeType::SegmentGroup);
        let mut level2 = Node::new("LEVEL2", NodeType::SegmentGroup);
        let leaf = Node::new("LEAF", NodeType::Element);

        level2.add_child(leaf);
        level1.add_child(level2);
        root.add_child(level1);

        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].children.len(), 1);
        assert_eq!(root.children[0].children[0].children.len(), 1);
        assert_eq!(root.children[0].children[0].children[0].name, "LEAF");
    }

    #[test]
    fn test_node_type_variants() {
        let root = Node::new("ROOT", NodeType::Root);
        assert_eq!(root.node_type, NodeType::Root);

        let interchange = Node::new("UNB", NodeType::Interchange);
        assert_eq!(interchange.node_type, NodeType::Interchange);

        let message = Node::new("UNH", NodeType::Message);
        assert_eq!(message.node_type, NodeType::Message);

        let segment_group = Node::new("SG1", NodeType::SegmentGroup);
        assert_eq!(segment_group.node_type, NodeType::SegmentGroup);

        let segment = Node::new("LIN", NodeType::Segment);
        assert_eq!(segment.node_type, NodeType::Segment);

        let element = Node::new("C212", NodeType::Element);
        assert_eq!(element.node_type, NodeType::Element);

        let component = Node::new("7140", NodeType::Component);
        assert_eq!(component.node_type, NodeType::Component);

        let field = Node::new("FIELD", NodeType::Field);
        assert_eq!(field.node_type, NodeType::Field);

        let record = Node::new("RECORD", NodeType::Record);
        assert_eq!(record.node_type, NodeType::Record);
    }

    #[test]
    fn test_value_as_string() {
        assert_eq!(
            Value::String("hello".to_string()).as_string(),
            Some("hello".to_string())
        );
        assert_eq!(Value::Integer(42).as_string(), Some("42".to_string()));
        assert_eq!(
            Value::Decimal(123.45).as_string(),
            Some("123.45".to_string())
        );
        assert_eq!(Value::Boolean(true).as_string(), Some("true".to_string()));
        assert_eq!(Value::Boolean(false).as_string(), Some("false".to_string()));
        assert_eq!(
            Value::Date("2024-01-15".to_string()).as_string(),
            Some("2024-01-15".to_string())
        );
        assert_eq!(
            Value::Time("14:30:00".to_string()).as_string(),
            Some("14:30:00".to_string())
        );
        assert_eq!(
            Value::DateTime("2024-01-15T14:30:00Z".to_string()).as_string(),
            Some("2024-01-15T14:30:00Z".to_string())
        );
        assert_eq!(Value::Binary(vec![0x00, 0x01, 0x02]).as_string(), None);
        assert_eq!(Value::Null.as_string(), None);
    }

    #[test]
    fn test_value_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::String(String::new()).is_null());
        assert!(!Value::Integer(0).is_null());
        assert!(!Value::Decimal(0.0).is_null());
        assert!(!Value::Boolean(false).is_null());
        assert!(!Value::Date(String::new()).is_null());
        assert!(!Value::Time(String::new()).is_null());
        assert!(!Value::DateTime(String::new()).is_null());
        assert!(!Value::Binary(vec![]).is_null());
    }

    #[test]
    fn test_value_conversions() {
        let int_val = Value::Integer(-12345);
        assert_eq!(int_val.as_string(), Some("-12345".to_string()));

        let dec_val = Value::Decimal(-9876.54321);
        assert_eq!(dec_val.as_string(), Some("-9876.54321".to_string()));

        let bool_val = Value::Boolean(true);
        assert_eq!(bool_val.as_string(), Some("true".to_string()));
    }
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            Value::Binary(_) => None,
            Value::Null => None,
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

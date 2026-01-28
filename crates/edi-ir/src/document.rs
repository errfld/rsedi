//! Document representation for the Intermediate Representation

use crate::metadata::SourceInfo;
use crate::node::Node;
use serde::{Deserialize, Serialize};

/// A document in the Intermediate Representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Root node of the document
    pub root: Node,

    /// Document-level metadata
    pub metadata: DocumentMetadata,

    /// Schema reference (if any)
    pub schema_ref: Option<String>,
}

/// Metadata associated with a document
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocumentMetadata {
    /// Source information (file, position, etc.)
    pub source: Option<SourceInfo>,

    /// Document type identifier
    pub doc_type: Option<String>,

    /// Version information
    pub version: Option<String>,

    /// Partner identifier
    pub partner_id: Option<String>,

    /// Interchange control reference
    pub interchange_ref: Option<String>,

    /// Message reference numbers
    pub message_refs: Vec<String>,

    /// Creation timestamp
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl Document {
    /// Create a new document with the given root node
    pub fn new(root: Node) -> Self {
        Self {
            root,
            metadata: DocumentMetadata::default(),
            schema_ref: None,
        }
    }

    /// Create a new document with metadata
    pub fn with_metadata(root: Node, metadata: DocumentMetadata) -> Self {
        Self {
            root,
            metadata,
            schema_ref: None,
        }
    }

    /// Set the schema reference
    pub fn with_schema(mut self, schema_ref: impl Into<String>) -> Self {
        self.schema_ref = Some(schema_ref.into());
        self
    }
}

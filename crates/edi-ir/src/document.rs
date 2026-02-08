//! Document representation for the Intermediate Representation
#![allow(clippy::must_use_candidate)] // Builder/constructor API intentionally omits pervasive #[must_use].
#![allow(clippy::return_self_not_must_use)] // Fluent builder methods return Self for ergonomics.

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Position, SourceInfo};
    use crate::node::{Node, NodeType};

    #[test]
    fn test_document_creation() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = Document::new(root);

        assert_eq!(doc.root.name, "ROOT");
        assert_eq!(doc.root.node_type, NodeType::Root);
        assert!(doc.schema_ref.is_none());
    }

    #[test]
    fn test_document_with_metadata() {
        let root = Node::new("ROOT", NodeType::Root);
        let position = Position::new(1, 1, 0, 100);
        let source = SourceInfo::new("test.edi", position);

        let metadata = DocumentMetadata {
            source: Some(source),
            doc_type: Some("ORDERS".to_string()),
            version: Some("D96A".to_string()),
            partner_id: Some("PARTNER001".to_string()),
            interchange_ref: Some("12345".to_string()),
            message_refs: vec!["MSG001".to_string(), "MSG002".to_string()],
            created_at: Some(chrono::Utc::now()),
        };

        let doc = Document::with_metadata(root, metadata.clone());

        assert_eq!(doc.metadata.doc_type, Some("ORDERS".to_string()));
        assert_eq!(doc.metadata.version, Some("D96A".to_string()));
        assert_eq!(doc.metadata.partner_id, Some("PARTNER001".to_string()));
        assert_eq!(doc.metadata.interchange_ref, Some("12345".to_string()));
        assert_eq!(doc.metadata.message_refs.len(), 2);
        assert!(doc.metadata.source.is_some());
        assert!(doc.metadata.created_at.is_some());
    }

    #[test]
    fn test_document_with_schema() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = Document::new(root).with_schema("EANCOM_D96A");

        assert_eq!(doc.schema_ref, Some("EANCOM_D96A".to_string()));
    }

    #[test]
    fn test_document_metadata_fields() {
        let root = Node::new("ROOT", NodeType::Root);
        let mut metadata = DocumentMetadata::default();

        // Test all fields can be set
        let position = Position::new(10, 5, 100, 50);
        metadata.source =
            Some(SourceInfo::new("/path/to/file.edi", position).with_context("UNB segment"));
        metadata.doc_type = Some("DESADV".to_string());
        metadata.version = Some("D01B".to_string());
        metadata.partner_id = Some("SUPPLIER_123".to_string());
        metadata.interchange_ref = Some("REF-2024-001".to_string());
        metadata.message_refs = vec!["001".to_string(), "002".to_string(), "003".to_string()];
        metadata.created_at = Some(chrono::DateTime::UNIX_EPOCH);

        let doc = Document::with_metadata(root, metadata);

        assert_eq!(doc.metadata.doc_type, Some("DESADV".to_string()));
        assert_eq!(doc.metadata.version, Some("D01B".to_string()));
        assert_eq!(doc.metadata.partner_id, Some("SUPPLIER_123".to_string()));
        assert_eq!(
            doc.metadata.interchange_ref,
            Some("REF-2024-001".to_string())
        );
        assert_eq!(doc.metadata.message_refs, vec!["001", "002", "003"]);

        let source_info = doc.metadata.source.as_ref().unwrap();
        assert_eq!(source_info.source, "/path/to/file.edi");
        assert_eq!(source_info.context, Some("UNB segment".to_string()));
        assert_eq!(source_info.position.line, 10);
        assert_eq!(source_info.position.column, 5);
        assert_eq!(source_info.position.offset, 100);
        assert_eq!(source_info.position.length, 50);

        assert_eq!(doc.metadata.created_at, Some(chrono::DateTime::UNIX_EPOCH));
    }

    #[test]
    fn test_document_default_metadata() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = Document::new(root);

        // Test that default metadata has all None/empty values
        assert!(doc.metadata.source.is_none());
        assert!(doc.metadata.doc_type.is_none());
        assert!(doc.metadata.version.is_none());
        assert!(doc.metadata.partner_id.is_none());
        assert!(doc.metadata.interchange_ref.is_none());
        assert!(doc.metadata.message_refs.is_empty());
        assert!(doc.metadata.created_at.is_none());
    }
}

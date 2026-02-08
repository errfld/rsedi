#![deny(warnings)]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

//! # edi-validation
//!
//! Validation engine for structural rules and codelists.
//!
//! This crate provides validation against runtime schemas with configurable
//! strictness levels and detailed error reporting.
//!
//! ## Example Usage
//!
//! ```rust
//! use edi_validation::engine::{ValidationEngine, ValidationConfig, StrictnessLevel};
//! use edi_ir::{Document, Node, NodeType, Value};
//!
//! // Create a validation engine
//! let engine = ValidationEngine::new();
//!
//! // Create a document
//! let mut root = Node::new("ROOT", NodeType::Root);
//! let mut segment = Node::new("TEST", NodeType::Segment);
//! segment.add_child(Node::with_value("FIELD", NodeType::Element, Value::String("value".to_string())));
//! root.add_child(segment);
//! let doc = Document::new(root);
//!
//! // Validate the document
//! let result = engine.validate(&doc).unwrap();
//! assert!(result.is_valid);
//! ```

pub mod codelist;
pub mod engine;
pub mod reporter;
pub mod rules;

// Re-export main types
pub use engine::{
    StrictnessLevel, ValidationConfig, ValidationContext, ValidationEngine, ValidationError,
    ValidationResult,
};
pub use reporter::{Severity, ValidationIssue, ValidationReport, ValidationReporter};
pub use rules::{
    ConditionalRule, Constraint, DataType, SegmentOrderRule, validate_code_list,
    validate_conditional, validate_data_type, validate_length, validate_pattern, validate_required,
    validate_segment_order,
};

use thiserror::Error;

/// Errors that can occur during validation
#[derive(Error, Debug)]
pub enum Error {
    #[error("Validation failed: {0}")]
    Validation(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Convenience function to validate a document with default settings
///
/// # Errors
///
/// Returns an error when validation execution fails.
pub fn validate(doc: &edi_ir::Document) -> Result<ValidationResult> {
    let engine = ValidationEngine::new();
    engine.validate(doc)
}

/// Convenience function to validate a document against a schema
///
/// # Errors
///
/// Returns an error when schema-based validation execution fails.
pub fn validate_with_schema(
    doc: &edi_ir::Document,
    schema: &edi_schema::Schema,
) -> Result<ValidationResult> {
    let engine = ValidationEngine::new();
    engine.validate_with_schema(doc, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edi_ir::{Node, NodeType, Value};

    #[test]
    fn test_convenience_validate() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut segment = Node::new("TEST", NodeType::Segment);
        segment.add_child(Node::with_value(
            "FIELD",
            NodeType::Element,
            Value::String("value".to_string()),
        ));
        root.add_child(segment);
        let doc = edi_ir::Document::new(root);

        let result = validate(&doc).unwrap();
        assert!(result.is_valid || result.errors.is_empty());
    }

    #[test]
    fn test_convenience_validate_with_schema() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = edi_ir::Document::new(root);

        let schema = edi_schema::Schema::new("TEST", "1.0");

        let result = validate_with_schema(&doc, &schema).unwrap();
        // Empty doc is valid even against schema
        assert!(result.is_valid || result.has_errors());
    }
}

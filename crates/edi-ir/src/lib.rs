#![deny(warnings)]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

//! # edi-ir
//!
//! Intermediate Representation structures and traversal APIs for EDI documents.
//!
//! This crate provides a generic, schema-aware tree structure that can represent
//! EDI documents in a format-neutral way, enabling transformations between
//! different formats (EDIFACT, CSV, database, etc.).

/// Document container and top-level IR metadata accessors.
pub mod document;
/// Source and validation metadata attached to documents and nodes.
pub mod metadata;
/// Core tree node model used for EDI message representation.
pub mod node;
/// Cursor-based traversal helpers for navigating IR trees.
pub mod traversal;

/// Primary IR document type.
pub use document::Document;
/// Position and source metadata plus accumulated validation state.
pub use metadata::{Position, SourceInfo, ValidationState};
/// Node primitives for tree structure and value typing.
pub use node::{Node, NodeType, Value};
/// Traversal entry points for iterative tree navigation.
pub use traversal::{Cursor, Traversal};

use thiserror::Error;

/// Errors that can occur when working with the IR
#[derive(Error, Debug)]
pub enum Error {
    #[error("Node not found at path: {path}")]
    NodeNotFound { path: String },

    #[error("Invalid path '{path}': {reason}")]
    InvalidPath { path: String, reason: String },

    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Conversion error in {context}: {message}")]
    Conversion { context: String, message: String },
}

impl Error {
    /// Build a node-not-found error with path context.
    pub fn node_not_found(path: impl Into<String>) -> Self {
        Self::NodeNotFound { path: path.into() }
    }

    /// Build an invalid-path error with input path and parsing reason.
    pub fn invalid_path(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidPath {
            path: path.into(),
            reason: reason.into(),
        }
    }

    /// Build a conversion error with conversion context.
    pub fn conversion(context: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Conversion {
            context: context.into(),
            message: message.into(),
        }
    }
}

/// Crate-local result type for IR operations.
pub type Result<T> = std::result::Result<T, Error>;

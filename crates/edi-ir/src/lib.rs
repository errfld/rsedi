#![deny(warnings)]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::pedantic)] // Incrementally adopt pedantic lints without blocking current API.

//! # edi-ir
//!
//! Intermediate Representation structures and traversal APIs for EDI documents.
//!
//! This crate provides a generic, schema-aware tree structure that can represent
//! EDI documents in a format-neutral way, enabling transformations between
//! different formats (EDIFACT, CSV, database, etc.).

pub mod document;
pub mod metadata;
pub mod node;
pub mod traversal;

pub use document::Document;
pub use metadata::{Position, SourceInfo, ValidationState};
pub use node::{Node, NodeType, Value};
pub use traversal::{Cursor, Traversal};

use thiserror::Error;

/// Errors that can occur when working with the IR
#[derive(Error, Debug)]
pub enum Error {
    #[error("Node not found at path: {0}")]
    NodeNotFound(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Conversion error: {0}")]
    Conversion(String),
}

pub type Result<T> = std::result::Result<T, Error>;

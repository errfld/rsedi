//! # edi-schema
//!
//! Schema model, loader, and inheritance/merge logic for EDI.
//!
//! This crate provides runtime schema loading with hierarchical inheritance:
//! EDIFACT base → EANCOM version → Message type → Partner profile

pub mod inheritance;
pub mod loader;
pub mod model;
pub mod registry;

pub use loader::SchemaLoader;
pub use model::{
    Constraint, ElementDefinition, InheritanceMetadata, Schema, SchemaRef, SegmentDefinition,
};
pub use registry::{ConcurrentSchemaRegistry, SchemaRegistry};

use thiserror::Error;

/// Errors that can occur when working with schemas
#[derive(Error, Debug)]
pub enum Error {
    #[error("Schema not found: {0}")]
    NotFound(String),

    #[error("Invalid schema format: {0}")]
    InvalidFormat(String),

    #[error("Inheritance error: {0}")]
    Inheritance(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, Error>;

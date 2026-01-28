//! # edi-mapping
//!
//! DSL parser/runtime, transforms, and extension API for EDI mappings.
//!
//! This crate provides a declarative mapping DSL for transforming between
//! different EDI formats and custom schemas.

pub mod dsl;
pub mod runtime;
pub mod transforms;
pub mod extensions;

pub use dsl::MappingDsl;
pub use runtime::MappingRuntime;

use thiserror::Error;

/// Errors that can occur during mapping
#[derive(Error, Debug)]
pub enum Error {
    #[error("Mapping error: {0}")]
    Mapping(String),
    
    #[error("DSL parse error: {0}")]
    Parse(String),
    
    #[error("Runtime error: {0}")]
    Runtime(String),
    
    #[error("Transform error: {0}")]
    Transform(String),
}

pub type Result<T> = std::result::Result<T, Error>;

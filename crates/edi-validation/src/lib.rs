//! # edi-validation
//!
//! Validation engine for structural rules and codelists.
//!
//! This crate provides validation against runtime schemas with configurable
//! strictness levels and detailed error reporting.

pub mod engine;
pub mod rules;
pub mod codelist;
pub mod reporter;

pub use engine::ValidationEngine;
pub use reporter::ValidationReporter;

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

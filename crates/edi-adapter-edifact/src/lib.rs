//! # edi-adapter-edifact
//!
//! EDIFACT/EANCOM parser/serializer and envelope handling.
//!
//! This crate provides streaming parsing and serialization of EDIFACT
//! documents with support for EANCOM variants.

pub mod parser;
pub mod serializer;
pub mod envelopes;
pub mod syntax;

pub use parser::EdifactParser;
pub use serializer::EdifactSerializer;

use thiserror::Error;

/// Errors that can occur when parsing/serializing EDIFACT
#[derive(Error, Debug)]
pub enum Error {
    #[error("Parse error at line {line}, col {column}: {message}")]
    Parse {
        line: usize,
        column: usize,
        message: String,
    },
    
    #[error("Serialize error: {0}")]
    Serialize(String),
    
    #[error("Envelope error: {0}")]
    Envelope(String),
    
    #[error("Syntax error: {0}")]
    Syntax(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

//! # edi-adapter-csv
//!
//! CSV adapter for reading and writing EDI data.
//!
//! This crate provides CSV integration with runtime schema support
//! for column mapping and type conversion.

pub mod reader;
pub mod schema;
pub mod writer;

pub use reader::CsvReader;
pub use schema::{ColumnDef, ColumnType, CsvSchema, SchemaError};
pub use writer::CsvWriter;

use thiserror::Error;

/// Errors that can occur when working with CSV
#[derive(Error, Debug)]
pub enum Error {
    #[error("CSV read error: {0}")]
    Read(String),

    #[error("CSV write error: {0}")]
    Write(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Conversion error: {0}")]
    Conversion(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

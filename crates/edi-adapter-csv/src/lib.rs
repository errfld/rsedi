//! # edi-adapter-csv
//!
//! CSV adapter for reading and writing EDI data.
//!
//! This crate provides CSV integration with runtime schema support
//! for column mapping and type conversion.
//!
//! ## Example Usage
//!
//! ```rust
//! use edi_adapter_csv::{CsvReader, CsvWriter, CsvConfig, CsvSchema, ColumnDef, ColumnType};
//!
//! // Create a schema
//! let schema = CsvSchema::new()
//!     .with_header()
//!     .add_column(ColumnDef::new("name").with_type(ColumnType::String).required())
//!     .add_column(ColumnDef::new("age").with_type(ColumnType::Integer));
//!
//! // Create configuration
//! let config = CsvConfig::new()
//!     .delimiter(',')
//!     .has_header(true);
//! ```

pub mod config;
pub mod errors;
pub mod reader;
pub mod schema;
pub mod writer;

// Re-export main types
pub use config::{CsvConfig, Encoding, LineEnding, NullRepresentation, RecordTerminator};
pub use errors::{CsvError, CsvResult, ParseContext, RowLengthMismatchKind, SchemaError};
pub use reader::{CsvReader, CsvRecordIterator};
pub use schema::{ColumnDef, ColumnType, CsvSchema};
pub use writer::CsvWriter;

// Legacy re-exports for backward compatibility
pub use errors::CsvError as Error;
pub type Result<T> = CsvResult<T>;

/// CSV adapter that combines reader and writer functionality
#[derive(Debug, Clone)]
pub struct CsvAdapter {
    config: CsvConfig,
    schema: Option<CsvSchema>,
}

impl CsvAdapter {
    /// Create a new CSV adapter with default configuration
    pub fn new() -> Self {
        Self {
            config: CsvConfig::default(),
            schema: None,
        }
    }

    /// Create adapter with configuration
    pub fn with_config(mut self, config: CsvConfig) -> Self {
        self.config = config;
        self
    }

    /// Create adapter with schema
    pub fn with_schema(mut self, schema: CsvSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Get a reader configured with this adapter's settings
    pub fn reader(&self) -> CsvReader {
        let mut reader = CsvReader::new().with_config(self.config.clone());
        if let Some(schema) = &self.schema {
            reader = reader.with_schema(schema.clone());
        }
        reader
    }

    /// Get a writer configured with this adapter's settings
    pub fn writer(&self) -> CsvWriter {
        let mut writer = CsvWriter::new().with_config(self.config.clone());
        if let Some(schema) = &self.schema {
            writer = writer.with_schema(schema.clone());
        }
        writer
    }
}

impl Default for CsvAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_csv_adapter_creation() {
        let adapter = CsvAdapter::new();
        assert!(adapter.schema.is_none());

        let schema = CsvSchema::new().with_header();
        let adapter = CsvAdapter::new()
            .with_config(CsvConfig::new().delimiter(';'))
            .with_schema(schema);

        assert!(adapter.schema.is_some());
        assert_eq!(adapter.config.delimiter, ';');
    }

    #[test]
    fn test_csv_adapter_reader() {
        let data = "name,age\nJohn,30\nJane,25";
        let adapter = CsvAdapter::new();
        let reader = adapter.reader();

        let records = reader.read(Cursor::new(data)).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_csv_adapter_writer() {
        let adapter = CsvAdapter::new();
        let writer = adapter.writer();

        let records = vec![
            vec!["John".to_string(), "30".to_string()],
            vec!["Jane".to_string(), "25".to_string()],
        ];

        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("John,30"));
        assert!(result.contains("Jane,25"));
    }

    #[test]
    fn test_end_to_end_read_write() {
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name").with_type(ColumnType::String))
            .add_column(ColumnDef::new("age").with_type(ColumnType::Integer));

        let adapter = CsvAdapter::new().with_schema(schema);

        // Read
        let data = "name,age\nJohn,30\nJane,25";
        let doc = adapter.reader().read_to_ir(Cursor::new(data)).unwrap();
        assert_eq!(doc.root.children.len(), 2);

        // Write back
        let mut output = Vec::new();
        adapter.writer().write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("name,age"));
        assert!(result.contains("John,30"));
    }
}

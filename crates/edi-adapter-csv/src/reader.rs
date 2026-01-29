//! CSV reader

use crate::schema::{ColumnType, CsvSchema};
use crate::{Error, Result};
use edi_ir::document::DocumentMetadata;
use edi_ir::{Document, Node, NodeType, Value};
use std::io::Read;
use tracing::{debug, trace};

/// Reader for CSV files
pub struct CsvReader {
    schema: Option<CsvSchema>,
    has_header: bool,
    delimiter: u8,
}

impl CsvReader {
    /// Create a new CSV reader
    pub fn new() -> Self {
        Self {
            schema: None,
            has_header: true,
            delimiter: b',',
        }
    }

    /// Set schema for parsing
    pub fn with_schema(mut self, schema: CsvSchema) -> Self {
        self.has_header = schema.has_header;
        self.delimiter = schema.delimiter as u8;
        self.schema = Some(schema);
        self
    }

    /// Configure header presence
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set delimiter character
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter as u8;
        self
    }

    /// Read CSV from a reader
    pub fn read<R: Read>(&self, reader: R) -> Result<Vec<Vec<String>>> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(self.has_header)
            .delimiter(self.delimiter)
            .from_reader(reader);

        let mut records = Vec::new();

        for result in csv_reader.records() {
            let record = result.map_err(|e| Error::Read(e.to_string()))?;
            let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            trace!(?row, "Read CSV row");
            records.push(row);
        }

        debug!(row_count = records.len(), "Finished reading CSV");
        Ok(records)
    }

    /// Read CSV with headers
    pub fn read_with_headers<R: Read>(&self, reader: R) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(self.delimiter)
            .from_reader(reader);

        let headers: Vec<String> = csv_reader
            .headers()
            .map_err(|e| Error::Read(e.to_string()))?
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut records = Vec::new();
        for result in csv_reader.records() {
            let record = result.map_err(|e| Error::Read(e.to_string()))?;
            let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            records.push(row);
        }

        Ok((headers, records))
    }

    /// Read CSV and convert to IR Document
    pub fn read_to_ir<R: Read>(&self, reader: R) -> Result<Document> {
        let (headers, records) = if self.has_header {
            self.read_with_headers(reader)?
        } else {
            let records = self.read(reader)?;
            let headers: Vec<String> = (0..records.first().map(|r| r.len()).unwrap_or(0))
                .map(|i| format!("col_{}", i))
                .collect();
            (headers, records)
        };

        let mut root = Node::new("csv_data", NodeType::Root);

        for (row_idx, row) in records.iter().enumerate() {
            let mut record_node = Node::new(format!("record_{}", row_idx), NodeType::Record);

            for (col_idx, (header, value)) in headers.iter().zip(row.iter()).enumerate() {
                let value = if value.is_empty() {
                    Value::Null
                } else {
                    self.parse_value(value, col_idx)
                };

                let field_node = Node::with_value(header.clone(), NodeType::Field, value);
                record_node.add_child(field_node);
            }

            root.add_child(record_node);
        }

        let metadata = DocumentMetadata {
            doc_type: Some("CSV".to_string()),
            ..Default::default()
        };

        Ok(Document::with_metadata(root, metadata))
    }

    fn parse_value(&self, value: &str, col_idx: usize) -> Value {
        // Use schema type if available
        if let Some(schema) = &self.schema {
            if let Some(col_def) = schema.get_column_by_index(col_idx) {
                return match col_def.column_type {
                    ColumnType::Integer => value
                        .parse::<i64>()
                        .map(Value::Integer)
                        .unwrap_or_else(|_| Value::String(value.to_string())),
                    ColumnType::Decimal => value
                        .parse::<f64>()
                        .map(Value::Decimal)
                        .unwrap_or_else(|_| Value::String(value.to_string())),
                    ColumnType::Boolean => value
                        .parse::<bool>()
                        .map(Value::Boolean)
                        .unwrap_or_else(|_| Value::String(value.to_string())),
                    ColumnType::Date => Value::Date(value.to_string()),
                    ColumnType::DateTime => Value::DateTime(value.to_string()),
                    ColumnType::String => Value::String(value.to_string()),
                };
            }
        }

        // Default: try to infer type
        if let Ok(int_val) = value.parse::<i64>() {
            Value::Integer(int_val)
        } else if let Ok(float_val) = value.parse::<f64>() {
            Value::Decimal(float_val)
        } else if let Ok(bool_val) = value.parse::<bool>() {
            Value::Boolean(bool_val)
        } else {
            Value::String(value.to_string())
        }
    }
}

impl Default for CsvReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_simple_csv() {
        let data = "name,age\nJohn,30\nJane,25";
        let reader = CsvReader::new();
        let records = reader.read(Cursor::new(data)).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["John", "30"]);
        assert_eq!(records[1], vec!["Jane", "25"]);
    }

    #[test]
    fn test_read_no_header() {
        let data = "John,30\nJane,25";
        let reader = CsvReader::new().has_header(false);
        let records = reader.read(Cursor::new(data)).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["John", "30"]);
        assert_eq!(records[1], vec!["Jane", "25"]);
    }

    #[test]
    fn test_read_different_delimiters() {
        // Semicolon delimiter
        let data_semicolon = "name;value\nTest;123";
        let reader = CsvReader::new().with_delimiter(';');
        let records = reader.read(Cursor::new(data_semicolon)).unwrap();
        assert_eq!(records[0], vec!["Test", "123"]);

        // Tab delimiter
        let data_tab = "name\tvalue\nTest\t123";
        let reader = CsvReader::new().with_delimiter('\t');
        let records = reader.read(Cursor::new(data_tab)).unwrap();
        assert_eq!(records[0], vec!["Test", "123"]);
    }

    #[test]
    fn test_read_quoted_fields() {
        // CSV escapes quotes by doubling them
        let data =
            "description,price\n\"Item, with comma\",10.99\n\"Another \"\"quoted\"\" item\",5.00";
        let reader = CsvReader::new();
        let records = reader.read(Cursor::new(data)).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["Item, with comma", "10.99"]);
        assert_eq!(records[1], vec!["Another \"quoted\" item", "5.00"]);
    }

    #[test]
    fn test_read_empty_file() {
        let data = "header1,header2"; // Only header, no data
        let reader = CsvReader::new();
        let records = reader.read(Cursor::new(data)).unwrap();

        assert!(records.is_empty());
    }

    #[test]
    fn test_read_to_ir() {
        let data = "name,age,city\nJohn,30,NYC\nJane,25,LA";
        let reader = CsvReader::new();
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        assert_eq!(doc.root.node_type, NodeType::Root);
        assert_eq!(doc.root.children.len(), 2);

        // Check first record
        let first_record = &doc.root.children[0];
        assert_eq!(first_record.children.len(), 3);
        assert_eq!(first_record.children[0].name, "name");
        assert_eq!(
            first_record.children[0].value.as_ref().unwrap().as_string(),
            Some("John".to_string())
        );

        // Check second record
        let second_record = &doc.root.children[1];
        assert_eq!(second_record.children[1].name, "age");
        assert_eq!(
            second_record.children[1]
                .value
                .as_ref()
                .unwrap()
                .as_string(),
            Some("25".to_string())
        );
    }

    #[test]
    fn test_read_to_ir_no_header() {
        let data = "value1,value2\ndata1,data2";
        let reader = CsvReader::new().has_header(false);
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        assert_eq!(doc.root.children.len(), 2);

        // Should generate column names
        let first_record = &doc.root.children[0];
        assert_eq!(first_record.children[0].name, "col_0");
        assert_eq!(first_record.children[1].name, "col_1");
    }

    #[test]
    fn test_read_with_schema_type_conversion() {
        use crate::schema::{ColumnDef, ColumnType};

        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("count").with_type(ColumnType::Integer))
            .add_column(ColumnDef::new("price").with_type(ColumnType::Decimal))
            .add_column(ColumnDef::new("active").with_type(ColumnType::Boolean));

        let data = "name,count,price,active\nTest,42,19.99,true";
        let reader = CsvReader::new().with_schema(schema);
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        let record = &doc.root.children[0];

        // Check type conversions
        assert!(matches!(record.children[1].value, Some(Value::Integer(42))));
        assert!(matches!(record.children[2].value, Some(Value::Decimal(_))));
        assert!(matches!(
            record.children[3].value,
            Some(Value::Boolean(true))
        ));
    }
}

//! CSV writer with enhanced configuration support

use crate::config::{CsvConfig, NullRepresentation};
use crate::errors::{CsvError, CsvResult};
use crate::schema::CsvSchema;
use edi_ir::{Document, Node, NodeType, Value};
use std::io::Write;
use tracing::{debug, trace, warn};

/// Writer for CSV files
#[derive(Debug, Clone)]
pub struct CsvWriter {
    schema: Option<CsvSchema>,
    config: CsvConfig,
}

impl CsvWriter {
    /// Create a new CSV writer with default configuration
    pub fn new() -> Self {
        Self {
            schema: None,
            config: CsvConfig::default(),
        }
    }

    /// Set configuration for writing
    pub fn with_config(mut self, config: CsvConfig) -> Self {
        self.config = config;
        self
    }

    /// Set schema for writing
    pub fn with_schema(mut self, schema: CsvSchema) -> Self {
        self.config.has_header = schema.has_header;
        self.config.delimiter = schema.delimiter;
        self.config.quote_char = schema.quote_char;
        if let Some(null_val) = &schema.null_value {
            self.config.null_representation = NullRepresentation::Custom(null_val.clone());
        }
        self.schema = Some(schema);
        self
    }

    /// Configure header writing (legacy method, prefer with_config)
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.config.has_header = has_header;
        self
    }

    /// Set delimiter character (legacy method, prefer with_config)
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.config.delimiter = delimiter;
        self
    }

    /// Write records to CSV
    pub fn write<W: Write>(&self, writer: W, records: &[Vec<String>]) -> CsvResult<()> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.config.delimiter_u8())
            .quote(self.config.quote_char_u8())
            .from_writer(writer);

        for record in records {
            csv_writer
                .write_record(record)
                .map_err(|e| CsvError::write(e.to_string()))?;
            trace!(?record, "Wrote CSV row");
        }

        csv_writer
            .flush()
            .map_err(|e| CsvError::write(e.to_string()))?;
        debug!(record_count = records.len(), "Finished writing CSV");
        Ok(())
    }

    /// Write records with header
    pub fn write_with_headers<W: Write>(
        &self,
        writer: W,
        headers: &[String],
        records: &[Vec<String>],
    ) -> CsvResult<()> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.config.delimiter_u8())
            .quote(self.config.quote_char_u8())
            .from_writer(writer);

        // Write header
        csv_writer
            .write_record(headers)
            .map_err(|e| CsvError::write(e.to_string()))?;

        // Write records
        for record in records {
            csv_writer
                .write_record(record)
                .map_err(|e| CsvError::write(e.to_string()))?;
        }

        csv_writer
            .flush()
            .map_err(|e| CsvError::write(e.to_string()))?;
        Ok(())
    }

    /// Write IR Document to CSV
    pub fn write_from_ir<W: Write>(&self, writer: W, doc: &Document) -> CsvResult<()> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.config.delimiter_u8())
            .quote(self.config.quote_char_u8())
            .from_writer(writer);

        let records = Self::records_from_root(&doc.root);

        // Collect headers from schema or infer from first record
        let headers = if let Some(schema) = &self.schema {
            schema
                .get_headers()
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        } else {
            self.infer_headers(records)
        };

        if headers.is_empty() {
            warn!("No headers available for CSV output");
        }

        // Write header if configured
        if self.config.has_header && !headers.is_empty() {
            csv_writer
                .write_record(&headers)
                .map_err(|e| CsvError::write(e.to_string()))?;
        }

        // Write records
        for (idx, record) in records.iter().enumerate() {
            let row = self.node_to_row(record, &headers, idx)?;
            csv_writer
                .write_record(&row)
                .map_err(|e| CsvError::write(e.to_string()))?;
        }

        csv_writer
            .flush()
            .map_err(|e| CsvError::write(e.to_string()))?;
        Ok(())
    }

    /// Write a single record from an IR Node
    pub fn write_node<W: Write>(
        &self,
        writer: &mut csv::Writer<W>,
        node: &Node,
        headers: &[String],
    ) -> CsvResult<()> {
        let row = self.node_to_row(node, headers, 0)?;
        writer
            .write_record(&row)
            .map_err(|e| CsvError::write(e.to_string()))?;
        Ok(())
    }

    fn records_from_root(root: &Node) -> &[Node] {
        if root.children.len() == 1
            && root.children[0].node_type == NodeType::SegmentGroup
            && !root.children[0].children.is_empty()
        {
            &root.children[0].children
        } else {
            &root.children
        }
    }

    fn infer_headers(&self, records: &[Node]) -> Vec<String> {
        // Try to get headers from first child
        if let Some(first_record) = records.first() {
            first_record
                .children
                .iter()
                .map(|child| child.name.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    fn node_to_row(
        &self,
        node: &Node,
        headers: &[String],
        _row_idx: usize,
    ) -> CsvResult<Vec<String>> {
        headers
            .iter()
            .map(|header| {
                // Find child node with matching name
                if let Some(child) = node.find_child(header) {
                    self.value_to_string(child.value.as_ref())
                } else {
                    // Field not found - return null representation
                    Ok(self.null_to_string())
                }
            })
            .collect::<CsvResult<Vec<_>>>()
    }

    fn value_to_string(&self, value: Option<&Value>) -> CsvResult<String> {
        match value {
            Some(Value::String(s)) => Ok(s.clone()),
            Some(Value::Integer(i)) => Ok(i.to_string()),
            Some(Value::Decimal(d)) => Ok(self.format_decimal(*d)),
            Some(Value::Boolean(b)) => Ok(b.to_string()),
            Some(Value::Date(d)) => Ok(d.clone()),
            Some(Value::Time(t)) => Ok(t.clone()),
            Some(Value::DateTime(dt)) => Ok(dt.clone()),
            Some(Value::Binary(_)) => {
                warn!("Binary value cannot be serialized to CSV, using empty string");
                Ok(self.null_to_string())
            }
            Some(Value::Null) | None => Ok(self.null_to_string()),
        }
    }

    fn null_to_string(&self) -> String {
        match &self.config.null_representation {
            NullRepresentation::EmptyString => String::new(),
            NullRepresentation::NullString => "NULL".to_string(),
            NullRepresentation::BackslashN => "\\N".to_string(),
            NullRepresentation::Custom(s) => s.clone(),
        }
    }

    fn format_decimal(&self, value: f64) -> String {
        // Check if we have precision/scale from schema
        if let Some(_schema) = &self.schema {
            // Use schema-defined precision if available
            // For now, just format to avoid unnecessary trailing zeros
            if value.fract() == 0.0 {
                format!("{:.0}", value)
            } else {
                // Trim trailing zeros
                let s = format!("{}", value);
                s.trim_end_matches('0').trim_end_matches('.').to_string()
            }
        } else {
            // Default formatting
            if value.fract() == 0.0 {
                format!("{:.0}", value)
            } else {
                format!("{}", value)
            }
        }
    }

    /// Write multiple records incrementally (streaming write)
    ///
    /// This method returns a CsvRecordWriter that can be used to write
    /// records one at a time, which is useful for large datasets.
    pub fn streaming_writer<W: Write>(&self, writer: W) -> CsvResult<CsvRecordWriter<W>> {
        CsvRecordWriter::new(writer, self.config.clone(), self.schema.clone())
    }
}

impl Default for CsvWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Streaming writer for CSV records
pub struct CsvRecordWriter<W: Write> {
    csv_writer: csv::Writer<W>,
    headers_written: bool,
    config: CsvConfig,
    _schema: Option<CsvSchema>,
    headers: Vec<String>,
}

impl<W: Write> CsvRecordWriter<W> {
    /// Create a new streaming CSV writer
    fn new(writer: W, config: CsvConfig, schema: Option<CsvSchema>) -> CsvResult<Self> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(config.delimiter_u8())
            .quote(config.quote_char_u8())
            .from_writer(writer);

        let headers = if let Some(s) = &schema {
            s.get_headers().into_iter().map(|h| h.to_string()).collect()
        } else {
            Vec::new()
        };

        // Write headers immediately if configured and we have them from schema
        let headers_written = if config.has_header && !headers.is_empty() {
            csv_writer
                .write_record(&headers)
                .map_err(|e| CsvError::write(e.to_string()))?;
            true
        } else {
            false
        };

        Ok(Self {
            csv_writer,
            headers_written,
            config,
            _schema: schema,
            headers,
        })
    }

    /// Write a record from string values
    pub fn write_record(&mut self, record: &[String]) -> CsvResult<()> {
        self.csv_writer
            .write_record(record)
            .map_err(|e| CsvError::write(e.to_string()))?;
        Ok(())
    }

    /// Write a record from an IR Node
    pub fn write_node(&mut self, node: &Node) -> CsvResult<()> {
        // Infer headers from first record if needed
        if self.headers.is_empty() && !node.children.is_empty() {
            self.headers = node
                .children
                .iter()
                .map(|child| child.name.clone())
                .collect();

            // Write headers if not already written
            if self.config.has_header && !self.headers_written {
                self.csv_writer
                    .write_record(&self.headers)
                    .map_err(|e| CsvError::write(e.to_string()))?;
                self.headers_written = true;
            }
        }

        let row: Vec<String> = self
            .headers
            .iter()
            .map(|header| {
                if let Some(child) = node.find_child(header) {
                    self.value_to_string(child.value.as_ref())
                } else {
                    Ok(self.null_to_string())
                }
            })
            .collect::<CsvResult<Vec<_>>>()?;

        self.csv_writer
            .write_record(&row)
            .map_err(|e| CsvError::write(e.to_string()))?;
        Ok(())
    }

    /// Write headers explicitly (useful when schema not available)
    pub fn write_headers(&mut self, headers: &[String]) -> CsvResult<()> {
        if self.headers_written {
            return Err(CsvError::config("Headers already written"));
        }

        self.headers = headers.to_vec();
        self.csv_writer
            .write_record(headers)
            .map_err(|e| CsvError::write(e.to_string()))?;
        self.headers_written = true;
        Ok(())
    }

    /// Flush the writer
    pub fn flush(&mut self) -> CsvResult<()> {
        self.csv_writer
            .flush()
            .map_err(|e| CsvError::write(e.to_string()))?;
        Ok(())
    }

    fn value_to_string(&self, value: Option<&Value>) -> CsvResult<String> {
        match value {
            Some(Value::String(s)) => Ok(s.clone()),
            Some(Value::Integer(i)) => Ok(i.to_string()),
            Some(Value::Decimal(d)) => Ok(self.format_decimal(*d)),
            Some(Value::Boolean(b)) => Ok(b.to_string()),
            Some(Value::Date(d)) => Ok(d.clone()),
            Some(Value::Time(t)) => Ok(t.clone()),
            Some(Value::DateTime(dt)) => Ok(dt.clone()),
            Some(Value::Binary(_)) => {
                warn!("Binary value cannot be serialized to CSV");
                Ok(self.null_to_string())
            }
            Some(Value::Null) | None => Ok(self.null_to_string()),
        }
    }

    fn format_decimal(&self, value: f64) -> String {
        if value.fract() == 0.0 {
            format!("{:.0}", value)
        } else {
            format!("{}", value)
        }
    }

    fn null_to_string(&self) -> String {
        match &self.config.null_representation {
            NullRepresentation::EmptyString => String::new(),
            NullRepresentation::NullString => "NULL".to_string(),
            NullRepresentation::BackslashN => "\\N".to_string(),
            NullRepresentation::Custom(s) => s.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnDef;
    use edi_ir::{Document, Node, NodeType, Value};

    #[test]
    fn test_write_simple_csv() {
        let records = vec![
            vec!["John".to_string(), "30".to_string()],
            vec!["Jane".to_string(), "25".to_string()],
        ];

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("John,30"));
        assert!(result.contains("Jane,25"));
    }

    #[test]
    fn test_write_with_header() {
        let headers = vec!["name".to_string(), "age".to_string()];
        let records = vec![vec!["John".to_string(), "30".to_string()]];

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer
            .write_with_headers(&mut output, &headers, &records)
            .unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("name,age"));
        assert!(result.contains("John,30"));
    }

    #[test]
    fn test_write_no_header() {
        let records = vec![vec!["value1".to_string(), "value2".to_string()]];

        let writer = CsvWriter::new().has_header(false);
        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        // Should not contain header
        assert!(!result.contains("name"));
        assert!(result.contains("value1,value2"));
    }

    #[test]
    fn test_write_different_delimiters() {
        let records = vec![vec!["Test".to_string(), "123".to_string()]];

        // Semicolon delimiter
        let writer = CsvWriter::new().with_delimiter(';');
        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Test;123"));
        assert!(!result.contains("Test,123"));

        // Tab delimiter
        let config = CsvConfig::new().delimiter('\t');
        let writer = CsvWriter::new().with_config(config);
        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Test\t123"));
    }

    #[test]
    fn test_write_quoted_fields() {
        let records = vec![
            vec!["Item, with comma".to_string(), "10.99".to_string()],
            vec!["Value with \"quotes\"".to_string(), "5.00".to_string()],
        ];

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        // Fields with commas should be quoted
        assert!(result.contains("\"Item, with comma\""));
        // Quotes are escaped by doubling in CSV
        assert!(result.contains("\"Value with \"\"quotes\"\"\""));
    }

    #[test]
    fn test_write_from_ir() {
        // Create IR document
        let mut root = Node::new("csv_data", NodeType::Root);

        let mut record1 = Node::new("record_0", NodeType::Record);
        record1.add_child(Node::with_value(
            "name",
            NodeType::Field,
            Value::String("John".to_string()),
        ));
        record1.add_child(Node::with_value("age", NodeType::Field, Value::Integer(30)));
        root.add_child(record1);

        let mut record2 = Node::new("record_1", NodeType::Record);
        record2.add_child(Node::with_value(
            "name",
            NodeType::Field,
            Value::String("Jane".to_string()),
        ));
        record2.add_child(Node::with_value("age", NodeType::Field, Value::Integer(25)));
        root.add_child(record2);

        let doc = Document::new(root);

        // Write to CSV
        let writer = CsvWriter::new().has_header(true);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("name,age"));
        assert!(result.contains("John,30"));
        assert!(result.contains("Jane,25"));
    }

    #[test]
    fn test_write_from_ir_with_schema() {
        // Create schema
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("value"));

        // Create IR document
        let mut root = Node::new("csv_data", NodeType::Root);

        let mut record = Node::new("record_0", NodeType::Record);
        record.add_child(Node::with_value(
            "name",
            NodeType::Field,
            Value::String("Test".to_string()),
        ));
        record.add_child(Node::with_value(
            "value",
            NodeType::Field,
            Value::Integer(42),
        ));
        root.add_child(record);

        let doc = Document::new(root);

        // Write with schema
        let writer = CsvWriter::new().with_schema(schema);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("name,value"));
        assert!(result.contains("Test,42"));
    }

    #[test]
    fn test_write_empty_document() {
        // With a schema, empty document should still write headers
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("age"));

        let root = Node::new("csv_data", NodeType::Root);
        let doc = Document::new(root);

        let writer = CsvWriter::new().with_schema(schema);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        // Should write header but no data rows
        assert!(!result.is_empty());
        assert!(result.contains("name,age"));
    }

    #[test]
    fn test_write_empty_document_no_schema() {
        // Without a schema and no records, output should be empty (nothing to infer)
        let root = Node::new("csv_data", NodeType::Root);
        let doc = Document::new(root);

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        // Empty when no schema and no data (nothing to infer)
        assert!(result.is_empty());
    }

    #[test]
    fn test_write_different_types() {
        let mut root = Node::new("csv_data", NodeType::Root);

        let mut record = Node::new("record_0", NodeType::Record);
        record.add_child(Node::with_value(
            "string_field",
            NodeType::Field,
            Value::String("text".to_string()),
        ));
        record.add_child(Node::with_value(
            "int_field",
            NodeType::Field,
            Value::Integer(42),
        ));
        record.add_child(Node::with_value(
            "decimal_field",
            NodeType::Field,
            Value::Decimal(99.99),
        ));
        record.add_child(Node::with_value(
            "bool_field",
            NodeType::Field,
            Value::Boolean(true),
        ));
        record.add_child(Node::with_value("null_field", NodeType::Field, Value::Null));
        root.add_child(record);

        let doc = Document::new(root);

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("text"));
        assert!(result.contains("42"));
        assert!(result.contains("99.99"));
        assert!(result.contains("true"));
    }

    #[test]
    fn test_null_representation() {
        let mut root = Node::new("csv_data", NodeType::Root);
        let mut record = Node::new("record_0", NodeType::Record);
        record.add_child(Node::with_value(
            "name",
            NodeType::Field,
            Value::String("Test".to_string()),
        ));
        record.add_child(Node::with_value("value", NodeType::Field, Value::Null));
        root.add_child(record);

        let doc = Document::new(root);

        // Test NULL string representation
        let config = CsvConfig::new().null_representation(NullRepresentation::NullString);
        let writer = CsvWriter::new().with_config(config);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Test,NULL"));

        // Test backslash-N representation
        let config = CsvConfig::new().null_representation(NullRepresentation::BackslashN);
        let writer = CsvWriter::new().with_config(config);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Test,\\N"));

        // Test custom representation
        let config =
            CsvConfig::new().null_representation(NullRepresentation::Custom("N/A".to_string()));
        let writer = CsvWriter::new().with_config(config);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Test,N/A"));
    }

    #[test]
    fn test_decimal_formatting() {
        let mut root = Node::new("csv_data", NodeType::Root);
        let mut record = Node::new("record_0", NodeType::Record);
        record.add_child(Node::with_value(
            "int_decimal",
            NodeType::Field,
            Value::Decimal(100.0),
        ));
        record.add_child(Node::with_value(
            "frac_decimal",
            NodeType::Field,
            Value::Decimal(99.99),
        ));
        record.add_child(Node::with_value(
            "many_decimals",
            NodeType::Field,
            Value::Decimal(1.123456),
        ));
        root.add_child(record);

        let doc = Document::new(root);

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("100"));
        assert!(result.contains("99.99"));
    }

    #[test]
    fn test_streaming_writer() {
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("age"));

        let mut output = Vec::new();
        {
            let writer = CsvWriter::new().with_schema(schema);
            let mut streaming = writer.streaming_writer(&mut output).unwrap();

            // Write records incrementally
            let mut record1 = Node::new("record_0", NodeType::Record);
            record1.add_child(Node::with_value(
                "name",
                NodeType::Field,
                Value::String("John".to_string()),
            ));
            record1.add_child(Node::with_value("age", NodeType::Field, Value::Integer(30)));
            streaming.write_node(&record1).unwrap();

            let mut record2 = Node::new("record_1", NodeType::Record);
            record2.add_child(Node::with_value(
                "name",
                NodeType::Field,
                Value::String("Jane".to_string()),
            ));
            record2.add_child(Node::with_value("age", NodeType::Field, Value::Integer(25)));
            streaming.write_node(&record2).unwrap();

            streaming.flush().unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("name,age")); // Header written
        assert!(result.contains("John,30"));
        assert!(result.contains("Jane,25"));
    }

    #[test]
    fn test_streaming_writer_with_explicit_headers() {
        let mut output = Vec::new();
        {
            let writer = CsvWriter::new().has_header(true);
            let mut streaming = writer.streaming_writer(&mut output).unwrap();

            // Write headers explicitly
            streaming
                .write_headers(&["name".to_string(), "value".to_string()])
                .unwrap();

            // Write raw records
            streaming
                .write_record(&["Test".to_string(), "42".to_string()])
                .unwrap();
            streaming.flush().unwrap();
        }

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("name,value"));
        assert!(result.contains("Test,42"));
    }

    #[test]
    fn test_config_integration() {
        let records = vec![vec!["Test".to_string(), "123".to_string()]];

        let config = CsvConfig::new()
            .delimiter(';')
            .has_header(false)
            .null_representation(NullRepresentation::NullString);

        let writer = CsvWriter::new().with_config(config);
        let mut output = Vec::new();
        writer.write(&mut output, &records).unwrap();

        let result = String::from_utf8(output).unwrap();
        assert!(result.contains("Test;123"));
        assert!(!result.contains("Test,123"));
    }

    #[test]
    fn test_missing_field_handling() {
        let mut root = Node::new("csv_data", NodeType::Root);

        // Record missing the "city" field
        let mut record = Node::new("record_0", NodeType::Record);
        record.add_child(Node::with_value(
            "name",
            NodeType::Field,
            Value::String("John".to_string()),
        ));
        record.add_child(Node::with_value("age", NodeType::Field, Value::Integer(30)));
        // Note: no "city" field
        root.add_child(record);

        let doc = Document::new(root);

        // Schema expects city field
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("age"))
            .add_column(ColumnDef::new("city"));

        let writer = CsvWriter::new().with_schema(schema);
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        // Should have empty value for missing city field
        assert!(result.contains("John,30,"));
    }
}

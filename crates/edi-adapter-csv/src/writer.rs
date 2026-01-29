//! CSV writer

use crate::schema::CsvSchema;
use crate::{Error, Result};
use edi_ir::{Document, Node, Value};
use std::io::Write;
use tracing::{debug, trace};

/// Writer for CSV files
pub struct CsvWriter {
    schema: Option<CsvSchema>,
    has_header: bool,
    delimiter: u8,
}

impl CsvWriter {
    /// Create a new CSV writer
    pub fn new() -> Self {
        Self {
            schema: None,
            has_header: true,
            delimiter: b',',
        }
    }

    /// Set schema for writing
    pub fn with_schema(mut self, schema: CsvSchema) -> Self {
        self.has_header = schema.has_header;
        self.delimiter = schema.delimiter as u8;
        self.schema = Some(schema);
        self
    }

    /// Configure header writing
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Set delimiter character
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter as u8;
        self
    }

    /// Write records to CSV
    pub fn write<W: Write>(&self, writer: W, records: &[Vec<String>]) -> Result<()> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(writer);

        for record in records {
            csv_writer
                .write_record(record)
                .map_err(|e| Error::Write(e.to_string()))?;
            trace!(?record, "Wrote CSV row");
        }

        csv_writer
            .flush()
            .map_err(|e| Error::Write(e.to_string()))?;
        debug!(record_count = records.len(), "Finished writing CSV");
        Ok(())
    }

    /// Write records with header
    pub fn write_with_headers<W: Write>(
        &self,
        writer: W,
        headers: &[String],
        records: &[Vec<String>],
    ) -> Result<()> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(writer);

        // Write header
        csv_writer
            .write_record(headers)
            .map_err(|e| Error::Write(e.to_string()))?;

        // Write records
        for record in records {
            csv_writer
                .write_record(record)
                .map_err(|e| Error::Write(e.to_string()))?;
        }

        csv_writer
            .flush()
            .map_err(|e| Error::Write(e.to_string()))?;
        Ok(())
    }

    /// Write IR Document to CSV
    pub fn write_from_ir<W: Write>(&self, writer: W, doc: &Document) -> Result<()> {
        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(writer);

        // Collect headers from first record or schema
        let headers = if let Some(schema) = &self.schema {
            schema
                .get_headers()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            self.infer_headers(&doc.root)
        };

        // Write header if configured
        if self.has_header {
            csv_writer
                .write_record(&headers)
                .map_err(|e| Error::Write(e.to_string()))?;
        }

        // Write records
        for child in &doc.root.children {
            let row = self.node_to_row(child, &headers);
            csv_writer
                .write_record(&row)
                .map_err(|e| Error::Write(e.to_string()))?;
        }

        csv_writer
            .flush()
            .map_err(|e| Error::Write(e.to_string()))?;
        Ok(())
    }

    fn infer_headers(&self, root: &Node) -> Vec<String> {
        // Try to get headers from first child
        if let Some(first_record) = root.children.first() {
            first_record
                .children
                .iter()
                .map(|child| child.name.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    fn node_to_row(&self, node: &Node, headers: &[String]) -> Vec<String> {
        headers
            .iter()
            .map(|header| {
                // Find child node with matching name
                if let Some(child) = node.find_child(header) {
                    self.value_to_string(child.value.as_ref())
                } else {
                    String::new()
                }
            })
            .collect()
    }

    fn value_to_string(&self, value: Option<&Value>) -> String {
        match value {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Integer(i)) => i.to_string(),
            Some(Value::Decimal(d)) => d.to_string(),
            Some(Value::Boolean(b)) => b.to_string(),
            Some(Value::Date(d)) => d.clone(),
            Some(Value::Time(t)) => t.clone(),
            Some(Value::DateTime(dt)) => dt.clone(),
            Some(Value::Binary(_)) => String::new(),
            Some(Value::Null) | None => String::new(),
        }
    }
}

impl Default for CsvWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let writer = CsvWriter::new().with_delimiter('\t');
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
        use crate::schema::{ColumnDef, CsvSchema};

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
        let root = Node::new("csv_data", NodeType::Root);
        let doc = Document::new(root);

        let writer = CsvWriter::new();
        let mut output = Vec::new();
        writer.write_from_ir(&mut output, &doc).unwrap();

        let result = String::from_utf8(output).unwrap();
        // Should write header but no data rows
        assert!(!result.is_empty());
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
}

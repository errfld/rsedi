//! CSV reader with streaming support

use crate::config::{CsvConfig, NullRepresentation};
use crate::errors::{CsvError, CsvResult, RowLengthMismatchKind};
use crate::schema::{ColumnType, CsvSchema};
use edi_ir::document::DocumentMetadata;
use edi_ir::{Document, Node, NodeType, Value};
use std::io::Read;
use tracing::{debug, trace, warn};

/// Reader for CSV files
#[derive(Debug, Clone)]
pub struct CsvReader {
    schema: Option<CsvSchema>,
    config: CsvConfig,
}

impl CsvReader {
    /// Create a new CSV reader with default configuration
    pub fn new() -> Self {
        Self {
            schema: None,
            config: CsvConfig::default(),
        }
    }

    /// Set configuration for parsing
    pub fn with_config(mut self, config: CsvConfig) -> Self {
        self.config = config;
        self
    }

    /// Set schema for parsing
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

    /// Configure header presence (legacy method, prefer with_config)
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.config.has_header = has_header;
        self
    }

    /// Set delimiter character (legacy method, prefer with_config)
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.config.delimiter = delimiter;
        self
    }

    /// Read CSV from a reader into raw string records
    ///
    /// This method collects all records into memory. For large files,
    /// consider using `read_iter` instead.
    pub fn read<R: Read>(&self, reader: R) -> CsvResult<Vec<Vec<String>>> {
        self.read_impl(reader, false)
    }

    fn read_impl<R: Read>(&self, reader: R, flexible: bool) -> CsvResult<Vec<Vec<String>>> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(self.config.has_header)
            .flexible(flexible)
            .delimiter(self.config.delimiter_u8())
            .quote(self.config.quote_char_u8())
            .from_reader(reader);

        let mut records = Vec::new();

        for (line_num, result) in csv_reader.records().enumerate() {
            let record = result.map_err(|e| CsvError::read_at(line_num + 1, e.to_string()))?;
            let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            trace!(?row, line = line_num + 1, "Read CSV row");
            records.push(row);
        }

        debug!(row_count = records.len(), "Finished reading CSV");
        Ok(records)
    }

    /// Read CSV with headers
    ///
    /// Returns tuple of (headers, records). This method collects all
    /// records into memory. For large files, consider using `read_iter`.
    pub fn read_with_headers<R: Read>(
        &self,
        reader: R,
    ) -> CsvResult<(Vec<String>, Vec<Vec<String>>)> {
        self.read_with_headers_impl(reader, false)
    }

    fn read_with_headers_impl<R: Read>(
        &self,
        reader: R,
        flexible: bool,
    ) -> CsvResult<(Vec<String>, Vec<Vec<String>>)> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(flexible)
            .delimiter(self.config.delimiter_u8())
            .quote(self.config.quote_char_u8())
            .from_reader(reader);

        let headers: Vec<String> = csv_reader
            .headers()
            .map_err(|e| CsvError::read(e.to_string()))?
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut records = Vec::new();
        for (line_num, result) in csv_reader.records().enumerate() {
            let record = result.map_err(|e| CsvError::read_at(line_num + 2, e.to_string()))?;
            let row: Vec<String> = record.iter().map(|s| s.to_string()).collect();
            records.push(row);
        }

        Ok((headers, records))
    }

    /// Read CSV and convert to IR Document
    ///
    /// This method collects all records into memory. For large files,
    /// consider using `read_iter` and processing records incrementally.
    pub fn read_to_ir<R: Read>(&self, reader: R) -> CsvResult<Document> {
        let (headers, records) = if self.config.has_header {
            self.read_with_headers_impl(reader, true)?
        } else {
            let records = self.read_impl(reader, true)?;
            let headers: Vec<String> = (0..records.first().map(|r| r.len()).unwrap_or(0))
                .map(|i| format!("col_{}", i))
                .collect();
            (headers, records)
        };

        if records.is_empty() {
            warn!("CSV file contains no data rows");
        }

        let mut root = Node::new("csv_data", NodeType::Root);

        for (row_idx, row) in records.iter().enumerate() {
            let line_num = if self.config.has_header {
                row_idx + 2
            } else {
                row_idx + 1
            };
            let record_node = self.row_to_node(row, &headers, line_num)?;
            root.add_child(record_node);
        }

        let metadata = DocumentMetadata {
            doc_type: Some("CSV".to_string()),
            ..Default::default()
        };

        Ok(Document::with_metadata(root, metadata))
    }

    /// Create an iterator for streaming CSV processing
    ///
    /// This is the preferred method for large files as it doesn't
    /// load the entire file into memory.
    ///
    /// # Example
    ///
    /// ```rust
    /// use edi_adapter_csv::CsvReader;
    /// use std::io::Cursor;
    ///
    /// let data = "name,age\nJohn,30\nJane,25";
    /// let reader = CsvReader::new();
    /// let iter = reader.read_iter(Cursor::new(data));
    ///
    /// for record in iter {
    ///     match record {
    ///         Ok(record) => println!("Record: {:?}", record),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// ```
    pub fn read_iter<R: Read>(&self, reader: R) -> CsvRecordIterator<R> {
        CsvRecordIterator::new(reader, self.config.clone(), self.schema.clone())
    }

    /// Convert a CSV row to an IR Node
    fn row_to_node(&self, row: &[String], headers: &[String], line_num: usize) -> CsvResult<Node> {
        validate_row_length(row.len(), headers.len(), line_num)?;
        let mut record_node = Node::new(format!("record_{}", line_num - 1), NodeType::Record);

        for (col_idx, (header, value)) in headers.iter().zip(row.iter()).enumerate() {
            let value = if self.is_null_value(value) {
                Value::Null
            } else {
                self.parse_value(value, col_idx, line_num, header)?
            };

            let field_node = Node::with_value(header.clone(), NodeType::Field, value);
            record_node.add_child(field_node);
        }

        Ok(record_node)
    }

    /// Check if a value represents null
    fn is_null_value(&self, value: &str) -> bool {
        match &self.config.null_representation {
            NullRepresentation::NullString => value == "NULL",
            NullRepresentation::BackslashN => value == "\\N",
            NullRepresentation::Custom(s) => value == s.as_str(),
            NullRepresentation::EmptyString => value.is_empty(),
        }
    }

    /// Parse a string value to the appropriate IR Value type
    fn parse_value(
        &self,
        value: &str,
        col_idx: usize,
        line: usize,
        column: &str,
    ) -> CsvResult<Value> {
        // Use schema type if available
        if let Some(schema) = &self.schema {
            if let Some(col_def) = schema.get_column_by_index(col_idx) {
                return self.parse_typed_value(value, col_def.column_type, line, column);
            }
        }

        // Default: try to infer type
        Ok(self.infer_and_parse_value(value))
    }

    /// Parse a value according to a specific column type
    fn parse_typed_value(
        &self,
        value: &str,
        col_type: ColumnType,
        line: usize,
        column: &str,
    ) -> CsvResult<Value> {
        match col_type {
            ColumnType::Integer => value.parse::<i64>().map(Value::Integer).map_err(|_| {
                CsvError::conversion(line, column, format!("'{}' is not a valid integer", value))
            }),
            ColumnType::Decimal => value.parse::<f64>().map(Value::Decimal).map_err(|_| {
                CsvError::conversion(line, column, format!("'{}' is not a valid decimal", value))
            }),
            ColumnType::Boolean => self
                .parse_boolean(value)
                .map(Value::Boolean)
                .ok_or_else(|| {
                    CsvError::conversion(
                        line,
                        column,
                        format!("'{}' is not a valid boolean", value),
                    )
                }),
            ColumnType::Date => Ok(Value::Date(value.to_string())),
            ColumnType::DateTime => Ok(Value::DateTime(value.to_string())),
            ColumnType::Time => Ok(Value::Time(value.to_string())),
            ColumnType::String => Ok(Value::String(value.to_string())),
        }
    }

    /// Infer and parse a value without schema
    fn infer_and_parse_value(&self, value: &str) -> Value {
        // Try integer first
        if let Ok(int_val) = value.parse::<i64>() {
            return Value::Integer(int_val);
        }
        // Then decimal
        if let Ok(float_val) = value.parse::<f64>() {
            return Value::Decimal(float_val);
        }
        // Then boolean
        if let Some(bool_val) = self.parse_boolean(value) {
            return Value::Boolean(bool_val);
        }
        // Default to string
        Value::String(value.to_string())
    }

    /// Parse a boolean value (accepts various formats)
    fn parse_boolean(&self, value: &str) -> Option<bool> {
        match value.to_lowercase().as_str() {
            "true" | "yes" | "y" | "1" | "t" => Some(true),
            "false" | "no" | "n" | "0" | "f" => Some(false),
            _ => None,
        }
    }
}

impl Default for CsvReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator for streaming CSV record processing
pub struct CsvRecordIterator<R: Read> {
    csv_reader: csv::Reader<R>,
    headers: Vec<String>,
    _config: CsvConfig,
    _schema: Option<CsvSchema>,
    current_line: usize,
    has_header: bool,
}

impl<R: Read> CsvRecordIterator<R> {
    /// Create a new CSV record iterator
    fn new(reader: R, config: CsvConfig, schema: Option<CsvSchema>) -> Self {
        let has_header = config.has_header;

        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(has_header)
            .flexible(true)
            .delimiter(config.delimiter_u8())
            .quote(config.quote_char_u8())
            .from_reader(reader);

        let headers: Vec<String> = if has_header {
            csv_reader
                .headers()
                .map(|h| h.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default()
        } else {
            // We'll infer headers from the first row if needed
            Vec::new()
        };

        Self {
            csv_reader,
            headers,
            _config: config,
            _schema: schema,
            current_line: if has_header { 2 } else { 1 },
            has_header,
        }
    }

    /// Get the headers read from the CSV
    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    /// Get the current line number
    pub fn line_number(&self) -> usize {
        self.current_line
    }
}

/// Record type for streaming iteration
#[derive(Debug, Clone)]
pub struct CsvRecord {
    /// The row data as string values
    pub values: Vec<String>,
    /// Line number in the source file (1-indexed)
    pub line_number: usize,
    /// Column headers
    pub headers: Vec<String>,
}

impl CsvRecord {
    /// Get a value by column index
    pub fn get(&self, index: usize) -> Option<&str> {
        self.values.get(index).map(|s| s.as_str())
    }

    /// Get a value by column name
    pub fn get_by_name(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .position(|h| h == name)
            .and_then(|idx| self.get(idx))
    }

    /// Convert to IR Node
    pub fn to_node(
        &self,
        schema: Option<&CsvSchema>,
        null_rep: &NullRepresentation,
    ) -> CsvResult<Node> {
        validate_row_length(self.values.len(), self.headers.len(), self.line_number)?;
        let mut record_node =
            Node::new(format!("record_{}", self.line_number - 1), NodeType::Record);

        for (col_idx, (header, value)) in self.headers.iter().zip(self.values.iter()).enumerate() {
            let parsed_value = if is_null_string(value, null_rep) {
                Value::Null
            } else {
                parse_value_with_schema(value, schema, col_idx, self.line_number, header)?
            };

            let field_node = Node::with_value(header.clone(), NodeType::Field, parsed_value);
            record_node.add_child(field_node);
        }

        Ok(record_node)
    }
}

fn validate_row_length(
    actual_columns: usize,
    expected_columns: usize,
    line_num: usize,
) -> CsvResult<()> {
    if actual_columns == expected_columns {
        return Ok(());
    }

    let mismatch_kind = if actual_columns < expected_columns {
        RowLengthMismatchKind::Missing
    } else {
        RowLengthMismatchKind::Extra
    };

    Err(CsvError::row_length_mismatch(
        line_num,
        expected_columns,
        actual_columns,
        mismatch_kind,
    ))
}

impl<R: Read> Iterator for CsvRecordIterator<R> {
    type Item = CsvResult<CsvRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.csv_reader.records().next() {
            Some(Ok(record)) => {
                let line_number = self.current_line;
                let values: Vec<String> = record.iter().map(|s| s.to_string()).collect();

                // Infer headers from first row if no header row
                if self.headers.is_empty() && !self.has_header {
                    self.headers = (0..values.len()).map(|i| format!("col_{}", i)).collect();
                }

                if !self.headers.is_empty()
                    && let Err(err) =
                        validate_row_length(values.len(), self.headers.len(), line_number)
                {
                    self.current_line += 1;
                    return Some(Err(err));
                }

                let result = CsvRecord {
                    values,
                    line_number,
                    headers: self.headers.clone(),
                };

                self.current_line += 1;
                Some(Ok(result))
            }
            Some(Err(e)) => {
                self.current_line += 1;
                Some(Err(CsvError::read_at(self.current_line - 1, e.to_string())))
            }
            None => None,
        }
    }
}

/// Check if a value represents null
fn is_null_string(value: &str, null_rep: &NullRepresentation) -> bool {
    match null_rep {
        NullRepresentation::NullString => value == "NULL",
        NullRepresentation::BackslashN => value == "\\N",
        NullRepresentation::Custom(s) => value == s.as_str(),
        NullRepresentation::EmptyString => value.is_empty(),
    }
}

/// Parse a value with optional schema type
fn parse_value_with_schema(
    value: &str,
    schema: Option<&CsvSchema>,
    col_idx: usize,
    line: usize,
    column: &str,
) -> CsvResult<Value> {
    if let Some(s) = schema {
        if let Some(col_def) = s.get_column_by_index(col_idx) {
            return match col_def.column_type {
                ColumnType::Integer => value.parse::<i64>().map(Value::Integer).map_err(|_| {
                    CsvError::conversion(
                        line,
                        column,
                        format!("'{}' is not a valid integer", value),
                    )
                }),
                ColumnType::Decimal => value.parse::<f64>().map(Value::Decimal).map_err(|_| {
                    CsvError::conversion(
                        line,
                        column,
                        format!("'{}' is not a valid decimal", value),
                    )
                }),
                ColumnType::Boolean => parse_boolean(value).map(Value::Boolean).ok_or_else(|| {
                    CsvError::conversion(
                        line,
                        column,
                        format!("'{}' is not a valid boolean", value),
                    )
                }),
                ColumnType::Date => Ok(Value::Date(value.to_string())),
                ColumnType::DateTime => Ok(Value::DateTime(value.to_string())),
                ColumnType::Time => Ok(Value::Time(value.to_string())),
                ColumnType::String => Ok(Value::String(value.to_string())),
            };
        }
    }

    // Default type inference
    if let Ok(int_val) = value.parse::<i64>() {
        Ok(Value::Integer(int_val))
    } else if let Ok(float_val) = value.parse::<f64>() {
        Ok(Value::Decimal(float_val))
    } else if let Some(bool_val) = parse_boolean(value) {
        Ok(Value::Boolean(bool_val))
    } else {
        Ok(Value::String(value.to_string()))
    }
}

/// Parse a boolean value
fn parse_boolean(value: &str) -> Option<bool> {
    match value.to_lowercase().as_str() {
        "true" | "yes" | "y" | "1" | "t" => Some(true),
        "false" | "no" | "n" | "0" | "f" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnDef, CsvSchema};
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
    fn test_read_to_ir_errors_on_missing_columns() {
        let data = "name,age,city\nJohn,30";
        let reader = CsvReader::new();
        let err = reader.read_to_ir(Cursor::new(data)).unwrap_err();

        assert!(matches!(
            err,
            CsvError::RowLengthMismatch {
                line: 2,
                expected: 3,
                actual: 2,
                kind: RowLengthMismatchKind::Missing,
            }
        ));
    }

    #[test]
    fn test_read_to_ir_errors_on_extra_columns() {
        let data = "name,age\nJohn,30,unexpected";
        let reader = CsvReader::new();
        let err = reader.read_to_ir(Cursor::new(data)).unwrap_err();

        assert!(matches!(
            err,
            CsvError::RowLengthMismatch {
                line: 2,
                expected: 2,
                actual: 3,
                kind: RowLengthMismatchKind::Extra,
            }
        ));
    }

    #[test]
    fn test_read_with_schema_type_conversion() {
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

    #[test]
    fn test_read_iter_streaming() {
        let data = "name,age\nJohn,30\nJane,25\nBob,35";
        let reader = CsvReader::new();
        let iter = reader.read_iter(Cursor::new(data));

        let records: Vec<_> = iter.collect::<Result<_, _>>().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].values, vec!["John", "30"]);
        assert_eq!(records[0].line_number, 2);
        assert_eq!(records[1].line_number, 3);
        assert_eq!(records[2].line_number, 4);
    }

    #[test]
    fn test_read_iter_no_header() {
        let data = "John,30\nJane,25";
        let reader = CsvReader::new().has_header(false);
        let iter = reader.read_iter(Cursor::new(data));

        let records: Vec<_> = iter.collect::<Result<_, _>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].headers, vec!["col_0", "col_1"]);
    }

    #[test]
    fn test_read_iter_reports_row_length_mismatch() {
        let data = "name,age\nJohn,30\nJane,25,unexpected";
        let reader = CsvReader::new();
        let mut iter = reader.read_iter(Cursor::new(data));

        let _first = iter.next().unwrap().unwrap();
        let second = iter.next().unwrap().unwrap_err();
        assert!(matches!(
            second,
            CsvError::RowLengthMismatch {
                line: 3,
                expected: 2,
                actual: 3,
                kind: RowLengthMismatchKind::Extra,
            }
        ));
    }

    #[test]
    fn test_csv_record_get_methods() {
        let record = CsvRecord {
            values: vec!["John".to_string(), "30".to_string()],
            line_number: 2,
            headers: vec!["name".to_string(), "age".to_string()],
        };

        assert_eq!(record.get(0), Some("John"));
        assert_eq!(record.get(1), Some("30"));
        assert_eq!(record.get(2), None);

        assert_eq!(record.get_by_name("name"), Some("John"));
        assert_eq!(record.get_by_name("age"), Some("30"));
        assert_eq!(record.get_by_name("nonexistent"), None);
    }

    #[test]
    fn test_null_value_handling() {
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("value").with_type(ColumnType::Integer));

        // Test empty string as null
        let data = "name,value\nJohn,\nJane,42";
        let reader = CsvReader::new().with_schema(schema.clone());
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        let first_record = &doc.root.children[0];
        assert!(matches!(first_record.children[1].value, Some(Value::Null)));

        // Test NULL string
        let data = "name,value\nJohn,NULL\nJane,42";
        let config = CsvConfig::new().null_representation(NullRepresentation::NullString);
        let reader = CsvReader::new().with_config(config).with_schema(schema);
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        let first_record = &doc.root.children[0];
        assert!(matches!(first_record.children[1].value, Some(Value::Null)));
    }

    #[test]
    fn test_empty_string_not_null_when_null_representation_is_not_empty_string() {
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("value"));

        let config = CsvConfig::new().null_representation(NullRepresentation::NullString);
        let reader = CsvReader::new().with_config(config).with_schema(schema);
        let data = "name,value\nJohn,";
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        assert!(matches!(
            doc.root.children[0].children[1].value,
            Some(Value::String(ref value)) if value.is_empty()
        ));
    }

    #[test]
    fn test_record_to_node_keeps_empty_string_when_null_representation_is_null_string() {
        let record = CsvRecord {
            values: vec!["".to_string()],
            line_number: 2,
            headers: vec!["value".to_string()],
        };

        let node = record
            .to_node(None, &NullRepresentation::NullString)
            .unwrap();

        assert!(matches!(
            node.children[0].value,
            Some(Value::String(ref value)) if value.is_empty()
        ));
    }

    #[test]
    fn test_boolean_parsing() {
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("active").with_type(ColumnType::Boolean));

        let data = "name,active\nJohn,true\nJane,YES\nBob,1\nAlice,False\nCharlie,no\nDavid,0";
        let reader = CsvReader::new().with_schema(schema);
        let doc = reader.read_to_ir(Cursor::new(data)).unwrap();

        assert!(matches!(
            doc.root.children[0].children[1].value,
            Some(Value::Boolean(true))
        )); // true
        assert!(matches!(
            doc.root.children[1].children[1].value,
            Some(Value::Boolean(true))
        )); // YES
        assert!(matches!(
            doc.root.children[2].children[1].value,
            Some(Value::Boolean(true))
        )); // 1
        assert!(matches!(
            doc.root.children[3].children[1].value,
            Some(Value::Boolean(false))
        )); // False
        assert!(matches!(
            doc.root.children[4].children[1].value,
            Some(Value::Boolean(false))
        )); // no
        assert!(matches!(
            doc.root.children[5].children[1].value,
            Some(Value::Boolean(false))
        )); // 0
    }

    #[test]
    fn test_error_with_line_number() {
        let schema = CsvSchema::new()
            .with_header()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("count").with_type(ColumnType::Integer));

        // Invalid integer on line 4 (data line 3)
        let data = "name,count\nJohn,10\nJane,20\nBob,invalid";
        let reader = CsvReader::new().with_schema(schema);
        let result = reader.read_to_ir(Cursor::new(data));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("line 4"));
        assert!(err.to_string().contains("'invalid' is not a valid integer"));
    }

    #[test]
    fn test_config_integration() {
        let config = CsvConfig::new()
            .delimiter(';')
            .quote_char('\'')
            .without_header();

        let data = "John;30\nJane;25";
        let reader = CsvReader::new().with_config(config);
        let records = reader.read(Cursor::new(data)).unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["John", "30"]);
    }

    #[test]
    fn test_read_with_headers_keeps_strict_row_length_behavior() {
        let data = "name,age\nJohn,30,unexpected";
        let reader = CsvReader::new();
        let err = reader.read_with_headers(Cursor::new(data)).unwrap_err();

        assert!(matches!(err, CsvError::Read { line: 2, .. }));
    }

    #[test]
    fn test_record_to_node() {
        let record = CsvRecord {
            values: vec!["Test".to_string(), "42".to_string()],
            line_number: 2,
            headers: vec!["name".to_string(), "count".to_string()],
        };

        let schema = CsvSchema::new()
            .add_column(ColumnDef::new("name"))
            .add_column(ColumnDef::new("count").with_type(ColumnType::Integer));

        let node = record
            .to_node(Some(&schema), &NullRepresentation::EmptyString)
            .unwrap();

        assert_eq!(node.node_type, NodeType::Record);
        assert_eq!(node.children.len(), 2);
        assert_eq!(node.children[0].name, "name");
        assert!(matches!(node.children[1].value, Some(Value::Integer(42))));
    }

    #[test]
    fn test_record_to_node_errors_on_length_mismatch() {
        let record = CsvRecord {
            values: vec!["Test".to_string()],
            line_number: 2,
            headers: vec!["name".to_string(), "count".to_string()],
        };

        let err = record
            .to_node(None, &NullRepresentation::EmptyString)
            .unwrap_err();
        assert!(matches!(
            err,
            CsvError::RowLengthMismatch {
                line: 2,
                expected: 2,
                actual: 1,
                kind: RowLengthMismatchKind::Missing,
            }
        ));
    }

    #[test]
    fn test_type_inference() {
        let reader = CsvReader::new();

        // Integer
        let val = reader.infer_and_parse_value("123");
        assert!(matches!(val, Value::Integer(123)));

        // Decimal
        let val = reader.infer_and_parse_value("123.45");
        assert!(matches!(val, Value::Decimal(_)));

        // Boolean
        let val = reader.infer_and_parse_value("true");
        assert!(matches!(val, Value::Boolean(true)));

        // String
        let val = reader.infer_and_parse_value("hello");
        assert!(matches!(val, Value::String(s) if s == "hello"));
    }
}

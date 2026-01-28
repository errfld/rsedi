//! CSV writer

/// Writer for CSV files
pub struct CsvWriter;

impl CsvWriter {
    /// Create a new CSV writer
    pub fn new() -> Self {
        Self
    }
}

impl Default for CsvWriter {
    fn default() -> Self {
        Self::new()
    }
}

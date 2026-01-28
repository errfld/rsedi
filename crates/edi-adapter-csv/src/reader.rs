//! CSV reader

/// Reader for CSV files
pub struct CsvReader;

impl CsvReader {
    /// Create a new CSV reader
    pub fn new() -> Self {
        Self
    }
}

impl Default for CsvReader {
    fn default() -> Self {
        Self::new()
    }
}

//! CSV configuration options

/// Configuration for CSV reading and writing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvConfig {
    /// Field delimiter character (default: comma)
    pub delimiter: char,
    /// Quote character for fields containing special characters (default: double quote)
    pub quote_char: char,
    /// Escape character for escaping quotes (default: same as quote_char)
    pub escape_char: Option<char>,
    /// Whether the CSV has a header row (default: true)
    pub has_header: bool,
    /// Line ending style (default: platform native)
    pub line_ending: LineEnding,
    /// Text encoding (default: UTF-8)
    pub encoding: Encoding,
    /// How to represent null values in output (default: empty string)
    pub null_representation: NullRepresentation,
    /// Record terminator (default: CRLF for writing)
    pub record_terminator: RecordTerminator,
}

/// Line ending options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineEnding {
    /// Unix-style line feed (\n)
    LF,
    /// Windows-style carriage return + line feed (\r\n)
    CRLF,
    /// Platform native
    Native,
}

impl LineEnding {
    /// Get the line ending as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            LineEnding::LF => "\n",
            LineEnding::CRLF => "\r\n",
            LineEnding::Native => {
                if cfg!(windows) {
                    "\r\n"
                } else {
                    "\n"
                }
            }
        }
    }
}

/// Text encoding options
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// UTF-8 encoding (default)
    Utf8,
    /// ASCII encoding (7-bit)
    Ascii,
    // Note: Additional encodings can be added as needed (ISO-8859-1, etc.)
}

/// How to represent null values in CSV output
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NullRepresentation {
    /// Empty string (default)
    EmptyString,
    /// The string "NULL"
    NullString,
    /// The string "\\N"
    BackslashN,
    /// Custom string representation
    Custom(String),
}

/// Record terminator for writing CSV
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordTerminator {
    /// CRLF (Windows-style, default for RFC 4180)
    CRLF,
    /// LF (Unix-style)
    LF,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            delimiter: ',',
            quote_char: '"',
            escape_char: None, // Uses doubling by default
            has_header: true,
            line_ending: LineEnding::Native,
            encoding: Encoding::Utf8,
            null_representation: NullRepresentation::EmptyString,
            record_terminator: RecordTerminator::CRLF,
        }
    }
}

impl CsvConfig {
    /// Create a new configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the delimiter character
    pub fn delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set the quote character
    pub fn quote_char(mut self, quote_char: char) -> Self {
        self.quote_char = quote_char;
        self
    }

    /// Set the escape character
    pub fn escape_char(mut self, escape_char: char) -> Self {
        self.escape_char = Some(escape_char);
        self
    }

    /// Configure header presence
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Disable header row
    pub fn without_header(mut self) -> Self {
        self.has_header = false;
        self
    }

    /// Set line ending
    pub fn line_ending(mut self, line_ending: LineEnding) -> Self {
        self.line_ending = line_ending;
        self
    }

    /// Set encoding
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Set null representation
    pub fn null_representation(mut self, null_rep: NullRepresentation) -> Self {
        self.null_representation = null_rep;
        self
    }

    /// Set record terminator
    pub fn record_terminator(mut self, terminator: RecordTerminator) -> Self {
        self.record_terminator = terminator;
        self
    }

    /// Convert delimiter to u8 for csv crate
    pub fn delimiter_u8(&self) -> u8 {
        self.delimiter as u8
    }

    /// Convert quote char to u8 for csv crate
    pub fn quote_char_u8(&self) -> u8 {
        self.quote_char as u8
    }

    /// Get escape character as u8, or use quote char if not set
    pub fn escape_char_u8(&self) -> u8 {
        self.escape_char
            .map(|c| c as u8)
            .unwrap_or(self.quote_char as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CsvConfig::default();
        assert_eq!(config.delimiter, ',');
        assert_eq!(config.quote_char, '"');
        assert_eq!(config.escape_char, None);
        assert!(config.has_header);
        assert_eq!(config.encoding, Encoding::Utf8);
        assert_eq!(config.null_representation, NullRepresentation::EmptyString);
        assert_eq!(config.record_terminator, RecordTerminator::CRLF);
    }

    #[test]
    fn test_config_builder() {
        let config = CsvConfig::new()
            .delimiter(';')
            .quote_char('\'')
            .escape_char('\\')
            .without_header()
            .line_ending(LineEnding::LF)
            .encoding(Encoding::Ascii)
            .null_representation(NullRepresentation::NullString)
            .record_terminator(RecordTerminator::LF);

        assert_eq!(config.delimiter, ';');
        assert_eq!(config.quote_char, '\'');
        assert_eq!(config.escape_char, Some('\\'));
        assert!(!config.has_header);
        assert_eq!(config.line_ending, LineEnding::LF);
        assert_eq!(config.encoding, Encoding::Ascii);
        assert_eq!(config.null_representation, NullRepresentation::NullString);
        assert_eq!(config.record_terminator, RecordTerminator::LF);
    }

    #[test]
    fn test_line_ending_as_str() {
        assert_eq!(LineEnding::LF.as_str(), "\n");
        assert_eq!(LineEnding::CRLF.as_str(), "\r\n");
        // Native depends on platform
        let expected = if cfg!(windows) { "\r\n" } else { "\n" };
        assert_eq!(LineEnding::Native.as_str(), expected);
    }

    #[test]
    fn test_null_representation() {
        assert_eq!(
            NullRepresentation::EmptyString,
            NullRepresentation::EmptyString
        );
        assert_eq!(
            NullRepresentation::NullString,
            NullRepresentation::NullString
        );
        assert_eq!(
            NullRepresentation::BackslashN,
            NullRepresentation::BackslashN
        );
        assert_eq!(
            NullRepresentation::Custom("NA".to_string()),
            NullRepresentation::Custom("NA".to_string())
        );
    }

    #[test]
    fn test_config_conversions() {
        let config = CsvConfig::new()
            .delimiter('\t')
            .quote_char('\'')
            .escape_char('\\');

        assert_eq!(config.delimiter_u8(), b'\t');
        assert_eq!(config.quote_char_u8(), b'\'');
        assert_eq!(config.escape_char_u8(), b'\\');
    }

    #[test]
    fn test_escape_char_fallback() {
        let config = CsvConfig::new(); // No escape char set
        assert_eq!(config.escape_char_u8(), b'"'); // Falls back to quote char
    }
}

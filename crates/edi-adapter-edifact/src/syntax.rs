//! EDIFACT syntax definitions and delimiter handling
//!
//! This module handles the service string advice (UNA) and default
//! separators used in EDIFACT documents.

/// Default EDIFACT separators (when no UNA is present)
pub const DEFAULT_COMPONENT_SEPARATOR: u8 = b':';
pub const DEFAULT_ELEMENT_SEPARATOR: u8 = b'+';
pub const DEFAULT_DECIMAL_POINT: u8 = b'.';
pub const DEFAULT_RELEASE_CHARACTER: u8 = b'?';
pub const DEFAULT_SEGMENT_TERMINATOR: u8 = b'\'';

/// Separators used for parsing EDIFACT
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Separators {
    /// Component separator (default ':')
    pub component: u8,
    /// Element separator (default '+')
    pub element: u8,
    /// Decimal point (default '.')
    pub decimal: u8,
    /// Release character (default '?')
    pub release: u8,
    /// Segment terminator (default '\'')
    pub segment: u8,
}

impl Default for Separators {
    fn default() -> Self {
        Self {
            component: DEFAULT_COMPONENT_SEPARATOR,
            element: DEFAULT_ELEMENT_SEPARATOR,
            decimal: DEFAULT_DECIMAL_POINT,
            release: DEFAULT_RELEASE_CHARACTER,
            segment: DEFAULT_SEGMENT_TERMINATOR,
        }
    }
}

impl Separators {
    /// Parse separators from a UNA segment
    /// UNA format: UNA:+.? '
    /// Positions:  012345678
    ///              ^^^^ ^  (separators at positions 3,4,5,6,8)
    pub fn from_una(una: &[u8]) -> Option<Self> {
        if una.len() < 9 || &una[0..3] != b"UNA" {
            return None;
        }

        Some(Self {
            component: una[3],
            element: una[4],
            decimal: una[5],
            release: una[6],
            // Position 7 is reserved (space)
            segment: una[8],
        })
    }

    /// Create a UNA segment from these separators
    pub fn to_una(&self) -> Vec<u8> {
        vec![
            b'U',
            b'N',
            b'A',
            self.component,
            self.element,
            self.decimal,
            self.release,
            b' ', // reserved
            self.segment,
        ]
    }

    /// Check if a byte is a special character (needs escaping)
    pub fn is_special(&self, byte: u8) -> bool {
        byte == self.component
            || byte == self.element
            || byte == self.segment
            || byte == self.release
    }
}

/// A buffer for reading EDIFACT data with proper release character handling
pub struct SyntaxBuffer<'a> {
    /// The underlying data buffer
    pub data: &'a [u8],
    /// Current position in the buffer
    pub pos: usize,
    /// The separators used for parsing
    pub separators: Separators,
}

impl<'a> SyntaxBuffer<'a> {
    /// Create a new syntax buffer with default separators
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            separators: Separators::default(),
        }
    }

    /// Create a new syntax buffer with custom separators
    pub fn with_separators(data: &'a [u8], separators: Separators) -> Self {
        Self {
            data,
            pos: 0,
            separators,
        }
    }

    /// Update separators (e.g., after parsing UNA)
    pub fn set_separators(&mut self, separators: Separators) {
        self.separators = separators;
    }

    /// Get current position
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Check if we've reached the end
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// Peek at the next byte without consuming it
    pub fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    /// Read the next byte
    pub fn next_byte(&mut self) -> Option<u8> {
        let byte = self.data.get(self.pos).copied();
        if byte.is_some() {
            self.pos += 1;
        }
        byte
    }

    /// Read until a delimiter, handling release characters
    /// Returns the value and the delimiter that terminated it
    pub fn read_until_delimiter(&mut self) -> (Vec<u8>, Option<u8>) {
        let mut result = Vec::new();
        let mut released = false;

        while let Some(byte) = self.peek() {
            if released {
                // Previous char was release char, this char is literal
                result.push(byte);
                released = false;
                self.pos += 1;
            } else if byte == self.separators.release {
                // Release character - don't include it, mark next as literal
                released = true;
                self.pos += 1;
            } else if byte == self.separators.component
                || byte == self.separators.element
                || byte == self.separators.segment
            {
                // Delimiter found
                self.pos += 1;
                return (result, Some(byte));
            } else {
                result.push(byte);
                self.pos += 1;
            }
        }

        (result, None)
    }

    /// Read a segment tag (3 uppercase letters)
    pub fn read_tag(&mut self) -> Option<[u8; 3]> {
        if self.pos + 3 > self.data.len() {
            return None;
        }

        let tag = &self.data[self.pos..self.pos + 3];
        // Validate tag format (3 uppercase letters or digits)
        if tag.iter().all(|&b| b.is_ascii_alphanumeric()) {
            self.pos += 3;
            Some([tag[0], tag[1], tag[2]])
        } else {
            None
        }
    }

    /// Skip whitespace (typically only space after UNA)
    pub fn skip_whitespace(&mut self) {
        while let Some(b' ') = self.peek() {
            self.pos += 1;
        }
    }

    /// Get line and column for current position
    pub fn line_column(&self) -> (usize, usize) {
        let mut line = 1;
        let mut col = 1;

        for i in 0..self.pos.min(self.data.len()) {
            if self.data[i] == b'\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }

        (line, col)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_separators() {
        let sep = Separators::default();
        assert_eq!(sep.component, b':');
        assert_eq!(sep.element, b'+');
        assert_eq!(sep.decimal, b'.');
        assert_eq!(sep.release, b'?');
        assert_eq!(sep.segment, b'\'');
    }

    #[test]
    fn test_una_parsing() {
        let una = b"UNA:+.? '";
        let sep = Separators::from_una(una).unwrap();
        assert_eq!(sep.component, b':');
        assert_eq!(sep.element, b'+');
        assert_eq!(sep.decimal, b'.');
        assert_eq!(sep.release, b'?');
        assert_eq!(sep.segment, b'\'');
    }

    #[test]
    fn test_una_custom_separators() {
        let una = b"UNA*=_# ~";
        let sep = Separators::from_una(una).unwrap();
        assert_eq!(sep.component, b'*');
        assert_eq!(sep.element, b'=');
        assert_eq!(sep.decimal, b'_');
        assert_eq!(sep.release, b'#',);
        assert_eq!(sep.segment, b'~');
    }

    #[test]
    fn test_release_character_handling() {
        let data = b"ABC?+DEF+GHI'";
        let mut buf = SyntaxBuffer::new(data);

        let (value, delim) = buf.read_until_delimiter();
        assert_eq!(value, b"ABC+DEF");
        assert_eq!(delim, Some(b'+'));

        let (value, delim) = buf.read_until_delimiter();
        assert_eq!(value, b"GHI");
        assert_eq!(delim, Some(b'\''));
    }

    #[test]
    fn test_double_release_character() {
        // ?? represents literal ?
        let data = b"ABC??DEF'";
        let mut buf = SyntaxBuffer::new(data);

        let (value, delim) = buf.read_until_delimiter();
        assert_eq!(value, b"ABC?DEF");
        assert_eq!(delim, Some(b'\''));
    }

    #[test]
    fn test_read_tag() {
        let data = b"UNB+...";
        let mut buf = SyntaxBuffer::new(data);

        let tag = buf.read_tag().unwrap();
        assert_eq!(tag, [b'U', b'N', b'B']);
    }
}

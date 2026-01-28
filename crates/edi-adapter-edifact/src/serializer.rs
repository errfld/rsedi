//! EDIFACT serializer

/// Serializer for EDIFACT documents
pub struct EdifactSerializer;

impl EdifactSerializer {
    /// Create a new EDIFACT serializer
    pub fn new() -> Self {
        Self
    }
}

impl Default for EdifactSerializer {
    fn default() -> Self {
        Self::new()
    }
}

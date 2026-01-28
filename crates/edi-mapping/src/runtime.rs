//! Mapping runtime

/// Runtime for executing mappings
pub struct MappingRuntime;

impl MappingRuntime {
    /// Create a new mapping runtime
    pub fn new() -> Self {
        Self
    }
}

impl Default for MappingRuntime {
    fn default() -> Self {
        Self::new()
    }
}

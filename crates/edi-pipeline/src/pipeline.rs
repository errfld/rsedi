//! Pipeline orchestration

/// Main pipeline for processing EDI files
pub struct Pipeline;

impl Pipeline {
    /// Create a new pipeline
    pub fn new() -> Self {
        Self
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

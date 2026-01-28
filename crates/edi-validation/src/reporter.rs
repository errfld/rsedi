//! Validation reporter

/// Reports validation results
pub struct ValidationReporter;

impl ValidationReporter {
    /// Create a new validation reporter
    pub fn new() -> Self {
        Self
    }
}

impl Default for ValidationReporter {
    fn default() -> Self {
        Self::new()
    }
}

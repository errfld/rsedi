//! Database connection

/// Database connection handle
pub struct DbConnection;

impl DbConnection {
    /// Create a new database connection
    pub fn new() -> Self {
        Self
    }
}

impl Default for DbConnection {
    fn default() -> Self {
        Self::new()
    }
}

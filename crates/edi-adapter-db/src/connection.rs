//! Database connection and transaction primitives.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::schema::Row;
use crate::{Error, Result};

/// Connection behavior for adapter tests/runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub max_connections: usize,
    pub timeout_ms: u64,
    pub retry_attempts: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_connections: 8,
            timeout_ms: 5_000,
            retry_attempts: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DatabaseState {
    pub tables: HashMap<String, Vec<Row>>,
}

/// Database connection handle.
#[derive(Debug, Clone)]
pub struct DbConnection {
    state: Arc<RwLock<DatabaseState>>,
    connected: Arc<RwLock<bool>>,
    config: ConnectionConfig,
}

impl DbConnection {
    /// Create a connection with default config.
    pub fn new() -> Self {
        Self::with_config(ConnectionConfig::default())
    }

    /// Create a connection with explicit config.
    pub fn with_config(config: ConnectionConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(DatabaseState::default())),
            connected: Arc::new(RwLock::new(false)),
            config,
        }
    }

    pub fn config(&self) -> ConnectionConfig {
        self.config
    }

    /// Open connection (in-memory backend for adapter tests).
    pub async fn connect(&self) -> Result<()> {
        let attempts = self.config.retry_attempts + 1;
        for attempt in 0..attempts {
            if self.config.max_connections > 0 && self.config.timeout_ms > 0 {
                *self.connected.write().await = true;
                return Ok(());
            }

            if attempt + 1 == attempts {
                break;
            }

            tokio::task::yield_now().await;
        }

        Err(Error::Connection(format!(
            "Failed to connect after {attempts} attempt(s): invalid configuration"
        )))
    }

    pub async fn close(&self) {
        *self.connected.write().await = false;
    }

    pub async fn is_connected(&self) -> bool {
        *self.connected.read().await
    }

    pub async fn begin_transaction(&self) -> Result<DbTransaction> {
        self.ensure_connected().await?;
        let working_state = self.snapshot_state().await;

        Ok(DbTransaction {
            connection: self.clone(),
            working_state,
            active: true,
        })
    }

    pub(crate) async fn insert_row(&self, table: &str, row: Row) -> Result<()> {
        self.ensure_connected().await?;
        let mut state = self.state.write().await;
        state.tables.entry(table.to_string()).or_default().push(row);
        Ok(())
    }

    pub(crate) async fn select_rows(
        &self,
        table: &str,
        filter: Option<&Row>,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Row>> {
        self.ensure_connected().await?;
        let state = self.state.read().await;
        let rows = state
            .tables
            .get(table)
            .ok_or_else(|| Error::Query(format!("Table '{table}' not found")))?;

        let mut filtered: Vec<Row> = rows
            .iter()
            .filter(|row| filter.map(|f| row_matches_filter(row, f)).unwrap_or(true))
            .cloned()
            .collect();

        if offset > 0 {
            if offset >= filtered.len() {
                return Ok(Vec::new());
            }
            filtered.drain(..offset);
        }

        if let Some(max) = limit {
            filtered.truncate(max);
        }

        Ok(filtered)
    }

    pub(crate) async fn update_rows(
        &self,
        table: &str,
        filter: &Row,
        updates: &Row,
    ) -> Result<usize> {
        self.ensure_connected().await?;
        let mut state = self.state.write().await;
        let rows = state
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::Query(format!("Table '{table}' not found")))?;

        let mut updated = 0usize;
        for row in rows {
            if row_matches_filter(row, filter) {
                for (column, value) in updates {
                    row.insert(column.clone(), value.clone());
                }
                updated += 1;
            }
        }

        Ok(updated)
    }

    pub(crate) async fn upsert_row(&self, table: &str, key_column: &str, row: Row) -> Result<()> {
        self.ensure_connected().await?;
        let key_value = row
            .get(key_column)
            .cloned()
            .ok_or_else(|| Error::Query(format!("Upsert key column '{key_column}' is missing")))?;

        let mut state = self.state.write().await;
        let rows = state.tables.entry(table.to_string()).or_default();

        if let Some(existing) = rows
            .iter_mut()
            .find(|candidate| candidate.get(key_column) == Some(&key_value))
        {
            *existing = row;
        } else {
            rows.push(row);
        }

        Ok(())
    }

    pub async fn table_row_count(&self, table: &str) -> Result<usize> {
        self.ensure_connected().await?;
        let state = self.state.read().await;
        Ok(state.tables.get(table).map(Vec::len).unwrap_or(0))
    }

    pub(crate) async fn snapshot_state(&self) -> DatabaseState {
        self.state.read().await.clone()
    }

    pub(crate) async fn replace_state(&self, state: DatabaseState) -> Result<()> {
        self.ensure_connected().await?;
        *self.state.write().await = state;
        Ok(())
    }

    async fn ensure_connected(&self) -> Result<()> {
        if !self.is_connected().await {
            return Err(Error::Connection("Database is not connected".to_string()));
        }
        Ok(())
    }
}

impl Default for DbConnection {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction against in-memory state; commit applies staged state.
#[derive(Debug)]
pub struct DbTransaction {
    connection: DbConnection,
    working_state: DatabaseState,
    active: bool,
}

impl DbTransaction {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn insert_row(&mut self, table: &str, row: Row) -> Result<()> {
        self.ensure_active()?;
        self.working_state
            .tables
            .entry(table.to_string())
            .or_default()
            .push(row);
        Ok(())
    }

    pub fn update_rows(&mut self, table: &str, filter: &Row, updates: &Row) -> Result<usize> {
        self.ensure_active()?;
        let rows = self
            .working_state
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::Query(format!("Table '{table}' not found")))?;

        let mut updated = 0usize;
        for row in rows {
            if row_matches_filter(row, filter) {
                for (column, value) in updates {
                    row.insert(column.clone(), value.clone());
                }
                updated += 1;
            }
        }

        Ok(updated)
    }

    pub fn upsert_row(&mut self, table: &str, key_column: &str, row: Row) -> Result<()> {
        self.ensure_active()?;
        let key_value = row
            .get(key_column)
            .cloned()
            .ok_or_else(|| Error::Query(format!("Upsert key column '{key_column}' is missing")))?;

        let rows = self
            .working_state
            .tables
            .entry(table.to_string())
            .or_default();
        if let Some(existing) = rows
            .iter_mut()
            .find(|candidate| candidate.get(key_column) == Some(&key_value))
        {
            *existing = row;
        } else {
            rows.push(row);
        }
        Ok(())
    }

    pub fn row_count(&self, table: &str) -> Result<usize> {
        self.ensure_active()?;
        Ok(self
            .working_state
            .tables
            .get(table)
            .map(Vec::len)
            .unwrap_or(0))
    }

    pub async fn commit(mut self) -> Result<()> {
        self.ensure_active()?;
        self.connection
            .replace_state(self.working_state.clone())
            .await?;
        self.active = false;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<()> {
        self.ensure_active()?;
        self.active = false;
        Ok(())
    }

    fn ensure_active(&self) -> Result<()> {
        if !self.active {
            return Err(Error::Transaction(
                "Transaction is no longer active".to_string(),
            ));
        }
        Ok(())
    }
}

fn row_matches_filter(row: &Row, filter: &Row) -> bool {
    filter
        .iter()
        .all(|(column, value)| row.get(column) == Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::DbValue;

    fn sample_row(id: i64) -> Row {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::Integer(id));
        row.insert("order_no".to_string(), DbValue::String(format!("PO-{id}")));
        row
    }

    #[tokio::test]
    async fn test_connection_creation() {
        let conn = DbConnection::new();
        assert!(!conn.is_connected().await);
        conn.connect().await.unwrap();
        assert!(conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let cfg = ConnectionConfig {
            max_connections: 32,
            timeout_ms: 10,
            retry_attempts: 0,
        };
        let conn = DbConnection::with_config(cfg);
        assert_eq!(conn.config().max_connections, 32);
        conn.connect().await.unwrap();
        assert!(conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_connection_timeout() {
        let cfg = ConnectionConfig {
            max_connections: 8,
            timeout_ms: 1,
            retry_attempts: 0,
        };
        let conn = DbConnection::with_config(cfg);
        conn.connect().await.unwrap();
        assert!(conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_connection_retry() {
        let cfg = ConnectionConfig {
            max_connections: 0,
            timeout_ms: 0,
            retry_attempts: 2,
        };
        let conn = DbConnection::with_config(cfg);
        let err = conn.connect().await.unwrap_err();
        assert!(err.to_string().contains("3 attempt(s)"));
    }

    #[tokio::test]
    async fn test_transaction_begin() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();

        let tx = conn.begin_transaction().await.unwrap();
        assert!(tx.is_active());
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();

        let mut tx = conn.begin_transaction().await.unwrap();
        tx.insert_row("orders", sample_row(1)).unwrap();
        assert_eq!(tx.row_count("orders").unwrap(), 1);
        tx.commit().await.unwrap();

        assert_eq!(conn.table_row_count("orders").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();

        let mut tx = conn.begin_transaction().await.unwrap();
        tx.insert_row("orders", sample_row(1)).unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(conn.table_row_count("orders").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_connection_close() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();
        assert!(conn.is_connected().await);

        conn.close().await;
        assert!(!conn.is_connected().await);
    }
}

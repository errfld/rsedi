//! Database connection and transaction primitives.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use libsql::{Builder, Connection as LibsqlConnection, Database, Transaction, params_from_iter};
use tokio::sync::{RwLock, Semaphore};

use crate::schema::{ColumnType, DbValue, Row, SchemaMapping, TableSchema};
use crate::sql::quote_identifier;
use crate::{Error, Result};

/// Connection behavior for adapter tests/runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub database_url: String,
    pub auth_token: Option<String>,
    pub max_connections: usize,
    pub timeout_ms: u64,
    pub retry_attempts: usize,
}

impl ConnectionConfig {
    pub fn in_memory() -> Self {
        Self {
            database_url: ":memory:".to_string(),
            auth_token: None,
            max_connections: 1,
            timeout_ms: 5_000,
            retry_attempts: 0,
        }
    }

    pub fn local(path: impl Into<String>) -> Self {
        Self {
            database_url: path.into(),
            auth_token: None,
            max_connections: 8,
            timeout_ms: 5_000,
            retry_attempts: 0,
        }
    }

    pub fn remote(url: impl Into<String>, auth_token: impl Into<String>) -> Self {
        Self {
            database_url: url.into(),
            auth_token: Some(auth_token.into()),
            max_connections: 8,
            timeout_ms: 5_000,
            retry_attempts: 0,
        }
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self::in_memory()
    }
}

#[derive(Clone)]
pub struct DbConnection {
    backend: DbBackend,
    config: ConnectionConfig,
}

#[derive(Clone)]
enum DbBackend {
    Libsql(Arc<LibsqlState>),
    #[cfg(feature = "memory")]
    Memory(Arc<MemoryState>),
}

struct LibsqlState {
    pool: RwLock<Option<LibsqlPool>>,
    connected: AtomicBool,
}

#[cfg(feature = "memory")]
struct MemoryState {
    state: RwLock<DatabaseState>,
    schema: RwLock<Option<SchemaMapping>>,
    connected: RwLock<bool>,
}

#[cfg(feature = "memory")]
#[derive(Debug, Clone, Default)]
pub(crate) struct DatabaseState {
    pub tables: HashMap<String, Vec<Row>>,
}

impl DbConnection {
    /// Create a connection with default config (libsql in-memory).
    pub fn new() -> Self {
        Self::with_config(ConnectionConfig::default())
    }

    /// Create a connection with explicit config (libsql backend).
    pub fn with_config(config: ConnectionConfig) -> Self {
        Self {
            backend: DbBackend::Libsql(Arc::new(LibsqlState {
                pool: RwLock::new(None),
                connected: AtomicBool::new(false),
            })),
            config,
        }
    }

    /// Create a connection that uses the in-memory HashMap backend.
    #[cfg(feature = "memory")]
    pub fn memory() -> Self {
        Self {
            backend: DbBackend::Memory(Arc::new(MemoryState {
                state: RwLock::new(DatabaseState::default()),
                schema: RwLock::new(None),
                connected: RwLock::new(false),
            })),
            config: ConnectionConfig::in_memory(),
        }
    }

    pub fn config(&self) -> ConnectionConfig {
        self.config.clone()
    }

    /// Open connection.
    pub async fn connect(&self) -> Result<()> {
        match &self.backend {
            DbBackend::Libsql(state) => {
                if state.connected.load(Ordering::SeqCst) {
                    return Ok(());
                }

                let attempts = self.config.retry_attempts + 1;
                for attempt in 0..attempts {
                    match LibsqlPool::new(&self.config).await {
                        Ok(pool) => {
                            *state.pool.write().await = Some(pool);
                            state.connected.store(true, Ordering::SeqCst);
                            return Ok(());
                        }
                        Err(err) => {
                            if attempt + 1 == attempts {
                                return Err(err);
                            }
                            let delay_ms = 100 * (1_u64 << attempt.min(6));
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                    }
                }

                Err(Error::Connection {
                    details: format!(
                        "Failed to connect after {attempts} attempt(s): exhausted retries"
                    ),
                })
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                let attempts = self.config.retry_attempts + 1;
                for attempt in 0..attempts {
                    if self.config.max_connections > 0 && self.config.timeout_ms > 0 {
                        *state.connected.write().await = true;
                        return Ok(());
                    }

                    if attempt + 1 == attempts {
                        break;
                    }

                    tokio::task::yield_now().await;
                }

                Err(Error::Connection {
                    details: format!(
                        "Failed to connect after {attempts} attempt(s): invalid configuration"
                    ),
                })
            }
        }
    }

    pub async fn close(&self) {
        match &self.backend {
            DbBackend::Libsql(state) => {
                *state.pool.write().await = None;
                state.connected.store(false, Ordering::SeqCst);
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                *state.connected.write().await = false;
            }
        }
    }

    pub async fn is_connected(&self) -> bool {
        match &self.backend {
            DbBackend::Libsql(state) => state.connected.load(Ordering::SeqCst),
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => *state.connected.read().await,
        }
    }

    pub async fn begin_transaction(&self) -> Result<DbTransaction> {
        self.ensure_connected().await?;
        match &self.backend {
            DbBackend::Libsql(state) => {
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;
                let connection = pool.acquire().await?;
                let transaction =
                    connection
                        .connection()?
                        .transaction()
                        .await
                        .map_err(|source| Error::Libsql {
                            context: "begin transaction".to_string(),
                            source,
                        })?;
                Ok(DbTransaction {
                    backend: TransactionBackend::Libsql {
                        connection: Some(connection),
                        transaction: Some(transaction),
                    },
                    active: true,
                })
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                let working_state = state.state.read().await.clone();
                Ok(DbTransaction {
                    backend: TransactionBackend::Memory {
                        connection: self.clone(),
                        working_state,
                    },
                    active: true,
                })
            }
        }
    }

    pub async fn apply_schema(&self, mapping: &SchemaMapping) -> Result<()> {
        self.ensure_connected().await?;
        match &self.backend {
            DbBackend::Libsql(state) => {
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;

                let connection = pool.acquire().await?;
                for table in mapping.tables() {
                    let sql = table.create_table_sql();
                    connection
                        .connection()?
                        .execute(&sql, ())
                        .await
                        .map_err(|source| Error::Sql {
                            statement: sql.clone(),
                            source,
                        })?;
                }
                Ok(())
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                *state.schema.write().await = Some(mapping.clone());
                Ok(())
            }
        }
    }

    pub(crate) async fn insert_row(&self, table: &str, row: Row) -> Result<()> {
        self.ensure_connected().await?;
        match &self.backend {
            DbBackend::Libsql(state) => {
                let (sql, params) = build_insert_sql(table, &row)?;
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;
                let connection = pool.acquire().await?;
                connection
                    .connection()?
                    .execute(&sql, params_from_iter(params))
                    .await
                    .map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                Ok(())
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                if let Some(schema) = state.schema.read().await.as_ref() {
                    schema.validate_row(table, &row)?;
                }
                let mut state = state.state.write().await;
                state.tables.entry(table.to_string()).or_default().push(row);
                Ok(())
            }
        }
    }

    pub(crate) async fn select_rows(
        &self,
        table: &str,
        filter: Option<&Row>,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<Vec<Row>> {
        self.select_rows_with_schema(table, filter, offset, limit, None)
            .await
    }

    pub(crate) async fn select_rows_with_schema(
        &self,
        table: &str,
        filter: Option<&Row>,
        offset: usize,
        limit: Option<usize>,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Row>> {
        self.ensure_connected().await?;
        match &self.backend {
            DbBackend::Libsql(state) => {
                let (sql, params) = build_select_sql(table, filter, offset, limit)?;
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;
                let connection = pool.acquire().await?;
                query_rows(connection.connection()?, table, &sql, params, schema).await
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                let state = state.state.read().await;
                let rows = state.tables.get(table).ok_or_else(|| Error::Query {
                    table: table.to_string(),
                    details: "Table not found".to_string(),
                })?;

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
        }
    }

    pub(crate) async fn update_rows(
        &self,
        table: &str,
        filter: &Row,
        updates: &Row,
    ) -> Result<usize> {
        self.ensure_connected().await?;
        if filter.is_empty() {
            return Err(Error::Query {
                table: table.to_string(),
                details: "Update filter cannot be empty".to_string(),
            });
        }
        match &self.backend {
            DbBackend::Libsql(state) => {
                let (sql, params) = build_update_sql(table, filter, updates)?;
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;
                let connection = pool.acquire().await?;
                let changed = connection
                    .connection()?
                    .execute(&sql, params_from_iter(params))
                    .await
                    .map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                Ok(changed as usize)
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                let schema = state.schema.read().await.clone();
                let mut state = state.state.write().await;
                let rows = state.tables.get_mut(table).ok_or_else(|| Error::Query {
                    table: table.to_string(),
                    details: "Table not found".to_string(),
                })?;

                let mut updated = 0usize;
                for row in rows {
                    if row_matches_filter(row, filter) {
                        for (column, value) in updates {
                            row.insert(column.clone(), value.clone());
                        }
                        if let Some(schema) = schema.as_ref() {
                            schema.validate_row(table, row)?;
                        }
                        updated += 1;
                    }
                }

                Ok(updated)
            }
        }
    }

    pub(crate) async fn upsert_row(&self, table: &str, key_column: &str, row: Row) -> Result<()> {
        self.ensure_connected().await?;
        match &self.backend {
            DbBackend::Libsql(state) => {
                let (sql, params) = build_upsert_sql(table, key_column, &row)?;
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;
                let connection = pool.acquire().await?;
                connection
                    .connection()?
                    .execute(&sql, params_from_iter(params))
                    .await
                    .map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                Ok(())
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                if let Some(schema) = state.schema.read().await.as_ref() {
                    schema.validate_row(table, &row)?;
                }
                let key_value = row.get(key_column).cloned().ok_or_else(|| Error::Query {
                    table: table.to_string(),
                    details: format!("Upsert key column '{key_column}' is missing"),
                })?;

                let mut state = state.state.write().await;
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
        }
    }

    pub async fn table_row_count(&self, table: &str) -> Result<usize> {
        self.ensure_connected().await?;
        match &self.backend {
            DbBackend::Libsql(state) => {
                let sql = format!("SELECT COUNT(*) FROM {}", quote_identifier(table));
                let pool = state
                    .pool
                    .read()
                    .await
                    .clone()
                    .ok_or_else(|| Error::Connection {
                        details: "Connection pool is not initialized".to_string(),
                    })?;
                let connection = pool.acquire().await?;
                let mut rows =
                    connection
                        .connection()?
                        .query(&sql, ())
                        .await
                        .map_err(|source| Error::Sql {
                            statement: sql.clone(),
                            source,
                        })?;
                if let Some(row) = rows.next().await.map_err(|source| Error::Sql {
                    statement: sql.clone(),
                    source,
                })? {
                    let count: i64 = row.get(0).map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                    Ok(count.max(0) as usize)
                } else {
                    Ok(0)
                }
            }
            #[cfg(feature = "memory")]
            DbBackend::Memory(state) => {
                let state = state.state.read().await;
                Ok(state.tables.get(table).map(Vec::len).unwrap_or(0))
            }
        }
    }

    async fn ensure_connected(&self) -> Result<()> {
        if !self.is_connected().await {
            return Err(Error::Connection {
                details: "Database is not connected".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for DbConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct LibsqlPool {
    inner: Arc<LibsqlPoolInner>,
}

struct LibsqlPoolInner {
    // Keep the Database alive for the lifetime of pooled connections.
    _database: Database,
    connections: std::sync::Mutex<Vec<LibsqlConnection>>,
    semaphore: Arc<Semaphore>,
}

impl LibsqlPool {
    async fn new(config: &ConnectionConfig) -> Result<Self> {
        if config.max_connections == 0 {
            return Err(Error::Config {
                details: "max_connections must be greater than zero".to_string(),
            });
        }
        if config.timeout_ms == 0 {
            return Err(Error::Config {
                details: "timeout_ms must be greater than zero".to_string(),
            });
        }

        let build_future = build_database(config);
        let database = tokio::time::timeout(Duration::from_millis(config.timeout_ms), build_future)
            .await
            .map_err(|_| Error::Connection {
                details: format!(
                    "Timed out after {}ms while opening database",
                    config.timeout_ms
                ),
            })??;

        let pool_size = pool_size(config);
        let mut connections = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let connection = database.connect().map_err(|source| Error::Libsql {
                context: "connect database".to_string(),
                source,
            })?;
            connection
                .busy_timeout(Duration::from_millis(config.timeout_ms))
                .map_err(|source| Error::Libsql {
                    context: "set busy timeout".to_string(),
                    source,
                })?;
            connection
                .execute("PRAGMA foreign_keys = ON", ())
                .await
                .map_err(|source| Error::Sql {
                    statement: "PRAGMA foreign_keys = ON".to_string(),
                    source,
                })?;
            connections.push(connection);
        }

        Ok(Self {
            inner: Arc::new(LibsqlPoolInner {
                _database: database,
                connections: std::sync::Mutex::new(connections),
                semaphore: Arc::new(Semaphore::new(pool_size)),
            }),
        })
    }

    async fn acquire(&self) -> Result<PooledConnection> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Connection {
                details: "Connection pool is closed".to_string(),
            })?;

        let mut connections = self
            .inner
            .connections
            .lock()
            .map_err(|_| Error::Connection {
                details: "Connection pool mutex is poisoned".to_string(),
            })?;
        let connection = connections.pop().ok_or_else(|| Error::Connection {
            details: "Connection pool exhausted".to_string(),
        })?;
        Ok(PooledConnection {
            inner: self.inner.clone(),
            connection: Some(connection),
            _permit: permit,
        })
    }
}

struct PooledConnection {
    inner: Arc<LibsqlPoolInner>,
    connection: Option<LibsqlConnection>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledConnection {
    fn connection(&self) -> Result<&LibsqlConnection> {
        self.connection.as_ref().ok_or_else(|| Error::Connection {
            details: "Pooled connection missing".to_string(),
        })
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            if let Ok(mut connections) = self.inner.connections.lock() {
                connections.push(connection);
            }
        }
    }
}

/// Transaction against backend state.
pub struct DbTransaction {
    backend: TransactionBackend,
    active: bool,
}

enum TransactionBackend {
    Libsql {
        connection: Option<PooledConnection>,
        transaction: Option<Transaction>,
    },
    #[cfg(feature = "memory")]
    Memory {
        connection: DbConnection,
        working_state: DatabaseState,
    },
}

impl DbTransaction {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub async fn insert_row(&mut self, table: &str, row: Row) -> Result<()> {
        self.ensure_active()?;
        match &mut self.backend {
            TransactionBackend::Libsql { transaction, .. } => {
                let (sql, params) = build_insert_sql(table, &row)?;
                let tx = transaction.as_ref().ok_or_else(|| Error::Transaction {
                    details: "Transaction is no longer active".to_string(),
                })?;
                tx.execute(&sql, params_from_iter(params))
                    .await
                    .map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                Ok(())
            }
            #[cfg(feature = "memory")]
            TransactionBackend::Memory { working_state, .. } => {
                working_state
                    .tables
                    .entry(table.to_string())
                    .or_default()
                    .push(row);
                Ok(())
            }
        }
    }

    pub async fn update_rows(&mut self, table: &str, filter: &Row, updates: &Row) -> Result<usize> {
        self.ensure_active()?;
        match &mut self.backend {
            TransactionBackend::Libsql { transaction, .. } => {
                let (sql, params) = build_update_sql(table, filter, updates)?;
                let tx = transaction.as_ref().ok_or_else(|| Error::Transaction {
                    details: "Transaction is no longer active".to_string(),
                })?;
                let changed =
                    tx.execute(&sql, params_from_iter(params))
                        .await
                        .map_err(|source| Error::Sql {
                            statement: sql.clone(),
                            source,
                        })?;
                Ok(changed as usize)
            }
            #[cfg(feature = "memory")]
            TransactionBackend::Memory { working_state, .. } => {
                let rows = working_state
                    .tables
                    .get_mut(table)
                    .ok_or_else(|| Error::Query {
                        table: table.to_string(),
                        details: "Table not found".to_string(),
                    })?;

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
        }
    }

    pub async fn upsert_row(&mut self, table: &str, key_column: &str, row: Row) -> Result<()> {
        self.ensure_active()?;
        match &mut self.backend {
            TransactionBackend::Libsql { transaction, .. } => {
                let (sql, params) = build_upsert_sql(table, key_column, &row)?;
                let tx = transaction.as_ref().ok_or_else(|| Error::Transaction {
                    details: "Transaction is no longer active".to_string(),
                })?;
                tx.execute(&sql, params_from_iter(params))
                    .await
                    .map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                Ok(())
            }
            #[cfg(feature = "memory")]
            TransactionBackend::Memory { working_state, .. } => {
                let key_value = row.get(key_column).cloned().ok_or_else(|| Error::Query {
                    table: table.to_string(),
                    details: format!("Upsert key column '{key_column}' is missing"),
                })?;

                let rows = working_state.tables.entry(table.to_string()).or_default();
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
        }
    }

    pub async fn row_count(&self, table: &str) -> Result<usize> {
        self.ensure_active()?;
        match &self.backend {
            TransactionBackend::Libsql { transaction, .. } => {
                let sql = format!("SELECT COUNT(*) FROM {}", quote_identifier(table));
                let tx = transaction.as_ref().ok_or_else(|| Error::Transaction {
                    details: "Transaction is no longer active".to_string(),
                })?;
                let mut rows = tx.query(&sql, ()).await.map_err(|source| Error::Sql {
                    statement: sql.clone(),
                    source,
                })?;
                if let Some(row) = rows.next().await.map_err(|source| Error::Sql {
                    statement: sql.clone(),
                    source,
                })? {
                    let count: i64 = row.get(0).map_err(|source| Error::Sql {
                        statement: sql.clone(),
                        source,
                    })?;
                    Ok(count.max(0) as usize)
                } else {
                    Ok(0)
                }
            }
            #[cfg(feature = "memory")]
            TransactionBackend::Memory { working_state, .. } => {
                Ok(working_state.tables.get(table).map(Vec::len).unwrap_or(0))
            }
        }
    }

    pub async fn commit(mut self) -> Result<()> {
        self.ensure_active()?;
        match &mut self.backend {
            TransactionBackend::Libsql {
                transaction,
                connection,
            } => {
                let tx = transaction.take().ok_or_else(|| Error::Transaction {
                    details: "Transaction is no longer active".to_string(),
                })?;
                tx.commit().await.map_err(|source| Error::Libsql {
                    context: "commit transaction".to_string(),
                    source,
                })?;
                connection.take();
            }
            #[cfg(feature = "memory")]
            TransactionBackend::Memory {
                connection,
                working_state,
            } => {
                connection.replace_state(working_state.clone()).await?;
            }
        }
        self.active = false;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<()> {
        self.ensure_active()?;
        match &mut self.backend {
            TransactionBackend::Libsql {
                transaction,
                connection,
            } => {
                let tx = transaction.take().ok_or_else(|| Error::Transaction {
                    details: "Transaction is no longer active".to_string(),
                })?;
                tx.rollback().await.map_err(|source| Error::Libsql {
                    context: "rollback transaction".to_string(),
                    source,
                })?;
                connection.take();
            }
            #[cfg(feature = "memory")]
            TransactionBackend::Memory { .. } => {}
        }
        self.active = false;
        Ok(())
    }

    fn ensure_active(&self) -> Result<()> {
        if !self.active {
            return Err(Error::Transaction {
                details: "Transaction is no longer active".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(feature = "memory")]
impl DbConnection {
    pub(crate) async fn replace_state(&self, state: DatabaseState) -> Result<()> {
        match &self.backend {
            DbBackend::Memory(memory) => {
                *memory.state.write().await = state;
                Ok(())
            }
            DbBackend::Libsql(_) => Ok(()),
        }
    }
}

#[cfg(feature = "memory")]
fn row_matches_filter(row: &Row, filter: &Row) -> bool {
    filter
        .iter()
        .all(|(column, value)| row.get(column) == Some(value))
}

async fn build_database(config: &ConnectionConfig) -> Result<Database> {
    let url = config.database_url.trim();
    if url.is_empty() {
        return Err(Error::Config {
            details: "database_url must be provided".to_string(),
        });
    }

    if is_remote_url(url) {
        let token = config.auth_token.clone().ok_or_else(|| Error::Config {
            details: "auth_token is required for remote databases".to_string(),
        })?;
        let builder = Builder::new_remote(url.to_string(), token);
        builder.build().await.map_err(|source| Error::Libsql {
            context: "open remote database".to_string(),
            source,
        })
    } else {
        let path = url.strip_prefix("file:").unwrap_or(url);
        let builder = Builder::new_local(path);
        builder.build().await.map_err(|source| Error::Libsql {
            context: "open local database".to_string(),
            source,
        })
    }
}

fn is_remote_url(url: &str) -> bool {
    url.starts_with("libsql://") || url.starts_with("https://") || url.starts_with("http://")
}

fn is_in_memory_url(url: &str) -> bool {
    let url = url.trim();
    url == ":memory:" || url.starts_with("file::memory:") || url.contains("mode=memory")
}

fn pool_size(config: &ConnectionConfig) -> usize {
    if is_in_memory_url(&config.database_url) {
        1
    } else {
        config.max_connections
    }
}

async fn query_rows(
    connection: &LibsqlConnection,
    table: &str,
    sql: &str,
    params: Vec<libsql::Value>,
    schema: Option<&TableSchema>,
) -> Result<Vec<Row>> {
    let mut rows = connection
        .query(sql, params_from_iter(params))
        .await
        .map_err(|source| Error::Sql {
            statement: sql.to_string(),
            source,
        })?;

    let mut output = Vec::new();
    while let Some(row) = rows.next().await.map_err(|source| Error::Sql {
        statement: sql.to_string(),
        source,
    })? {
        let record = libsql_row_to_row(table, &row, schema)?;
        output.push(record);
    }

    Ok(output)
}

fn libsql_row_to_row(table: &str, row: &libsql::Row, schema: Option<&TableSchema>) -> Result<Row> {
    let mut record = Row::new();
    let column_count = row.column_count();
    for idx in 0..column_count {
        let column_name = row.column_name(idx).ok_or_else(|| Error::Query {
            table: table.to_string(),
            details: format!("Missing column name for index {idx}"),
        })?;
        let value = row.get_value(idx).map_err(|source| Error::Query {
            table: table.to_string(),
            details: format!("Failed to read column '{column_name}': {source}"),
        })?;

        let db_value = if let Some(schema) = schema.and_then(|schema| schema.column(column_name)) {
            libsql_value_to_db_typed(table, column_name, value, schema.column_type)?
        } else {
            libsql_value_to_db(value)
        };

        record.insert(column_name.to_string(), db_value);
    }
    Ok(record)
}

fn libsql_value_to_db(value: libsql::Value) -> DbValue {
    match value {
        libsql::Value::Null => DbValue::Null,
        libsql::Value::Integer(value) => DbValue::Integer(value),
        libsql::Value::Real(value) => DbValue::Decimal(value),
        libsql::Value::Text(value) => DbValue::String(value),
        libsql::Value::Blob(value) => DbValue::Blob(value),
    }
}

fn libsql_value_to_db_typed(
    table: &str,
    column: &str,
    value: libsql::Value,
    column_type: ColumnType,
) -> Result<DbValue> {
    match (value, column_type) {
        (libsql::Value::Null, _) => Ok(DbValue::Null),
        (libsql::Value::Text(value), ColumnType::String) => Ok(DbValue::String(value)),
        (libsql::Value::Blob(value), ColumnType::String) => String::from_utf8(value)
            .map(DbValue::String)
            .map_err(|_| Error::Schema {
                details: format!(
                    "Invalid UTF-8 for string column '{}.{}' while reading blob value",
                    table, column
                ),
            }),
        (libsql::Value::Integer(value), ColumnType::Integer) => Ok(DbValue::Integer(value)),
        (libsql::Value::Real(value), ColumnType::Decimal) => Ok(DbValue::Decimal(value)),
        (libsql::Value::Integer(value), ColumnType::Decimal) => Ok(DbValue::Decimal(value as f64)),
        (libsql::Value::Integer(value), ColumnType::Boolean) => Ok(DbValue::Boolean(value != 0)),
        (libsql::Value::Text(value), ColumnType::Boolean) => match value.as_str() {
            "true" | "TRUE" | "1" => Ok(DbValue::Boolean(true)),
            "false" | "FALSE" | "0" => Ok(DbValue::Boolean(false)),
            _ => Err(Error::Schema {
                details: format!(
                    "Invalid boolean value for '{}.{}': '{value}'",
                    table, column
                ),
            }),
        },
        (other, expected) => Err(Error::Schema {
            details: format!(
                "Type mismatch for '{}.{}': expected {:?}, found {:?}",
                table, column, expected, other
            ),
        }),
    }
}

fn db_value_to_libsql(value: &DbValue) -> libsql::Value {
    match value {
        DbValue::String(value) => libsql::Value::Text(value.clone()),
        DbValue::Blob(value) => libsql::Value::Blob(value.clone()),
        DbValue::Integer(value) => libsql::Value::Integer(*value),
        DbValue::Decimal(value) => libsql::Value::Real(*value),
        DbValue::Boolean(value) => libsql::Value::Integer(if *value { 1 } else { 0 }),
        DbValue::Null => libsql::Value::Null,
    }
}

fn build_insert_sql(table: &str, row: &Row) -> Result<(String, Vec<libsql::Value>)> {
    if row.is_empty() {
        return Err(Error::Query {
            table: table.to_string(),
            details: "Insert row cannot be empty".to_string(),
        });
    }

    let mut columns = Vec::new();
    let mut params = Vec::new();
    for (column, value) in row {
        columns.push(quote_identifier(column));
        params.push(db_value_to_libsql(value));
    }

    let placeholders: Vec<String> = (1..=columns.len()).map(|idx| format!("?{idx}")).collect();
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_identifier(table),
        columns.join(", "),
        placeholders.join(", ")
    );

    Ok((sql, params))
}

fn build_select_sql(
    table: &str,
    filter: Option<&Row>,
    offset: usize,
    limit: Option<usize>,
) -> Result<(String, Vec<libsql::Value>)> {
    let mut params = Vec::new();
    let mut clauses = Vec::new();

    if let Some(filter) = filter {
        for (column, value) in filter {
            if matches!(value, DbValue::Null) {
                clauses.push(format!("{} IS NULL", quote_identifier(column)));
            } else {
                params.push(db_value_to_libsql(value));
                clauses.push(format!("{} = ?{}", quote_identifier(column), params.len()));
            }
        }
    }

    let mut sql = format!("SELECT * FROM {}", quote_identifier(table));
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }

    if let Some(limit) = limit {
        params.push(libsql::Value::Integer(limit as i64));
        sql.push_str(&format!(" LIMIT ?{}", params.len()));
    } else if offset > 0 {
        sql.push_str(" LIMIT -1");
    }

    if offset > 0 {
        params.push(libsql::Value::Integer(offset as i64));
        sql.push_str(&format!(" OFFSET ?{}", params.len()));
    }

    Ok((sql, params))
}

fn build_update_sql(
    table: &str,
    filter: &Row,
    updates: &Row,
) -> Result<(String, Vec<libsql::Value>)> {
    if updates.is_empty() {
        return Err(Error::Query {
            table: table.to_string(),
            details: "Update row cannot be empty".to_string(),
        });
    }
    if filter.is_empty() {
        return Err(Error::Query {
            table: table.to_string(),
            details: "Update filter cannot be empty".to_string(),
        });
    }

    let mut params = Vec::new();
    let mut assignments = Vec::new();
    for (column, value) in updates {
        params.push(db_value_to_libsql(value));
        assignments.push(format!("{} = ?{}", quote_identifier(column), params.len()));
    }

    let mut sql = format!(
        "UPDATE {} SET {}",
        quote_identifier(table),
        assignments.join(", ")
    );

    let mut clauses = Vec::new();
    for (column, value) in filter {
        if matches!(value, DbValue::Null) {
            clauses.push(format!("{} IS NULL", quote_identifier(column)));
        } else {
            params.push(db_value_to_libsql(value));
            clauses.push(format!("{} = ?{}", quote_identifier(column), params.len()));
        }
    }
    sql.push_str(" WHERE ");
    sql.push_str(&clauses.join(" AND "));

    Ok((sql, params))
}

fn build_upsert_sql(
    table: &str,
    key_column: &str,
    row: &Row,
) -> Result<(String, Vec<libsql::Value>)> {
    if !row.contains_key(key_column) {
        return Err(Error::Query {
            table: table.to_string(),
            details: format!("Upsert key column '{key_column}' is missing"),
        });
    }

    let (insert_sql, params) = build_insert_sql(table, row)?;
    let mut update_columns: Vec<String> = row
        .keys()
        .filter(|column| column.as_str() != key_column)
        .map(|column| {
            let quoted = quote_identifier(column);
            format!("{quoted} = excluded.{quoted}")
        })
        .collect();

    if update_columns.is_empty() {
        update_columns.push(format!(
            "{} = excluded.{}",
            quote_identifier(key_column),
            quote_identifier(key_column)
        ));
    }

    let sql = format!(
        "{} ON CONFLICT({}) DO UPDATE SET {}",
        insert_sql,
        quote_identifier(key_column),
        update_columns.join(", ")
    );

    Ok((sql, params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::schema::{ColumnDef, SchemaMapping};

    fn sample_row(id: i64) -> Row {
        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::Integer(id));
        row.insert("order_no".to_string(), DbValue::String(format!("PO-{id}")));
        row
    }

    fn sample_schema() -> SchemaMapping {
        let table = TableSchema::new("orders")
            .with_column(ColumnDef::new("id", ColumnType::Integer).primary_key())
            .with_column(ColumnDef::new("order_no", ColumnType::String));
        let mut mapping = SchemaMapping::new();
        mapping.add_table(table);
        mapping
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
            database_url: ":memory:".to_string(),
            auth_token: None,
            max_connections: 4,
            timeout_ms: 10,
            retry_attempts: 0,
        };
        let conn = DbConnection::with_config(cfg.clone());
        assert_eq!(conn.config(), cfg);
        conn.connect().await.unwrap();
        assert!(conn.is_connected().await);
    }

    #[tokio::test]
    async fn test_connection_retry() {
        let cfg = ConnectionConfig {
            database_url: ":memory:".to_string(),
            auth_token: None,
            max_connections: 0,
            timeout_ms: 0,
            retry_attempts: 2,
        };
        let conn = DbConnection::with_config(cfg);
        let err = conn.connect().await.unwrap_err();
        assert!(matches!(err, Error::Config { .. }));
    }

    #[tokio::test]
    async fn test_transaction_commit() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();
        conn.apply_schema(&sample_schema()).await.unwrap();

        let mut tx = conn.begin_transaction().await.unwrap();
        tx.insert_row("orders", sample_row(1)).await.unwrap();
        assert_eq!(tx.row_count("orders").await.unwrap(), 1);
        tx.commit().await.unwrap();

        assert_eq!(conn.table_row_count("orders").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();
        conn.apply_schema(&sample_schema()).await.unwrap();

        let mut tx = conn.begin_transaction().await.unwrap();
        tx.insert_row("orders", sample_row(1)).await.unwrap();
        tx.rollback().await.unwrap();

        assert_eq!(conn.table_row_count("orders").await.unwrap(), 0);
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_memory_backend() {
        let conn = DbConnection::memory();
        conn.connect().await.unwrap();
        conn.insert_row("orders", sample_row(1)).await.unwrap();
        assert_eq!(conn.table_row_count("orders").await.unwrap(), 1);
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn test_memory_backend_validates_schema() {
        let conn = DbConnection::memory();
        conn.connect().await.unwrap();
        conn.apply_schema(&sample_schema()).await.unwrap();

        let mut invalid_row = Row::new();
        invalid_row.insert("id".to_string(), DbValue::String("wrong-type".to_string()));
        invalid_row.insert("order_no".to_string(), DbValue::String("PO-1".to_string()));

        let err = conn.insert_row("orders", invalid_row).await.unwrap_err();
        assert!(matches!(err, Error::Schema { .. }));
    }

    #[tokio::test]
    async fn test_update_rows_requires_filter() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();
        conn.apply_schema(&sample_schema()).await.unwrap();
        conn.insert_row("orders", sample_row(1)).await.unwrap();

        let filter = Row::new();
        let mut updates = Row::new();
        updates.insert(
            "order_no".to_string(),
            DbValue::String("PO-1-UPDATED".to_string()),
        );

        let err = conn
            .update_rows("orders", &filter, &updates)
            .await
            .unwrap_err();
        match err {
            Error::Query { details, .. } => assert!(details.contains("filter cannot be empty")),
            _ => panic!("expected query error when update filter is empty"),
        }
    }

    #[tokio::test]
    async fn test_blob_values_round_trip() {
        let conn = DbConnection::new();
        conn.connect().await.unwrap();
        conn.apply_schema(&sample_schema()).await.unwrap();

        let mut row = Row::new();
        row.insert("id".to_string(), DbValue::Integer(1));
        row.insert(
            "order_no".to_string(),
            DbValue::Blob(vec![0, 159, 146, 150]),
        );
        conn.insert_row("orders", row).await.unwrap();

        let rows = conn.select_rows("orders", None, 0, Some(1)).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("order_no"),
            Some(&DbValue::Blob(vec![0, 159, 146, 150]))
        );
    }
}

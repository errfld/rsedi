//! # edi-adapter-db
//!
//! Database adapter for reading and writing EDI data.
//!
//! This crate provides database integration with async support
//! for batch operations and transaction management.

pub mod connection;
pub mod reader;
pub mod schema;
pub mod writer;

pub use connection::{ConnectionConfig, DbConnection, DbTransaction};
pub use reader::{DbReader, QueryOptions};
pub use schema::{ColumnDef, ColumnType, DbValue, ForeignKey, Row, SchemaMapping, TableSchema};
pub use writer::DbWriter;

use thiserror::Error;

/// Errors that can occur when working with the database.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Configuration error: {details}")]
    Config { details: String },

    #[error("Connection error: {details}")]
    Connection { details: String },

    #[error("Libsql error during {context}: {source}")]
    Libsql {
        context: String,
        #[source]
        source: libsql::Error,
    },

    #[error("SQL error executing `{statement}`: {source}")]
    Sql {
        statement: String,
        #[source]
        source: libsql::Error,
    },

    #[error("Query error on `{table}`: {details}")]
    Query { table: String, details: String },

    #[error("Schema error: {details}")]
    Schema { details: String },

    #[error("Transaction error: {details}")]
    Transaction { details: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

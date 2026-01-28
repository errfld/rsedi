//! # edi-adapter-db
//!
//! Database adapter for reading and writing EDI data.
//!
//! This crate provides database integration with async support
//! for batch operations and transaction management.

pub mod connection;
pub mod reader;
pub mod writer;
pub mod schema;

pub use connection::DbConnection;

use thiserror::Error;

/// Errors that can occur when working with the database
#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Query error: {0}")]
    Query(String),
    
    #[error("Schema error: {0}")]
    Schema(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

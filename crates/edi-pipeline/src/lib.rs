//! # edi-pipeline
//!
//! Streaming orchestration, batching, and partial acceptance policies.
//!
//! This crate provides the pipeline infrastructure for processing
//! EDI files with configurable error handling and streaming support.

pub mod pipeline;
pub mod batch;
pub mod streaming;
pub mod quarantine;
pub mod policies;

pub use pipeline::Pipeline;
pub use policies::{AcceptancePolicy, StrictnessLevel};

use thiserror::Error;

/// Errors that can occur in the pipeline
#[derive(Error, Debug)]
pub enum Error {
    #[error("Pipeline error: {0}")]
    Pipeline(String),
    
    #[error("Batch error: {0}")]
    Batch(String),
    
    #[error("Streaming error: {0}")]
    Streaming(String),
    
    #[error("Quarantine error: {0}")]
    Quarantine(String),
    
    #[error("Policy error: {0}")]
    Policy(String),
}

pub type Result<T> = std::result::Result<T, Error>;

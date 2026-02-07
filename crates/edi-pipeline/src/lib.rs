#![deny(warnings)]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::pedantic)] // Incrementally adopt pedantic lints without blocking existing code.

//! # edi-pipeline
//!
//! Streaming orchestration, batching, and partial acceptance policies.
//!
//! This crate provides the pipeline infrastructure for processing
//! EDI files with configurable error handling and streaming support.

pub mod batch;
pub mod pipeline;
pub mod policies;
pub mod quarantine;
pub mod streaming;

pub use batch::{Batch, BatchConfig, BatchItem, BatchResult, ItemStatus};
pub use pipeline::{
    ErrorSeverity, FileResult, Mapper, OutputFormat, Pipeline, PipelineBatchResult, PipelineConfig,
    PipelineMetrics, PipelineStats, ValidationError, Validator,
};
pub use policies::{AcceptancePolicy, StrictnessLevel};
pub use quarantine::{
    ErrorCategory, ErrorContext, QuarantineConfig, QuarantineReason, QuarantineStats,
    QuarantineStore, QuarantinedMessage,
};
pub use streaming::{
    Checkpoint, ProcessResult, StreamConfig, StreamMessage, StreamProcessor, StreamStats,
};

use thiserror::Error;

/// Errors that can occur in the pipeline
#[derive(Error, Debug, Clone)]
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

    #[error("IO error: {0}")]
    Io(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

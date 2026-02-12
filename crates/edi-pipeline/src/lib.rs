#![deny(warnings)]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]

//! # edi-pipeline
//!
//! Streaming orchestration, batching, and partial acceptance policies.
//!
//! This crate provides the pipeline infrastructure for processing
//! EDI files with configurable error handling and streaming support.

pub mod batch;
mod numeric;
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
    #[error("Pipeline error during {operation} for '{path}': {message}")]
    Pipeline {
        operation: String,
        path: String,
        message: String,
    },

    #[error("Batch error: {0}")]
    Batch(String),

    #[error("Streaming error: {0}")]
    Streaming(String),

    #[error("Quarantine error: {0}")]
    Quarantine(String),

    #[error("Policy error: {0}")]
    Policy(String),

    #[error("IO error during {operation} for '{path}': {message}")]
    Io {
        operation: String,
        path: String,
        message: String,
    },
}

impl Error {
    /// Create a structured pipeline error with operation/path context.
    pub fn pipeline(
        operation: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Pipeline {
            operation: operation.into(),
            path: path.into(),
            message: message.into(),
        }
    }

    /// Create a structured I/O error with operation/path context.
    pub fn io(
        operation: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::Io {
            operation: operation.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::io("io", "<unknown>", e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_error_preserves_operation_and_path_context() {
        let error = Error::pipeline("parse", "/tmp/input.edi", "invalid envelope");
        match error {
            Error::Pipeline {
                operation,
                path,
                message,
            } => {
                assert_eq!(operation, "parse");
                assert_eq!(path, "/tmp/input.edi");
                assert_eq!(message, "invalid envelope");
            }
            _ => panic!("expected pipeline variant"),
        }
    }

    #[test]
    fn io_error_from_std_error_has_fallback_context() {
        let io_error = std::fs::File::open("/path/that/does/not/exist")
            .map_err(Error::from)
            .expect_err("open should fail");

        match io_error {
            Error::Io {
                operation,
                path,
                message,
            } => {
                assert_eq!(operation, "io");
                assert_eq!(path, "<unknown>");
                assert!(!message.is_empty());
            }
            _ => panic!("expected io variant"),
        }
    }
}

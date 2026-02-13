//! Pipeline orchestration
//!
//! This module provides the main `Pipeline` for processing EDI files with
//! support for validation, mapping, batching, streaming, and quarantine.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::{
    AcceptancePolicy, Batch, BatchConfig, Error, QuarantineReason, QuarantineStore, Result,
    StreamConfig, StreamMessage, StreamProcessor, StrictnessLevel, numeric::u128_to_f64,
    numeric::usize_to_f64,
};
use edi_adapter_edifact::EdifactParser;
use edi_adapter_edifact::parser::ParseWarning;
use edi_ir::{Document, Node, NodeType, Value};
use tracing::{debug, info_span, warn};

/// Configuration for the pipeline.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Acceptance policy for handling errors.
    pub acceptance_policy: AcceptancePolicy,
    /// Strictness level for validation.
    pub strictness: StrictnessLevel,
    /// Batch configuration.
    pub batch_config: BatchConfig,
    /// Whether to enable streaming mode.
    pub streaming: bool,
    /// Maximum file size in bytes.
    pub max_file_size: usize,
    /// Whether to validate before processing.
    pub validate_before_processing: bool,
    /// Whether to apply mapping transformations.
    pub enable_mapping: bool,
    /// Output format.
    pub output_format: OutputFormat,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
            batch_config: BatchConfig::default(),
            streaming: false,
            max_file_size: 100 * 1024 * 1024,
            validate_before_processing: true,
            enable_mapping: true,
            output_format: OutputFormat::default(),
        }
    }
}

/// Output format for processed documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Native EDI format.
    #[default]
    Edifact,
    /// JSON format.
    Json,
    /// CSV format.
    Csv,
    /// XML format.
    Xml,
}

/// Main pipeline for processing EDI files.
#[derive(Debug)]
pub struct Pipeline {
    config: PipelineConfig,
    quarantine: QuarantineStore<Vec<u8>>,
    stats: PipelineStats,
    running: bool,
}

/// Statistics for pipeline processing.
#[derive(Debug, Default, Clone)]
pub struct PipelineStats {
    /// Total files processed.
    pub files_processed: usize,
    /// Total files successful.
    pub files_successful: usize,
    /// Total files failed.
    pub files_failed: usize,
    /// Total files quarantined.
    pub files_quarantined: usize,
    /// Total messages processed.
    pub messages_processed: usize,
    /// Total messages successful.
    pub messages_successful: usize,
    /// Total messages failed.
    pub messages_failed: usize,
    /// Total validation errors.
    pub validation_errors: usize,
    /// Total bytes processed.
    pub bytes_processed: usize,
    /// Total processing time.
    pub total_processing_time: Duration,
    /// Pipeline start time.
    pub started_at: Option<Instant>,
}

/// Result of processing a single file.
#[derive(Debug, Clone)]
pub struct FileResult {
    /// File path.
    pub path: String,
    /// Whether processing succeeded under the configured policy.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Number of processed messages in file.
    pub message_count: usize,
    /// Number of successful messages.
    pub success_count: usize,
    /// Number of failed messages.
    pub failure_count: usize,
    /// Processing duration.
    pub duration: Duration,
    /// Whether any message was quarantined.
    pub quarantined: bool,
}

/// Result of processing a batch of files.
#[derive(Debug)]
pub struct PipelineBatchResult {
    /// Results for individual files.
    pub file_results: Vec<FileResult>,
    /// Total files requested.
    pub total_files: usize,
    /// Successful files.
    pub successful_files: usize,
    /// Failed files.
    pub failed_files: usize,
    /// Quarantined files.
    pub quarantined_files: usize,
    /// Total processing time.
    pub total_duration: Duration,
    /// Whether batch succeeded overall.
    pub batch_success: bool,
}

/// Metrics collected during processing.
#[derive(Debug, Default, Clone)]
pub struct PipelineMetrics {
    /// Files per second.
    pub files_per_second: f64,
    /// Messages per second.
    pub messages_per_second: f64,
    /// Average file processing time.
    pub avg_file_time_ms: f64,
    /// Error rate percentage.
    pub error_rate: f64,
    /// Current throughput.
    pub throughput_mbps: f64,
}

#[derive(Debug, Clone, Copy)]
struct MessageProcessingConfig {
    validate_before_processing: bool,
    enable_mapping: bool,
    strictness: StrictnessLevel,
    output_format: OutputFormat,
}

#[derive(Debug, Clone)]
struct MessageOutcome {
    message_id: String,
    success: bool,
    error: Option<String>,
    validation_failures: usize,
    quarantine_reason: QuarantineReason,
    quarantine_payload: Vec<u8>,
}

#[derive(Debug, Default)]
struct FileSummary {
    message_count: usize,
    success_count: usize,
    failure_count: usize,
    validation_failures: usize,
    quarantined: bool,
    file_error: Option<String>,
    fatal_error: Option<String>,
}

#[derive(Debug)]
struct StreamWorkItem {
    index: usize,
    warning_count: usize,
    document: Document,
}

impl Pipeline {
    /// Create a new pipeline with the given configuration.
    #[must_use]
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config,
            quarantine: QuarantineStore::with_defaults(),
            stats: PipelineStats::default(),
            running: false,
        }
    }

    /// Create a pipeline with default configuration.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(PipelineConfig::default())
    }

    /// Start the pipeline.
    pub fn start(&mut self) {
        self.running = true;
        self.stats.started_at = Some(Instant::now());
    }

    /// Stop the pipeline.
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Check if pipeline is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Process a single file.
    ///
    /// # Errors
    ///
    /// Returns an error if reading, parsing, validation, mapping, or policy
    /// handling fails.
    pub fn process_file<P: AsRef<Path>>(&mut self, path: P) -> Result<FileResult> {
        self.process_file_internal(path.as_ref(), None, None)
    }

    fn process_file_internal(
        &mut self,
        path: &Path,
        validator: Option<&dyn Validator>,
        mapper: Option<&dyn Mapper>,
    ) -> Result<FileResult> {
        let path_str = path.to_string_lossy().to_string();

        let file_span = info_span!(
            "pipeline.process_file",
            path = %path_str,
            streaming = self.config.streaming,
            policy = ?self.config.acceptance_policy,
        );
        let _file_guard = file_span.enter();

        if !path.exists() {
            return Err(Error::Pipeline(format!("File not found: {path_str}")));
        }

        let metadata = std::fs::metadata(path)?;
        if metadata.len() > self.config.max_file_size as u64 {
            return Err(Error::Pipeline(format!(
                "File too large: {} bytes (max: {} bytes)",
                metadata.len(),
                self.config.max_file_size
            )));
        }

        self.stats.started_at.get_or_insert_with(Instant::now);
        let start = Instant::now();

        let content = std::fs::read(path)?;

        let processing_result = self.process_content(&content, &path_str, validator, mapper);
        let duration = start.elapsed();

        self.stats.files_processed += 1;
        self.stats.bytes_processed += content.len();
        self.stats.total_processing_time += duration;

        match processing_result {
            Ok(mut summary) => {
                if summary.message_count == 0
                    && matches!(self.config.acceptance_policy, AcceptancePolicy::Quarantine)
                {
                    let quarantine_message = summary
                        .file_error
                        .clone()
                        .unwrap_or_else(|| "No messages were parsed from file".to_string());
                    self.quarantine.quarantine(
                        &path_str,
                        content.clone(),
                        QuarantineReason::ProcessingError,
                        quarantine_message,
                    )?;
                    summary.quarantined = true;
                }

                self.stats.messages_processed += summary.message_count;
                self.stats.messages_successful += summary.success_count;
                self.stats.messages_failed += summary.failure_count;
                self.stats.validation_errors += summary.validation_failures;

                if summary.quarantined {
                    self.stats.files_quarantined += 1;
                }

                if let Some(fatal_error) = summary.fatal_error {
                    self.stats.files_failed += 1;
                    return Err(Error::Pipeline(fatal_error));
                }

                let mut success = true;
                let mut error = None;

                if summary.message_count == 0 {
                    success = false;
                    error = summary
                        .file_error
                        .clone()
                        .or_else(|| Some("No messages were parsed from file".to_string()));
                } else if matches!(self.config.acceptance_policy, AcceptancePolicy::FailAll)
                    && summary.failure_count > 0
                {
                    success = false;
                    error.clone_from(&summary.file_error);
                }

                if success {
                    self.stats.files_successful += 1;
                } else {
                    self.stats.files_failed += 1;
                }

                Ok(FileResult {
                    path: path_str,
                    success,
                    error,
                    message_count: summary.message_count,
                    success_count: summary.success_count,
                    failure_count: summary.failure_count,
                    duration,
                    quarantined: summary.quarantined,
                })
            }
            Err(error) => {
                self.stats.files_failed += 1;
                if matches!(self.config.acceptance_policy, AcceptancePolicy::Quarantine) {
                    self.quarantine.quarantine(
                        &path_str,
                        content,
                        QuarantineReason::ProcessingError,
                        error.to_string(),
                    )?;
                    self.stats.files_quarantined += 1;
                }
                Err(error)
            }
        }
    }

    fn process_content(
        &mut self,
        content: &[u8],
        path: &str,
        validator: Option<&dyn Validator>,
        mapper: Option<&dyn Mapper>,
    ) -> Result<FileSummary> {
        let parser = EdifactParser::new();
        let parse_outcome = parser
            .parse_with_warnings(content, path)
            .map_err(|error| Error::Pipeline(format!("Failed to parse {path}: {error}")))?;

        if parse_outcome.documents.is_empty() {
            let mut summary = FileSummary {
                file_error: Some("No messages were parsed from file".to_string()),
                ..FileSummary::default()
            };

            if matches!(self.config.acceptance_policy, AcceptancePolicy::FailAll) {
                summary.fatal_error = summary.file_error.clone();
            }

            return Ok(summary);
        }

        let warning_counts =
            warning_counts_for_documents(&parse_outcome.documents, &parse_outcome.warnings);
        let processing_config = self.processing_config();
        let stop_on_failure = matches!(self.config.acceptance_policy, AcceptancePolicy::FailAll);

        let outcomes = if self.config.streaming && validator.is_none() && mapper.is_none() {
            self.process_documents_streaming(
                processing_config,
                parse_outcome.documents,
                &warning_counts,
                stop_on_failure,
            )?
        } else {
            if self.config.streaming && (validator.is_some() || mapper.is_some()) {
                warn!(
                    "Streaming mode is enabled but custom validator/mapper is in use; falling back to sequential processing"
                );
            }

            process_documents_sequential(
                processing_config,
                parse_outcome.documents,
                &warning_counts,
                validator,
                mapper,
                stop_on_failure,
            )
        };

        let mut summary = FileSummary {
            message_count: outcomes.len(),
            ..FileSummary::default()
        };

        for outcome in outcomes {
            let message_span = info_span!(
                "pipeline.process_message",
                path = %path,
                message_id = %outcome.message_id,
                success = outcome.success,
            );
            let _message_guard = message_span.enter();

            if outcome.success {
                summary.success_count += 1;
                continue;
            }

            summary.failure_count += 1;
            summary.validation_failures += outcome.validation_failures;

            if summary.file_error.is_none() {
                summary.file_error.clone_from(&outcome.error);
            }

            match self.config.acceptance_policy {
                AcceptancePolicy::AcceptAll => {}
                AcceptancePolicy::FailAll => {
                    summary.fatal_error = outcome.error;
                    break;
                }
                AcceptancePolicy::Quarantine => {
                    let quarantine_id = format!("{}:{}", path, outcome.message_id);
                    self.quarantine.quarantine(
                        quarantine_id,
                        outcome.quarantine_payload,
                        outcome.quarantine_reason,
                        outcome
                            .error
                            .unwrap_or_else(|| "Message failed without detailed error".to_string()),
                    )?;
                    summary.quarantined = true;
                }
            }
        }

        Ok(summary)
    }

    fn process_documents_streaming(
        &self,
        processing_config: MessageProcessingConfig,
        documents: Vec<Document>,
        warning_counts: &[usize],
        stop_on_failure: bool,
    ) -> Result<Vec<MessageOutcome>> {
        let stream_config = StreamConfig {
            max_concurrency: self.config.batch_config.max_size.clamp(1, 64),
            channel_buffer_size: self.config.batch_config.max_size.saturating_mul(2).max(1),
            ..StreamConfig::default()
        };

        let work_items = documents
            .into_iter()
            .enumerate()
            .map(|(index, document)| StreamWorkItem {
                index,
                warning_count: warning_counts.get(index).copied().unwrap_or(0),
                document,
            })
            .collect::<Vec<_>>();

        let run = move || async move {
            let processor = StreamProcessor::new(stream_config);
            let (tx, mut rx) = tokio::sync::mpsc::channel::<MessageOutcome>(1);
            let mut outcomes = Vec::with_capacity(work_items.len());

            for work in work_items {
                processor
                    .submit(StreamMessage::new(work.index, work))
                    .await
                    .map_err(|error| Error::Streaming(error.to_string()))?;

                let sender = tx.clone();
                let config_for_message = processing_config;

                processor
                    .process_single(move |item| async move {
                        let outcome = process_single_message(
                            config_for_message,
                            item.index,
                            item.warning_count,
                            &item.document,
                            None,
                            None,
                        );
                        let should_abort = stop_on_failure && !outcome.success;
                        sender.send(outcome).await.map_err(|_| {
                            Error::Streaming(
                                "Streaming processor failed to publish message result".to_string(),
                            )
                        })?;
                        if should_abort {
                            Err(Error::Streaming("Message processing failed".to_string()))
                        } else {
                            Ok(())
                        }
                    })
                    .await
                    .map_err(|error| Error::Streaming(error.to_string()))?;

                let outcome = rx.recv().await.ok_or_else(|| {
                    Error::Streaming(
                        "Streaming processor did not return a message result".to_string(),
                    )
                })?;

                let is_failure = !outcome.success;
                outcomes.push(outcome);

                if is_failure && stop_on_failure {
                    break;
                }
            }

            let stream_stats = processor.get_stats().await;
            let checkpoint = processor.get_checkpoint().await;
            debug!(
                received = stream_stats.received,
                succeeded = stream_stats.succeeded,
                failed = stream_stats.failed,
                checkpoint_position = checkpoint.position,
                "Completed streaming file processing"
            );

            Ok(outcomes)
        };

        run_streaming_task(run)
    }

    fn processing_config(&self) -> MessageProcessingConfig {
        MessageProcessingConfig {
            validate_before_processing: self.config.validate_before_processing,
            enable_mapping: self.config.enable_mapping,
            strictness: self.config.strictness,
            output_format: self.config.output_format,
        }
    }

    /// Process multiple files as a batch.
    ///
    /// This is intended to be used by CLI batch commands and supports retry and
    /// partial-success policies through `BatchConfig`.
    ///
    /// # Errors
    ///
    /// Returns an error if batch processing cannot continue under configured
    /// acceptance policy or underlying I/O/processing fails.
    pub fn process_batch<P: AsRef<Path>>(&mut self, paths: &[P]) -> Result<PipelineBatchResult> {
        let start = Instant::now();
        let mut file_results = Vec::with_capacity(paths.len());
        let mut path_index = 0usize;
        let mut stop_processing = false;

        while path_index < paths.len() && !stop_processing {
            let mut batch = Batch::new(&self.config.batch_config);

            while path_index < paths.len() && !batch.is_full() {
                let item_path = paths[path_index].as_ref().to_path_buf();
                let item_id = format!("file-{path_index}");
                batch.add(item_id, item_path)?;
                path_index += 1;
            }

            for item in batch.items() {
                if stop_processing {
                    break;
                }

                let file_path = item.data.clone();
                let max_attempts = self.config.batch_config.max_retries.saturating_add(1);
                let mut attempt = 0u32;

                loop {
                    attempt += 1;

                    match self.process_file(&file_path) {
                        Ok(file_result) => {
                            let should_retry = !file_result.success && attempt < max_attempts;
                            if should_retry {
                                warn!(
                                    path = %file_path.display(),
                                    attempt,
                                    max_attempts,
                                    "Retrying file after policy-level failure"
                                );
                                continue;
                            }

                            if matches!(self.config.acceptance_policy, AcceptancePolicy::FailAll)
                                && !file_result.success
                            {
                                stop_processing = true;
                            }

                            file_results.push(file_result);
                            break;
                        }
                        Err(error) => {
                            if attempt < max_attempts {
                                warn!(
                                    path = %file_path.display(),
                                    attempt,
                                    max_attempts,
                                    error = %error,
                                    "Retrying file after processing error"
                                );
                                continue;
                            }

                            file_results.push(FileResult {
                                path: file_path.to_string_lossy().to_string(),
                                success: false,
                                error: Some(error.to_string()),
                                message_count: 0,
                                success_count: 0,
                                failure_count: 0,
                                duration: Duration::ZERO,
                                quarantined: false,
                            });

                            if matches!(self.config.acceptance_policy, AcceptancePolicy::FailAll) {
                                stop_processing = true;
                            }

                            break;
                        }
                    }
                }
            }
        }

        let successful_files = file_results.iter().filter(|result| result.success).count();
        let failed_files = file_results.len().saturating_sub(successful_files);
        let quarantined_files = file_results
            .iter()
            .filter(|result| result.quarantined)
            .count();

        let batch_success = match self.config.acceptance_policy {
            AcceptancePolicy::FailAll => failed_files == 0,
            AcceptancePolicy::AcceptAll | AcceptancePolicy::Quarantine => true,
        };

        Ok(PipelineBatchResult {
            file_results,
            total_files: paths.len(),
            successful_files,
            failed_files,
            quarantined_files,
            total_duration: start.elapsed(),
            batch_success,
        })
    }

    /// Process a file with a validator integration point.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing/validation/policy handling fails.
    pub fn process_with_validation<P: AsRef<Path>>(
        &mut self,
        path: P,
        validator: &dyn Validator,
    ) -> Result<FileResult> {
        self.process_file_internal(path.as_ref(), Some(validator), None)
    }

    /// Process a file with a mapper integration point.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing/mapping/policy handling fails.
    pub fn process_with_mapping<P: AsRef<Path>>(
        &mut self,
        path: P,
        mapper: &dyn Mapper,
    ) -> Result<FileResult> {
        self.process_file_internal(path.as_ref(), None, Some(mapper))
    }

    /// Get current statistics.
    #[must_use]
    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// Get metrics.
    #[must_use]
    pub fn metrics(&self) -> PipelineMetrics {
        let elapsed = self
            .stats
            .started_at
            .map(|started| started.elapsed())
            .filter(|duration| !duration.is_zero())
            .unwrap_or(self.stats.total_processing_time);

        let elapsed_secs = elapsed.as_secs_f64();
        let bytes_per_second = if elapsed_secs > 0.0 {
            usize_to_f64(self.stats.bytes_processed) / elapsed_secs
        } else {
            0.0
        };

        PipelineMetrics {
            files_per_second: if elapsed_secs > 0.0 {
                usize_to_f64(self.stats.files_processed) / elapsed_secs
            } else {
                0.0
            },
            messages_per_second: if elapsed_secs > 0.0 {
                usize_to_f64(self.stats.messages_processed) / elapsed_secs
            } else {
                0.0
            },
            avg_file_time_ms: if self.stats.files_processed > 0 {
                u128_to_f64(self.stats.total_processing_time.as_millis())
                    / usize_to_f64(self.stats.files_processed)
            } else {
                0.0
            },
            error_rate: if self.stats.files_processed > 0 {
                (usize_to_f64(self.stats.files_failed) / usize_to_f64(self.stats.files_processed))
                    * 100.0
            } else {
                0.0
            },
            throughput_mbps: bytes_per_second * 8.0 / 1_000_000.0,
        }
    }

    /// Get quarantine store.
    #[must_use]
    pub fn quarantine(&self) -> &QuarantineStore<Vec<u8>> {
        &self.quarantine
    }

    /// Get mutable quarantine store.
    pub fn quarantine_mut(&mut self) -> &mut QuarantineStore<Vec<u8>> {
        &mut self.quarantine
    }

    /// Reset statistics.
    pub fn reset_stats(&mut self) {
        self.stats = PipelineStats::default();
    }

    /// Get configuration.
    #[must_use]
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }
}

/// Trait for validation integration.
pub trait Validator {
    /// Validate content and return validation messages.
    ///
    /// # Errors
    ///
    /// Returns an error when validation execution fails.
    fn validate(&self, content: &str) -> Result<Vec<ValidationError>>;
}

/// Validation error.
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error message.
    pub message: String,
    /// Error location.
    pub location: Option<String>,
    /// Severity.
    pub severity: ErrorSeverity,
}

/// Error severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Warning.
    Warning,
    /// Error.
    Error,
    /// Critical.
    Critical,
}

/// Trait for mapping integration.
pub trait Mapper {
    /// Apply mapping transformation.
    ///
    /// # Errors
    ///
    /// Returns an error when mapping execution fails.
    fn map(&self, content: &str) -> Result<String>;
}

fn process_documents_sequential(
    config: MessageProcessingConfig,
    documents: Vec<Document>,
    warning_counts: &[usize],
    validator: Option<&dyn Validator>,
    mapper: Option<&dyn Mapper>,
    stop_on_failure: bool,
) -> Vec<MessageOutcome> {
    let mut outcomes = Vec::with_capacity(documents.len());

    for (index, document) in documents.into_iter().enumerate() {
        let warning_count = warning_counts.get(index).copied().unwrap_or(0);
        let outcome =
            process_single_message(config, index, warning_count, &document, validator, mapper);
        let failed = !outcome.success;
        outcomes.push(outcome);
        if failed && stop_on_failure {
            break;
        }
    }

    outcomes
}

fn process_single_message(
    config: MessageProcessingConfig,
    index: usize,
    warning_count: usize,
    document: &Document,
    validator: Option<&dyn Validator>,
    mapper: Option<&dyn Mapper>,
) -> MessageOutcome {
    let message_id = message_id(document, index);

    let canonical_json = match serde_json::to_string(document) {
        Ok(json) => json,
        Err(error) => {
            return MessageOutcome {
                message_id,
                success: false,
                error: Some(format!("Failed to serialize document: {error}")),
                validation_failures: 0,
                quarantine_reason: QuarantineReason::ProcessingError,
                quarantine_payload: Vec::new(),
            };
        }
    };

    let validation_errors = match collect_validation_errors(
        config,
        warning_count,
        document,
        &canonical_json,
        validator,
    ) {
        Ok(errors) => errors,
        Err(error) => {
            return MessageOutcome {
                message_id,
                success: false,
                error: Some(error),
                validation_failures: 1,
                quarantine_reason: QuarantineReason::ValidationFailed,
                quarantine_payload: canonical_json.into_bytes(),
            };
        }
    };

    let validation_failure_count = validation_errors
        .iter()
        .filter(|error| should_fail_validation(config.strictness, error.severity))
        .count();

    if validation_failure_count > 0 {
        return MessageOutcome {
            message_id,
            success: false,
            error: Some(first_validation_failure_message(
                &validation_errors,
                config.strictness,
            )),
            validation_failures: validation_failure_count,
            quarantine_reason: QuarantineReason::ValidationFailed,
            quarantine_payload: canonical_json.into_bytes(),
        };
    }

    let mapped_payload = if config.enable_mapping {
        if let Some(mapper) = mapper {
            match mapper.map(&canonical_json) {
                Ok(mapped_doc) => Some(mapped_doc),
                Err(error) => {
                    return MessageOutcome {
                        message_id,
                        success: false,
                        error: Some(format!("Mapping failed: {error}")),
                        validation_failures: 0,
                        quarantine_reason: QuarantineReason::ProcessingError,
                        quarantine_payload: canonical_json.into_bytes(),
                    };
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    let final_payload =
        match render_output(config.output_format, document, mapped_payload.as_deref()) {
            Ok(payload) => payload,
            Err(error) => {
                return MessageOutcome {
                    message_id,
                    success: false,
                    error: Some(error),
                    validation_failures: 0,
                    quarantine_reason: QuarantineReason::ProcessingError,
                    quarantine_payload: mapped_payload.unwrap_or(canonical_json).into_bytes(),
                };
            }
        };

    debug!(
        message_id = %message_id,
        output_bytes = final_payload.len(),
        "Successfully processed message"
    );

    MessageOutcome {
        message_id,
        success: true,
        error: None,
        validation_failures: 0,
        quarantine_reason: QuarantineReason::ProcessingError,
        quarantine_payload: Vec::new(),
    }
}

fn first_validation_failure_message(
    validation_errors: &[ValidationError],
    strictness: StrictnessLevel,
) -> String {
    validation_errors
        .iter()
        .find(|error| should_fail_validation(strictness, error.severity))
        .map_or_else(
            || "Validation failed".to_string(),
            |error| error.message.clone(),
        )
}

fn collect_validation_errors(
    config: MessageProcessingConfig,
    warning_count: usize,
    document: &Document,
    payload: &str,
    validator: Option<&dyn Validator>,
) -> std::result::Result<Vec<ValidationError>, String> {
    if !config.validate_before_processing {
        return Ok(Vec::new());
    }

    if let Some(validator) = validator {
        return validator
            .validate(payload)
            .map_err(|error| error.to_string());
    }

    let mut errors = Vec::new();

    if document.root.children.is_empty() {
        errors.push(ValidationError {
            message: "Document has no segment content".to_string(),
            location: Some("/MESSAGE".to_string()),
            severity: ErrorSeverity::Error,
        });
    }

    if warning_count > 0 {
        errors.push(ValidationError {
            message: format!("Parser emitted {warning_count} warning(s) for this message"),
            location: document.metadata.message_refs.first().cloned(),
            severity: ErrorSeverity::Warning,
        });
    }

    Ok(errors)
}

fn should_fail_validation(strictness: StrictnessLevel, severity: ErrorSeverity) -> bool {
    match strictness {
        StrictnessLevel::Strict => true,
        StrictnessLevel::Permissive | StrictnessLevel::Standard => {
            matches!(severity, ErrorSeverity::Error | ErrorSeverity::Critical)
        }
    }
}

fn warning_counts_for_documents(documents: &[Document], warnings: &[ParseWarning]) -> Vec<usize> {
    documents
        .iter()
        .enumerate()
        .map(|(index, document)| {
            let doc_ref = document.metadata.message_refs.first();
            warnings
                .iter()
                .filter(|warning| match (&warning.message_ref, doc_ref) {
                    (Some(warning_ref), Some(message_ref)) => warning_ref == message_ref,
                    (None, None) => true,
                    _ => false,
                })
                .count()
                .max(usize::from(
                    doc_ref.is_none() && index == 0 && !warnings.is_empty(),
                ))
        })
        .collect()
}

fn message_id(document: &Document, index: usize) -> String {
    document
        .metadata
        .message_refs
        .first()
        .cloned()
        .unwrap_or_else(|| format!("message-{}", index + 1))
}

fn run_streaming_task<F, Fut>(run: F) -> Result<Vec<MessageOutcome>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Vec<MessageOutcome>>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    Error::Streaming(format!("Failed to build streaming runtime: {error}"))
                })?;
            runtime.block_on(run())
        });

        handle.join().map_err(|panic_payload| {
            let message = panic_payload
                .downcast_ref::<&str>()
                .map(|value| (*value).to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            Error::Streaming(format!("Streaming worker thread panicked: {message}"))
        })?
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                Error::Streaming(format!("Failed to build streaming runtime: {error}"))
            })?;
        runtime.block_on(run())
    }
}

fn render_output(
    format: OutputFormat,
    document: &Document,
    mapped_payload: Option<&str>,
) -> std::result::Result<String, String> {
    match format {
        OutputFormat::Json => mapped_payload.map_or_else(
            || serde_json::to_string_pretty(document).map_err(|error| error.to_string()),
            |mapped| Ok(mapped.to_string()),
        ),
        OutputFormat::Csv => {
            Ok(mapped_payload.map_or_else(|| serialize_csv(document), str::to_string))
        }
        OutputFormat::Xml => {
            Ok(mapped_payload.map_or_else(|| serialize_xml(document), str::to_string))
        }
        OutputFormat::Edifact => mapped_payload.map_or_else(
            || serialize_edifact(document),
            |mapped| Ok(mapped.to_string()),
        ),
    }
}

fn serialize_csv(document: &Document) -> String {
    let mut rows = Vec::new();
    collect_rows(&document.root, &mut rows);

    let mut output = String::from("name,node_type,value\n");
    for (name, node_type, value) in rows {
        output.push_str(&escape_csv_field(&name));
        output.push(',');
        output.push_str(&escape_csv_field(&node_type));
        output.push(',');
        output.push_str(&escape_csv_field(&value));
        output.push('\n');
    }

    output
}

fn collect_rows(node: &Node, rows: &mut Vec<(String, String, String)>) {
    let value = node
        .value
        .as_ref()
        .and_then(Value::as_string)
        .unwrap_or_default();
    rows.push((node.name.clone(), format!("{:?}", node.node_type), value));

    for child in &node.children {
        collect_rows(child, rows);
    }
}

fn escape_csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_string()
    }
}

fn serialize_xml(document: &Document) -> String {
    let mut xml = String::from("<document>");
    append_node_xml(&document.root, &mut xml);
    xml.push_str("</document>");
    xml
}

fn append_node_xml(node: &Node, output: &mut String) {
    let element_name = sanitize_xml_name(&node.name);
    output.push('<');
    output.push_str(&element_name);
    output.push('>');

    if let Some(value) = node.value.as_ref().and_then(Value::as_string) {
        output.push_str(&xml_escape(&value));
    }

    for child in &node.children {
        append_node_xml(child, output);
    }

    output.push_str("</");
    output.push_str(&element_name);
    output.push('>');
}

fn sanitize_xml_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return "item".to_string();
    }

    let mut sanitized = String::with_capacity(trimmed.len() + 1);
    for (index, ch) in trimmed.chars().enumerate() {
        if (index == 0 && is_valid_xml_name_start(ch)) || (index > 0 && is_valid_xml_name_char(ch))
        {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    if sanitized
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("xml"))
    {
        sanitized.insert(0, '_');
    }

    sanitized
}

fn is_valid_xml_name_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_valid_xml_name_char(ch: char) -> bool {
    is_valid_xml_name_start(ch) || ch.is_ascii_digit() || ch == '-' || ch == '.'
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn serialize_edifact(document: &Document) -> std::result::Result<String, String> {
    let mut segments = Vec::new();
    collect_edifact_segments(&document.root, &mut segments);

    if segments.is_empty() {
        return Err("Cannot serialize document without segment nodes".to_string());
    }

    Ok(segments.join("\n"))
}

fn escape_edifact_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '?' | '+' | ':' | '\'' => {
                escaped.push('?');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn collect_edifact_segments(node: &Node, segments: &mut Vec<String>) {
    if matches!(node.node_type, NodeType::Segment) {
        let mut segment = node.name.clone();
        let mut elements = Vec::new();

        for element in &node.children {
            if !matches!(element.node_type, NodeType::Element) {
                continue;
            }

            let value = if element.children.is_empty() {
                element
                    .value
                    .as_ref()
                    .and_then(Value::as_string)
                    .map(|value| escape_edifact_value(&value))
                    .unwrap_or_default()
            } else {
                element
                    .children
                    .iter()
                    .filter_map(|component| component.value.as_ref().and_then(Value::as_string))
                    .map(|component| escape_edifact_value(&component))
                    .collect::<Vec<_>>()
                    .join(":")
            };

            elements.push(value);
        }

        if !elements.is_empty() {
            segment.push('+');
            segment.push_str(&elements.join("+"));
        }

        segment.push('\'');
        segments.push(segment);
    }

    for child in &node.children {
        collect_edifact_segments(child, segments);
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::NamedTempFile;

    fn create_test_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(content.as_bytes()).expect("write file");
        file
    }

    fn valid_multi_message_file(message_count: usize) -> String {
        let mut edi = String::from("UNA:+.? '\nUNB+UNOA:3+SENDER+RECEIVER+240101:1200+1'\n");

        for index in 1..=message_count {
            writeln!(
                edi,
                "UNH+{index}+ORDERS:D:96A:UN'\nBGM+220+PO{index}+9'\nUNT+3+{index}'"
            )
            .expect("write to string");
        }

        writeln!(edi, "UNZ+{message_count}+1'").expect("write to string");
        edi
    }

    fn partial_message_file() -> String {
        [
            "UNA:+.? '",
            "UNB+UNOA:3+SENDER+RECEIVER+240101:1200+1'",
            "UNH+1+ORDERS:D:96A:UN'",
            "BGM+220+PO1+9'",
            "UNZ+1+1'",
            "",
        ]
        .join("\n")
    }

    struct FailNthValidator {
        fail_on_call: usize,
        calls: AtomicUsize,
    }

    impl FailNthValidator {
        fn new(fail_on_call: usize) -> Self {
            Self {
                fail_on_call,
                calls: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Validator for FailNthValidator {
        fn validate(&self, _content: &str) -> Result<Vec<ValidationError>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_on_call {
                Ok(vec![ValidationError {
                    message: format!("Validation failed on message {call}"),
                    location: Some(format!("/MESSAGE[{call}]")),
                    severity: ErrorSeverity::Error,
                }])
            } else {
                Ok(Vec::new())
            }
        }
    }

    struct CountingMapper {
        calls: AtomicUsize,
    }

    impl CountingMapper {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Mapper for CountingMapper {
        fn map(&self, content: &str) -> Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(content.to_string())
        }
    }

    fn sample_document() -> Document {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut segment = Node::new("BGM", NodeType::Segment);
        segment.add_child(Node::with_value(
            "1004",
            NodeType::Element,
            Value::String("PO1".to_string()),
        ));
        root.add_child(segment);
        Document::new(root)
    }

    #[test]
    fn test_render_output_without_mapper_uses_selected_format() {
        let document = sample_document();

        let json = render_output(OutputFormat::Json, &document, None).expect("json");
        assert!(json.contains("\"name\": \"ROOT\""));

        let csv = render_output(OutputFormat::Csv, &document, None).expect("csv");
        assert!(csv.starts_with("name,node_type,value\n"));

        let xml = render_output(OutputFormat::Xml, &document, None).expect("xml");
        assert!(xml.starts_with("<document>"));

        let edi = render_output(OutputFormat::Edifact, &document, None).expect("edi");
        assert_eq!(edi, "BGM+PO1'");
    }

    #[test]
    fn test_render_output_prefers_mapper_payload_when_present() {
        let document = sample_document();
        let mapped = "mapped-payload";

        let json = render_output(OutputFormat::Json, &document, Some(mapped)).expect("json");
        let csv = render_output(OutputFormat::Csv, &document, Some(mapped)).expect("csv");
        let xml = render_output(OutputFormat::Xml, &document, Some(mapped)).expect("xml");
        let edi = render_output(OutputFormat::Edifact, &document, Some(mapped)).expect("edi");

        assert_eq!(json, mapped);
        assert_eq!(csv, mapped);
        assert_eq!(xml, mapped);
        assert_eq!(edi, mapped);
    }

    #[test]
    fn test_xml_output_sanitizes_invalid_element_names() {
        let mut root = Node::new("xml root", NodeType::Root);
        root.add_child(Node::with_value(
            "9 bad tag",
            NodeType::Element,
            Value::String("value".to_string()),
        ));

        let xml = render_output(OutputFormat::Xml, &Document::new(root), None).expect("xml");

        assert!(xml.contains("<_xml_root>"));
        assert!(xml.contains("<__bad_tag>value</__bad_tag>"));
    }

    #[test]
    fn test_edifact_output_escapes_release_characters() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut segment = Node::new("FTX", NodeType::Segment);
        segment.add_child(Node::with_value(
            "4440",
            NodeType::Element,
            Value::String("A+B:C'?D".to_string()),
        ));
        root.add_child(segment);

        let edi = render_output(OutputFormat::Edifact, &Document::new(root), None).expect("edi");

        assert_eq!(edi, "FTX+A?+B?:C?'??D'");
    }

    #[test]
    fn test_process_file_multi_message_counts() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let file = create_test_file(&valid_multi_message_file(2));
        let result = pipeline.process_file(file.path()).expect("process file");

        assert!(result.success);
        assert_eq!(result.message_count, 2);
        assert_eq!(result.success_count, 2);
        assert_eq!(result.failure_count, 0);

        let stats = pipeline.stats();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.files_successful, 1);
        assert_eq!(stats.messages_processed, 2);
        assert_eq!(stats.messages_successful, 2);
        assert_eq!(stats.messages_failed, 0);
    }

    #[test]
    fn test_process_with_validation_fail_all_stops_on_first_failure() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::FailAll,
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let validator = FailNthValidator::new(2);
        let file = create_test_file(&valid_multi_message_file(3));

        let error = pipeline
            .process_with_validation(file.path(), &validator)
            .expect_err("should fail fast");

        assert!(error.to_string().contains("Validation failed on message 2"));
        assert_eq!(validator.call_count(), 2);

        let stats = pipeline.stats();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.files_failed, 1);
        assert_eq!(stats.messages_processed, 2);
        assert_eq!(stats.messages_successful, 1);
        assert_eq!(stats.messages_failed, 1);
    }

    #[test]
    fn test_process_with_validation_quarantine_continues() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::Quarantine,
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let validator = FailNthValidator::new(2);
        let file = create_test_file(&valid_multi_message_file(3));

        let result = pipeline
            .process_with_validation(file.path(), &validator)
            .expect("quarantine should continue");

        assert!(result.success);
        assert_eq!(result.message_count, 3);
        assert_eq!(result.success_count, 2);
        assert_eq!(result.failure_count, 1);
        assert!(result.quarantined);
        assert_eq!(pipeline.quarantine().len(), 1);

        let stats = pipeline.stats();
        assert_eq!(stats.files_quarantined, 1);
        assert_eq!(stats.messages_processed, 3);
        assert_eq!(stats.messages_successful, 2);
        assert_eq!(stats.messages_failed, 1);
    }

    #[test]
    fn test_process_with_validation_accept_all_continues() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::AcceptAll,
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let validator = FailNthValidator::new(2);
        let file = create_test_file(&valid_multi_message_file(3));

        let result = pipeline
            .process_with_validation(file.path(), &validator)
            .expect("accept-all should continue");

        assert!(result.success);
        assert_eq!(result.message_count, 3);
        assert_eq!(result.success_count, 2);
        assert_eq!(result.failure_count, 1);
        assert!(!result.quarantined);
        assert!(pipeline.quarantine().is_empty());
    }

    #[test]
    fn test_streaming_mode_processes_messages() {
        let config = PipelineConfig {
            streaming: true,
            batch_config: BatchConfig {
                max_size: 2,
                ..BatchConfig::default()
            },
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let file = create_test_file(&valid_multi_message_file(4));
        let result = pipeline.process_file(file.path()).expect("stream process");

        assert!(result.success);
        assert_eq!(result.message_count, 4);
        assert_eq!(result.success_count, 4);
        assert_eq!(pipeline.stats().messages_processed, 4);
    }

    #[test]
    fn test_process_with_mapping_invokes_mapper_per_message() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let mapper = CountingMapper::new();
        let file = create_test_file(&valid_multi_message_file(2));

        let result = pipeline
            .process_with_mapping(file.path(), &mapper)
            .expect("process with mapping");

        assert!(result.success);
        assert_eq!(result.message_count, 2);
        assert_eq!(mapper.call_count(), 2);
    }

    #[test]
    fn test_process_batch_applies_retries_and_max_size() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::AcceptAll,
            batch_config: BatchConfig {
                max_size: 1,
                max_retries: 1,
                ..BatchConfig::default()
            },
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let bad_file = create_test_file("");
        let good_file = create_test_file(&valid_multi_message_file(1));

        let paths = vec![bad_file.path(), good_file.path()];
        let result = pipeline.process_batch(&paths).expect("batch processing");

        assert_eq!(result.total_files, 2);
        assert_eq!(result.file_results.len(), 2);
        assert_eq!(result.successful_files, 1);
        assert_eq!(result.failed_files, 1);
        assert!(result.batch_success);

        // bad file retried once (2 attempts) + good file once = 3 total file attempts
        assert_eq!(pipeline.stats().files_processed, 3);
    }

    #[test]
    fn test_process_batch_fail_all_stops_early() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::FailAll,
            strictness: StrictnessLevel::Strict,
            batch_config: BatchConfig {
                max_retries: 0,
                ..BatchConfig::default()
            },
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let bad_file = create_test_file(&partial_message_file());
        let good_file = create_test_file(&valid_multi_message_file(1));

        let paths = vec![bad_file.path(), good_file.path()];
        let result = pipeline.process_batch(&paths).expect("batch result");

        assert_eq!(result.total_files, 2);
        assert_eq!(result.file_results.len(), 1);
        assert_eq!(result.failed_files, 1);
        assert!(!result.batch_success);
        assert_eq!(pipeline.stats().files_processed, 1);
    }

    #[test]
    fn test_metrics_include_throughput() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let file = create_test_file(&valid_multi_message_file(1));
        let _ = pipeline.process_file(file.path()).expect("process file");

        let metrics = pipeline.metrics();
        assert!(metrics.files_per_second >= 0.0);
        assert!(metrics.messages_per_second >= 0.0);
        assert!(metrics.throughput_mbps >= 0.0);
    }

    #[test]
    fn test_metrics_fallback_to_total_processing_time_without_started_at() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.stats.files_processed = 2;
        pipeline.stats.messages_processed = 4;
        pipeline.stats.bytes_processed = 1_000_000;
        pipeline.stats.total_processing_time = Duration::from_secs(2);

        let metrics = pipeline.metrics();
        assert!(metrics.throughput_mbps > 0.0);
        assert!(metrics.messages_per_second > 0.0);
        assert!(metrics.files_per_second > 0.0);
    }

    #[test]
    fn test_process_file_initializes_started_at_without_explicit_start() {
        let mut pipeline = Pipeline::with_defaults();

        let file = create_test_file(&valid_multi_message_file(1));
        let result = pipeline.process_file(file.path()).expect("process file");

        assert!(result.success);
        assert_eq!(pipeline.stats().messages_processed, 1);
        assert!(pipeline.stats().started_at.is_some());
    }

    #[test]
    fn test_process_file_preflight_error_does_not_initialize_started_at() {
        let mut pipeline = Pipeline::with_defaults();

        let result = pipeline.process_file(PathBuf::from("/path/does/not/exist.edi"));
        assert!(result.is_err());
        assert!(pipeline.stats().started_at.is_none());
    }

    #[test]
    fn test_file_not_found_returns_error() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let result = pipeline.process_file(PathBuf::from("/path/does/not/exist.edi"));
        assert!(result.is_err());
    }
}

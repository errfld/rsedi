//! Pipeline orchestration
//!
//! This module provides the main Pipeline for processing EDI files
//! with support for validation, mapping, batching, and streaming.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::{
    AcceptancePolicy, BatchConfig, Error, QuarantineReason, QuarantineStore, Result,
    StrictnessLevel,
};

/// Configuration for the pipeline
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Acceptance policy for handling errors
    pub acceptance_policy: AcceptancePolicy,
    /// Strictness level for validation
    pub strictness: StrictnessLevel,
    /// Batch configuration
    pub batch_config: BatchConfig,
    /// Whether to enable streaming mode
    pub streaming: bool,
    /// Maximum file size in bytes
    pub max_file_size: usize,
    /// Whether to validate before processing
    pub validate_before_processing: bool,
    /// Whether to apply mapping transformations
    pub enable_mapping: bool,
    /// Output format
    pub output_format: OutputFormat,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
            batch_config: BatchConfig::default(),
            streaming: false,
            max_file_size: 100 * 1024 * 1024, // 100MB
            validate_before_processing: true,
            enable_mapping: true,
            output_format: OutputFormat::default(),
        }
    }
}

/// Output format for processed documents
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Native EDI format
    #[default]
    Edifact,
    /// JSON format
    Json,
    /// CSV format
    Csv,
    /// XML format
    Xml,
}

/// Main pipeline for processing EDI files
#[derive(Debug)]
pub struct Pipeline {
    /// Pipeline configuration
    config: PipelineConfig,
    /// Quarantine store for failed messages
    quarantine: QuarantineStore<String>,
    /// Processing statistics
    stats: PipelineStats,
    /// Whether pipeline is running
    running: bool,
}

/// Statistics for pipeline processing
#[derive(Debug, Default, Clone)]
pub struct PipelineStats {
    /// Total files processed
    pub files_processed: usize,
    /// Total files successful
    pub files_successful: usize,
    /// Total files failed
    pub files_failed: usize,
    /// Total files quarantined
    pub files_quarantined: usize,
    /// Total messages processed
    pub messages_processed: usize,
    /// Total messages successful
    pub messages_successful: usize,
    /// Total messages failed
    pub messages_failed: usize,
    /// Total validation errors
    pub validation_errors: usize,
    /// Total processing time
    pub total_processing_time: Duration,
    /// Pipeline start time
    pub started_at: Option<Instant>,
}

/// Result of processing a single file
#[derive(Debug, Clone)]
pub struct FileResult {
    /// File path
    pub path: String,
    /// Whether processing succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Number of messages in file
    pub message_count: usize,
    /// Number of successful messages
    pub success_count: usize,
    /// Number of failed messages
    pub failure_count: usize,
    /// Processing duration
    pub duration: Duration,
    /// Whether file was quarantined
    pub quarantined: bool,
}

/// Result of processing a batch of files
#[derive(Debug)]
pub struct PipelineBatchResult {
    /// Results for individual files
    pub file_results: Vec<FileResult>,
    /// Total files processed
    pub total_files: usize,
    /// Successful files
    pub successful_files: usize,
    /// Failed files
    pub failed_files: usize,
    /// Quarantined files
    pub quarantined_files: usize,
    /// Total processing time
    pub total_duration: Duration,
    /// Whether batch succeeded overall
    pub batch_success: bool,
}

/// Metrics collected during processing
#[derive(Debug, Default, Clone)]
pub struct PipelineMetrics {
    /// Files per second
    pub files_per_second: f64,
    /// Messages per second
    pub messages_per_second: f64,
    /// Average file processing time
    pub avg_file_time_ms: f64,
    /// Error rate percentage
    pub error_rate: f64,
    /// Current throughput
    pub throughput_mbps: f64,
}

impl Pipeline {
    /// Create a new pipeline with the given configuration
    pub fn new(config: PipelineConfig) -> Self {
        Self {
            config: config.clone(),
            quarantine: QuarantineStore::with_defaults(),
            stats: PipelineStats::default(),
            running: false,
        }
    }

    /// Create a pipeline with default configuration
    pub fn with_defaults() -> Self {
        Self::new(PipelineConfig::default())
    }

    /// Start the pipeline
    pub fn start(&mut self) {
        self.running = true;
        self.stats.started_at = Some(Instant::now());
    }

    /// Stop the pipeline
    pub fn stop(&mut self) {
        self.running = false;
    }

    /// Check if pipeline is running
    pub fn is_running(&self) -> bool {
        self.running
    }

    /// Process a single file
    pub fn process_file<P: AsRef<Path>>(&mut self, path: P) -> Result<FileResult> {
        let start = Instant::now();
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Check if file exists
        if !path.as_ref().exists() {
            return Err(Error::Pipeline(format!("File not found: {}", path_str)));
        }

        // Check file size
        let metadata = std::fs::metadata(&path)?;
        if metadata.len() > self.config.max_file_size as u64 {
            return Err(Error::Pipeline(format!(
                "File too large: {} bytes (max: {} bytes)",
                metadata.len(),
                self.config.max_file_size
            )));
        }

        // Read file content
        let content = std::fs::read_to_string(&path)?;

        // Simulate processing (in real implementation, this would parse and process EDI)
        let result = self.process_content(&content, &path_str);

        let duration = start.elapsed();

        // Update stats
        self.stats.files_processed += 1;
        self.stats.total_processing_time += duration;

        match &result {
            Ok(file_result) => {
                if file_result.success {
                    self.stats.files_successful += 1;
                    self.stats.messages_successful += file_result.success_count;
                } else {
                    self.stats.files_failed += 1;
                    self.stats.messages_failed += file_result.failure_count;

                    // Quarantine if configured
                    if matches!(self.config.acceptance_policy, AcceptancePolicy::Quarantine) {
                        self.quarantine.quarantine(
                            &path_str,
                            content,
                            QuarantineReason::ProcessingError,
                            file_result.error.clone().unwrap_or_default(),
                        )?;
                    }
                }
            }
            Err(e) => {
                self.stats.files_failed += 1;

                // Handle error based on policy
                match self.config.acceptance_policy {
                    AcceptancePolicy::AcceptAll => {
                        // Continue processing
                    }
                    AcceptancePolicy::FailAll => {
                        return Err(e.clone());
                    }
                    AcceptancePolicy::Quarantine => {
                        self.quarantine.quarantine(
                            &path_str,
                            content,
                            QuarantineReason::ProcessingError,
                            e.to_string(),
                        )?;
                        self.stats.files_quarantined += 1;
                    }
                }
            }
        }

        result
    }

    /// Process file content (simulated)
    fn process_content(&self, content: &str, path: &str) -> Result<FileResult> {
        let start = Instant::now();

        // Simulate validation
        if self.config.validate_before_processing {
            // In real implementation, validate against schema
            if content.is_empty() {
                return Ok(FileResult {
                    path: path.to_string(),
                    success: false,
                    error: Some("Empty file".to_string()),
                    message_count: 0,
                    success_count: 0,
                    failure_count: 0,
                    duration: start.elapsed(),
                    quarantined: false,
                });
            }
        }

        // Simulate message processing
        // In real implementation, parse EDI and process messages
        let message_count = content.lines().count().max(1);
        let success_count = message_count;
        let failure_count = 0;

        // Simulate mapping if enabled
        if self.config.enable_mapping {
            // Apply transformations
        }

        let duration = start.elapsed();

        Ok(FileResult {
            path: path.to_string(),
            success: true,
            error: None,
            message_count,
            success_count,
            failure_count,
            duration,
            quarantined: false,
        })
    }

    /// Process multiple files as a batch
    pub fn process_batch<P: AsRef<Path>>(&mut self, paths: &[P]) -> Result<PipelineBatchResult> {
        let start = Instant::now();
        let mut file_results = Vec::new();
        let mut successful = 0;
        let mut failed = 0;
        let mut quarantined = 0;

        for path in paths {
            match self.process_file(path) {
                Ok(result) => {
                    if result.success {
                        successful += 1;
                    } else {
                        failed += 1;
                    }
                    if result.quarantined {
                        quarantined += 1;
                    }
                    file_results.push(result);
                }
                Err(e) => {
                    failed += 1;
                    file_results.push(FileResult {
                        path: path.as_ref().to_string_lossy().to_string(),
                        success: false,
                        error: Some(e.to_string()),
                        message_count: 0,
                        success_count: 0,
                        failure_count: 0,
                        duration: Duration::ZERO,
                        quarantined: false,
                    });

                    // Handle based on policy
                    match self.config.acceptance_policy {
                        AcceptancePolicy::FailAll => {
                            break;
                        }
                        _ => continue,
                    }
                }
            }
        }

        let total_duration = start.elapsed();

        // Determine overall batch success based on policy
        let batch_success = match self.config.acceptance_policy {
            AcceptancePolicy::AcceptAll => true,
            AcceptancePolicy::FailAll => failed == 0,
            AcceptancePolicy::Quarantine => true,
        };

        Ok(PipelineBatchResult {
            file_results,
            total_files: paths.len(),
            successful_files: successful,
            failed_files: failed,
            quarantined_files: quarantined,
            total_duration,
            batch_success,
        })
    }

    /// Process with validation integration
    pub fn process_with_validation<P: AsRef<Path>>(
        &mut self,
        path: P,
        _validator: &dyn Validator,
    ) -> Result<FileResult> {
        // In real implementation, integrate with validation engine
        self.process_file(path)
    }

    /// Process with mapping integration
    pub fn process_with_mapping<P: AsRef<Path>>(
        &mut self,
        path: P,
        _mapper: &dyn Mapper,
    ) -> Result<FileResult> {
        // In real implementation, integrate with mapping engine
        self.process_file(path)
    }

    /// Get current statistics
    pub fn stats(&self) -> &PipelineStats {
        &self.stats
    }

    /// Get metrics
    pub fn metrics(&self) -> PipelineMetrics {
        let elapsed = self
            .stats
            .started_at
            .map(|t| t.elapsed())
            .unwrap_or_default();

        let elapsed_secs = elapsed.as_secs_f64();

        PipelineMetrics {
            files_per_second: if elapsed_secs > 0.0 {
                self.stats.files_processed as f64 / elapsed_secs
            } else {
                0.0
            },
            messages_per_second: if elapsed_secs > 0.0 {
                self.stats.messages_processed as f64 / elapsed_secs
            } else {
                0.0
            },
            avg_file_time_ms: if self.stats.files_processed > 0 {
                self.stats.total_processing_time.as_millis() as f64
                    / self.stats.files_processed as f64
            } else {
                0.0
            },
            error_rate: if self.stats.files_processed > 0 {
                (self.stats.files_failed as f64 / self.stats.files_processed as f64) * 100.0
            } else {
                0.0
            },
            throughput_mbps: 0.0, // Would calculate from actual bytes processed
        }
    }

    /// Get quarantine store
    pub fn quarantine(&self) -> &QuarantineStore<String> {
        &self.quarantine
    }

    /// Get mutable quarantine store
    pub fn quarantine_mut(&mut self) -> &mut QuarantineStore<String> {
        &mut self.quarantine
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = PipelineStats::default();
    }

    /// Get configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }
}

/// Trait for validation integration
pub trait Validator {
    /// Validate content
    fn validate(&self, content: &str) -> Result<Vec<ValidationError>>;
}

/// Validation error
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error message
    pub message: String,
    /// Error location
    pub location: Option<String>,
    /// Severity
    pub severity: ErrorSeverity,
}

/// Error severity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Warning
    Warning,
    /// Error
    Error,
    /// Critical
    Critical,
}

/// Trait for mapping integration
pub trait Mapper {
    /// Apply mapping transformation
    fn map(&self, content: &str) -> Result<String>;
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[test]
    fn test_pipeline_creation() {
        let config = PipelineConfig::default();
        let pipeline = Pipeline::new(config);

        assert!(!pipeline.is_running());
        assert_eq!(pipeline.stats().files_processed, 0);
    }

    #[test]
    fn test_process_single_file() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let file = create_test_file("UNA:+.? 'UNB+UNOA:3+SENDER+RECEIVER+200101:1200+1234567'");

        let result = pipeline.process_file(file.path()).unwrap();
        assert!(result.success);
        assert_eq!(result.message_count, 1);

        assert_eq!(pipeline.stats().files_processed, 1);
        assert_eq!(pipeline.stats().files_successful, 1);
    }

    #[test]
    fn test_process_batch() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let file1 = create_test_file("Message 1");
        let file2 = create_test_file("Message 2");
        let file3 = create_test_file("Message 3");

        let paths = vec![file1.path(), file2.path(), file3.path()];
        let result = pipeline.process_batch(&paths).unwrap();

        assert_eq!(result.total_files, 3);
        assert_eq!(result.successful_files, 3);
        assert_eq!(result.failed_files, 0);
        assert!(result.batch_success);
    }

    #[test]
    fn test_pipeline_with_validation() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        // Test with empty file (should fail validation)
        let empty_file = create_test_file("");

        let result = pipeline.process_file(empty_file.path()).unwrap();
        assert!(!result.success);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_pipeline_with_mapping() {
        let config = PipelineConfig {
            enable_mapping: true,
            ..Default::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let file = create_test_file("Test message");
        let result = pipeline.process_file(file.path()).unwrap();

        assert!(result.success);
    }

    #[test]
    fn test_error_handling() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        // Try to process non-existent file
        let result = pipeline.process_file("/nonexistent/file.edi");
        assert!(result.is_err());

        assert_eq!(pipeline.stats().files_processed, 0);
    }

    #[test]
    fn test_metrics_collection() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        // Process some files
        for i in 0..5 {
            let file = create_test_file(&format!("Message {}", i));
            pipeline.process_file(file.path()).unwrap();
        }

        let metrics = pipeline.metrics();
        assert!(metrics.files_per_second >= 0.0);
        assert!(metrics.avg_file_time_ms >= 0.0);
        assert_eq!(metrics.error_rate, 0.0);
    }

    #[test]
    fn test_accept_all_policy() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::AcceptAll,
            ..Default::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let file1 = create_test_file("Valid");
        let file2 = create_test_file("Also valid");

        let paths = vec![file1.path(), file2.path()];
        let result = pipeline.process_batch(&paths).unwrap();

        assert!(result.batch_success);
    }

    #[test]
    fn test_reject_all_policy() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::FailAll,
            ..Default::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        // This policy would stop on first error
        // For this test, we just verify the policy is set correctly
        assert!(matches!(
            pipeline.config().acceptance_policy,
            AcceptancePolicy::FailAll
        ));
    }

    #[test]
    fn test_quarantine_policy() {
        let config = PipelineConfig {
            acceptance_policy: AcceptancePolicy::Quarantine,
            validate_before_processing: true,
            ..Default::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        // Process an empty file which should fail validation
        let empty_file = create_test_file("");
        let result = pipeline.process_file(empty_file.path()).unwrap();

        assert!(!result.success);
        // File should be in quarantine
        assert!(!pipeline.quarantine().is_empty());
    }

    #[test]
    fn test_strict_strictness() {
        let config = PipelineConfig {
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        };
        let pipeline = Pipeline::new(config);

        assert!(matches!(
            pipeline.config().strictness,
            StrictnessLevel::Strict
        ));
    }

    #[test]
    fn test_moderate_strictness() {
        let config = PipelineConfig {
            strictness: StrictnessLevel::Standard,
            ..Default::default()
        };
        let pipeline = Pipeline::new(config);

        assert!(matches!(
            pipeline.config().strictness,
            StrictnessLevel::Standard
        ));
    }

    #[test]
    fn test_lenient_strictness() {
        let config = PipelineConfig {
            strictness: StrictnessLevel::Permissive,
            ..Default::default()
        };
        let pipeline = Pipeline::new(config);

        assert!(matches!(
            pipeline.config().strictness,
            StrictnessLevel::Permissive
        ));
    }

    #[test]
    fn test_policy_combinations() {
        // Test all combinations of Policy and Strictness
        let policies = vec![
            AcceptancePolicy::AcceptAll,
            AcceptancePolicy::FailAll,
            AcceptancePolicy::Quarantine,
        ];

        let strictness_levels = vec![
            StrictnessLevel::Permissive,
            StrictnessLevel::Standard,
            StrictnessLevel::Strict,
        ];

        for policy in &policies {
            for strictness in &strictness_levels {
                let config = PipelineConfig {
                    acceptance_policy: *policy,
                    strictness: *strictness,
                    ..Default::default()
                };
                let _pipeline = Pipeline::new(config);
                // Just verify creation succeeds for all combinations
            }
        }
    }

    #[test]
    fn test_file_too_large() {
        let config = PipelineConfig {
            max_file_size: 10, // Very small
            ..Default::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let file = create_test_file("This content is definitely more than 10 bytes");
        let result = pipeline.process_file(file.path());

        assert!(result.is_err());
    }

    #[test]
    fn test_reset_stats() {
        let mut pipeline = Pipeline::with_defaults();
        pipeline.start();

        let file = create_test_file("Test");
        pipeline.process_file(file.path()).unwrap();

        assert_eq!(pipeline.stats().files_processed, 1);

        pipeline.reset_stats();

        assert_eq!(pipeline.stats().files_processed, 0);
    }

    #[test]
    fn test_pipeline_start_stop() {
        let mut pipeline = Pipeline::with_defaults();

        assert!(!pipeline.is_running());

        pipeline.start();
        assert!(pipeline.is_running());

        pipeline.stop();
        assert!(!pipeline.is_running());
    }

    #[test]
    fn test_output_formats() {
        let formats = vec![
            OutputFormat::Edifact,
            OutputFormat::Json,
            OutputFormat::Csv,
            OutputFormat::Xml,
        ];

        for format in formats {
            let config = PipelineConfig {
                output_format: format,
                ..Default::default()
            };
            let _pipeline = Pipeline::new(config);
        }
    }
}

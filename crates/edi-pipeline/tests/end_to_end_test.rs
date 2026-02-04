//! End-to-end integration tests for edi-pipeline

use std::io::Write;
use tempfile::NamedTempFile;

use edi_pipeline::{
    AcceptancePolicy, Batch, BatchConfig, Pipeline, PipelineConfig, QuarantineStore,
    StrictnessLevel,
};

/// Helper to create a test file with EDI content
fn create_edi_file(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file
}

/// Helper to create a valid EDI message
fn valid_edi_message() -> String {
    "UNA:+.? '\n\
     UNB+UNOA:3+SENDER+RECEIVER+200101:1200+1234567'\n\
     UNH+1+ORDERS:D:96A:UN'\n\
     BGM+220+PO12345+9'\n\
     DTM+137:20200101:102'\n\
     UNT+5+1'\n\
     UNZ+1+1234567'\n"
        .to_string()
}

/// Helper to create an invalid EDI message
fn invalid_edi_message() -> String {
    "INVALID EDI CONTENT\nTHIS IS NOT VALID\n".to_string()
}

#[test]
fn test_end_to_end_single_file_processing() {
    let mut pipeline = Pipeline::with_defaults();
    pipeline.start();

    let file = create_edi_file(&valid_edi_message());
    let result = pipeline.process_file(file.path()).unwrap();

    assert!(result.success);
    assert!(result.message_count > 0);
    assert_eq!(result.failure_count, 0);

    let stats = pipeline.stats();
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_successful, 1);
}

#[test]
fn test_end_to_end_batch_processing() {
    let config = PipelineConfig {
        batch_config: BatchConfig {
            max_size: 10,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut pipeline = Pipeline::new(config);
    pipeline.start();

    // Create multiple test files
    let files: Vec<_> = (0..5)
        .map(|i| create_edi_file(&format!("{}Message {}", valid_edi_message(), i)))
        .collect();

    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let result = pipeline.process_batch(&paths).unwrap();

    assert_eq!(result.total_files, 5);
    assert_eq!(result.successful_files, 5);
    assert!(result.batch_success);

    let stats = pipeline.stats();
    assert_eq!(stats.files_processed, 5);
}

#[test]
fn test_end_to_end_mixed_batch() {
    let config = PipelineConfig {
        acceptance_policy: AcceptancePolicy::AcceptAll,
        ..Default::default()
    };
    let mut pipeline = Pipeline::new(config);
    pipeline.start();

    // Create mix of valid and invalid files
    let valid_file = create_edi_file(&valid_edi_message());
    let invalid_file = create_edi_file(&invalid_edi_message());

    let paths = vec![valid_file.path(), invalid_file.path()];
    let result = pipeline.process_batch(&paths).unwrap();

    // AcceptAll policy means batch succeeds even with failures
    assert!(result.batch_success);
    assert_eq!(result.total_files, 2);
}

#[test]
fn test_end_to_end_quarantine_workflow() {
    let config = PipelineConfig {
        acceptance_policy: AcceptancePolicy::Quarantine,
        validate_before_processing: true,
        ..Default::default()
    };
    let mut pipeline = Pipeline::new(config);
    pipeline.start();

    // Process a file that will fail validation
    let invalid_file = create_edi_file(""); // Empty file
    let result = pipeline.process_file(invalid_file.path()).unwrap();

    assert!(!result.success);
    assert!(!pipeline.quarantine().is_empty());

    // Verify message is in quarantine with metadata
    let quarantined = pipeline.quarantine().get_all();
    assert_eq!(quarantined.len(), 1);
}

#[test]
fn test_end_to_end_strict_validation() {
    let config = PipelineConfig {
        strictness: StrictnessLevel::Strict,
        acceptance_policy: AcceptancePolicy::FailAll,
        ..Default::default()
    };
    let mut pipeline = Pipeline::new(config);
    pipeline.start();

    // Process valid file
    let file = create_edi_file(&valid_edi_message());
    let result = pipeline.process_file(file.path()).unwrap();

    assert!(result.success);
    assert!(matches!(
        pipeline.config().strictness,
        StrictnessLevel::Strict
    ));
}

#[test]
fn test_end_to_end_pipeline_metrics() {
    let mut pipeline = Pipeline::with_defaults();
    pipeline.start();

    // Process several files
    for _ in 0..10 {
        let file = create_edi_file(&valid_edi_message());
        pipeline.process_file(file.path()).unwrap();
    }

    let metrics = pipeline.metrics();
    assert!(metrics.files_per_second > 0.0);
    assert!(metrics.avg_file_time_ms >= 0.0);
    assert_eq!(metrics.error_rate, 0.0);

    let stats = pipeline.stats();
    assert_eq!(stats.files_processed, 10);
}

#[test]
fn test_end_to_end_error_recovery() {
    let config = PipelineConfig {
        acceptance_policy: AcceptancePolicy::AcceptAll,
        ..Default::default()
    };
    let mut pipeline = Pipeline::new(config);
    pipeline.start();

    // Process files where some fail
    let files: Vec<_> = (0..5)
        .map(|i| {
            if i % 2 == 0 {
                create_edi_file(&valid_edi_message())
            } else {
                create_edi_file(&invalid_edi_message())
            }
        })
        .collect();

    let paths: Vec<_> = files.iter().map(|f| f.path()).collect();
    let result = pipeline.process_batch(&paths).unwrap();

    // Should process all files despite errors
    assert_eq!(result.total_files, 5);
    assert!(result.batch_success); // AcceptAll means success even with failures
}

#[test]
fn test_batch_integration() {
    let items: Vec<(String, i32)> = (0..10).map(|i| (format!("item-{}", i), i)).collect();

    let config = BatchConfig {
        max_size: 10,
        preserve_order: true,
        ..Default::default()
    };

    let mut batch = Batch::from_items(items, config).unwrap();

    // Mark some as success, some as failed
    for i in 0..10 {
        if i % 2 == 0 {
            batch.mark_success(&format!("item-{}", i)).unwrap();
        } else {
            batch.mark_failed(&format!("item-{}", i), "error").unwrap();
        }
    }

    let result = batch.into_result();
    assert_eq!(result.successful.len(), 5);
    // Failed items go to retry bucket when batch.retry_count < batch.max_retries (default 3)
    assert_eq!(result.retry.len(), 5);
    assert_eq!(result.failed.len(), 0);
}

#[test]
fn test_quarantine_retry_workflow() {
    let mut quarantine = QuarantineStore::with_defaults();

    // Quarantine a message
    let id = quarantine
        .quarantine(
            "test-msg",
            "data",
            edi_pipeline::QuarantineReason::ProcessingError,
            "error",
        )
        .unwrap();

    assert_eq!(quarantine.len(), 1);

    // Mark for retry
    quarantine.mark_for_retry(&id).unwrap();

    // Retry the message
    let (retry_id, data) = quarantine.retry(&id).unwrap();
    assert_eq!(retry_id, id);
    assert_eq!(data, "data");
    assert_eq!(quarantine.len(), 0);
}

#[test]
fn test_pipeline_with_different_policies() {
    let policies = vec![
        AcceptancePolicy::AcceptAll,
        AcceptancePolicy::FailAll,
        AcceptancePolicy::Quarantine,
    ];

    for policy in policies {
        let config = PipelineConfig {
            acceptance_policy: policy,
            ..Default::default()
        };
        let mut pipeline = Pipeline::new(config);
        pipeline.start();

        let file = create_edi_file(&valid_edi_message());
        let result = pipeline.process_file(file.path()).unwrap();

        assert!(result.success);
    }
}

#[test]
fn test_pipeline_state_management() {
    let mut pipeline = Pipeline::with_defaults();

    assert!(!pipeline.is_running());

    pipeline.start();
    assert!(pipeline.is_running());

    let file = create_edi_file(&valid_edi_message());
    pipeline.process_file(file.path()).unwrap();

    pipeline.stop();
    assert!(!pipeline.is_running());

    // Stats should persist
    assert_eq!(pipeline.stats().files_processed, 1);

    // Reset stats
    pipeline.reset_stats();
    assert_eq!(pipeline.stats().files_processed, 0);
}

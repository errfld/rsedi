//! Streaming support for EDI processing
//!
//! This module provides streaming capabilities for processing EDI files
//! with support for backpressure, checkpoints, and parallel processing.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

use crate::{AcceptancePolicy, Error, Result, StrictnessLevel};

/// Configuration for streaming processing
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Maximum number of messages to process in parallel
    pub max_concurrency: usize,
    /// Channel buffer size for backpressure
    pub channel_buffer_size: usize,
    /// Timeout for individual message processing
    pub message_timeout: Duration,
    /// Whether to enable checkpointing
    pub enable_checkpointing: bool,
    /// Checkpoint interval
    pub checkpoint_interval: Option<Duration>,
    /// Acceptance policy for errors
    pub acceptance_policy: AcceptancePolicy,
    /// Strictness level
    pub strictness: StrictnessLevel,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 4,
            channel_buffer_size: 100,
            message_timeout: Duration::from_secs(30),
            enable_checkpointing: true,
            checkpoint_interval: Some(Duration::from_secs(60)),
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
        }
    }
}

/// A checkpoint for resuming processing
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Position in the stream (message index)
    pub position: usize,
    /// Byte offset in source
    pub byte_offset: usize,
    /// Timestamp when checkpoint was created
    pub timestamp: Instant,
    /// Number of messages successfully processed
    pub processed_count: usize,
    /// Number of messages failed
    pub failed_count: usize,
}

impl Checkpoint {
    /// Create a new checkpoint at the given position
    pub fn new(position: usize, byte_offset: usize) -> Self {
        Self {
            position,
            byte_offset,
            timestamp: Instant::now(),
            processed_count: 0,
            failed_count: 0,
        }
    }

    /// Create a checkpoint at position 0
    pub fn origin() -> Self {
        Self::new(0, 0)
    }
}

/// A message in the stream
#[derive(Debug)]
pub struct StreamMessage<T> {
    /// Message index in stream
    pub index: usize,
    /// Message data
    pub data: T,
    /// Whether message has been processed
    pub processed: bool,
    /// Error if processing failed
    pub error: Option<String>,
    /// Processing timestamp
    pub processed_at: Option<Instant>,
}

impl<T> StreamMessage<T> {
    /// Create a new stream message
    pub fn new(index: usize, data: T) -> Self {
        Self {
            index,
            data,
            processed: false,
            error: None,
            processed_at: None,
        }
    }

    /// Mark as processed successfully
    pub fn mark_success(&mut self) {
        self.processed = true;
        self.processed_at = Some(Instant::now());
    }

    /// Mark as failed
    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.processed = true;
        self.error = Some(error.into());
        self.processed_at = Some(Instant::now());
    }

    /// Check if message succeeded
    pub fn is_success(&self) -> bool {
        self.processed && self.error.is_none()
    }

    /// Check if message failed
    pub fn is_failed(&self) -> bool {
        self.processed && self.error.is_some()
    }
}

/// Streaming processor for EDI messages
pub struct StreamProcessor<T> {
    /// Configuration
    config: StreamConfig,
    /// Current checkpoint
    checkpoint: Arc<Mutex<Checkpoint>>,
    /// Message queue
    message_queue: Arc<Mutex<VecDeque<StreamMessage<T>>>>,
    /// Semaphore for limiting concurrency
    semaphore: Arc<Semaphore>,
    /// Statistics
    stats: Arc<Mutex<StreamStats>>,
}

/// Statistics for stream processing
#[derive(Debug, Default)]
pub struct StreamStats {
    /// Total messages received
    pub received: usize,
    /// Messages successfully processed
    pub succeeded: usize,
    /// Messages failed
    pub failed: usize,
    /// Messages currently in flight
    pub in_flight: usize,
    /// Processing start time
    pub started_at: Option<Instant>,
}

impl StreamStats {
    /// Create new stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Get processing rate (messages per second)
    pub fn rate(&self) -> f64 {
        match self.started_at {
            Some(start) => {
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    (self.succeeded + self.failed) as f64 / elapsed
                } else {
                    0.0
                }
            }
            None => 0.0,
        }
    }

    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.succeeded + self.failed;
        if total == 0 {
            100.0
        } else {
            (self.succeeded as f64 / total as f64) * 100.0
        }
    }
}

impl<T: Send + 'static> StreamProcessor<T> {
    /// Create a new stream processor
    pub fn new(config: StreamConfig) -> Self {
        Self {
            config: config.clone(),
            checkpoint: Arc::new(Mutex::new(Checkpoint::origin())),
            message_queue: Arc::new(Mutex::new(VecDeque::with_capacity(
                config.channel_buffer_size,
            ))),
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            stats: Arc::new(Mutex::new(StreamStats::new())),
        }
    }

    /// Submit a message to the stream
    pub async fn submit(&self, message: StreamMessage<T>) -> Result<()> {
        let mut queue = self.message_queue.lock().await;

        if queue.len() >= self.config.channel_buffer_size {
            return Err(Error::Streaming("Channel buffer full".to_string()));
        }

        queue.push_back(message);

        let mut stats = self.stats.lock().await;
        stats.received += 1;

        Ok(())
    }

    /// Process a single message
    pub async fn process_single<F, Fut>(&self, processor: F) -> Result<()>
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::Streaming(format!("Semaphore error: {}", e)))?;

        let mut queue = self.message_queue.lock().await;
        let message = queue
            .pop_front()
            .ok_or_else(|| Error::Streaming("No messages to process".to_string()))?;

        drop(queue); // Release lock before processing

        let mut stats = self.stats.lock().await;
        if stats.started_at.is_none() {
            stats.started_at = Some(Instant::now());
        }
        stats.in_flight += 1;
        drop(stats);

        // Extract data before processing (avoid borrow after move)
        let data = message.data;
        let index = message.index;
        
        // Process with timeout
        let result = tokio::time::timeout(self.config.message_timeout, processor(data))
            .await;

        let mut stats = self.stats.lock().await;
        stats.in_flight -= 1;

        match result {
            Ok(Ok(())) => {
                stats.succeeded += 1;
            }
            Ok(Err(_)) => {
                stats.failed += 1;
            }
            Err(_) => {
                stats.failed += 1;
            }
        }

        // Update checkpoint
        let mut checkpoint = self.checkpoint.lock().await;
        checkpoint.position = index;
        
        // Note: We track processed/failed in stats, not checkpoint
        drop(checkpoint);

        Ok(())
    }

    /// Get current checkpoint
    pub async fn get_checkpoint(&self) -> Checkpoint {
        self.checkpoint.lock().await.clone()
    }

    /// Set checkpoint for resuming
    pub async fn set_checkpoint(&self, checkpoint: Checkpoint) {
        *self.checkpoint.lock().await = checkpoint;
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> StreamStats {
        self.stats.lock().await.clone()
    }

    /// Check if there's backpressure
    pub async fn has_backpressure(&self) -> bool {
        let queue = self.message_queue.lock().await;
        queue.len() >= self.config.channel_buffer_size / 2
    }

    /// Get queue size
    pub async fn queue_size(&self) -> usize {
        self.message_queue.lock().await.len()
    }

    /// Clear the stream
    pub async fn clear(&self) {
        let mut queue = self.message_queue.lock().await;
        queue.clear();

        let mut stats = self.stats.lock().await;
        *stats = StreamStats::new();

        let mut checkpoint = self.checkpoint.lock().await;
        *checkpoint = Checkpoint::origin();
    }
}

/// Result of processing a message
#[derive(Debug)]
pub struct ProcessResult {
    /// Whether processing succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Processing duration
    pub duration: Duration,
}

impl ProcessResult {
    /// Create a successful result
    pub fn success() -> Self {
        Self {
            success: true,
            error: None,
            duration: Duration::ZERO,
        }
    }

    /// Create a failed result
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            success: false,
            error: Some(error.into()),
            duration: Duration::ZERO,
        }
    }

    /// Create a result with duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }
}

impl Clone for StreamStats {
    fn clone(&self) -> Self {
        Self {
            received: self.received,
            succeeded: self.succeeded,
            failed: self.failed,
            in_flight: self.in_flight,
            started_at: self.started_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_stream_single_message() {
        let config = StreamConfig::default();
        let processor = StreamProcessor::new(config);

        let message = StreamMessage::new(0, "test data".to_string());
        processor.submit(message).await.unwrap();

        assert_eq!(processor.queue_size().await, 1);

        processor
            .process_single(|data| async move {
                assert_eq!(data, "test data");
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(processor.queue_size().await, 0);

        let stats = processor.get_stats().await;
        assert_eq!(stats.succeeded, 1);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn test_stream_large_file() {
        let config = StreamConfig {
            max_concurrency: 2,
            channel_buffer_size: 10,
            ..Default::default()
        };
        let processor: StreamProcessor<String> = StreamProcessor::new(config);

        // Submit and process messages incrementally to avoid buffer overflow
        for i in 0..20 {
            let message = StreamMessage::new(i, format!("message-{}", i));
            processor.submit(message).await.unwrap();
            
            // Process immediately to keep buffer from filling
            processor
                .process_single(|_data| async move {
                    // Simulate some work
                    sleep(Duration::from_millis(1)).await;
                    Ok(())
                })
                .await
                .unwrap();
        }

        let stats = processor.get_stats().await;
        assert_eq!(stats.succeeded, 20);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn test_stream_backpressure() {
        let config = StreamConfig {
            channel_buffer_size: 4,
            ..Default::default()
        };
        let processor = StreamProcessor::new(config);

        // Fill the buffer
        for i in 0..4 {
            let message = StreamMessage::new(i, i);
            processor.submit(message).await.unwrap();
        }

        // Should have backpressure now (4 >= 4/2 = 2)
        assert!(processor.has_backpressure().await);

        // Next submit should fail (buffer full at capacity)
        let message = StreamMessage::new(4, 4);
        assert!(processor.submit(message).await.is_err());
    }

    #[tokio::test]
    async fn test_stream_resume() {
        let config = StreamConfig::default();
        let processor: StreamProcessor<i32> = StreamProcessor::new(config);

        // Set a checkpoint
        let checkpoint = Checkpoint::new(10, 1024);
        processor.set_checkpoint(checkpoint.clone()).await;

        // Verify checkpoint
        let retrieved = processor.get_checkpoint().await;
        assert_eq!(retrieved.position, 10);
        assert_eq!(retrieved.byte_offset, 1024);
    }

    #[tokio::test]
    async fn test_stream_parallel() {
        let config = StreamConfig {
            max_concurrency: 4,
            channel_buffer_size: 10,
            ..Default::default()
        };
        let processor: StreamProcessor<usize> = StreamProcessor::new(config);
        let processor_arc = Arc::new(processor);

        // Submit messages
        for i in 0..10 {
            let message = StreamMessage::new(i, i);
            processor_arc.submit(message).await.unwrap();
        }

        // Process with parallel workers
        let mut handles = vec![];
        for _ in 0..4 {
            let processor_clone = Arc::clone(&processor_arc);
            let handle = tokio::spawn(async move {
                for _ in 0..3 {
                    if processor_clone
                        .process_single(|_data| async move {
                            sleep(Duration::from_millis(5)).await;
                            Ok(())
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all workers
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_stream_error_recovery() {
        let config = StreamConfig::default();
        let processor: StreamProcessor<usize> = StreamProcessor::new(config);

        // Submit messages
        for i in 0..5 {
            let message = StreamMessage::new(i, i);
            processor.submit(message).await.unwrap();
        }

        // Process with some failures
        for _ in 0..5 {
            processor
                .process_single(|data| async move {
                    if data == 2 {
                        Err(Error::Streaming("Intentional failure".to_string()))
                    } else {
                        Ok(())
                    }
                })
                .await
                .unwrap();
        }

        let stats = processor.get_stats().await;
        assert_eq!(stats.succeeded, 4);
        assert_eq!(stats.failed, 1);

        // Processing continued after error
        assert_eq!(stats.succeeded + stats.failed, 5);
    }

    #[test]
    fn test_checkpoint_creation() {
        let checkpoint = Checkpoint::new(100, 2048);
        assert_eq!(checkpoint.position, 100);
        assert_eq!(checkpoint.byte_offset, 2048);
        assert_eq!(checkpoint.processed_count, 0);
    }

    #[test]
    fn test_checkpoint_origin() {
        let checkpoint = Checkpoint::origin();
        assert_eq!(checkpoint.position, 0);
        assert_eq!(checkpoint.byte_offset, 0);
    }

    #[test]
    fn test_stream_message() {
        let mut message = StreamMessage::new(5, "test");
        assert_eq!(message.index, 5);
        assert!(!message.processed);
        assert!(message.error.is_none());

        message.mark_success();
        assert!(message.processed);
        assert!(message.is_success());
        assert!(!message.is_failed());

        let mut failed_message = StreamMessage::new(6, "fail");
        failed_message.mark_failed("error message");
        assert!(failed_message.is_failed());
        assert_eq!(failed_message.error, Some("error message".to_string()));
    }

    #[test]
    fn test_stream_stats() {
        let mut stats = StreamStats::new();
        assert_eq!(stats.received, 0);
        assert_eq!(stats.succeeded, 0);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.rate(), 0.0);
        assert_eq!(stats.success_rate(), 100.0);

        stats.received = 100;
        stats.succeeded = 90;
        stats.failed = 10;
        stats.started_at = Some(Instant::now() - Duration::from_secs(10));

        assert_eq!(stats.success_rate(), 90.0);
        assert!(stats.rate() > 0.0);
    }

    #[test]
    fn test_process_result() {
        let success = ProcessResult::success();
        assert!(success.success);
        assert!(success.error.is_none());

        let failure = ProcessResult::failure("error");
        assert!(!failure.success);
        assert_eq!(failure.error, Some("error".to_string()));

        let timed = ProcessResult::success().with_duration(Duration::from_millis(100));
        assert_eq!(timed.duration, Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_stream_clear() {
        let config = StreamConfig::default();
        let processor = StreamProcessor::new(config);

        // Add messages
        for i in 0..5 {
            let message = StreamMessage::new(i, i);
            processor.submit(message).await.unwrap();
        }

        assert_eq!(processor.queue_size().await, 5);

        // Clear
        processor.clear().await;

        assert_eq!(processor.queue_size().await, 0);

        let stats = processor.get_stats().await;
        assert_eq!(stats.received, 0);
    }
}

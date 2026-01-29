//! Batch processing logic
//!
//! This module provides batch processing capabilities for EDI files,
//! supporting size limits, timeouts, and partial success handling.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::{AcceptancePolicy, Error, Result, StrictnessLevel};

/// A batch of EDI files to be processed together
#[derive(Debug)]
pub struct Batch<T> {
    /// Items in the batch
    items: VecDeque<BatchItem<T>>,
    /// Maximum number of items allowed
    max_size: usize,
    /// Maximum time before auto-flushing
    max_duration: Option<Duration>,
    /// When the batch was created
    created_at: Instant,
    /// Current retry count for failed items
    retry_count: u32,
    /// Maximum retry attempts
    max_retries: u32,
}

/// An item within a batch
#[derive(Debug, Clone)]
pub struct BatchItem<T> {
    /// The item data
    pub data: T,
    /// Unique identifier for this item
    pub id: String,
    /// Processing status
    pub status: ItemStatus,
    /// Error message if failed
    pub error: Option<String>,
    /// Original position in batch
    pub position: usize,
}

/// Status of a batch item
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ItemStatus {
    /// Item is pending processing
    Pending,
    /// Item is currently being processed
    Processing,
    /// Item processed successfully
    Success,
    /// Item processing failed
    Failed,
    /// Item is being retried
    Retrying,
}

/// Result of processing a batch
#[derive(Debug)]
pub struct BatchResult<T> {
    /// Successfully processed items
    pub successful: Vec<BatchItem<T>>,
    /// Failed items
    pub failed: Vec<BatchItem<T>>,
    /// Items that should be retried
    pub retry: Vec<BatchItem<T>>,
    /// Total processing time
    pub processing_time: Duration,
    /// Number of items processed
    pub processed_count: usize,
}

/// Configuration for batch processing
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of items in a batch
    pub max_size: usize,
    /// Maximum time before flushing
    pub max_duration: Option<Duration>,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Whether to preserve item order
    pub preserve_order: bool,
    /// Acceptance policy for partial failures
    pub acceptance_policy: AcceptancePolicy,
    /// Strictness level
    pub strictness: StrictnessLevel,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 100,
            max_duration: Some(Duration::from_secs(60)),
            max_retries: 3,
            preserve_order: true,
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
        }
    }
}

impl<T> Batch<T> {
    /// Create a new batch with the given configuration
    pub fn new(config: BatchConfig) -> Self {
        Self {
            items: VecDeque::with_capacity(config.max_size),
            max_size: config.max_size,
            max_duration: config.max_duration,
            created_at: Instant::now(),
            retry_count: 0,
            max_retries: config.max_retries,
        }
    }

    /// Create a batch with default configuration
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(BatchConfig {
            max_size: capacity,
            ..Default::default()
        })
    }

    /// Add an item to the batch
    pub fn add(&mut self, id: impl Into<String>, data: T) -> Result<bool> {
        if self.is_full() {
            return Ok(false);
        }

        let position = self.items.len();
        let item = BatchItem {
            id: id.into(),
            data,
            status: ItemStatus::Pending,
            error: None,
            position,
        };

        self.items.push_back(item);
        Ok(true)
    }

    /// Check if the batch is full
    pub fn is_full(&self) -> bool {
        self.items.len() >= self.max_size
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get the number of items in the batch
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if the batch has timed out
    pub fn is_timed_out(&self) -> bool {
        match self.max_duration {
            Some(duration) => self.created_at.elapsed() >= duration,
            None => false,
        }
    }

    /// Check if the batch should be flushed
    pub fn should_flush(&self) -> bool {
        self.is_full() || self.is_timed_out()
    }

    /// Get all items in the batch
    pub fn items(&self) -> &VecDeque<BatchItem<T>> {
        &self.items
    }

    /// Get mutable items
    pub fn items_mut(&mut self) -> &mut VecDeque<BatchItem<T>> {
        &mut self.items
    }

    /// Mark an item as successful
    pub fn mark_success(&mut self, id: &str) -> Result<()> {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = ItemStatus::Success;
            Ok(())
        } else {
            Err(Error::Batch(format!("Item not found: {}", id)))
        }
    }

    /// Mark an item as failed
    pub fn mark_failed(&mut self, id: &str, error: impl Into<String>) -> Result<()> {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = ItemStatus::Failed;
            item.error = Some(error.into());
            Ok(())
        } else {
            Err(Error::Batch(format!("Item not found: {}", id)))
        }
    }

    /// Mark an item for retry
    pub fn mark_retry(&mut self, id: &str) -> Result<()> {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = ItemStatus::Retrying;
            Ok(())
        } else {
            Err(Error::Batch(format!("Item not found: {}", id)))
        }
    }

    /// Get items that need to be retried
    pub fn get_retry_items(&self) -> Vec<&BatchItem<T>> {
        self.items
            .iter()
            .filter(|i| matches!(i.status, ItemStatus::Retrying))
            .collect()
    }

    /// Clear all items
    pub fn clear(&mut self) {
        self.items.clear();
        self.created_at = Instant::now();
    }

    /// Drain items into a result
    pub fn into_result(self) -> BatchResult<T> {
        let start = Instant::now();

        let mut successful = Vec::new();
        let mut failed = Vec::new();
        let mut retry = Vec::new();

        for item in self.items {
            match item.status {
                ItemStatus::Success => successful.push(item),
                ItemStatus::Failed => {
                    if self.retry_count < self.max_retries {
                        retry.push(item);
                    } else {
                        failed.push(item);
                    }
                }
                ItemStatus::Retrying => retry.push(item),
                _ => {
                    // Pending/Processing items are treated as failed
                    let mut failed_item = item;
                    failed_item.status = ItemStatus::Failed;
                    failed_item.error = Some("Item was not processed".to_string());
                    failed.push(failed_item);
                }
            }
        }

        let processed_count = successful.len() + failed.len();

        BatchResult {
            successful,
            failed,
            retry,
            processing_time: start.elapsed(),
            processed_count,
        }
    }

    /// Create a new batch from a collection of items
    pub fn from_items(
        items: impl IntoIterator<Item = (impl Into<String>, T)>,
        config: BatchConfig,
    ) -> Result<Self> {
        let mut batch = Self::new(config);

        for (idx, (id, data)) in items.into_iter().enumerate() {
            if !batch.add(id, data)? {
                return Err(Error::Batch(format!(
                    "Batch capacity exceeded at item {}",
                    idx
                )));
            }
        }

        Ok(batch)
    }
}

impl<T> BatchItem<T> {
    /// Create a new batch item
    pub fn new(id: impl Into<String>, data: T) -> Self {
        Self {
            id: id.into(),
            data,
            status: ItemStatus::Pending,
            error: None,
            position: 0,
        }
    }

    /// Check if the item should be retried
    pub fn should_retry(&self, _max_retries: u32) -> bool {
        matches!(self.status, ItemStatus::Failed | ItemStatus::Retrying)
    }
}

impl BatchResult<()> {
    /// Create an empty batch result
    pub fn empty() -> Self {
        Self {
            successful: Vec::new(),
            failed: Vec::new(),
            retry: Vec::new(),
            processing_time: Duration::ZERO,
            processed_count: 0,
        }
    }

    /// Check if all items succeeded
    pub fn all_succeeded(&self) -> bool {
        self.failed.is_empty() && self.retry.is_empty()
    }

    /// Get success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.successful.len() + self.failed.len() + self.retry.len();
        if total == 0 {
            100.0
        } else {
            (self.successful.len() as f64 / total as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_creation() {
        let batch = Batch::<String>::with_capacity(10);
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());
        assert!(!batch.is_full());
    }

    #[test]
    fn test_batch_add_items() {
        let mut batch = Batch::with_capacity(3);

        assert!(batch.add("item1", "data1".to_string()).unwrap());
        assert!(batch.add("item2", "data2".to_string()).unwrap());
        assert_eq!(batch.len(), 2);

        // Third item should succeed
        assert!(batch.add("item3", "data3".to_string()).unwrap());
        assert!(batch.is_full());

        // Fourth item should fail (batch full)
        assert!(!batch.add("item4", "data4".to_string()).unwrap());
    }

    #[test]
    fn test_batch_size_limits() {
        let mut batch = Batch::with_capacity(2);

        batch.add("1", 1).unwrap();
        batch.add("2", 2).unwrap();

        assert!(batch.is_full());
        assert_eq!(batch.len(), 2);

        // Should not be able to add more
        let result = batch.add("3", 3).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_batch_timeout() {
        let config = BatchConfig {
            max_size: 10,
            max_duration: Some(Duration::from_millis(50)),
            max_retries: 3,
            preserve_order: true,
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
        };

        let batch = Batch::<i32>::new(config);
        assert!(!batch.is_timed_out());

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(60));
        assert!(batch.is_timed_out());
        assert!(batch.should_flush());
    }

    #[test]
    fn test_batch_partial_success() {
        let mut batch = Batch::with_capacity(3);

        batch.add("1", 1).unwrap();
        batch.add("2", 2).unwrap();
        batch.add("3", 3).unwrap();

        // Mark some as success, some as failed
        batch.mark_success("1").unwrap();
        batch.mark_failed("2", "error").unwrap();
        // Item 3 remains pending

        let result = batch.into_result();

        assert_eq!(result.successful.len(), 1);
        // Item 2 goes to retry (retry_count < max_retries), Item 3 goes to failed (pending)
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.retry.len(), 1);
        assert_eq!(result.processed_count, 2); // 1 success + 1 failed (pending treated as failed)
    }

    #[test]
    fn test_batch_ordering() {
        let config = BatchConfig {
            max_size: 5,
            max_duration: None,
            max_retries: 3,
            preserve_order: true,
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
        };

        let items = vec![("first", 1), ("second", 2), ("third", 3)];

        let batch = Batch::from_items(items, config).unwrap();

        let ids: Vec<_> = batch.items().iter().map(|i| i.id.clone()).collect();
        assert_eq!(ids, vec!["first", "second", "third"]);
    }

    #[test]
    fn test_batch_retry() {
        let mut batch = Batch::with_capacity(2);
        batch.max_retries = 2;

        batch.add("1", 1).unwrap();
        batch.mark_failed("1", "error").unwrap();

        // First retry - item goes to retry bucket due to batch.retry_count < max_retries
        batch.retry_count = 0; // Make sure retry_count is 0 so Failed items go to retry
        let result1 = batch.into_result();
        assert_eq!(result1.retry.len(), 1); // Item should be in retry

        // Create new batch with exhausted retries
        let mut batch2 = Batch::with_capacity(2);
        batch2.max_retries = 2;
        batch2.retry_count = 2; // Exhausted retries
        batch2.add("1", 1).unwrap();
        batch2.mark_failed("1", "error").unwrap();

        let result2 = batch2.into_result();
        // With exhausted retries, Failed items go to failed bucket
        assert_eq!(result2.failed.len(), 1);
        assert!(result2.retry.is_empty());
    }

    #[test]
    fn test_batch_item_creation() {
        let item = BatchItem::new("test-123", "data");
        assert_eq!(item.id, "test-123");
        assert_eq!(item.status, ItemStatus::Pending);
        assert!(item.error.is_none());
    }

    #[test]
    fn test_batch_result_success_rate() {
        let result = BatchResult {
            successful: vec![BatchItem::new("1", ()), BatchItem::new("2", ())],
            failed: vec![BatchItem::new("3", ())],
            retry: vec![],
            processing_time: Duration::ZERO,
            processed_count: 3,
        };

        assert_eq!(result.success_rate(), 66.66666666666666);
        assert!(!result.all_succeeded());
    }

    #[test]
    fn test_batch_result_empty() {
        let result = BatchResult::empty();
        assert!(result.successful.is_empty());
        assert!(result.failed.is_empty());
        assert!(result.retry.is_empty());
        assert_eq!(result.success_rate(), 100.0);
        assert!(result.all_succeeded());
    }

    #[test]
    fn test_batch_from_items_capacity_exceeded() {
        let config = BatchConfig {
            max_size: 2,
            max_duration: None,
            max_retries: 3,
            preserve_order: true,
            acceptance_policy: AcceptancePolicy::default(),
            strictness: StrictnessLevel::default(),
        };

        let items = vec![("1", 1), ("2", 2), ("3", 3)];
        let result = Batch::from_items(items, config);

        assert!(result.is_err());
    }

    #[test]
    fn test_batch_mark_nonexistent_item() {
        let mut batch = Batch::with_capacity(2);
        batch.add("1", 1).unwrap();

        // Try to mark non-existent item
        assert!(batch.mark_success("nonexistent").is_err());
        assert!(batch.mark_failed("nonexistent", "error").is_err());
        assert!(batch.mark_retry("nonexistent").is_err());
    }
}

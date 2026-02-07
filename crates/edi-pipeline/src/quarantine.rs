//! Quarantine for bad messages
//!
//! This module provides quarantine functionality for messages that fail
//! validation or processing, allowing for later review and retry.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::{Error, Result};

/// A quarantined message with metadata
#[derive(Debug, Clone)]
pub struct QuarantinedMessage<T> {
    /// Unique identifier for the quarantined message
    pub id: String,
    /// The message data (may be partially processed)
    pub data: T,
    /// Error context
    pub error_context: ErrorContext,
    /// When the message was quarantined
    pub quarantined_at: SystemTime,
    /// When the message was last retried
    pub last_retry_at: Option<SystemTime>,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Whether the message has been resolved
    pub resolved: bool,
    /// Quarantine reason
    pub reason: QuarantineReason,
    /// Original batch/file identifier
    pub source_id: Option<String>,
}

/// Context about the error that caused quarantine
#[derive(Debug, Clone)]
pub struct ErrorContext {
    /// Error message
    pub message: String,
    /// Error category
    pub category: ErrorCategory,
    /// Position in source (if applicable)
    pub position: Option<String>,
    /// Additional details
    pub details: HashMap<String, String>,
}

/// Category of error that caused quarantine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorCategory {
    /// Validation error
    Validation,
    /// Parsing error
    Parsing,
    /// Processing error
    Processing,
    /// Timeout
    Timeout,
    /// Unknown error
    #[default]
    Unknown,
}

/// Reason for quarantining a message
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QuarantineReason {
    /// Failed validation
    ValidationFailed,
    /// Processing error
    #[default]
    ProcessingError,
    /// Policy violation
    PolicyViolation,
    /// Manual quarantine
    Manual,
    /// Timeout
    Timeout,
}

/// Quarantine store for holding bad messages
#[derive(Debug)]
pub struct QuarantineStore<T> {
    /// Quarantined messages
    messages: HashMap<String, QuarantinedMessage<T>>,
    /// Configuration
    config: QuarantineConfig,
    /// Statistics
    stats: QuarantineStats,
}

/// Configuration for quarantine behavior
#[derive(Debug, Clone)]
pub struct QuarantineConfig {
    /// Maximum number of messages in quarantine
    pub max_size: usize,
    /// Maximum age before cleanup (None = no automatic cleanup)
    pub max_age: Option<Duration>,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Whether to persist quarantine
    pub persistent: bool,
}

impl Default for QuarantineConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            max_age: Some(Duration::from_secs(7 * 24 * 60 * 60)), // 7 days
            max_retries: 3,
            persistent: false,
        }
    }
}

/// Statistics for quarantine
#[derive(Debug, Default)]
pub struct QuarantineStats {
    /// Total messages quarantined
    pub total_quarantined: usize,
    /// Messages currently in quarantine
    pub current_count: usize,
    /// Messages successfully retried
    pub successful_retries: usize,
    /// Messages permanently failed
    pub permanent_failures: usize,
    /// Messages cleaned up
    pub cleaned_up: usize,
}

impl<T> QuarantineStore<T> {
    /// Create a new quarantine store
    pub fn new(config: QuarantineConfig) -> Self {
        Self {
            messages: HashMap::new(),
            config,
            stats: QuarantineStats::default(),
        }
    }

    /// Create a store with default config
    pub fn with_defaults() -> Self {
        Self::new(QuarantineConfig::default())
    }

    /// Quarantine a message
    pub fn quarantine(
        &mut self,
        id: impl Into<String>,
        data: T,
        reason: QuarantineReason,
        error: impl Into<String>,
    ) -> Result<String> {
        if self.messages.len() >= self.config.max_size {
            return Err(Error::Quarantine("Quarantine store full".to_string()));
        }

        let id = id.into();
        let message = QuarantinedMessage {
            id: id.clone(),
            data,
            error_context: ErrorContext {
                message: error.into(),
                category: ErrorCategory::Unknown,
                position: None,
                details: HashMap::new(),
            },
            quarantined_at: SystemTime::now(),
            last_retry_at: None,
            retry_count: 0,
            resolved: false,
            reason,
            source_id: None,
        };

        self.messages.insert(id.clone(), message);
        self.stats.total_quarantined += 1;
        self.stats.current_count = self.messages.len();

        Ok(id)
    }

    /// Quarantine with full error context
    pub fn quarantine_with_context(
        &mut self,
        id: impl Into<String>,
        data: T,
        reason: QuarantineReason,
        context: ErrorContext,
    ) -> Result<String> {
        if self.messages.len() >= self.config.max_size {
            return Err(Error::Quarantine("Quarantine store full".to_string()));
        }

        let id = id.into();
        let message = QuarantinedMessage {
            id: id.clone(),
            data,
            error_context: context,
            quarantined_at: SystemTime::now(),
            last_retry_at: None,
            retry_count: 0,
            resolved: false,
            reason,
            source_id: None,
        };

        self.messages.insert(id.clone(), message);
        self.stats.total_quarantined += 1;
        self.stats.current_count = self.messages.len();

        Ok(id)
    }

    /// Retrieve a quarantined message by ID
    pub fn get(&self, id: &str) -> Option<&QuarantinedMessage<T>> {
        self.messages.get(id)
    }

    /// Get mutable reference to a message
    pub fn get_mut(&mut self, id: &str) -> Option<&mut QuarantinedMessage<T>> {
        self.messages.get_mut(id)
    }

    /// Get all quarantined messages
    pub fn get_all(&self) -> Vec<&QuarantinedMessage<T>> {
        self.messages.values().collect()
    }

    /// Get messages by reason
    pub fn get_by_reason(&self, reason: QuarantineReason) -> Vec<&QuarantinedMessage<T>> {
        self.messages
            .values()
            .filter(|m| m.reason == reason)
            .collect()
    }

    /// Get messages that can be retried
    pub fn get_retryable(&self) -> Vec<&QuarantinedMessage<T>> {
        self.messages
            .values()
            .filter(|m| !m.resolved && m.retry_count < self.config.max_retries)
            .collect()
    }

    /// Mark a message for retry
    pub fn mark_for_retry(&mut self, id: &str) -> Result<()> {
        if let Some(message) = self.messages.get_mut(id) {
            message.last_retry_at = Some(SystemTime::now());
            message.retry_count += 1;
            Ok(())
        } else {
            Err(Error::Quarantine(format!("Message not found: {}", id)))
        }
    }

    /// Remove a message from quarantine (successful retry)
    pub fn remove(&mut self, id: &str) -> Result<QuarantinedMessage<T>> {
        let message = self
            .messages
            .remove(id)
            .ok_or_else(|| Error::Quarantine(format!("Message not found: {}", id)))?;
        self.stats.current_count = self.messages.len();
        self.stats.successful_retries += 1;
        Ok(message)
    }

    /// Mark a message as permanently failed
    pub fn mark_permanent_failure(&mut self, id: &str) -> Result<()> {
        if let Some(message) = self.messages.get_mut(id) {
            message.resolved = true;
            self.stats.permanent_failures += 1;
            Ok(())
        } else {
            Err(Error::Quarantine(format!("Message not found: {}", id)))
        }
    }

    /// Retry a message (remove from quarantine and return data)
    pub fn retry(&mut self, id: &str) -> Result<(String, T)> {
        // Check max_retries first to avoid borrow issues
        let max_retries = self.config.max_retries;

        let message = self
            .get_mut(id)
            .ok_or_else(|| Error::Quarantine(format!("Message not found: {}", id)))?;

        if message.retry_count >= max_retries {
            return Err(Error::Quarantine(format!(
                "Max retries exceeded for message: {}",
                id
            )));
        }

        message.last_retry_at = Some(SystemTime::now());
        message.retry_count += 1;

        let message_id = message.id.clone();
        // Release the mutable borrow by ending the scope
        let _ = message;

        let data = self
            .messages
            .remove(&message_id)
            .map(|m| m.data)
            .ok_or_else(|| Error::Quarantine("Failed to retrieve data".to_string()))?;

        self.stats.current_count = self.messages.len();

        Ok((message_id, data))
    }

    /// Clean up old messages
    pub fn cleanup(&mut self) -> usize {
        let mut removed = 0;

        if let Some(max_age) = self.config.max_age {
            let now = SystemTime::now();
            let to_remove: Vec<String> = self
                .messages
                .iter()
                .filter(|(_, msg)| {
                    if let Ok(age) = now.duration_since(msg.quarantined_at) {
                        age > max_age
                    } else {
                        false
                    }
                })
                .map(|(id, _)| id.clone())
                .collect();

            for id in to_remove {
                self.messages.remove(&id);
                removed += 1;
            }
        }

        self.stats.cleaned_up += removed;
        self.stats.current_count = self.messages.len();

        removed
    }

    /// Get statistics
    pub fn stats(&self) -> &QuarantineStats {
        &self.stats
    }

    /// Get count of quarantined messages
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Check if quarantine is empty
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Clear all messages
    pub fn clear(&mut self) {
        self.messages.clear();
        self.stats.current_count = 0;
    }
}

impl ErrorContext {
    /// Create a new error context
    pub fn new(message: impl Into<String>, category: ErrorCategory) -> Self {
        Self {
            message: message.into(),
            category,
            position: None,
            details: HashMap::new(),
        }
    }

    /// Add detail
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    /// Set position
    pub fn with_position(mut self, position: impl Into<String>) -> Self {
        self.position = Some(position.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quarantine_message() {
        let mut store = QuarantineStore::with_defaults();
        let data = "test message data";

        let id = store
            .quarantine(
                "msg-1",
                data.to_string(),
                QuarantineReason::ValidationFailed,
                "Invalid format",
            )
            .unwrap();

        assert_eq!(id, "msg-1");
        assert_eq!(store.len(), 1);
        assert_eq!(store.stats().total_quarantined, 1);
    }

    #[test]
    fn test_quarantine_metadata() {
        let mut store = QuarantineStore::<String>::with_defaults();

        let context = ErrorContext::new("Validation failed", ErrorCategory::Validation)
            .with_position("line 10, column 5")
            .with_detail("field", "DTM")
            .with_detail("code", "2379");

        let id = store
            .quarantine_with_context(
                "msg-2",
                "data".to_string(),
                QuarantineReason::ValidationFailed,
                context,
            )
            .unwrap();

        let message = store.get(&id).unwrap();
        assert_eq!(message.error_context.message, "Validation failed");
        assert_eq!(message.error_context.category, ErrorCategory::Validation);
        assert_eq!(
            message.error_context.position,
            Some("line 10, column 5".to_string())
        );
        assert_eq!(
            message.error_context.details.get("field"),
            Some(&"DTM".to_string())
        );
        assert!(!message.resolved);
        assert_eq!(message.retry_count, 0);
    }

    #[test]
    fn test_quarantine_retrieve() {
        let mut store = QuarantineStore::with_defaults();

        store
            .quarantine("msg-1", 1, QuarantineReason::ProcessingError, "error")
            .unwrap();
        store
            .quarantine("msg-2", 2, QuarantineReason::ValidationFailed, "error")
            .unwrap();
        store
            .quarantine("msg-3", 3, QuarantineReason::Timeout, "error")
            .unwrap();

        // Get by ID
        let msg = store.get("msg-2").unwrap();
        assert_eq!(msg.id, "msg-2");

        // Get all
        let all = store.get_all();
        assert_eq!(all.len(), 3);

        // Get by reason
        let validation = store.get_by_reason(QuarantineReason::ValidationFailed);
        assert_eq!(validation.len(), 1);
        assert_eq!(validation[0].id, "msg-2");
    }

    #[test]
    fn test_quarantine_retry() {
        let mut store = QuarantineStore::with_defaults();

        store
            .quarantine("msg-1", "data", QuarantineReason::ProcessingError, "error")
            .unwrap();

        // Mark for retry
        store.mark_for_retry("msg-1").unwrap();

        let msg = store.get("msg-1").unwrap();
        assert_eq!(msg.retry_count, 1);
        assert!(msg.last_retry_at.is_some());

        // Retry (remove from quarantine)
        let (id, data) = store.retry("msg-1").unwrap();
        assert_eq!(id, "msg-1");
        assert_eq!(data, "data");
        assert_eq!(store.len(), 0);
        // retry() extracts for retry but doesn't mark as successful
        // successful_retries is only incremented by remove()
        assert_eq!(store.stats().successful_retries, 0);
    }

    #[test]
    fn test_quarantine_cleanup() {
        let config = QuarantineConfig {
            max_size: 100,
            max_age: Some(Duration::from_millis(50)),
            max_retries: 3,
            persistent: false,
        };

        let mut store = QuarantineStore::new(config);

        // Add messages
        for i in 0..5 {
            store
                .quarantine(
                    format!("msg-{}", i),
                    i,
                    QuarantineReason::ProcessingError,
                    "error",
                )
                .unwrap();
        }

        assert_eq!(store.len(), 5);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(60));

        // Cleanup
        let removed = store.cleanup();
        assert_eq!(removed, 5);
        assert_eq!(store.len(), 0);
        assert_eq!(store.stats().cleaned_up, 5);
    }

    #[test]
    fn test_quarantine_persistence() {
        // Test that store survives operations
        let mut store = QuarantineStore::with_defaults();

        store
            .quarantine("msg-1", 1, QuarantineReason::ProcessingError, "error")
            .unwrap();
        store
            .quarantine("msg-2", 2, QuarantineReason::ValidationFailed, "error")
            .unwrap();

        // Stats should track state
        assert_eq!(store.stats().current_count, 2);

        // Remove one
        store.remove("msg-1").unwrap();
        assert_eq!(store.stats().current_count, 1);
        assert_eq!(store.stats().successful_retries, 1);

        // Clear all
        store.clear();
        assert_eq!(store.stats().current_count, 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_quarantine_max_size() {
        let config = QuarantineConfig {
            max_size: 2,
            max_age: None,
            max_retries: 3,
            persistent: false,
        };

        let mut store = QuarantineStore::new(config);

        store
            .quarantine("msg-1", 1, QuarantineReason::ProcessingError, "error")
            .unwrap();
        store
            .quarantine("msg-2", 2, QuarantineReason::ProcessingError, "error")
            .unwrap();

        // Third should fail
        assert!(
            store
                .quarantine("msg-3", 3, QuarantineReason::ProcessingError, "error")
                .is_err()
        );
    }

    #[test]
    fn test_quarantine_max_retries() {
        let config = QuarantineConfig {
            max_size: 10,
            max_age: None,
            max_retries: 2,
            persistent: false,
        };

        let mut store = QuarantineStore::new(config);

        store
            .quarantine("msg-1", "data", QuarantineReason::ProcessingError, "error")
            .unwrap();

        // Retry twice (successfully)
        store.mark_for_retry("msg-1").unwrap();
        store.mark_for_retry("msg-1").unwrap();

        // Third retry should fail (max retries exceeded when trying to remove)
        store.mark_for_retry("msg-1").unwrap();

        let result = store.retry("msg-1");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Max retries exceeded")
        );
    }

    #[test]
    fn test_error_context_creation() {
        let context = ErrorContext::new("Parse error", ErrorCategory::Parsing)
            .with_position("segment 5")
            .with_detail("segment_tag", "UNH")
            .with_detail("line", "42");

        assert_eq!(context.message, "Parse error");
        assert_eq!(context.category, ErrorCategory::Parsing);
        assert_eq!(context.position, Some("segment 5".to_string()));
        assert_eq!(context.details.len(), 2);
    }

    #[test]
    fn test_get_retryable() {
        let mut store = QuarantineStore::with_defaults();

        store
            .quarantine("msg-1", 1, QuarantineReason::ProcessingError, "error")
            .unwrap();
        store
            .quarantine("msg-2", 2, QuarantineReason::ValidationFailed, "error")
            .unwrap();

        // Mark one as resolved
        store.mark_permanent_failure("msg-2").unwrap();

        let retryable = store.get_retryable();
        assert_eq!(retryable.len(), 1);
        assert_eq!(retryable[0].id, "msg-1");
    }

    #[test]
    fn test_mark_nonexistent_message() {
        let mut store = QuarantineStore::<String>::with_defaults();

        assert!(store.mark_for_retry("nonexistent").is_err());
        assert!(store.remove("nonexistent").is_err());
        assert!(store.mark_permanent_failure("nonexistent").is_err());
        assert!(store.retry("nonexistent").is_err());
    }

    #[test]
    fn test_quarantine_stats_default() {
        let stats = QuarantineStats::default();
        assert_eq!(stats.total_quarantined, 0);
        assert_eq!(stats.current_count, 0);
        assert_eq!(stats.successful_retries, 0);
        assert_eq!(stats.permanent_failures, 0);
        assert_eq!(stats.cleaned_up, 0);
    }
}

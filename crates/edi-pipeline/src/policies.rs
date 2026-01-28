//! Acceptance policies and strictness levels

/// Policy for handling validation errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptancePolicy {
    /// Accept all messages, report errors as warnings
    AcceptAll,

    /// Fail entire file if any message has errors
    FailAll,

    /// Quarantine damaged messages, continue with valid ones
    Quarantine,
}

/// Strictness level for validation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrictnessLevel {
    /// Accept with warnings (real-world EDI)
    Permissive,

    /// Standard validation
    Standard,

    /// Strict validation (fail on warnings)
    Strict,
}

impl Default for AcceptancePolicy {
    fn default() -> Self {
        Self::AcceptAll
    }
}

impl Default for StrictnessLevel {
    fn default() -> Self {
        Self::Permissive
    }
}

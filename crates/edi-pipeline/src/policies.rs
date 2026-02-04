//! Acceptance policies and strictness levels

/// Policy for handling validation errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcceptancePolicy {
    /// Accept all messages, report errors as warnings
    #[default]
    AcceptAll,

    /// Fail entire file if any message has errors
    FailAll,

    /// Quarantine damaged messages, continue with valid ones
    Quarantine,
}

/// Strictness level for validation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StrictnessLevel {
    /// Accept with warnings (real-world EDI)
    #[default]
    Permissive,

    /// Standard validation
    Standard,

    /// Strict validation (fail on warnings)
    Strict,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accept_all_policy() {
        let policy = AcceptancePolicy::AcceptAll;
        assert!(matches!(policy, AcceptancePolicy::AcceptAll));

        // AcceptAll should be the default
        let default: AcceptancePolicy = Default::default();
        assert!(matches!(default, AcceptancePolicy::AcceptAll));
    }

    #[test]
    fn test_reject_all_policy() {
        let policy = AcceptancePolicy::FailAll;
        assert!(matches!(policy, AcceptancePolicy::FailAll));
        assert_ne!(policy, AcceptancePolicy::AcceptAll);
    }

    #[test]
    fn test_quarantine_policy() {
        let policy = AcceptancePolicy::Quarantine;
        assert!(matches!(policy, AcceptancePolicy::Quarantine));
    }

    #[test]
    fn test_policy_equality() {
        assert_eq!(AcceptancePolicy::AcceptAll, AcceptancePolicy::AcceptAll);
        assert_eq!(AcceptancePolicy::FailAll, AcceptancePolicy::FailAll);
        assert_eq!(AcceptancePolicy::Quarantine, AcceptancePolicy::Quarantine);

        assert_ne!(AcceptancePolicy::AcceptAll, AcceptancePolicy::FailAll);
        assert_ne!(AcceptancePolicy::AcceptAll, AcceptancePolicy::Quarantine);
        assert_ne!(AcceptancePolicy::FailAll, AcceptancePolicy::Quarantine);
    }

    #[test]
    fn test_policy_clone() {
        let policy = AcceptancePolicy::Quarantine;
        let cloned = policy;
        assert_eq!(policy, cloned);
    }

    #[test]
    fn test_policy_copy() {
        let policy = AcceptancePolicy::FailAll;
        let copied = policy; // Copy trait allows this
        assert_eq!(policy, copied);
    }

    #[test]
    fn test_strict_strictness() {
        let strictness = StrictnessLevel::Strict;
        assert!(matches!(strictness, StrictnessLevel::Strict));
    }

    #[test]
    fn test_moderate_strictness() {
        let strictness = StrictnessLevel::Standard;
        assert!(matches!(strictness, StrictnessLevel::Standard));
    }

    #[test]
    fn test_lenient_strictness() {
        let strictness = StrictnessLevel::Permissive;
        assert!(matches!(strictness, StrictnessLevel::Permissive));

        // Permissive should be the default
        let default: StrictnessLevel = Default::default();
        assert!(matches!(default, StrictnessLevel::Permissive));
    }

    #[test]
    fn test_strictness_equality() {
        assert_eq!(StrictnessLevel::Strict, StrictnessLevel::Strict);
        assert_eq!(StrictnessLevel::Standard, StrictnessLevel::Standard);
        assert_eq!(StrictnessLevel::Permissive, StrictnessLevel::Permissive);

        assert_ne!(StrictnessLevel::Strict, StrictnessLevel::Standard);
        assert_ne!(StrictnessLevel::Strict, StrictnessLevel::Permissive);
        assert_ne!(StrictnessLevel::Standard, StrictnessLevel::Permissive);
    }

    #[test]
    fn test_strictness_clone() {
        let strictness = StrictnessLevel::Strict;
        let cloned = strictness;
        assert_eq!(strictness, cloned);
    }

    #[test]
    fn test_strictness_copy() {
        let strictness = StrictnessLevel::Standard;
        let copied = strictness; // Copy trait allows this
        assert_eq!(strictness, copied);
    }

    #[test]
    fn test_policy_combinations() {
        // Test all 9 combinations
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
                // Verify each combination can be created
                let _combo = (*policy, *strictness);
            }
        }
    }

    #[test]
    fn test_policy_debug() {
        let policy = AcceptancePolicy::Quarantine;
        let debug_str = format!("{:?}", policy);
        assert!(debug_str.contains("Quarantine"));
    }

    #[test]
    fn test_strictness_debug() {
        let strictness = StrictnessLevel::Strict;
        let debug_str = format!("{:?}", strictness);
        assert!(debug_str.contains("Strict"));
    }
}

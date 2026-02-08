//! Code list validation

use std::collections::HashSet;

/// A code list containing allowed values
#[derive(Debug, Clone)]
pub struct CodeList {
    /// Name/identifier of the code list
    pub name: String,
    /// Set of allowed codes
    codes: HashSet<String>,
    /// Whether validation is case-sensitive
    pub case_sensitive: bool,
    /// Description for documentation
    pub description: Option<String>,
}

impl CodeList {
    /// Create a new empty code list
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            codes: HashSet::new(),
            case_sensitive: true,
            description: None,
        }
    }

    /// Create with a set of codes
    pub fn with_codes(name: impl Into<String>, codes: Vec<impl Into<String>>) -> Self {
        let codes: HashSet<String> = codes.into_iter().map(std::convert::Into::into).collect();
        Self {
            name: name.into(),
            codes,
            case_sensitive: true,
            description: None,
        }
    }

    /// Set case sensitivity
    #[must_use]
    pub fn case_sensitive(mut self, sensitive: bool) -> Self {
        self.case_sensitive = sensitive;
        self
    }

    /// Set description
    #[must_use]
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Add a code to the list
    pub fn add(&mut self, code: impl Into<String>) {
        self.codes.insert(code.into());
    }

    /// Remove a code from the list
    pub fn remove(&mut self, code: &str) -> bool {
        self.codes.remove(code)
    }

    /// Check if a code is valid
    #[must_use]
    pub fn is_valid(&self, code: &str) -> bool {
        if self.case_sensitive {
            self.codes.contains(code)
        } else {
            let upper = code.to_uppercase();
            self.codes.iter().any(|c| c.to_uppercase() == upper)
        }
    }

    /// Get all codes as a sorted vector
    #[must_use]
    pub fn all_codes(&self) -> Vec<&String> {
        let mut codes: Vec<_> = self.codes.iter().collect();
        codes.sort();
        codes
    }

    /// Check if code list is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.codes.is_empty()
    }

    /// Get number of codes
    #[must_use]
    pub fn len(&self) -> usize {
        self.codes.len()
    }
}

impl Default for CodeList {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Code list registry for managing multiple code lists
#[derive(Debug, Clone, Default)]
pub struct CodeListRegistry {
    lists: std::collections::HashMap<String, CodeList>,
}

impl CodeListRegistry {
    /// Create a new registry
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a code list
    pub fn register(&mut self, list: CodeList) {
        self.lists.insert(list.name.clone(), list);
    }

    /// Get a code list by name
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&CodeList> {
        self.lists.get(name)
    }

    /// Validate a value against a named code list
    #[must_use]
    pub fn validate(&self, list_name: &str, value: &str) -> bool {
        self.get(list_name).is_none_or(|list| list.is_valid(value)) // If list doesn't exist, assume valid
    }

    /// Remove a code list
    pub fn remove(&mut self, name: &str) -> Option<CodeList> {
        self.lists.remove(name)
    }

    /// List all registered code list names
    #[must_use]
    pub fn list_names(&self) -> Vec<&String> {
        self.lists.keys().collect()
    }
}

/// Validation result for code list checks
#[derive(Debug, Clone)]
pub enum CodeListResult {
    /// Code is valid
    Valid,
    /// Code is invalid
    Invalid { code: String, list_name: String },
    /// Code list not found
    ListNotFound { list_name: String },
}

impl CodeListResult {
    /// Check if result is valid
    #[must_use]
    pub fn is_valid(&self) -> bool {
        matches!(self, CodeListResult::Valid)
    }

    /// Get error message if invalid
    #[must_use]
    pub fn error_message(&self) -> Option<String> {
        match self {
            CodeListResult::Invalid { code, list_name } => Some(format!(
                "'{code}' is not a valid code in list '{list_name}'"
            )),
            CodeListResult::ListNotFound { list_name } => {
                Some(format!("Code list '{list_name}' not found"))
            }
            CodeListResult::Valid => None,
        }
    }
}

/// Validate a code against a code list
#[must_use]
pub fn validate_code(code: &str, list: &CodeList) -> CodeListResult {
    if list.is_valid(code) {
        CodeListResult::Valid
    } else {
        CodeListResult::Invalid {
            code: code.to_string(),
            list_name: list.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_code() {
        let mut list = CodeList::new("country_codes");
        list.add("US");
        list.add("GB");
        list.add("DE");

        assert!(list.is_valid("US"));
        assert!(list.is_valid("GB"));
        assert!(list.is_valid("DE"));
    }

    #[test]
    fn test_invalid_code() {
        let list = CodeList::with_codes("country_codes", vec!["US", "GB", "DE"]);

        assert!(!list.is_valid("XX"));
        assert!(!list.is_valid("USA"));
        assert!(!list.is_valid(""));
        assert!(!list.is_valid("us")); // Case sensitive by default
    }

    #[test]
    fn test_case_sensitivity() {
        let list_case_sensitive = CodeList::with_codes("test", vec!["ABC", "DEF"]);
        assert!(list_case_sensitive.is_valid("ABC"));
        assert!(!list_case_sensitive.is_valid("abc"));

        let list_case_insensitive =
            CodeList::with_codes("test", vec!["ABC", "DEF"]).case_sensitive(false);
        assert!(list_case_insensitive.is_valid("ABC"));
        assert!(list_case_insensitive.is_valid("abc"));
        assert!(list_case_insensitive.is_valid("Abc"));
    }

    #[test]
    fn test_case_sensitivity_partial_match() {
        let list = CodeList::with_codes("test", vec!["USA", "Usa", "usa"]).case_sensitive(false);
        // All variations should match in case-insensitive mode
        assert!(list.is_valid("USA"));
        assert!(list.is_valid("Usa"));
        assert!(list.is_valid("usa"));
        assert!(list.is_valid("uSa"));
    }

    #[test]
    fn test_empty_codelist() {
        let empty_list = CodeList::new("empty");

        assert!(empty_list.is_empty());
        assert_eq!(empty_list.len(), 0);
        assert!(!empty_list.is_valid("ANY"));
        assert!(!empty_list.is_valid(""));
    }

    #[test]
    fn test_add_and_remove_codes() {
        let mut list = CodeList::new("test");

        list.add("CODE1");
        assert!(list.is_valid("CODE1"));
        assert_eq!(list.len(), 1);

        list.add("CODE2");
        assert!(list.is_valid("CODE2"));
        assert_eq!(list.len(), 2);

        // Adding duplicate shouldn't increase size
        list.add("CODE1");
        assert_eq!(list.len(), 2);

        // Remove a code
        assert!(list.remove("CODE1"));
        assert!(!list.is_valid("CODE1"));
        assert_eq!(list.len(), 1);

        // Remove non-existent code
        assert!(!list.remove("NONEXISTENT"));
    }

    #[test]
    fn test_all_codes_sorted() {
        let list = CodeList::with_codes("test", vec!["Z", "A", "M", "B"]);
        let codes = list.all_codes();

        assert_eq!(codes.len(), 4);
        assert_eq!(codes[0], "A");
        assert_eq!(codes[1], "B");
        assert_eq!(codes[2], "M");
        assert_eq!(codes[3], "Z");
    }

    #[test]
    fn test_codelist_with_description() {
        let list = CodeList::new("test").with_description("Test description");

        assert_eq!(list.description, Some("Test description".to_string()));
    }

    #[test]
    fn test_code_list_registry() {
        let mut registry = CodeListRegistry::new();

        // Register some code lists
        let countries = CodeList::with_codes("countries", vec!["US", "GB", "DE"]);
        let currencies = CodeList::with_codes("currencies", vec!["USD", "EUR", "GBP"]);

        registry.register(countries);
        registry.register(currencies);

        // Test validation
        assert!(registry.validate("countries", "US"));
        assert!(!registry.validate("countries", "XX"));
        assert!(registry.validate("currencies", "EUR"));
        assert!(!registry.validate("currencies", "JPY"));

        // Non-existent list should return true (permissive)
        assert!(registry.validate("nonexistent", "ANY"));
    }

    #[test]
    fn test_registry_get_and_remove() {
        let mut registry = CodeListRegistry::new();
        let list = CodeList::with_codes("test", vec!["A", "B"]);
        registry.register(list);

        // Get existing
        let retrieved = registry.get("test");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "test");

        // Get non-existent
        assert!(registry.get("nonexistent").is_none());

        // Remove existing
        let removed = registry.remove("test");
        assert!(removed.is_some());
        assert!(registry.get("test").is_none());

        // Remove non-existent
        assert!(registry.remove("nonexistent").is_none());
    }

    #[test]
    fn test_registry_list_names() {
        let mut registry = CodeListRegistry::new();

        let names = registry.list_names();
        assert!(names.is_empty());

        registry.register(CodeList::new("list1"));
        registry.register(CodeList::new("list2"));

        let names = registry.list_names();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_validate_code_function() {
        let list = CodeList::with_codes("test", vec!["VALID"]);

        let result = validate_code("VALID", &list);
        assert!(result.is_valid());
        assert!(result.error_message().is_none());

        let result = validate_code("INVALID", &list);
        assert!(!result.is_valid());
        let msg = result.error_message().unwrap();
        assert!(msg.contains("INVALID"));
        assert!(msg.contains("test"));
    }

    #[test]
    fn test_codelist_result_variants() {
        // Valid result
        let valid = CodeListResult::Valid;
        assert!(valid.is_valid());
        assert!(valid.error_message().is_none());

        // Invalid result
        let invalid = CodeListResult::Invalid {
            code: "BAD".to_string(),
            list_name: "mylist".to_string(),
        };
        assert!(!invalid.is_valid());
        let msg = invalid.error_message().unwrap();
        assert!(msg.contains("BAD"));
        assert!(msg.contains("mylist"));

        // Not found result
        let not_found = CodeListResult::ListNotFound {
            list_name: "missing".to_string(),
        };
        assert!(!not_found.is_valid());
        let msg = not_found.error_message().unwrap();
        assert!(msg.contains("missing"));
    }

    #[test]
    fn test_codelist_edge_cases() {
        // Empty string code
        let mut list = CodeList::new("test");
        list.add("");
        assert!(list.is_valid(""));

        // Whitespace codes
        list.add(" ");
        assert!(list.is_valid(" "));
        assert!(!list.is_valid("  ")); // Different whitespace

        // Unicode codes
        list.add("日本");
        assert!(list.is_valid("日本"));
    }

    #[test]
    fn test_large_codelist() {
        let mut list = CodeList::new("large");
        for i in 0..1000 {
            list.add(format!("CODE{i:04}"));
        }

        assert_eq!(list.len(), 1000);
        assert!(list.is_valid("CODE0000"));
        assert!(list.is_valid("CODE0999"));
        assert!(!list.is_valid("CODE1000"));
    }

    #[test]
    fn test_default_codelist() {
        let list = CodeList::default();
        assert_eq!(list.name, "default");
        assert!(list.is_empty());
        assert!(list.case_sensitive);
    }
}

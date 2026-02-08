//! Mapping DSL
//!
//! Provides a declarative DSL for defining mappings between EDI formats.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A complete mapping definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Mapping {
    /// Mapping name
    pub name: String,

    /// Source document type
    pub source_type: String,

    /// Target document type  
    pub target_type: String,

    /// Root mapping rules
    pub rules: Vec<MappingRule>,

    /// Named lookups for reference data
    #[serde(default)]
    pub lookups: HashMap<String, LookupDefinition>,
}

/// Individual mapping rule
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MappingRule {
    /// Simple field-to-field mapping
    Field {
        source: String,
        target: String,
        #[serde(default)]
        transform: Option<Transform>,
    },

    /// Loop over repeating groups
    Foreach {
        source: String,
        target: String,
        #[serde(default)]
        rules: Vec<MappingRule>,
    },

    /// Conditional mapping
    Condition {
        when: Condition,
        #[serde(default)]
        then: Vec<MappingRule>,
        #[serde(default)]
        else_rules: Vec<MappingRule>,
    },

    /// Lookup reference
    Lookup {
        table: String,
        key_source: String,
        target: String,
        #[serde(default)]
        default_value: Option<String>,
    },

    /// Nested mapping block
    Block {
        #[serde(default)]
        rules: Vec<MappingRule>,
    },
}

/// Condition for conditional mappings
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Condition {
    /// Check if field exists and is not empty
    Exists { field: String },

    /// Check if field equals value
    Equals { field: String, value: String },

    /// Check if field contains value
    Contains { field: String, value: String },

    /// Check if field matches regex pattern
    Matches { field: String, pattern: String },

    /// Logical AND of conditions
    And { conditions: Vec<Condition> },

    /// Logical OR of conditions
    Or { conditions: Vec<Condition> },

    /// Logical NOT of condition
    Not { condition: Box<Condition> },
}

/// Transform operation to apply to a value
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Transform {
    /// Convert to uppercase
    Uppercase,

    /// Convert to lowercase
    Lowercase,

    /// Trim whitespace
    Trim,

    /// Format date
    DateFormat { from: String, to: String },

    /// Format number
    NumberFormat {
        decimals: u32,
        thousands_sep: Option<String>,
    },

    /// Concatenate values
    Concatenate {
        values: Vec<ConcatValue>,
        separator: Option<String>,
    },

    /// Split string
    Split { delimiter: String, index: usize },

    /// Default value if null/empty
    Default { value: String },

    /// Conditional transform
    Conditional {
        when: Condition,
        then: Box<Transform>,
        else_transform: Option<Box<Transform>>,
    },

    /// Chain multiple transforms
    Chain { transforms: Vec<Transform> },
}

/// Value for concatenation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConcatValue {
    Field { path: String },
    Literal { value: String },
}

/// Lookup table definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LookupDefinition {
    pub name: String,
    #[serde(default)]
    pub entries: HashMap<String, String>,
}

/// DSL Parser
pub struct MappingDsl;

/// Parse error type
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        if let (Some(line), Some(col)) = (self.line, self.column) {
            write!(f, " at line {line}, column {col}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ParseError {}

impl MappingDsl {
    /// Create a new mapping DSL instance
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Parse a mapping from YAML DSL
    ///
    /// # Errors
    ///
    /// Returns an error when YAML parsing fails.
    pub fn parse(yaml: &str) -> Result<Mapping, ParseError> {
        serde_yaml::from_str(yaml).map_err(|e| ParseError {
            message: format!("Failed to parse DSL: {e}"),
            line: e.location().map(|l| l.line()),
            column: e.location().map(|l| l.column()),
        })
    }

    /// Parse a mapping from a file
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or parsed.
    pub fn parse_file(path: &std::path::Path) -> Result<Mapping, ParseError> {
        let content = std::fs::read_to_string(path).map_err(|e| ParseError {
            message: format!("Failed to read file: {e}"),
            line: None,
            column: None,
        })?;
        Self::parse(&content)
    }

    /// Serialize a mapping to YAML
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails.
    pub fn to_yaml(mapping: &Mapping) -> Result<String, ParseError> {
        serde_yaml::to_string(mapping).map_err(|e| ParseError {
            message: format!("Failed to serialize: {e}"),
            line: None,
            column: None,
        })
    }
}

impl Default for MappingDsl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_mapping() {
        let dsl = r"
name: simple_orders_mapping
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: field
    source: /UNH/MessageReference
    target: message_ref
  - type: field
    source: /BGM/DocumentNumber
    target: order_number
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.name, "simple_orders_mapping");
        assert_eq!(mapping.source_type, "EANCOM_ORDERS");
        assert_eq!(mapping.target_type, "CSV_ORDERS");
        assert_eq!(mapping.rules.len(), 2);

        match &mapping.rules[0] {
            MappingRule::Field {
                source,
                target,
                transform,
            } => {
                assert_eq!(source, "/UNH/MessageReference");
                assert_eq!(target, "message_ref");
                assert!(transform.is_none());
            }
            _ => panic!("Expected Field rule"),
        }

        match &mapping.rules[1] {
            MappingRule::Field {
                source,
                target,
                transform,
            } => {
                assert_eq!(source, "/BGM/DocumentNumber");
                assert_eq!(target, "order_number");
                assert!(transform.is_none());
            }
            _ => panic!("Expected Field rule"),
        }
    }

    #[test]
    fn test_parse_foreach() {
        let dsl = r"
name: orders_with_items
source_type: EANCOM_ORDERS
target_type: CSV_ITEMS
rules:
  - type: foreach
    source: /LIN_Items
    target: items
    rules:
      - type: field
        source: LineNumber
        target: line_no
      - type: field
        source: ItemNumber
        target: sku
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.rules.len(), 1);

        match &mapping.rules[0] {
            MappingRule::Foreach {
                source,
                target,
                rules,
            } => {
                assert_eq!(source, "/LIN_Items");
                assert_eq!(target, "items");
                assert_eq!(rules.len(), 2);
            }
            _ => panic!("Expected Foreach rule"),
        }
    }

    #[test]
    fn test_parse_condition() {
        let dsl = r#"
name: conditional_mapping
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: condition
    when:
      op: equals
      field: /BGM/MessageName
      value: "220"
    then:
      - type: field
        source: /BGM/DocumentNumber
        target: order_type
        transform:
          op: default
          value: "Standard Order"
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.rules.len(), 1);

        match &mapping.rules[0] {
            MappingRule::Condition {
                when,
                then,
                else_rules,
            } => {
                match when {
                    Condition::Equals { field, value } => {
                        assert_eq!(field, "/BGM/MessageName");
                        assert_eq!(value, "220");
                    }
                    _ => panic!("Expected Equals condition"),
                }
                assert_eq!(then.len(), 1);
                assert!(else_rules.is_empty());
            }
            _ => panic!("Expected Condition rule"),
        }
    }

    #[test]
    fn test_parse_lookup() {
        let dsl = r#"
name: mapping_with_lookup
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: lookup
    table: country_codes
    key_source: /NAD/CountryCode
    target: country_name
    default_value: "Unknown"
lookups:
  country_codes:
    name: country_codes
    entries:
      DE: Germany
      FR: France
      US: "United States"
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.rules.len(), 1);

        match &mapping.rules[0] {
            MappingRule::Lookup {
                table,
                key_source,
                target,
                default_value,
            } => {
                assert_eq!(table, "country_codes");
                assert_eq!(key_source, "/NAD/CountryCode");
                assert_eq!(target, "country_name");
                assert_eq!(default_value.as_ref().unwrap(), "Unknown");
            }
            _ => panic!("Expected Lookup rule"),
        }

        assert!(mapping.lookups.contains_key("country_codes"));
        let lookup = mapping.lookups.get("country_codes").unwrap();
        assert_eq!(lookup.entries.get("DE").unwrap(), "Germany");
        assert_eq!(lookup.entries.get("FR").unwrap(), "France");
    }

    #[test]
    fn test_parse_nested_mappings() {
        let dsl = r"
name: deeply_nested_mapping
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: block
    rules:
      - type: field
        source: /UNB/Sender
        target: sender_id
      - type: condition
        when:
          op: exists
          field: /UNB/Recipient
        then:
          - type: field
            source: /UNB/Recipient
            target: recipient_id
          - type: foreach
            source: /Messages
            target: messages
            rules:
              - type: field
                source: MessageType
                target: type
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.rules.len(), 1);

        match &mapping.rules[0] {
            MappingRule::Block { rules } => {
                assert_eq!(rules.len(), 2);
                match &rules[1] {
                    MappingRule::Condition { then, .. } => {
                        assert_eq!(then.len(), 2);
                        match &then[1] {
                            MappingRule::Foreach { rules, .. } => {
                                assert_eq!(rules.len(), 1);
                            }
                            _ => panic!("Expected Foreach rule"),
                        }
                    }
                    _ => panic!("Expected Condition rule"),
                }
            }
            _ => panic!("Expected Block rule"),
        }
    }

    #[test]
    fn test_dsl_error_handling() {
        // Invalid YAML syntax
        let invalid_yaml = r"
name: broken_mapping
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: field
    source: /ORDER/HEADER/ORDER_NUMBER
    target: order_number
  - invalid_yaml_here: [
";

        let result = MappingDsl::parse(invalid_yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Failed to parse DSL"));
        assert!(err.line.is_some());
    }

    #[test]
    fn test_dsl_missing_required_field() {
        // Missing 'name' field
        let incomplete = r"
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules: []
";

        let result = MappingDsl::parse(incomplete);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_complex_expression() {
        let dsl = r#"
name: complex_transforms
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: field
    source: /NAD/PartyName
    target: customer_name
    transform:
      op: chain
      transforms:
        - op: trim
        - op: uppercase
  - type: field
    source: /DTM/Date
    target: order_date
    transform:
      op: date_format
      from: "YYYYMMDD"
      to: "YYYY-MM-DD"
  - type: field
    source: /MOA/Amount
    target: total_amount
    transform:
      op: default
      value: "0.00"
  - type: field
    source: /RFF/Reference
    target: full_reference
    transform:
      op: concatenate
      values:
        - type: literal
          value: "REF-"
        - type: field
          path: /RFF/Reference
      separator: ""
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.rules.len(), 4);

        // Check chain transform
        match &mapping.rules[0] {
            MappingRule::Field { transform, .. } => match transform.as_ref().unwrap() {
                Transform::Chain { transforms } => {
                    assert_eq!(transforms.len(), 2);
                    assert!(matches!(transforms[0], Transform::Trim));
                    assert!(matches!(transforms[1], Transform::Uppercase));
                }
                _ => panic!("Expected Chain transform"),
            },
            _ => panic!("Expected Field rule"),
        }

        // Check date format transform
        match &mapping.rules[1] {
            MappingRule::Field { transform, .. } => match transform.as_ref().unwrap() {
                Transform::DateFormat { from, to } => {
                    assert_eq!(from, "YYYYMMDD");
                    assert_eq!(to, "YYYY-MM-DD");
                }
                _ => panic!("Expected DateFormat transform"),
            },
            _ => panic!("Expected Field rule"),
        }

        // Check concatenate transform
        match &mapping.rules[3] {
            MappingRule::Field { transform, .. } => match transform.as_ref().unwrap() {
                Transform::Concatenate { values, separator } => {
                    assert_eq!(values.len(), 2);
                    assert!(matches!(values[0], ConcatValue::Literal { .. }));
                    assert!(matches!(values[1], ConcatValue::Field { .. }));
                    assert_eq!(separator.as_ref().unwrap(), "");
                }
                _ => panic!("Expected Concatenate transform"),
            },
            _ => panic!("Expected Field rule"),
        }
    }

    #[test]
    fn test_parse_complex_conditions() {
        let dsl = r#"
name: complex_conditions
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: condition
    when:
      op: and
      conditions:
        - op: exists
          field: /BGM/DocumentNumber
        - op: equals
          field: /BGM/MessageName
          value: "220"
    then:
      - type: field
        source: /BGM/DocumentNumber
        target: valid_order
  - type: condition
    when:
      op: or
      conditions:
        - op: equals
          field: /NAD/PartyQualifier
          value: "BY"
        - op: equals
          field: /NAD/PartyQualifier
          value: "SU"
    then:
      - type: field
        source: /NAD/PartyName
        target: party_name
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.rules.len(), 2);

        // Check AND condition
        match &mapping.rules[0] {
            MappingRule::Condition { when, .. } => match when {
                Condition::And { conditions } => {
                    assert_eq!(conditions.len(), 2);
                    assert!(matches!(conditions[0], Condition::Exists { .. }));
                    assert!(matches!(conditions[1], Condition::Equals { .. }));
                }
                _ => panic!("Expected And condition"),
            },
            _ => panic!("Expected Condition rule"),
        }

        // Check OR condition
        match &mapping.rules[1] {
            MappingRule::Condition { when, .. } => match when {
                Condition::Or { conditions } => {
                    assert_eq!(conditions.len(), 2);
                }
                _ => panic!("Expected Or condition"),
            },
            _ => panic!("Expected Condition rule"),
        }
    }

    #[test]
    fn test_serialize_roundtrip() {
        let original = Mapping {
            name: "test_mapping".to_string(),
            source_type: "EANCOM".to_string(),
            target_type: "CSV".to_string(),
            rules: vec![MappingRule::Field {
                source: "/test".to_string(),
                target: "output".to_string(),
                transform: Some(Transform::Uppercase),
            }],
            lookups: HashMap::new(),
        };

        let yaml = MappingDsl::to_yaml(&original).unwrap();
        let parsed = MappingDsl::parse(&yaml).unwrap();

        assert_eq!(original.name, parsed.name);
        assert_eq!(original.source_type, parsed.source_type);
        assert_eq!(original.target_type, parsed.target_type);
        assert_eq!(original.rules.len(), parsed.rules.len());
    }

    #[test]
    fn test_parse_number_format_transform() {
        let dsl = r#"
name: number_format_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: field
    source: /MOA/Amount
    target: formatted_amount
    transform:
      op: number_format
      decimals: 2
      thousands_sep: ","
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Field { transform, .. } => match transform.as_ref().unwrap() {
                Transform::NumberFormat {
                    decimals,
                    thousands_sep,
                } => {
                    assert_eq!(*decimals, 2);
                    assert_eq!(thousands_sep.as_ref().unwrap(), ",");
                }
                _ => panic!("Expected NumberFormat transform"),
            },
            _ => panic!("Expected Field rule"),
        }
    }

    #[test]
    fn test_parse_split_transform() {
        let dsl = r#"
name: split_transform_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: field
    source: /NAD/PartyId
    target: party_prefix
    transform:
      op: split
      delimiter: "-"
      index: 0
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Field { transform, .. } => match transform.as_ref().unwrap() {
                Transform::Split { delimiter, index } => {
                    assert_eq!(delimiter, "-");
                    assert_eq!(*index, 0);
                }
                _ => panic!("Expected Split transform"),
            },
            _ => panic!("Expected Field rule"),
        }
    }

    #[test]
    fn test_parse_conditional_transform() {
        let dsl = r#"
name: conditional_transform_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: field
    source: /NAD/PartyName
    target: formatted_name
    transform:
      op: conditional
      when:
        op: exists
        field: /NAD/PartyName
      then:
        op: uppercase
      else_transform:
        op: default
        value: "UNKNOWN"
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Field { transform, .. } => match transform.as_ref().unwrap() {
                Transform::Conditional {
                    when,
                    then,
                    else_transform,
                } => {
                    assert!(matches!(when, Condition::Exists { .. }));
                    assert!(matches!(then.as_ref(), Transform::Uppercase));
                    assert!(else_transform.is_some());
                    assert!(matches!(
                        else_transform.as_ref().unwrap().as_ref(),
                        Transform::Default { .. }
                    ));
                }
                _ => panic!("Expected Conditional transform"),
            },
            _ => panic!("Expected Field rule"),
        }
    }

    #[test]
    fn test_parse_contains_condition() {
        let dsl = r#"
name: contains_condition_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: condition
    when:
      op: contains
      field: /NAD/PartyName
      value: "GmbH"
    then:
      - type: field
        source: /NAD/PartyName
        target: company_type
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Condition { when, .. } => match when {
                Condition::Contains { field, value } => {
                    assert_eq!(field, "/NAD/PartyName");
                    assert_eq!(value, "GmbH");
                }
                _ => panic!("Expected Contains condition"),
            },
            _ => panic!("Expected Condition rule"),
        }
    }

    #[test]
    fn test_parse_matches_condition() {
        let dsl = r#"
name: matches_condition_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: condition
    when:
      op: matches
      field: /BGM/DocumentNumber
      pattern: "^ORD[0-9]+$"
    then:
      - type: field
        source: /BGM/DocumentNumber
        target: order_ref
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Condition { when, .. } => match when {
                Condition::Matches { field, pattern } => {
                    assert_eq!(field, "/BGM/DocumentNumber");
                    assert_eq!(pattern, "^ORD[0-9]+$");
                }
                _ => panic!("Expected Matches condition"),
            },
            _ => panic!("Expected Condition rule"),
        }
    }

    #[test]
    fn test_parse_not_condition() {
        let dsl = r#"
name: not_condition_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: condition
    when:
      op: not
      condition:
        op: equals
        field: /BGM/MessageName
        value: "220"
    then:
      - type: field
        source: /BGM/DocumentNumber
        target: non_standard_order
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Condition { when, .. } => match when {
                Condition::Not { condition } => {
                    assert!(matches!(condition.as_ref(), Condition::Equals { .. }));
                }
                _ => panic!("Expected Not condition"),
            },
            _ => panic!("Expected Condition rule"),
        }
    }

    #[test]
    fn test_parse_empty_rules() {
        let dsl = r"
name: empty_rules_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules: []
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert!(mapping.rules.is_empty());
        assert!(mapping.lookups.is_empty());
    }

    #[test]
    fn test_parse_multiple_lookups() {
        let dsl = r#"
name: multiple_lookups_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules: []
lookups:
  countries:
    name: countries
    entries:
      DE: Germany
  currencies:
    name: currencies
    entries:
      EUR: Euro
      USD: "US Dollar"
"#;

        let mapping = MappingDsl::parse(dsl).unwrap();
        assert_eq!(mapping.lookups.len(), 2);
        assert!(mapping.lookups.contains_key("countries"));
        assert!(mapping.lookups.contains_key("currencies"));

        let currencies = mapping.lookups.get("currencies").unwrap();
        assert_eq!(currencies.entries.get("EUR").unwrap(), "Euro");
        assert_eq!(currencies.entries.get("USD").unwrap(), "US Dollar");
    }

    #[test]
    fn test_parse_nested_foreach() {
        let dsl = r"
name: nested_foreach_test
source_type: EANCOM_ORDERS
target_type: CSV_ORDERS
rules:
  - type: foreach
    source: /Orders
    target: orders
    rules:
      - type: field
        source: OrderNumber
        target: order_no
      - type: foreach
        source: LineItems
        target: items
        rules:
          - type: field
            source: ItemNumber
            target: sku
";

        let mapping = MappingDsl::parse(dsl).unwrap();
        match &mapping.rules[0] {
            MappingRule::Foreach { rules, .. } => {
                assert_eq!(rules.len(), 2);
                assert!(matches!(rules[1], MappingRule::Foreach { .. }));
            }
            _ => panic!("Expected Foreach rule"),
        }
    }
}

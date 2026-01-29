//! Schema model definitions

/// A complete EDI schema
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub version: String,
    pub segments: Vec<SegmentDefinition>,
}

/// Definition of a segment
#[derive(Debug, Clone)]
pub struct SegmentDefinition {
    pub tag: String,
    pub elements: Vec<ElementDefinition>,
    pub is_mandatory: bool,
    pub max_repetitions: Option<usize>,
}

/// Definition of a data element
#[derive(Debug, Clone)]
pub struct ElementDefinition {
    pub id: String,
    pub name: String,
    pub data_type: String,
    pub min_length: usize,
    pub max_length: usize,
    pub is_mandatory: bool,
}

/// Constraint rules for validation
#[derive(Debug, Clone)]
pub enum Constraint {
    Required(String),
    Length {
        path: String,
        min: usize,
        max: usize,
    },
    Pattern {
        path: String,
        regex: String,
    },
    CodeList {
        path: String,
        codes: Vec<String>,
    },
}

impl Constraint {
    /// Validate a value against this constraint
    pub fn validate(&self, value: Option<&str>) -> Result<(), String> {
        match self {
            Constraint::Required(path) => {
                if value.is_none() || value.unwrap().is_empty() {
                    return Err(format!("Field {} is required", path));
                }
            }
            Constraint::Length { path, min, max } => {
                if let Some(v) = value {
                    let len = v.len();
                    if len < *min || len > *max {
                        return Err(format!(
                            "Field {} length {} is outside range {}-{}",
                            path, len, min, max
                        ));
                    }
                }
            }
            Constraint::Pattern { path, regex } => {
                if let Some(v) = value {
                    let re =
                        regex::Regex::new(regex).map_err(|e| format!("Invalid regex: {}", e))?;
                    if !re.is_match(v) {
                        return Err(format!("Field {} does not match pattern {}", path, regex));
                    }
                }
            }
            Constraint::CodeList { path, codes } => {
                if let Some(v) = value {
                    if !codes.contains(&v.to_string()) {
                        return Err(format!(
                            "Field {} value '{}' not in allowed codes: {:?}",
                            path, v, codes
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = Schema {
            name: "ORDERS".to_string(),
            version: "D96A".to_string(),
            segments: vec![],
        };
        assert_eq!(schema.name, "ORDERS");
        assert_eq!(schema.version, "D96A");
        assert!(schema.segments.is_empty());
    }

    #[test]
    fn test_schema_with_segments() {
        let segment = SegmentDefinition {
            tag: "UNH".to_string(),
            elements: vec![],
            is_mandatory: true,
            max_repetitions: None,
        };
        let schema = Schema {
            name: "ORDERS".to_string(),
            version: "D96A".to_string(),
            segments: vec![segment],
        };
        assert_eq!(schema.segments.len(), 1);
        assert_eq!(schema.segments[0].tag, "UNH");
    }

    #[test]
    fn test_segment_definition() {
        let element = ElementDefinition {
            id: "0062".to_string(),
            name: "Message reference number".to_string(),
            data_type: "an".to_string(),
            min_length: 1,
            max_length: 14,
            is_mandatory: true,
        };
        let segment = SegmentDefinition {
            tag: "UNH".to_string(),
            elements: vec![element],
            is_mandatory: true,
            max_repetitions: Some(1),
        };
        assert_eq!(segment.tag, "UNH");
        assert!(segment.is_mandatory);
        assert_eq!(segment.max_repetitions, Some(1));
        assert_eq!(segment.elements.len(), 1);
    }

    #[test]
    fn test_segment_definition_optional() {
        let segment = SegmentDefinition {
            tag: "DTM".to_string(),
            elements: vec![],
            is_mandatory: false,
            max_repetitions: Some(99),
        };
        assert!(!segment.is_mandatory);
        assert_eq!(segment.max_repetitions, Some(99));
    }

    #[test]
    fn test_element_definition() {
        let element = ElementDefinition {
            id: "0062".to_string(),
            name: "Message reference number".to_string(),
            data_type: "an".to_string(),
            min_length: 1,
            max_length: 14,
            is_mandatory: true,
        };
        assert_eq!(element.id, "0062");
        assert_eq!(element.name, "Message reference number");
        assert_eq!(element.data_type, "an");
        assert_eq!(element.min_length, 1);
        assert_eq!(element.max_length, 14);
        assert!(element.is_mandatory);
    }

    #[test]
    fn test_element_definition_numeric() {
        let element = ElementDefinition {
            id: "1082".to_string(),
            name: "Line item number".to_string(),
            data_type: "n".to_string(),
            min_length: 1,
            max_length: 6,
            is_mandatory: false,
        };
        assert_eq!(element.data_type, "n");
        assert!(!element.is_mandatory);
    }

    #[test]
    fn test_constraint_required() {
        let constraint = Constraint::Required("BGM/1004".to_string());
        match &constraint {
            Constraint::Required(path) => assert_eq!(path, "BGM/1004"),
            _ => panic!("Expected Required constraint"),
        }
    }

    #[test]
    fn test_constraint_length() {
        let constraint = Constraint::Length {
            path: "NAD/3035".to_string(),
            min: 1,
            max: 3,
        };
        match &constraint {
            Constraint::Length { path, min, max } => {
                assert_eq!(path, "NAD/3035");
                assert_eq!(*min, 1);
                assert_eq!(*max, 3);
            }
            _ => panic!("Expected Length constraint"),
        }
    }

    #[test]
    fn test_constraint_pattern() {
        let constraint = Constraint::Pattern {
            path: "DTM/C507/2380".to_string(),
            regex: r"^\d{8}$".to_string(),
        };
        match &constraint {
            Constraint::Pattern { path, regex } => {
                assert_eq!(path, "DTM/C507/2380");
                assert_eq!(regex, r"^\d{8}$");
            }
            _ => panic!("Expected Pattern constraint"),
        }
    }

    #[test]
    fn test_constraint_codelist() {
        let constraint = Constraint::CodeList {
            path: "BGM/C002/1001".to_string(),
            codes: vec!["220".to_string(), "221".to_string(), "224".to_string()],
        };
        match &constraint {
            Constraint::CodeList { path, codes } => {
                assert_eq!(path, "BGM/C002/1001");
                assert_eq!(codes.len(), 3);
                assert!(codes.contains(&"220".to_string()));
            }
            _ => panic!("Expected CodeList constraint"),
        }
    }

    #[test]
    fn test_constraint_validation_required_pass() {
        let constraint = Constraint::Required("test_field".to_string());
        assert!(constraint.validate(Some("value")).is_ok());
    }

    #[test]
    fn test_constraint_validation_required_fail_none() {
        let constraint = Constraint::Required("test_field".to_string());
        let result = constraint.validate(None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("required"));
    }

    #[test]
    fn test_constraint_validation_required_fail_empty() {
        let constraint = Constraint::Required("test_field".to_string());
        let result = constraint.validate(Some(""));
        assert!(result.is_err());
    }

    #[test]
    fn test_constraint_validation_length_pass() {
        let constraint = Constraint::Length {
            path: "field".to_string(),
            min: 2,
            max: 10,
        };
        assert!(constraint.validate(Some("hello")).is_ok());
    }

    #[test]
    fn test_constraint_validation_length_fail_too_short() {
        let constraint = Constraint::Length {
            path: "field".to_string(),
            min: 5,
            max: 10,
        };
        let result = constraint.validate(Some("hi"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("outside range"));
    }

    #[test]
    fn test_constraint_validation_length_fail_too_long() {
        let constraint = Constraint::Length {
            path: "field".to_string(),
            min: 1,
            max: 5,
        };
        let result = constraint.validate(Some("this is too long"));
        assert!(result.is_err());
    }

    #[test]
    fn test_constraint_validation_codelist_pass() {
        let constraint = Constraint::CodeList {
            path: "code_field".to_string(),
            codes: vec!["A".to_string(), "B".to_string(), "C".to_string()],
        };
        assert!(constraint.validate(Some("B")).is_ok());
    }

    #[test]
    fn test_constraint_validation_codelist_fail() {
        let constraint = Constraint::CodeList {
            path: "code_field".to_string(),
            codes: vec!["A".to_string(), "B".to_string()],
        };
        let result = constraint.validate(Some("Z"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not in allowed codes"));
    }
}

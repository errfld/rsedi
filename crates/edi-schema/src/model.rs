//! Schema model definitions

/// Reference to a parent schema for inheritance
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaRef {
    pub name: String,
    pub version: String,
}

impl SchemaRef {
    /// Create a new schema reference
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
        }
    }

    /// Get the fully qualified name for this schema reference
    pub fn qualified_name(&self) -> String {
        format!("{}: {}", self.name, self.version)
    }
}

/// Metadata about schema inheritance
#[derive(Debug, Clone, Default)]
pub struct InheritanceMetadata {
    /// Direct parent schema reference
    pub parent: Option<SchemaRef>,
    /// Chain of inheritance from root to this schema
    pub inheritance_chain: Vec<SchemaRef>,
    /// Whether this schema has been merged with parents
    pub is_merged: bool,
}

/// A complete EDI schema
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub version: String,
    pub segments: Vec<SegmentDefinition>,
    /// Inheritance metadata
    pub inheritance: InheritanceMetadata,
}

impl Schema {
    /// Create a new schema with the given name and version
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            segments: Vec::new(),
            inheritance: InheritanceMetadata::default(),
        }
    }

    /// Get the fully qualified name (name:version)
    pub fn qualified_name(&self) -> String {
        format!("{}: {}", self.name, self.version)
    }

    /// Set the parent schema reference
    pub fn with_parent(mut self, parent: SchemaRef) -> Self {
        self.inheritance.parent = Some(parent);
        self
    }

    /// Add segments to the schema
    pub fn with_segments(mut self, segments: Vec<SegmentDefinition>) -> Self {
        self.segments = segments;
        self
    }

    /// Find a segment by tag
    pub fn find_segment(&self, tag: &str) -> Option<&SegmentDefinition> {
        self.segments.iter().find(|s| s.tag == tag)
    }

    /// Find a segment by tag (mutable)
    pub fn find_segment_mut(&mut self, tag: &str) -> Option<&mut SegmentDefinition> {
        self.segments.iter_mut().find(|s| s.tag == tag)
    }
}

/// Definition of a segment
#[derive(Debug, Clone)]
pub struct SegmentDefinition {
    pub tag: String,
    pub elements: Vec<ElementDefinition>,
    pub is_mandatory: bool,
    pub max_repetitions: Option<usize>,
}

impl SegmentDefinition {
    /// Create a new segment definition
    pub fn new(tag: impl Into<String>) -> Self {
        Self {
            tag: tag.into(),
            elements: Vec::new(),
            is_mandatory: false,
            max_repetitions: None,
        }
    }

    /// Set mandatory flag
    pub fn mandatory(mut self, value: bool) -> Self {
        self.is_mandatory = value;
        self
    }

    /// Set max repetitions
    pub fn max_repetitions(mut self, value: usize) -> Self {
        self.max_repetitions = Some(value);
        self
    }

    /// Add elements
    pub fn with_elements(mut self, elements: Vec<ElementDefinition>) -> Self {
        self.elements = elements;
        self
    }

    /// Find an element by ID
    pub fn find_element(&self, id: &str) -> Option<&ElementDefinition> {
        self.elements.iter().find(|e| e.id == id)
    }

    /// Find an element by ID (mutable)
    pub fn find_element_mut(&mut self, id: &str) -> Option<&mut ElementDefinition> {
        self.elements.iter_mut().find(|e| e.id == id)
    }

    /// Merge another segment definition into this one
    /// Child (self) properties take precedence
    pub fn merge(&mut self, parent: &SegmentDefinition) {
        // Collect child element IDs
        let child_ids: std::collections::HashSet<String> =
            self.elements.iter().map(|e| e.id.clone()).collect();

        // Add parent elements that child doesn't have
        for parent_element in &parent.elements {
            if !child_ids.contains(&parent_element.id) {
                self.elements.push(parent_element.clone());
            }
        }

        // Child mandatory overrides parent optional
        if self.is_mandatory || parent.is_mandatory {
            self.is_mandatory = true;
        }

        // Child max_repetitions overrides parent
        // (keep child's value, it's already set)
    }
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

impl ElementDefinition {
    /// Create a new element definition
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        data_type: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            data_type: data_type.into(),
            min_length: 1,
            max_length: 35,
            is_mandatory: false,
        }
    }

    /// Set length constraints
    pub fn length(mut self, min: usize, max: usize) -> Self {
        self.min_length = min;
        self.max_length = max;
        self
    }

    /// Set mandatory flag
    pub fn mandatory(mut self, value: bool) -> Self {
        self.is_mandatory = value;
        self
    }
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

    /// Get the path for this constraint
    pub fn path(&self) -> &str {
        match self {
            Constraint::Required(path) => path,
            Constraint::Length { path, .. } => path,
            Constraint::Pattern { path, .. } => path,
            Constraint::CodeList { path, .. } => path,
        }
    }

    /// Check if this constraint conflicts with another
    /// Returns true if they target the same path and type
    pub fn conflicts_with(&self, other: &Constraint) -> bool {
        match (self, other) {
            (Constraint::Required(p1), Constraint::Required(p2)) => p1 == p2,
            (Constraint::Length { path: p1, .. }, Constraint::Length { path: p2, .. }) => p1 == p2,
            (Constraint::Pattern { path: p1, .. }, Constraint::Pattern { path: p2, .. }) => {
                p1 == p2
            }
            (Constraint::CodeList { path: p1, .. }, Constraint::CodeList { path: p2, .. }) => {
                p1 == p2
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new("ORDERS", "D96A");
        assert_eq!(schema.name, "ORDERS");
        assert_eq!(schema.version, "D96A");
        assert!(schema.segments.is_empty());
    }

    #[test]
    fn test_schema_with_builder_pattern() {
        let schema = Schema::new("ORDERS", "D96A")
            .with_parent(SchemaRef::new("EANCOM", "D96A"))
            .with_segments(vec![SegmentDefinition::new("UNH").mandatory(true)]);

        assert_eq!(schema.name, "ORDERS");
        assert!(schema.inheritance.parent.is_some());
        assert_eq!(schema.segments.len(), 1);
    }

    #[test]
    fn test_schema_qualified_name() {
        let schema = Schema::new("ORDERS", "D96A");
        assert_eq!(schema.qualified_name(), "ORDERS: D96A");
    }

    #[test]
    fn test_find_segment() {
        let schema = Schema::new("TEST", "1.0").with_segments(vec![
            SegmentDefinition::new("UNH"),
            SegmentDefinition::new("BGM"),
        ]);

        assert!(schema.find_segment("UNH").is_some());
        assert!(schema.find_segment("BGM").is_some());
        assert!(schema.find_segment("XXX").is_none());
    }

    #[test]
    fn test_segment_builder() {
        let segment = SegmentDefinition::new("UNH")
            .mandatory(true)
            .max_repetitions(1)
            .with_elements(vec![
                ElementDefinition::new("0062", "Reference", "an")
                    .mandatory(true)
                    .length(1, 14),
            ]);

        assert_eq!(segment.tag, "UNH");
        assert!(segment.is_mandatory);
        assert_eq!(segment.max_repetitions, Some(1));
        assert_eq!(segment.elements.len(), 1);
    }

    #[test]
    fn test_element_builder() {
        let element = ElementDefinition::new("0062", "Reference", "an")
            .length(1, 14)
            .mandatory(true);

        assert_eq!(element.id, "0062");
        assert_eq!(element.name, "Reference");
        assert_eq!(element.data_type, "an");
        assert_eq!(element.min_length, 1);
        assert_eq!(element.max_length, 14);
        assert!(element.is_mandatory);
    }

    #[test]
    fn test_segment_merge() {
        let parent = SegmentDefinition::new("BGM")
            .mandatory(false)
            .with_elements(vec![
                ElementDefinition::new("C002", "Name", "c").mandatory(true),
                ElementDefinition::new("1004", "Number", "an"),
            ]);

        let mut child = SegmentDefinition::new("BGM")
            .mandatory(true)
            .with_elements(vec![
                ElementDefinition::new("C002", "Overridden Name", "c"),
                ElementDefinition::new("1225", "Function", "an"),
            ]);

        child.merge(&parent);

        // Should have 3 elements total
        assert_eq!(child.elements.len(), 3);

        // Child's C002 is preserved
        let c002 = child.find_element("C002").unwrap();
        assert_eq!(c002.name, "Overridden Name");

        // Parent's 1004 is added
        assert!(child.find_element("1004").is_some());

        // Child's 1225 is preserved
        assert!(child.find_element("1225").is_some());

        // Mandatory should be true (child overrides)
        assert!(child.is_mandatory);
    }

    #[test]
    fn test_schema_ref() {
        let schema_ref = SchemaRef::new("EANCOM", "D96A");
        assert_eq!(schema_ref.name, "EANCOM");
        assert_eq!(schema_ref.version, "D96A");
        assert_eq!(schema_ref.qualified_name(), "EANCOM: D96A");
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
    fn test_constraint_path() {
        let required = Constraint::Required("field".to_string());
        let length = Constraint::Length {
            path: "field".to_string(),
            min: 1,
            max: 10,
        };
        let pattern = Constraint::Pattern {
            path: "field".to_string(),
            regex: r".*".to_string(),
        };
        let codelist = Constraint::CodeList {
            path: "field".to_string(),
            codes: vec![],
        };

        assert_eq!(required.path(), "field");
        assert_eq!(length.path(), "field");
        assert_eq!(pattern.path(), "field");
        assert_eq!(codelist.path(), "field");
    }

    #[test]
    fn test_constraint_conflicts_with() {
        let required1 = Constraint::Required("field".to_string());
        let required2 = Constraint::Required("other".to_string());
        let length1 = Constraint::Length {
            path: "field".to_string(),
            min: 1,
            max: 10,
        };
        let length2 = Constraint::Length {
            path: "field".to_string(),
            min: 5,
            max: 20,
        };
        let length3 = Constraint::Length {
            path: "other".to_string(),
            min: 1,
            max: 10,
        };
        let pattern = Constraint::Pattern {
            path: "field".to_string(),
            regex: r".*".to_string(),
        };

        assert!(required1.conflicts_with(&required1));
        assert!(!required1.conflicts_with(&required2)); // Different paths
        assert!(length1.conflicts_with(&length2)); // Same path
        assert!(!length1.conflicts_with(&length3)); // Different paths
        assert!(!length1.conflicts_with(&pattern)); // Different types
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

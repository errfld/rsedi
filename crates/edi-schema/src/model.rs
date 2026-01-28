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

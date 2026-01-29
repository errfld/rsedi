//! Schema inheritance and merge logic

use crate::model::{Constraint, Schema, SegmentDefinition};
use std::collections::HashSet;

/// Error type for inheritance operations
#[derive(Debug, Clone)]
pub enum InheritanceError {
    CircularDependency(String),
    ParentNotFound(String),
    InvalidOverride(String),
}

/// Tracks inheritance relationships to detect cycles
pub struct InheritanceGraph {
    edges: Vec<(String, String)>, // (child, parent)
}

impl InheritanceGraph {
    pub fn new() -> Self {
        Self { edges: Vec::new() }
    }

    pub fn add_edge(&mut self, child: impl Into<String>, parent: impl Into<String>) {
        self.edges.push((child.into(), parent.into()));
    }

    /// Detect if adding this edge would create a cycle
    pub fn would_create_cycle(&self, child: &str, parent: &str) -> bool {
        if child == parent {
            return true;
        }

        // Check if parent depends on child (directly or transitively)
        let mut to_visit = vec![parent.to_string()];
        let mut visited = HashSet::new();

        while let Some(current) = to_visit.pop() {
            if current == child {
                return true;
            }
            if visited.insert(current.clone()) {
                for (c, p) in &self.edges {
                    if c == &current {
                        to_visit.push(p.clone());
                    }
                }
            }
        }

        false
    }
}

impl Default for InheritanceGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Merge parent schema into child schema
/// Child properties take precedence over parent properties
pub fn merge_schemas(parent: &Schema, child: &mut Schema) {
    // Collect child tag names first (owned Strings to avoid borrow issues)
    let child_tags: HashSet<String> = child.segments.iter().map(|s| s.tag.clone()).collect();

    // Add parent segments that child doesn't have
    for parent_segment in &parent.segments {
        if !child_tags.contains(&parent_segment.tag) {
            child.segments.push(parent_segment.clone());
        }
    }

    // For segments that exist in both, merge elements
    // Collect tags to iterate over to avoid borrow issues
    let segment_tags_to_merge: Vec<String> = parent
        .segments
        .iter()
        .filter(|ps| child.segments.iter().any(|cs| cs.tag == ps.tag))
        .map(|ps| ps.tag.clone())
        .collect();

    for tag in segment_tags_to_merge {
        if let Some(parent_segment) = parent.segments.iter().find(|s| s.tag == tag) {
            if let Some(child_segment) = child.segments.iter_mut().find(|s| s.tag == tag) {
                merge_segment_definitions(parent_segment, child_segment);
            }
        }
    }
}

fn merge_segment_definitions(parent: &SegmentDefinition, child: &mut SegmentDefinition) {
    // Collect child element IDs first (owned Strings to avoid borrow issues)
    let child_element_ids: HashSet<String> = child.elements.iter().map(|e| e.id.clone()).collect();

    // Add parent elements that child doesn't have
    for parent_element in &parent.elements {
        if !child_element_ids.contains(&parent_element.id) {
            child.elements.push(parent_element.clone());
        }
    }
}

/// Build inheritance chain from base to most specific
/// Returns schemas in order: EDIFACT -> EANCOM -> Message -> Partner
pub fn build_inheritance_chain<'a>(
    base: Option<&'a Schema>,
    eancom: Option<&'a Schema>,
    message: Option<&'a Schema>,
    partner: Option<&'a Schema>,
) -> Vec<&'a Schema> {
    let mut chain = Vec::new();
    if let Some(s) = base {
        chain.push(s);
    }
    if let Some(s) = eancom {
        chain.push(s);
    }
    if let Some(s) = message {
        chain.push(s);
    }
    if let Some(s) = partner {
        chain.push(s);
    }
    chain
}

/// Apply full inheritance chain to create final schema
pub fn apply_inheritance_chain(chain: &[&Schema]) -> Option<Schema> {
    if chain.is_empty() {
        return None;
    }

    // Start with the first (most base) schema
    let mut result = chain[0].clone();

    // Apply each subsequent level
    for parent in &chain[1..] {
        merge_schemas(parent, &mut result);
    }

    Some(result)
}

/// Merge constraints, with child constraints taking precedence
pub fn merge_constraints(parent: &[Constraint], child: &[Constraint]) -> Vec<Constraint> {
    let mut result = parent.to_vec();

    // Add child constraints, potentially overriding parent ones
    for child_constraint in child {
        // Check if this constraint type already exists
        let should_replace = result
            .iter()
            .enumerate()
            .find(|(_, c)| match (c, child_constraint) {
                (Constraint::Required(_), Constraint::Required(_)) => true,
                (Constraint::Length { path: p1, .. }, Constraint::Length { path: p2, .. }) => {
                    p1 == p2
                }
                (Constraint::Pattern { path: p1, .. }, Constraint::Pattern { path: p2, .. }) => {
                    p1 == p2
                }
                (Constraint::CodeList { path: p1, .. }, Constraint::CodeList { path: p2, .. }) => {
                    p1 == p2
                }
                _ => false,
            });

        if let Some((idx, _)) = should_replace {
            result[idx] = child_constraint.clone();
        } else {
            result.push(child_constraint.clone());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ElementDefinition;

    fn create_test_schema(name: &str, segments: Vec<SegmentDefinition>) -> Schema {
        Schema {
            name: name.to_string(),
            version: "1.0".to_string(),
            segments,
        }
    }

    fn create_segment(
        tag: &str,
        elements: Vec<ElementDefinition>,
        mandatory: bool,
    ) -> SegmentDefinition {
        SegmentDefinition {
            tag: tag.to_string(),
            elements,
            is_mandatory: mandatory,
            max_repetitions: None,
        }
    }

    fn create_element(id: &str, name: &str, mandatory: bool) -> ElementDefinition {
        ElementDefinition {
            id: id.to_string(),
            name: name.to_string(),
            data_type: "an".to_string(),
            min_length: 1,
            max_length: 35,
            is_mandatory: mandatory,
        }
    }

    #[test]
    fn test_merge_schemas() {
        let parent = create_test_schema(
            "parent",
            vec![
                create_segment("UNH", vec![create_element("0062", "Reference", true)], true),
                create_segment("BGM", vec![create_element("C002", "Name", true)], true),
            ],
        );

        let mut child = create_test_schema(
            "child",
            vec![create_segment(
                "DTM",
                vec![create_element("C507", "Date", false)],
                false,
            )],
        );

        merge_schemas(&parent, &mut child);

        assert_eq!(child.segments.len(), 3);
        let tags: Vec<String> = child.segments.iter().map(|s| s.tag.clone()).collect();
        assert!(tags.contains(&"UNH".to_string()));
        assert!(tags.contains(&"BGM".to_string()));
        assert!(tags.contains(&"DTM".to_string()));
    }

    #[test]
    fn test_inheritance_chain() {
        let edifact = create_test_schema("EDIFACT", vec![create_segment("UNA", vec![], true)]);

        let eancom = create_test_schema("EANCOM", vec![create_segment("UNH", vec![], true)]);

        let message = create_test_schema("ORDERS", vec![create_segment("BGM", vec![], true)]);

        let partner = create_test_schema("PartnerA", vec![create_segment("NAD", vec![], false)]);

        let chain = build_inheritance_chain(
            Some(&edifact),
            Some(&eancom),
            Some(&message),
            Some(&partner),
        );

        assert_eq!(chain.len(), 4);
        assert_eq!(chain[0].name, "EDIFACT");
        assert_eq!(chain[3].name, "PartnerA");
    }

    #[test]
    fn test_override_properties() {
        let parent = create_test_schema(
            "parent",
            vec![create_segment(
                "BGM",
                vec![create_element("C002", "Name", true)],
                true,
            )],
        );

        let mut child = create_test_schema(
            "child",
            vec![SegmentDefinition {
                tag: "BGM".to_string(),
                elements: vec![create_element("C002", "Overridden Name", false)],
                is_mandatory: false, // Override: was true, now false
                max_repetitions: Some(99),
            }],
        );

        merge_schemas(&parent, &mut child);

        let bgm = child.segments.iter().find(|s| s.tag == "BGM").unwrap();
        assert!(!bgm.is_mandatory); // Child value preserved
        assert_eq!(bgm.elements[0].name, "Overridden Name"); // Child value preserved
    }

    #[test]
    fn test_merge_constraints() {
        let parent_constraints = vec![
            Constraint::Required("field1".to_string()),
            Constraint::Length {
                path: "field2".to_string(),
                min: 1,
                max: 10,
            },
        ];

        let child_constraints = vec![
            Constraint::Length {
                path: "field2".to_string(), // Override parent's Length
                min: 5,
                max: 20,
            },
            Constraint::CodeList {
                path: "field3".to_string(),
                codes: vec!["A".to_string(), "B".to_string()],
            },
        ];

        let merged = merge_constraints(&parent_constraints, &child_constraints);

        assert_eq!(merged.len(), 3);

        // Check that child Length replaced parent Length
        let length = merged
            .iter()
            .find(|c| matches!(c, Constraint::Length { path, .. } if path == "field2"))
            .unwrap();
        if let Constraint::Length { min, max, .. } = length {
            assert_eq!(*min, 5); // From child
            assert_eq!(*max, 20); // From child
        }
    }

    #[test]
    fn test_circular_dependency_detection() {
        let mut graph = InheritanceGraph::new();

        // A -> B -> C
        graph.add_edge("A", "B");
        graph.add_edge("B", "C");

        // C -> A would create a cycle
        assert!(graph.would_create_cycle("C", "A"));

        // A -> D would not create a cycle
        assert!(!graph.would_create_cycle("A", "D"));

        // Self-reference is a cycle
        assert!(graph.would_create_cycle("A", "A"));
    }

    #[test]
    fn test_partial_inheritance() {
        let parent = create_test_schema(
            "parent",
            vec![
                create_segment("UNH", vec![], true),
                create_segment("BGM", vec![], true),
                create_segment("DTM", vec![], false),
            ],
        );

        // Child only inherits UNH and adds NAD
        let mut child = create_test_schema("child", vec![create_segment("NAD", vec![], false)]);

        merge_schemas(&parent, &mut child);

        assert_eq!(child.segments.len(), 4);
        assert!(child.segments.iter().any(|s| s.tag == "UNH"));
        assert!(child.segments.iter().any(|s| s.tag == "BGM"));
        assert!(child.segments.iter().any(|s| s.tag == "DTM"));
        assert!(child.segments.iter().any(|s| s.tag == "NAD"));
    }

    #[test]
    fn test_apply_inheritance_chain() {
        let base = create_test_schema("base", vec![create_segment("UNH", vec![], true)]);

        let level1 = create_test_schema("level1", vec![create_segment("BGM", vec![], true)]);

        let level2 = create_test_schema("level2", vec![create_segment("DTM", vec![], false)]);

        let chain = vec![&base, &level1, &level2];
        let result = apply_inheritance_chain(&chain).unwrap();

        assert_eq!(result.segments.len(), 3);
        assert!(result.segments.iter().any(|s| s.tag == "UNH"));
        assert!(result.segments.iter().any(|s| s.tag == "BGM"));
        assert!(result.segments.iter().any(|s| s.tag == "DTM"));
    }

    #[test]
    fn test_apply_empty_chain() {
        let chain: Vec<&Schema> = vec![];
        assert!(apply_inheritance_chain(&chain).is_none());
    }

    #[test]
    fn test_element_merge_in_segment() {
        let parent_segment = create_segment(
            "BGM",
            vec![
                create_element("C002", "Name", true),
                create_element("1004", "Number", false),
            ],
            true,
        );

        let mut child_segment = create_segment(
            "BGM",
            vec![
                create_element("C002", "Overridden", true),
                create_element("1225", "New Element", true),
            ],
            true,
        );

        merge_segment_definitions(&parent_segment, &mut child_segment);

        assert_eq!(child_segment.elements.len(), 3);

        // Child's C002 is preserved
        let c002 = child_segment
            .elements
            .iter()
            .find(|e| e.id == "C002")
            .unwrap();
        assert_eq!(c002.name, "Overridden");

        // Parent's 1004 is added
        assert!(child_segment.elements.iter().any(|e| e.id == "1004"));

        // Child's 1225 is preserved
        assert!(child_segment.elements.iter().any(|e| e.id == "1225"));
    }
}

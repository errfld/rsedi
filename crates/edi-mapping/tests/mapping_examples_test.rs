//! Integration tests for mapping example files in testdata/mappings.

use edi_mapping::MappingDsl;
use edi_mapping::dsl::{Mapping, MappingRule, Transform};
use std::fs;
use std::path::PathBuf;

fn mapping_examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/mappings")
}

fn parse_mapping_file(file_name: &str) -> Mapping {
    let mapping_path = mapping_examples_dir().join(file_name);
    MappingDsl::parse_file(&mapping_path)
        .unwrap_or_else(|err| panic!("failed to parse {}: {}", mapping_path.display(), err))
}

fn has_transform(transform: &Transform, predicate: &impl Fn(&Transform) -> bool) -> bool {
    if predicate(transform) {
        return true;
    }

    match transform {
        Transform::Chain { transforms } => {
            transforms.iter().any(|item| has_transform(item, predicate))
        }
        Transform::Conditional {
            then,
            else_transform,
            ..
        } => {
            has_transform(then, predicate)
                || else_transform
                    .as_deref()
                    .map(|item| has_transform(item, predicate))
                    .unwrap_or(false)
        }
        _ => false,
    }
}

fn has_rule(rules: &[MappingRule], predicate: &impl Fn(&MappingRule) -> bool) -> bool {
    for rule in rules {
        if predicate(rule) {
            return true;
        }

        let nested = match rule {
            MappingRule::Foreach { rules, .. } | MappingRule::Block { rules } => {
                has_rule(rules, predicate)
            }
            MappingRule::Condition {
                then, else_rules, ..
            } => has_rule(then, predicate) || has_rule(else_rules, predicate),
            MappingRule::Field { .. } | MappingRule::Lookup { .. } => false,
        };

        if nested {
            return true;
        }
    }

    false
}

#[test]
fn mapping_examples_parse_successfully() {
    let orders_to_csv = parse_mapping_file("orders_to_csv.yaml");
    let orders_to_json = parse_mapping_file("orders_to_json.yaml");
    let csv_to_orders = parse_mapping_file("csv_to_orders.yaml");

    assert_eq!(orders_to_csv.name, "orders_to_csv");
    assert_eq!(orders_to_json.name, "orders_to_json");
    assert_eq!(csv_to_orders.name, "csv_to_orders");
}

#[test]
fn mapping_examples_cover_expected_constructs() {
    let orders_to_csv = parse_mapping_file("orders_to_csv.yaml");
    let orders_to_json = parse_mapping_file("orders_to_json.yaml");
    let csv_to_orders = parse_mapping_file("csv_to_orders.yaml");

    assert!(has_rule(&orders_to_csv.rules, &|rule| {
        matches!(rule, MappingRule::Foreach { .. })
    }));
    assert!(has_rule(&orders_to_csv.rules, &|rule| {
        matches!(rule, MappingRule::Condition { .. })
    }));

    assert!(has_rule(&orders_to_json.rules, &|rule| {
        matches!(rule, MappingRule::Foreach { .. })
    }));
    assert!(has_rule(&orders_to_json.rules, &|rule| {
        matches!(rule, MappingRule::Condition { .. })
    }));
    assert!(has_rule(&orders_to_json.rules, &|rule| match rule {
        MappingRule::Field {
            transform: Some(transform),
            ..
        } => has_transform(transform, &|item| matches!(
            item,
            Transform::DateFormat { .. }
        )),
        _ => false,
    }));

    assert!(has_rule(&csv_to_orders.rules, &|rule| {
        matches!(rule, MappingRule::Foreach { .. })
    }));
    assert!(has_rule(&csv_to_orders.rules, &|rule| {
        matches!(rule, MappingRule::Condition { .. })
    }));
    assert!(has_rule(&csv_to_orders.rules, &|rule| {
        matches!(rule, MappingRule::Lookup { .. })
    }));
}

#[test]
fn mapping_examples_readme_lists_files() {
    let readme_path = mapping_examples_dir().join("README.md");
    let readme = fs::read_to_string(&readme_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {}", readme_path.display(), err));

    assert!(readme.contains("orders_to_csv.yaml"));
    assert!(readme.contains("orders_to_json.yaml"));
    assert!(readme.contains("csv_to_orders.yaml"));
}

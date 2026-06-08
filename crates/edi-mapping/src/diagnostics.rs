//! Mapping diagnostics for linting, explanation, and execution tracing.
//!
//! The diagnostics layer is intentionally schema-light: it validates the DSL
//! shape that the runtime can understand before execution and renders the rule
//! tree in the same terms used by the runtime.

use std::fmt::Write as _;

use edi_schema::Schema;

use crate::dsl::{Condition, Mapping, MappingRule, Transform};

/// Severity of a mapping lint diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    /// The mapping can run, but behavior is likely surprising.
    Warning,
}

impl DiagnosticSeverity {
    /// Stable lowercase representation for human and machine output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
        }
    }
}

/// A static mapping lint diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingDiagnostic {
    /// Diagnostic severity.
    pub severity: DiagnosticSeverity,
    /// Rule path within the mapping tree, e.g. `rules[0].then[1]`.
    pub rule_path: String,
    /// Source/condition path that triggered the diagnostic.
    pub source_path: String,
    /// Actionable message.
    pub message: String,
}

/// Analyze a mapping for constructs that the current runtime cannot evaluate.
#[must_use]
pub fn lint_mapping(mapping: &Mapping) -> Vec<MappingDiagnostic> {
    let mut diagnostics = Vec::new();
    lint_rules(&mapping.rules, "rules", &mut diagnostics);
    diagnostics
}

/// Analyze a mapping using schema metadata for additional path diagnostics.
#[must_use]
pub fn lint_mapping_with_schema(mapping: &Mapping, schema: &Schema) -> Vec<MappingDiagnostic> {
    let mut diagnostics = lint_mapping(mapping);
    lint_rules_against_schema(&mapping.rules, "rules", schema, &mut diagnostics);
    diagnostics
}

/// Render a human-readable rule tree for a mapping.
#[must_use]
pub fn explain_mapping(mapping: &Mapping) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Mapping {}", mapping.name);
    let _ = writeln!(output, "source_type: {}", mapping.source_type);
    let _ = writeln!(output, "target_type: {}", mapping.target_type);
    if !mapping.lookups.is_empty() {
        output.push_str("lookups:\n");
        let mut lookup_names: Vec<_> = mapping.lookups.keys().collect();
        lookup_names.sort();
        for name in lookup_names {
            let _ = writeln!(output, "  - {name}");
        }
    }
    output.push_str("rules:\n");
    explain_rules(&mapping.rules, 1, &mut output);
    output
}

fn lint_rules(rules: &[MappingRule], prefix: &str, diagnostics: &mut Vec<MappingDiagnostic>) {
    for (index, rule) in rules.iter().enumerate() {
        let rule_path = format!("{prefix}[{index}]");
        match rule {
            MappingRule::Field {
                source, transform, ..
            } => {
                lint_path(source, &rule_path, diagnostics);
                if let Some(transform) = transform {
                    lint_transform(transform, &rule_path, diagnostics);
                }
            }
            MappingRule::Foreach { source, rules, .. } => {
                lint_path(source, &rule_path, diagnostics);
                lint_rules(rules, &format!("{rule_path}.rules"), diagnostics);
            }
            MappingRule::Condition {
                when,
                then,
                else_rules,
            } => {
                lint_condition(when, &rule_path, diagnostics);
                lint_rules(then, &format!("{rule_path}.then"), diagnostics);
                lint_rules(else_rules, &format!("{rule_path}.else_rules"), diagnostics);
            }
            MappingRule::Lookup { key_source, .. } => {
                lint_path(key_source, &rule_path, diagnostics);
            }
            MappingRule::Block { rules } => {
                lint_rules(rules, &format!("{rule_path}.rules"), diagnostics);
            }
        }
    }
}

fn lint_rules_against_schema(
    rules: &[MappingRule],
    prefix: &str,
    schema: &Schema,
    diagnostics: &mut Vec<MappingDiagnostic>,
) {
    for (index, rule) in rules.iter().enumerate() {
        let rule_path = format!("{prefix}[{index}]");
        match rule {
            MappingRule::Field { source, .. } => {
                lint_path_against_schema(source, &rule_path, schema, diagnostics);
            }
            MappingRule::Foreach { source, rules, .. } => {
                lint_path_against_schema(source, &rule_path, schema, diagnostics);
                lint_rules_against_schema(
                    rules,
                    &format!("{rule_path}.rules"),
                    schema,
                    diagnostics,
                );
            }
            MappingRule::Condition {
                when,
                then,
                else_rules,
            } => {
                lint_condition_against_schema(when, &rule_path, schema, diagnostics);
                lint_rules_against_schema(then, &format!("{rule_path}.then"), schema, diagnostics);
                lint_rules_against_schema(
                    else_rules,
                    &format!("{rule_path}.else_rules"),
                    schema,
                    diagnostics,
                );
            }
            MappingRule::Lookup { key_source, .. } => {
                lint_path_against_schema(key_source, &rule_path, schema, diagnostics);
            }
            MappingRule::Block { rules } => {
                lint_rules_against_schema(
                    rules,
                    &format!("{rule_path}.rules"),
                    schema,
                    diagnostics,
                );
            }
        }
    }
}

fn lint_condition_against_schema(
    condition: &Condition,
    rule_path: &str,
    schema: &Schema,
    diagnostics: &mut Vec<MappingDiagnostic>,
) {
    match condition {
        Condition::Exists { field }
        | Condition::Equals { field, .. }
        | Condition::Contains { field, .. }
        | Condition::Matches { field, .. } => {
            lint_path_against_schema(field, rule_path, schema, diagnostics);
        }
        Condition::And { conditions } | Condition::Or { conditions } => {
            for condition in conditions {
                lint_condition_against_schema(condition, rule_path, schema, diagnostics);
            }
        }
        Condition::Not { condition } => {
            lint_condition_against_schema(condition, rule_path, schema, diagnostics);
        }
    }
}

fn lint_path_against_schema(
    path: &str,
    rule_path: &str,
    schema: &Schema,
    diagnostics: &mut Vec<MappingDiagnostic>,
) {
    let Some(segment) = path
        .split('/')
        .find(|part| !part.is_empty())
        .map(strip_selector)
    else {
        return;
    };

    if schema.find_segment(segment).is_some() || is_schema_agnostic_path(segment) {
        return;
    }

    let suggestion = closest_segment(segment, schema);
    let message = if let Some(suggestion) = suggestion {
        format!("unknown segment '{segment}' in path '{path}'; did you mean '{suggestion}'?")
    } else {
        format!("unknown segment '{segment}' in path '{path}'")
    };
    diagnostics.push(MappingDiagnostic {
        severity: DiagnosticSeverity::Warning,
        rule_path: rule_path.to_string(),
        source_path: path.to_string(),
        message,
    });
}

fn strip_selector(component: &str) -> &str {
    component
        .split_once('[')
        .map_or(component, |(name, _)| name)
}

fn is_schema_agnostic_path(segment: &str) -> bool {
    segment == "*"
        || segment == "LINE_ITEM"
        || segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '_' || c == '-')
}

fn closest_segment<'a>(segment: &str, schema: &'a Schema) -> Option<&'a str> {
    schema
        .segments
        .iter()
        .map(|candidate| {
            (
                candidate.tag.as_str(),
                edit_distance(segment, &candidate.tag),
            )
        })
        .filter(|(_, distance)| *distance <= 2)
        .min_by_key(|(_, distance)| *distance)
        .map(|(tag, _)| tag)
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right_len = right.chars().count();
    let mut previous: Vec<usize> = (0..=right_len).collect();
    let mut current = vec![0; right_len + 1];

    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right.chars().enumerate() {
            let cost = usize::from(left_char != right_char);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + cost);
        }
        std::mem::swap(&mut previous, &mut current);
    }

    previous[right_len]
}

fn lint_transform(
    transform: &Transform,
    rule_path: &str,
    diagnostics: &mut Vec<MappingDiagnostic>,
) {
    match transform {
        Transform::Conditional {
            when,
            then,
            else_transform,
        } => {
            lint_condition(when, rule_path, diagnostics);
            lint_transform(then, rule_path, diagnostics);
            if let Some(else_transform) = else_transform {
                lint_transform(else_transform, rule_path, diagnostics);
            }
        }
        Transform::Chain { transforms } => {
            for transform in transforms {
                lint_transform(transform, rule_path, diagnostics);
            }
        }
        Transform::Uppercase
        | Transform::Lowercase
        | Transform::Trim
        | Transform::DateFormat { .. }
        | Transform::NumberFormat { .. }
        | Transform::Concatenate { .. }
        | Transform::Split { .. }
        | Transform::Default { .. } => {}
    }
}

fn lint_condition(
    condition: &Condition,
    rule_path: &str,
    diagnostics: &mut Vec<MappingDiagnostic>,
) {
    match condition {
        Condition::Exists { field }
        | Condition::Equals { field, .. }
        | Condition::Contains { field, .. }
        | Condition::Matches { field, .. } => lint_path(field, rule_path, diagnostics),
        Condition::And { conditions } | Condition::Or { conditions } => {
            for condition in conditions {
                lint_condition(condition, rule_path, diagnostics);
            }
        }
        Condition::Not { condition } => lint_condition(condition, rule_path, diagnostics),
    }
}

fn lint_path(path: &str, rule_path: &str, diagnostics: &mut Vec<MappingDiagnostic>) {
    for component in path.split('/').filter(|part| !part.is_empty()) {
        let Some((raw_key, raw_value)) = extract_selector(component) else {
            continue;
        };
        let key = raw_key.trim();
        let value = clean_selector_literal(raw_value);
        if key == "*" || (key.is_empty() && value == "*") {
            continue;
        }
        let normalized_key = if key.is_empty() { "c1" } else { key };
        if !is_supported_selector_key(normalized_key) {
            diagnostics.push(MappingDiagnostic {
                severity: DiagnosticSeverity::Warning,
                rule_path: rule_path.to_string(),
                source_path: path.to_string(),
                message: format!(
                    "unsupported selector key '{normalized_key}' in path '{path}'; supported selector keys are cN, eN, and known EDI qualifier codes (1153, 2005, 3035, 5025, 5125, 6063)"
                ),
            });
        }
    }
}

fn extract_selector(component: &str) -> Option<(&str, &str)> {
    let selector_start = component.find('[')?;
    if !component.ends_with(']') || selector_start == 0 {
        return None;
    }
    let selector = &component[selector_start + 1..component.len() - 1];
    selector.split_once('=').or(Some(("", selector)))
}

fn clean_selector_literal(value: &str) -> &str {
    value.trim().trim_matches('\'').trim_matches('"')
}

fn is_supported_selector_key(key: &str) -> bool {
    let normalized = key.trim();
    normalized.eq_ignore_ascii_case("c1")
        || normalized
            .strip_prefix(['c', 'C', 'e', 'E'])
            .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()))
        || matches!(
            normalized,
            "1153" | "2005" | "3035" | "5025" | "5125" | "6063"
        )
}

fn explain_rules(rules: &[MappingRule], indent: usize, output: &mut String) {
    for rule in rules {
        let prefix = "  ".repeat(indent);
        match rule {
            MappingRule::Field {
                source,
                target,
                transform,
            } => {
                let _ = writeln!(output, "{prefix}- field {source} -> {target}");
                if let Some(transform) = transform {
                    let _ = writeln!(
                        output,
                        "{prefix}  transform: {}",
                        describe_transform(transform)
                    );
                }
            }
            MappingRule::Foreach {
                source,
                target,
                rules,
            } => {
                let _ = writeln!(output, "{prefix}- foreach {source} -> {target}");
                explain_rules(rules, indent + 1, output);
            }
            MappingRule::Condition {
                when,
                then,
                else_rules,
            } => {
                let _ = writeln!(output, "{prefix}- condition {}", describe_condition(when));
                if !then.is_empty() {
                    let _ = writeln!(output, "{prefix}  then:");
                    explain_rules(then, indent + 2, output);
                }
                if !else_rules.is_empty() {
                    let _ = writeln!(output, "{prefix}  else:");
                    explain_rules(else_rules, indent + 2, output);
                }
            }
            MappingRule::Lookup {
                table,
                key_source,
                target,
                default_value,
            } => {
                let _ = write!(output, "{prefix}- lookup {table}[{key_source}] -> {target}");
                if let Some(default_value) = default_value {
                    let _ = write!(output, " default={default_value}");
                }
                output.push('\n');
            }
            MappingRule::Block { rules } => {
                let _ = writeln!(output, "{prefix}- block");
                explain_rules(rules, indent + 1, output);
            }
        }
    }
}

fn describe_condition(condition: &Condition) -> String {
    match condition {
        Condition::Exists { field } => format!("exists({field})"),
        Condition::Equals { field, value } => format!("{field} == {value}"),
        Condition::Contains { field, value } => format!("contains({field}, {value})"),
        Condition::Matches { field, pattern } => format!("matches({field}, {pattern})"),
        Condition::And { conditions } => conditions
            .iter()
            .map(describe_condition)
            .collect::<Vec<_>>()
            .join(" AND "),
        Condition::Or { conditions } => conditions
            .iter()
            .map(describe_condition)
            .collect::<Vec<_>>()
            .join(" OR "),
        Condition::Not { condition } => format!("NOT ({})", describe_condition(condition)),
    }
}

fn describe_transform(transform: &Transform) -> String {
    match transform {
        Transform::Uppercase => "uppercase".to_string(),
        Transform::Lowercase => "lowercase".to_string(),
        Transform::Trim => "trim".to_string(),
        Transform::DateFormat { from, to } => format!("date_format({from} -> {to})"),
        Transform::NumberFormat { decimals, .. } => format!("number_format(decimals={decimals})"),
        Transform::Concatenate { values, separator } => {
            format!(
                "concatenate(values={}, separator={separator:?})",
                values.len()
            )
        }
        Transform::Split { delimiter, index } => format!("split({delimiter:?})[{index}]"),
        Transform::Default { value } => format!("default({value})"),
        Transform::Conditional { when, .. } => format!("conditional({})", describe_condition(when)),
        Transform::Chain { transforms } => transforms
            .iter()
            .map(describe_transform)
            .collect::<Vec<_>>()
            .join(" | "),
    }
}

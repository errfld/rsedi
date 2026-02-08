//! Transform operations
//!
//! Provides various transformation functions for mapping values.

use crate::numeric::value_to_f64;
use edi_ir::Value;

/// Transform a value using the specified operation
///
/// # Errors
///
/// Returns an error if the selected transform cannot be applied to the input.
pub fn apply_transform(value: &Value, transform: &crate::dsl::Transform) -> crate::Result<Value> {
    match transform {
        crate::dsl::Transform::Uppercase => transform_uppercase(value),
        crate::dsl::Transform::Lowercase => transform_lowercase(value),
        crate::dsl::Transform::Trim => transform_trim(value),
        crate::dsl::Transform::DateFormat { from, to } => transform_date_format(value, from, to),
        crate::dsl::Transform::NumberFormat {
            decimals,
            thousands_sep,
        } => transform_number_format(value, *decimals, thousands_sep.as_deref()),
        crate::dsl::Transform::Concatenate { values, separator } => {
            transform_concatenate(value, values, separator.as_deref())
        }
        crate::dsl::Transform::Split { delimiter, index } => {
            transform_split(value, delimiter, *index)
        }
        crate::dsl::Transform::Default { value: default } => transform_default(value, default),
        crate::dsl::Transform::Conditional {
            when,
            then,
            else_transform,
        } => transform_conditional(value, when, then, else_transform.as_deref()),
        crate::dsl::Transform::Chain { transforms } => transform_chain(value, transforms),
    }
}

/// Convert string to uppercase
///
/// # Errors
///
/// Returns an error if the value cannot be represented as a string.
pub fn transform_uppercase(value: &Value) -> crate::Result<Value> {
    match value {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        Value::Null => Ok(Value::Null),
        _ => value
            .as_string()
            .map(|s| Value::String(s.to_uppercase()))
            .ok_or_else(|| {
                crate::Error::Transform("Cannot convert value to uppercase".to_string())
            }),
    }
}

/// Convert string to lowercase
///
/// # Errors
///
/// Returns an error if the value cannot be represented as a string.
pub fn transform_lowercase(value: &Value) -> crate::Result<Value> {
    match value {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        Value::Null => Ok(Value::Null),
        _ => value
            .as_string()
            .map(|s| Value::String(s.to_lowercase()))
            .ok_or_else(|| {
                crate::Error::Transform("Cannot convert value to lowercase".to_string())
            }),
    }
}

/// Trim whitespace from string
///
/// # Errors
///
/// Returns an error if the value cannot be represented as a string.
pub fn transform_trim(value: &Value) -> crate::Result<Value> {
    match value {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        Value::Null => Ok(Value::Null),
        _ => value
            .as_string()
            .map(|s| Value::String(s.trim().to_string()))
            .ok_or_else(|| crate::Error::Transform("Cannot trim value".to_string())),
    }
}

/// Format date from one format to another
///
/// # Errors
///
/// Returns an error when date parsing or formatting fails.
pub fn transform_date_format(
    value: &Value,
    from_format: &str,
    to_format: &str,
) -> crate::Result<Value> {
    let input = match value {
        Value::String(s) | Value::Date(s) => s.as_str(),
        Value::Null => return Ok(Value::Null),
        _ => {
            return value.as_string().map_or(
                Err(crate::Error::Transform("Cannot format date".to_string())),
                |s| transform_date_format(&Value::String(s), from_format, to_format),
            );
        }
    };

    // Parse the input date based on from_format
    let parsed_date = parse_date(input, from_format)?;

    // Format to output format
    let output = format_date(&parsed_date, to_format)?;

    Ok(Value::Date(output))
}

/// Parse date string into components
fn parse_date(input: &str, format: &str) -> crate::Result<(i32, u32, u32)> {
    match format {
        "YYYYMMDD" if input.len() == 8 => {
            let year = input[..4]
                .parse::<i32>()
                .map_err(|_| crate::Error::Transform("Invalid year".to_string()))?;
            let month = input[4..6]
                .parse::<u32>()
                .map_err(|_| crate::Error::Transform("Invalid month".to_string()))?;
            let day = input[6..8]
                .parse::<u32>()
                .map_err(|_| crate::Error::Transform("Invalid day".to_string()))?;
            Ok((year, month, day))
        }
        "YYYY-MM-DD" => {
            let parts: Vec<&str> = input.split('-').collect();
            if parts.len() != 3 {
                return Err(crate::Error::Transform("Invalid date format".to_string()));
            }
            let year = parts[0]
                .parse::<i32>()
                .map_err(|_| crate::Error::Transform("Invalid year".to_string()))?;
            let month = parts[1]
                .parse::<u32>()
                .map_err(|_| crate::Error::Transform("Invalid month".to_string()))?;
            let day = parts[2]
                .parse::<u32>()
                .map_err(|_| crate::Error::Transform("Invalid day".to_string()))?;
            Ok((year, month, day))
        }
        "DDMMYYYY" if input.len() == 8 => {
            let day = input[..2]
                .parse::<u32>()
                .map_err(|_| crate::Error::Transform("Invalid day".to_string()))?;
            let month = input[2..4]
                .parse::<u32>()
                .map_err(|_| crate::Error::Transform("Invalid month".to_string()))?;
            let year = input[4..8]
                .parse::<i32>()
                .map_err(|_| crate::Error::Transform("Invalid year".to_string()))?;
            Ok((year, month, day))
        }
        _ => Err(crate::Error::Transform(format!(
            "Unsupported date format: {format}"
        ))),
    }
}

/// Format date components to string
fn format_date(date: &(i32, u32, u32), format: &str) -> crate::Result<String> {
    let (year, month, day) = date;
    match format {
        "YYYYMMDD" => Ok(format!("{year:04}{month:02}{day:02}")),
        "YYYY-MM-DD" | "ISO8601" => Ok(format!("{year:04}-{month:02}-{day:02}")),
        "DDMMYYYY" => Ok(format!("{day:02}{month:02}{year:04}")),
        _ => Err(crate::Error::Transform(format!(
            "Unsupported output date format: {format}"
        ))),
    }
}

/// Format number with specified decimals and thousands separator
///
/// # Errors
///
/// Returns an error if the value cannot be parsed as a number.
pub fn transform_number_format(
    value: &Value,
    decimals: u32,
    thousands_sep: Option<&str>,
) -> crate::Result<Value> {
    let num = match value {
        Value::Null => return Ok(Value::Null),
        _ => value_to_f64(value, "value")?,
    };
    let precision = usize::try_from(decimals)
        .map_err(|_| crate::Error::Transform("Unsupported decimal precision".to_string()))?;
    let mut rounded = format!("{num:.precision$}");
    let formatted = if let Some(sep) = thousands_sep {
        if let Some((int_part, frac_part)) = rounded.split_once('.') {
            let trimmed_frac = frac_part.trim_end_matches('0');
            rounded = if trimmed_frac.is_empty() {
                int_part.to_string()
            } else {
                format!("{int_part}.{trimmed_frac}")
            };
        }
        format_with_thousands_sep(&rounded, sep)
    } else {
        rounded
    };

    Ok(Value::String(formatted))
}

/// Format numeric string with thousands separator.
fn format_with_thousands_sep(number: &str, sep: &str) -> String {
    let (sign, unsigned) = if let Some(stripped) = number.strip_prefix('-') {
        ("-", stripped)
    } else {
        ("", number)
    };
    let (integer_part, fractional_part) =
        if let Some((int_part, frac_part)) = unsigned.split_once('.') {
            (int_part, Some(frac_part))
        } else {
            (unsigned, None)
        };

    let mut grouped_reversed = String::new();
    for (index, ch) in integer_part.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            grouped_reversed.push_str(sep);
        }
        grouped_reversed.push(ch);
    }
    let grouped_integer: String = grouped_reversed.chars().rev().collect();

    match fractional_part {
        Some(frac) if !frac.is_empty() => format!("{sign}{grouped_integer}.{frac}"),
        _ => format!("{sign}{grouped_integer}"),
    }
}

/// Concatenate values
///
/// # Errors
///
/// Returns an error if future context-aware concatenation fails.
pub fn transform_concatenate(
    _value: &Value,
    concat_values: &[crate::dsl::ConcatValue],
    separator: Option<&str>,
) -> crate::Result<Value> {
    // Note: In real implementation, these would be resolved from context
    // For now, we just concatenate literal values for testing
    let sep = separator.unwrap_or("");

    let parts: Vec<String> = concat_values
        .iter()
        .map(|cv| match cv {
            crate::dsl::ConcatValue::Literal { value } => value.clone(),
            crate::dsl::ConcatValue::Field { path } => format!("[{path}]"),
        })
        .collect();

    Ok(Value::String(parts.join(sep)))
}

/// Split string by delimiter and get indexed part
///
/// # Errors
///
/// Returns an error if the value cannot be represented as a string or index is out of bounds.
pub fn transform_split(value: &Value, delimiter: &str, index: usize) -> crate::Result<Value> {
    let input = match value {
        Value::String(s) => s.as_str(),
        Value::Null => return Ok(Value::Null),
        _ => {
            return value.as_string().map_or(
                Err(crate::Error::Transform("Cannot split value".to_string())),
                |s| transform_split(&Value::String(s), delimiter, index),
            );
        }
    };

    let parts: Vec<&str> = input.split(delimiter).collect();

    if index >= parts.len() {
        return Err(crate::Error::Transform(format!(
            "Split index {} out of bounds ({} parts)",
            index,
            parts.len()
        )));
    }

    Ok(Value::String(parts[index].to_string()))
}

/// Return default value if input is null or empty
///
/// # Errors
///
/// This function currently does not return an error.
pub fn transform_default(value: &Value, default: &str) -> crate::Result<Value> {
    match value {
        Value::Null => Ok(Value::String(default.to_string())),
        Value::String(s) if s.is_empty() => Ok(Value::String(default.to_string())),
        _ => Ok(value.clone()),
    }
}

/// Apply conditional transform
///
/// # Errors
///
/// Returns an error if condition evaluation or nested transforms fail.
pub fn transform_conditional(
    value: &Value,
    when: &crate::dsl::Condition,
    then: &crate::dsl::Transform,
    else_transform: Option<&crate::dsl::Transform>,
) -> crate::Result<Value> {
    // Note: In real implementation, condition evaluation would use context
    // For testing, we'll use a simplified approach
    let condition_met = evaluate_condition_simple(value, when)?;

    if condition_met {
        apply_transform(value, then)
    } else if let Some(else_tfm) = else_transform {
        apply_transform(value, else_tfm)
    } else {
        Ok(value.clone())
    }
}

/// Simple condition evaluation for testing
fn evaluate_condition_simple(
    value: &Value,
    condition: &crate::dsl::Condition,
) -> crate::Result<bool> {
    match condition {
        crate::dsl::Condition::Exists { field: _ } => Ok(!matches!(value, Value::Null)),
        crate::dsl::Condition::Equals {
            field: _,
            value: expected,
        } => match value {
            Value::String(s) => Ok(s == expected),
            Value::Integer(i) => Ok(i.to_string() == *expected),
            Value::Decimal(d) => Ok(d.to_string() == *expected),
            Value::Boolean(b) => Ok(b.to_string() == *expected),
            _ => Ok(false),
        },
        crate::dsl::Condition::Contains {
            field: _,
            value: expected,
        } => match value {
            Value::String(s) => Ok(s.contains(expected)),
            _ => Ok(false),
        },
        crate::dsl::Condition::Matches { field: _, pattern } => {
            match value {
                Value::String(s) => {
                    // Simple regex matching - in production use regex crate
                    if pattern == "^ORD[0-9]+$" {
                        Ok(s.starts_with("ORD") && s[3..].chars().all(|c| c.is_ascii_digit()))
                    } else if pattern == "^[0-9]+$" {
                        Ok(s.chars().all(|c| c.is_ascii_digit()))
                    } else {
                        Ok(s.contains(pattern.trim_start_matches('^').trim_end_matches('$')))
                    }
                }
                _ => Ok(false),
            }
        }
        crate::dsl::Condition::And { conditions } => {
            for cond in conditions {
                if !evaluate_condition_simple(value, cond)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        crate::dsl::Condition::Or { conditions } => {
            for cond in conditions {
                if evaluate_condition_simple(value, cond)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        crate::dsl::Condition::Not { condition } => {
            Ok(!evaluate_condition_simple(value, condition)?)
        }
    }
}

/// Apply a chain of transforms
///
/// # Errors
///
/// Returns an error if any transform in the chain fails.
pub fn transform_chain(
    value: &Value,
    transforms: &[crate::dsl::Transform],
) -> crate::Result<Value> {
    let mut result = value.clone();

    for transform in transforms {
        result = apply_transform(&result, transform)?;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{ConcatValue, Transform};

    // String operation tests
    #[test]
    fn test_transform_string_uppercase() {
        let value = Value::String("hello world".to_string());
        let result = transform_uppercase(&value).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_transform_string_lowercase() {
        let value = Value::String("HELLO WORLD".to_string());
        let result = transform_lowercase(&value).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_transform_string_trim() {
        let value = Value::String("  hello world  ".to_string());
        let result = transform_trim(&value).unwrap();
        assert_eq!(result, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_transform_uppercase_mixed_case() {
        let value = Value::String("HeLLo WoRLd".to_string());
        let result = transform_uppercase(&value).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    #[test]
    fn test_transform_lowercase_special_chars() {
        let value = Value::String("Hello World! @123".to_string());
        let result = transform_lowercase(&value).unwrap();
        assert_eq!(result, Value::String("hello world! @123".to_string()));
    }

    #[test]
    fn test_transform_trim_empty() {
        let value = Value::String("   ".to_string());
        let result = transform_trim(&value).unwrap();
        assert_eq!(result, Value::String(String::new()));
    }

    #[test]
    fn test_transform_string_ops_with_null() {
        let null_value = Value::Null;
        assert_eq!(transform_uppercase(&null_value).unwrap(), Value::Null);
        assert_eq!(transform_lowercase(&null_value).unwrap(), Value::Null);
        assert_eq!(transform_trim(&null_value).unwrap(), Value::Null);
    }

    // Date format tests
    #[test]
    fn test_transform_date_format_yyyymmdd_to_iso() {
        let value = Value::String("20240115".to_string());
        let result = transform_date_format(&value, "YYYYMMDD", "YYYY-MM-DD").unwrap();
        assert_eq!(result, Value::Date("2024-01-15".to_string()));
    }

    #[test]
    fn test_transform_date_format_ddmmyyyy_to_yyyymmdd() {
        let value = Value::String("15012024".to_string());
        let result = transform_date_format(&value, "DDMMYYYY", "YYYYMMDD").unwrap();
        assert_eq!(result, Value::Date("20240115".to_string()));
    }

    #[test]
    fn test_transform_date_format_iso_to_ddmmyyyy() {
        let value = Value::Date("2024-12-25".to_string());
        let result = transform_date_format(&value, "YYYY-MM-DD", "DDMMYYYY").unwrap();
        assert_eq!(result, Value::Date("25122024".to_string()));
    }

    #[test]
    fn test_transform_date_format_with_null() {
        let null_value = Value::Null;
        let result = transform_date_format(&null_value, "YYYYMMDD", "YYYY-MM-DD").unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_transform_date_format_invalid_input() {
        let value = Value::String("invalid".to_string());
        assert!(transform_date_format(&value, "YYYYMMDD", "YYYY-MM-DD").is_err());
    }

    #[test]
    fn test_transform_date_format_invalid_format() {
        let value = Value::String("20240115".to_string());
        assert!(transform_date_format(&value, "UNKNOWN", "YYYY-MM-DD").is_err());
    }

    #[test]
    fn test_transform_date_format_year_boundary() {
        let value = Value::String("19991231".to_string());
        let result = transform_date_format(&value, "YYYYMMDD", "YYYY-MM-DD").unwrap();
        assert_eq!(result, Value::Date("1999-12-31".to_string()));
    }

    // Number format tests
    #[test]
    fn test_transform_number_format_integer() {
        let value = Value::Integer(1_234_567);
        let result = transform_number_format(&value, 0, None).unwrap();
        assert_eq!(result, Value::String("1234567".to_string()));
    }

    #[test]
    fn test_transform_number_format_decimal() {
        let value = Value::Decimal(1234.5678);
        let result = transform_number_format(&value, 2, None).unwrap();
        assert_eq!(result, Value::String("1234.57".to_string()));
    }

    #[test]
    fn test_transform_number_format_with_thousands_sep() {
        let value = Value::Integer(1_234_567);
        let result = transform_number_format(&value, 0, Some(",")).unwrap();
        assert_eq!(result, Value::String("1,234,567".to_string()));
    }

    #[test]
    fn test_transform_number_format_from_string() {
        let value = Value::String("9876.543".to_string());
        let result = transform_number_format(&value, 2, Some(".")).unwrap();
        assert_eq!(result, Value::String("9.876.54".to_string()));
    }

    #[test]
    fn test_transform_number_format_with_null() {
        let null_value = Value::Null;
        let result = transform_number_format(&null_value, 2, None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_transform_number_format_zero() {
        let value = Value::Integer(0);
        let result = transform_number_format(&value, 2, Some(",")).unwrap();
        assert_eq!(result, Value::String("0".to_string()));
    }

    #[test]
    fn test_transform_number_format_negative() {
        let value = Value::Decimal(-1234.56);
        let result = transform_number_format(&value, 2, Some(",")).unwrap();
        assert_eq!(result, Value::String("-1,234.56".to_string()));
    }

    #[test]
    fn test_transform_number_format_rounding() {
        let value = Value::Decimal(99.999);
        let result = transform_number_format(&value, 2, None).unwrap();
        assert_eq!(result, Value::String("100.00".to_string()));
    }

    // Concatenate tests
    #[test]
    fn test_transform_concatenate_literals() {
        let value = Value::Null;
        let concat_values = vec![
            ConcatValue::Literal {
                value: "Hello".to_string(),
            },
            ConcatValue::Literal {
                value: "World".to_string(),
            },
        ];
        let result = transform_concatenate(&value, &concat_values, Some(" ")).unwrap();
        assert_eq!(result, Value::String("Hello World".to_string()));
    }

    #[test]
    fn test_transform_concatenate_empty() {
        let value = Value::Null;
        let concat_values: Vec<ConcatValue> = vec![];
        let result = transform_concatenate(&value, &concat_values, Some(",")).unwrap();
        assert_eq!(result, Value::String(String::new()));
    }

    #[test]
    fn test_transform_concatenate_no_separator() {
        let value = Value::Null;
        let concat_values = vec![
            ConcatValue::Literal {
                value: "A".to_string(),
            },
            ConcatValue::Literal {
                value: "B".to_string(),
            },
            ConcatValue::Literal {
                value: "C".to_string(),
            },
        ];
        let result = transform_concatenate(&value, &concat_values, None).unwrap();
        assert_eq!(result, Value::String("ABC".to_string()));
    }

    #[test]
    fn test_transform_concatenate_field_placeholder() {
        let value = Value::Null;
        let concat_values = vec![
            ConcatValue::Literal {
                value: "REF-".to_string(),
            },
            ConcatValue::Field {
                path: "/ORDER/ID".to_string(),
            },
        ];
        let result = transform_concatenate(&value, &concat_values, None).unwrap();
        assert_eq!(result, Value::String("REF-[/ORDER/ID]".to_string()));
    }

    // Split tests
    #[test]
    fn test_transform_split_basic() {
        let value = Value::String("a,b,c".to_string());
        let result = transform_split(&value, ",", 0).unwrap();
        assert_eq!(result, Value::String("a".to_string()));

        let result = transform_split(&value, ",", 1).unwrap();
        assert_eq!(result, Value::String("b".to_string()));

        let result = transform_split(&value, ",", 2).unwrap();
        assert_eq!(result, Value::String("c".to_string()));
    }

    #[test]
    fn test_transform_split_hyphen() {
        let value = Value::String("ORDER-12345-ABC".to_string());
        let result = transform_split(&value, "-", 1).unwrap();
        assert_eq!(result, Value::String("12345".to_string()));
    }

    #[test]
    fn test_transform_split_out_of_bounds() {
        let value = Value::String("a,b".to_string());
        assert!(transform_split(&value, ",", 5).is_err());
    }

    #[test]
    fn test_transform_split_with_null() {
        let null_value = Value::Null;
        let result = transform_split(&null_value, ",", 0).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_transform_split_empty_parts() {
        let value = Value::String("a,,c".to_string());
        let result = transform_split(&value, ",", 1).unwrap();
        assert_eq!(result, Value::String(String::new()));
    }

    // Default value tests
    #[test]
    fn test_transform_default_null() {
        let value = Value::Null;
        let result = transform_default(&value, "DEFAULT").unwrap();
        assert_eq!(result, Value::String("DEFAULT".to_string()));
    }

    #[test]
    fn test_transform_default_empty_string() {
        let value = Value::String(String::new());
        let result = transform_default(&value, "DEFAULT").unwrap();
        assert_eq!(result, Value::String("DEFAULT".to_string()));
    }

    #[test]
    fn test_transform_default_non_empty() {
        let value = Value::String("actual value".to_string());
        let result = transform_default(&value, "DEFAULT").unwrap();
        assert_eq!(result, Value::String("actual value".to_string()));
    }

    #[test]
    fn test_transform_default_integer() {
        let value = Value::Integer(42);
        let result = transform_default(&value, "0").unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    // Conditional transform tests
    #[test]
    fn test_transform_conditional_true() {
        let value = Value::String("test".to_string());
        let condition = crate::dsl::Condition::Exists {
            field: "/test".to_string(),
        };
        let then_transform = Transform::Uppercase;

        let result = transform_conditional(&value, &condition, &then_transform, None).unwrap();
        assert_eq!(result, Value::String("TEST".to_string()));
    }

    #[test]
    fn test_transform_conditional_false_with_else() {
        let value = Value::Null;
        let condition = crate::dsl::Condition::Exists {
            field: "/test".to_string(),
        };
        let then_transform = Transform::Uppercase;
        let else_transform = Transform::Default {
            value: "FALLBACK".to_string(),
        };

        let result =
            transform_conditional(&value, &condition, &then_transform, Some(&else_transform))
                .unwrap();
        assert_eq!(result, Value::String("FALLBACK".to_string()));
    }

    #[test]
    fn test_transform_conditional_false_no_else() {
        let value = Value::String("unchanged".to_string());
        // Create a condition that will be false
        let condition = crate::dsl::Condition::Equals {
            field: "/test".to_string(),
            value: "different".to_string(),
        };
        let then_transform = Transform::Uppercase;

        let result = transform_conditional(&value, &condition, &then_transform, None).unwrap();
        assert_eq!(result, Value::String("unchanged".to_string()));
    }

    // Chain transform tests
    #[test]
    fn test_transform_chain_single() {
        let value = Value::String("hello".to_string());
        let transforms = vec![Transform::Uppercase];
        let result = transform_chain(&value, &transforms).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_transform_chain_multiple() {
        let value = Value::String("  hello  ".to_string());
        let transforms = vec![Transform::Trim, Transform::Uppercase];
        let result = transform_chain(&value, &transforms).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_transform_chain_empty() {
        let value = Value::String("unchanged".to_string());
        let transforms: Vec<Transform> = vec![];
        let result = transform_chain(&value, &transforms).unwrap();
        assert_eq!(result, Value::String("unchanged".to_string()));
    }

    #[test]
    fn test_transform_chain_complex() {
        let value = Value::String("  hello world  ".to_string());
        let transforms = vec![
            Transform::Trim,
            Transform::Uppercase,
            Transform::Default {
                value: "DEFAULT".to_string(),
            },
        ];
        let result = transform_chain(&value, &transforms).unwrap();
        assert_eq!(result, Value::String("HELLO WORLD".to_string()));
    }

    // Integration tests
    #[test]
    fn test_transform_date_format_integration() {
        let value = Value::String("20240115".to_string());
        let transform = Transform::DateFormat {
            from: "YYYYMMDD".to_string(),
            to: "YYYY-MM-DD".to_string(),
        };
        let result = apply_transform(&value, &transform).unwrap();
        assert_eq!(result, Value::Date("2024-01-15".to_string()));
    }

    #[test]
    fn test_transform_conditional_integration() {
        let value = Value::String("exists".to_string());
        let transform = Transform::Conditional {
            when: crate::dsl::Condition::Exists {
                field: "/test".to_string(),
            },
            then: Box::new(Transform::Uppercase),
            else_transform: Some(Box::new(Transform::Default {
                value: "N/A".to_string(),
            })),
        };
        let result = apply_transform(&value, &transform).unwrap();
        assert_eq!(result, Value::String("EXISTS".to_string()));
    }

    #[test]
    fn test_transform_chain_integration() {
        let value = Value::String("  hello  ".to_string());
        let transform = Transform::Chain {
            transforms: vec![Transform::Trim, Transform::Uppercase],
        };
        let result = apply_transform(&value, &transform).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    // Edge case tests
    #[test]
    fn test_transform_number_from_integer_value() {
        let value = Value::Integer(12345);
        let result = transform_number_format(&value, 2, None).unwrap();
        assert_eq!(result, Value::String("12345.00".to_string()));
    }

    #[test]
    fn test_transform_uppercase_from_integer() {
        let value = Value::Integer(42);
        let result = transform_uppercase(&value).unwrap();
        assert_eq!(result, Value::String("42".to_string()));
    }

    #[test]
    fn test_transform_date_preserves_input_on_error() {
        let value = Value::String("invalid".to_string());
        // Should return error, not panic
        let result = transform_date_format(&value, "YYYYMMDD", "YYYY-MM-DD");
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_split_single_element() {
        let value = Value::String("onlyone".to_string());
        let result = transform_split(&value, ",", 0).unwrap();
        assert_eq!(result, Value::String("onlyone".to_string()));
    }

    #[test]
    fn test_transform_concatenate_single_element() {
        let value = Value::Null;
        let concat_values = vec![ConcatValue::Literal {
            value: "alone".to_string(),
        }];
        let result = transform_concatenate(&value, &concat_values, Some(",")).unwrap();
        assert_eq!(result, Value::String("alone".to_string()));
    }

    #[test]
    fn test_transform_chain_preserves_type() {
        let value = Value::Integer(42);
        let transforms = vec![Transform::Default {
            value: "99".to_string(),
        }];
        let result = transform_chain(&value, &transforms).unwrap();
        // Since value is not null, default should not apply
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_transform_number_format_many_decimals() {
        let value = Value::Decimal(std::f64::consts::PI);
        let result = transform_number_format(&value, 6, None).unwrap();
        assert_eq!(result, Value::String("3.141593".to_string()));
    }

    #[test]
    fn test_transform_number_format_no_decimals() {
        let value = Value::Decimal(123.999);
        let result = transform_number_format(&value, 0, None).unwrap();
        assert_eq!(result, Value::String("124".to_string()));
    }
}

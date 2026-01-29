//! Integration test: Extension API usage
//!
//! Tests custom extension registration and invocation.

use edi_ir::{Document, Node, NodeType, Value};
use edi_mapping::{
    extensions::{
        create_math_utils_extension, create_string_utils_extension, Extension, ExtensionRegistry,
    },
    MappingDsl, MappingRuntime,
};

#[test]
fn test_extension_registration() {
    let registry = ExtensionRegistry::new();

    // Register string utilities
    let string_ext = create_string_utils_extension();
    assert!(registry.register(string_ext).is_ok());

    // Verify extension was registered
    assert!(registry.has_extension("string_utils").unwrap());
    assert_eq!(registry.len().unwrap(), 1);
}

#[test]
fn test_multiple_extensions() {
    let registry = ExtensionRegistry::new();

    // Register both extensions
    let string_ext = create_string_utils_extension();
    let math_ext = create_math_utils_extension();

    assert!(registry.register(string_ext).is_ok());
    assert!(registry.register(math_ext).is_ok());

    assert_eq!(registry.len().unwrap(), 2);

    // Call functions from both
    let result = registry
        .call(
            "string_utils",
            "reverse",
            &[Value::String("hello".to_string())],
        )
        .unwrap();
    assert_eq!(result, Value::String("olleh".to_string()));

    let result = registry
        .call("math_utils", "add", &[Value::Integer(5), Value::Integer(3)])
        .unwrap();
    assert_eq!(result, Value::Decimal(8.0));
}

#[test]
fn test_custom_extension_creation() {
    let mut custom_ext = Extension::new("custom_logic", "1.0.0");

    custom_ext
        .register_function("double", |args| {
            if args.is_empty() {
                return Err(edi_mapping::Error::Transform(
                    "double requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Value::Integer(i) => Ok(Value::Integer(i * 2)),
                Value::Decimal(d) => Ok(Value::Decimal(d * 2.0)),
                _ => Err(edi_mapping::Error::Transform(
                    "Can only double numbers".to_string(),
                )),
            }
        })
        .register_function("prefix", |args| {
            if args.len() < 2 {
                return Err(edi_mapping::Error::Transform(
                    "prefix requires 2 arguments".to_string(),
                ));
            }
            let prefix = args[0].as_string().ok_or_else(|| {
                edi_mapping::Error::Transform("First arg must be string".to_string())
            })?;
            let value = args[1].as_string().ok_or_else(|| {
                edi_mapping::Error::Transform("Second arg must be string".to_string())
            })?;
            Ok(Value::String(format!("{}{}", prefix, value)))
        })
        .on_init(|| {
            tracing::debug!("Custom extension initialized");
            Ok(())
        })
        .on_cleanup(|| {
            tracing::debug!("Custom extension cleaned up");
            Ok(())
        });

    let registry = ExtensionRegistry::new();
    assert!(registry.register(custom_ext).is_ok());

    // Test double function
    let result = registry
        .call("custom_logic", "double", &[Value::Integer(21)])
        .unwrap();
    assert_eq!(result, Value::Integer(42));

    // Test prefix function
    let result = registry
        .call(
            "custom_logic",
            "prefix",
            &[
                Value::String("ORDER-".to_string()),
                Value::String("12345".to_string()),
            ],
        )
        .unwrap();
    assert_eq!(result, Value::String("ORDER-12345".to_string()));
}

#[test]
fn test_extension_lifecycle_hooks() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let init_count = Arc::new(AtomicUsize::new(0));
    let cleanup_count = Arc::new(AtomicUsize::new(0));

    let init_clone = init_count.clone();
    let cleanup_clone = cleanup_count.clone();

    let mut ext = Extension::new("lifecycle_test", "1.0.0");
    ext.on_init(move || {
        init_clone.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .on_cleanup(move || {
        cleanup_clone.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .register_function("noop", |_args| Ok(Value::Null));

    let registry = ExtensionRegistry::new();

    // Init should be called on register
    assert_eq!(init_count.load(Ordering::SeqCst), 0);
    registry.register(ext).unwrap();
    assert_eq!(init_count.load(Ordering::SeqCst), 1);

    // Cleanup should be called on unregister
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 0);
    registry.unregister("lifecycle_test").unwrap();
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_extension_error_handling() {
    let registry = ExtensionRegistry::new();

    let mut ext = Extension::new("error_test", "1.0.0");
    ext.register_function("always_fails", |_args| {
        Err(edi_mapping::Error::Transform(
            "Intentional failure".to_string(),
        ))
    });

    registry.register(ext).unwrap();

    let result = registry.call("error_test", "always_fails", &[]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Intentional failure"));
}

#[test]
fn test_extension_not_found() {
    let registry = ExtensionRegistry::new();

    let result = registry.call("nonexistent", "function", &[]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Extension 'nonexistent' not found"));
}

#[test]
fn test_extension_function_not_found() {
    let registry = ExtensionRegistry::new();
    let ext = create_string_utils_extension();
    registry.register(ext).unwrap();

    let result = registry.call("string_utils", "nonexistent", &[]);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Function 'nonexistent' not found"));
}

#[test]
fn test_extension_with_runtime() {
    // Create extension registry with custom functions
    let registry = ExtensionRegistry::new();

    let mut ext = Extension::new("validation", "1.0.0");
    ext.register_function("is_valid_order", |args| {
        if args.is_empty() {
            return Ok(Value::Boolean(false));
        }
        match &args[0] {
            Value::String(s) => {
                // Order is valid if it starts with "ORD" and has at least 5 more characters
                let valid = s.starts_with("ORD") && s.len() >= 8;
                Ok(Value::Boolean(valid))
            }
            _ => Ok(Value::Boolean(false)),
        }
    });

    registry.register(ext).unwrap();

    // Create runtime with extensions
    let mut runtime = MappingRuntime::with_extensions(registry);

    // Use extension through runtime
    let result = runtime
        .extensions()
        .call(
            "validation",
            "is_valid_order",
            &[Value::String("ORD12345".to_string())],
        )
        .unwrap();
    assert_eq!(result, Value::Boolean(true));

    let result = runtime
        .extensions()
        .call(
            "validation",
            "is_valid_order",
            &[Value::String("INVALID".to_string())],
        )
        .unwrap();
    assert_eq!(result, Value::Boolean(false));
}

#[test]
fn test_string_utils_functions() {
    let registry = ExtensionRegistry::new();
    let ext = create_string_utils_extension();
    registry.register(ext).unwrap();

    // Test reverse
    let result = registry
        .call(
            "string_utils",
            "reverse",
            &[Value::String("Rust".to_string())],
        )
        .unwrap();
    assert_eq!(result, Value::String("tsuR".to_string()));

    // Test replace
    let result = registry
        .call(
            "string_utils",
            "replace",
            &[
                Value::String("Hello World".to_string()),
                Value::String("World".to_string()),
                Value::String("Rust".to_string()),
            ],
        )
        .unwrap();
    assert_eq!(result, Value::String("Hello Rust".to_string()));

    // Test substring
    let result = registry
        .call(
            "string_utils",
            "substring",
            &[
                Value::String("Hello World".to_string()),
                Value::String("6".to_string()),
                Value::String("11".to_string()),
            ],
        )
        .unwrap();
    assert_eq!(result, Value::String("World".to_string()));
}

#[test]
fn test_math_utils_functions() {
    let registry = ExtensionRegistry::new();
    let ext = create_math_utils_extension();
    registry.register(ext).unwrap();

    // Test add with integers
    let result = registry
        .call(
            "math_utils",
            "add",
            &[Value::Integer(10), Value::Integer(20)],
        )
        .unwrap();
    assert_eq!(result, Value::Decimal(30.0));

    // Test add with decimals
    let result = registry
        .call(
            "math_utils",
            "add",
            &[Value::Decimal(10.5), Value::Decimal(20.5)],
        )
        .unwrap();
    assert_eq!(result, Value::Decimal(31.0));

    // Test multiply
    let result = registry
        .call(
            "math_utils",
            "multiply",
            &[Value::Integer(5), Value::Integer(6)],
        )
        .unwrap();
    assert_eq!(result, Value::Decimal(30.0));

    // Test multiply with mixed types
    let result = registry
        .call(
            "math_utils",
            "multiply",
            &[Value::Decimal(2.5), Value::Integer(4)],
        )
        .unwrap();
    assert_eq!(result, Value::Decimal(10.0));
}

#[test]
fn test_extension_with_various_types() {
    let registry = ExtensionRegistry::new();

    let mut ext = Extension::new("type_test", "1.0.0");
    ext.register_function("type_checker", |args| {
        if args.is_empty() {
            return Ok(Value::String("no args".to_string()));
        }
        let type_name = match &args[0] {
            Value::String(_) => "string",
            Value::Integer(_) => "integer",
            Value::Decimal(_) => "decimal",
            Value::Boolean(_) => "boolean",
            Value::Null => "null",
            _ => "other",
        };
        Ok(Value::String(type_name.to_string()))
    });

    registry.register(ext).unwrap();

    assert_eq!(
        registry
            .call(
                "type_test",
                "type_checker",
                &[Value::String("test".to_string())]
            )
            .unwrap(),
        Value::String("string".to_string())
    );

    assert_eq!(
        registry
            .call("type_test", "type_checker", &[Value::Integer(42)])
            .unwrap(),
        Value::String("integer".to_string())
    );

    assert_eq!(
        registry
            .call("type_test", "type_checker", &[Value::Decimal(3.14)])
            .unwrap(),
        Value::String("decimal".to_string())
    );

    assert_eq!(
        registry
            .call("type_test", "type_checker", &[Value::Boolean(true)])
            .unwrap(),
        Value::String("boolean".to_string())
    );

    assert_eq!(
        registry
            .call("type_test", "type_checker", &[Value::Null])
            .unwrap(),
        Value::String("null".to_string())
    );
}

#[test]
fn test_extension_cleanup_all() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let cleanup_count = Arc::new(AtomicUsize::new(0));
    let registry = ExtensionRegistry::new();

    // Register multiple extensions with cleanup hooks
    for i in 0..5 {
        let count = cleanup_count.clone();
        let mut ext = Extension::new(format!("ext_{}", i), "1.0.0");
        ext.on_cleanup(move || {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        registry.register(ext).unwrap();
    }

    assert_eq!(cleanup_count.load(Ordering::SeqCst), 0);
    registry.cleanup_all().unwrap();
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 5);
    assert!(registry.is_empty().unwrap());
}

#[test]
fn test_extension_null_handling() {
    let registry = ExtensionRegistry::new();

    let mut ext = Extension::new("null_handler", "1.0.0");
    ext.register_function("handle_null", |args| {
        if args.is_empty() || args[0].is_null() {
            return Ok(Value::String("was null".to_string()));
        }
        Ok(Value::String("was not null".to_string()))
    });

    registry.register(ext).unwrap();

    let result = registry
        .call("null_handler", "handle_null", &[Value::Null])
        .unwrap();
    assert_eq!(result, Value::String("was null".to_string()));

    let result = registry
        .call(
            "null_handler",
            "handle_null",
            &[Value::String("test".to_string())],
        )
        .unwrap();
    assert_eq!(result, Value::String("was not null".to_string()));
}

#[test]
fn test_extension_function_list() {
    let mut ext = Extension::new("multi_func", "1.0.0");
    ext.register_function("func_a", |_args| Ok(Value::Null))
        .register_function("func_b", |_args| Ok(Value::Null))
        .register_function("func_c", |_args| Ok(Value::Null));

    let names = ext.function_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"func_a".to_string()));
    assert!(names.contains(&"func_b".to_string()));
    assert!(names.contains(&"func_c".to_string()));
}

#[test]
fn test_extension_version_tracking() {
    let ext_v1 = Extension::new("test_ext", "1.0.0");
    let ext_v2 = Extension::new("test_ext", "2.0.0");

    assert_eq!(ext_v1.version, "1.0.0");
    assert_eq!(ext_v2.version, "2.0.0");

    let registry = ExtensionRegistry::new();
    registry.register(ext_v1).unwrap();

    // Registering with same name should replace
    registry.register(ext_v2).unwrap();

    let ext = registry.get_extension("test_ext").unwrap().unwrap();
    assert_eq!(ext.version, "2.0.0");
}

// Required for tests using Arc
use std::sync::Arc;

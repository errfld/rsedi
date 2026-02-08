//! Extension API
//!
//! Provides mechanisms for registering and calling custom extension functions.

use crate::numeric::value_to_f64;
use edi_ir::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Type alias for extension function
pub type ExtensionFn = Arc<dyn Fn(&[Value]) -> crate::Result<Value> + Send + Sync>;

/// Type alias for initialization hook
pub type InitFn = Arc<dyn Fn() -> crate::Result<()> + Send + Sync>;

/// Type alias for cleanup hook
pub type CleanupFn = Arc<dyn Fn() -> crate::Result<()> + Send + Sync>;

/// An extension providing custom functionality
#[derive(Clone)]
pub struct Extension {
    /// Extension name
    pub name: String,

    /// Extension version
    pub version: String,

    /// Registered functions
    functions: HashMap<String, ExtensionFn>,

    /// Initialization hook
    init_hook: Option<InitFn>,

    /// Cleanup hook
    cleanup_hook: Option<CleanupFn>,
}

impl Extension {
    /// Create a new extension
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            functions: HashMap::new(),
            init_hook: None,
            cleanup_hook: None,
        }
    }

    /// Register a function
    pub fn register_function(
        &mut self,
        name: impl Into<String>,
        func: impl Fn(&[Value]) -> crate::Result<Value> + Send + Sync + 'static,
    ) -> &mut Self {
        self.functions.insert(name.into(), Arc::new(func));
        self
    }

    /// Set initialization hook
    pub fn on_init(
        &mut self,
        hook: impl Fn() -> crate::Result<()> + Send + Sync + 'static,
    ) -> &mut Self {
        self.init_hook = Some(Arc::new(hook));
        self
    }

    /// Set cleanup hook
    pub fn on_cleanup(
        &mut self,
        hook: impl Fn() -> crate::Result<()> + Send + Sync + 'static,
    ) -> &mut Self {
        self.cleanup_hook = Some(Arc::new(hook));
        self
    }

    /// Get a function by name
    #[must_use]
    pub fn get_function(&self, name: &str) -> Option<ExtensionFn> {
        self.functions.get(name).cloned()
    }

    /// Check if function exists
    #[must_use]
    pub fn has_function(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    /// Initialize the extension
    ///
    /// # Errors
    ///
    /// Returns an error if the extension initialization hook fails.
    pub fn initialize(&self) -> crate::Result<()> {
        if let Some(hook) = &self.init_hook {
            hook()
        } else {
            Ok(())
        }
    }

    /// Cleanup the extension
    ///
    /// # Errors
    ///
    /// Returns an error if the extension cleanup hook fails.
    pub fn cleanup(&self) -> crate::Result<()> {
        if let Some(hook) = &self.cleanup_hook {
            hook()
        } else {
            Ok(())
        }
    }

    /// Get list of registered function names
    #[must_use]
    pub fn function_names(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }
}

impl std::fmt::Debug for Extension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extension")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("functions", &self.function_names())
            .field("has_init", &self.init_hook.is_some())
            .field("has_cleanup", &self.cleanup_hook.is_some())
            .finish()
    }
}

/// Extension registry for managing multiple extensions
#[derive(Debug, Default, Clone)]
pub struct ExtensionRegistry {
    extensions: Arc<Mutex<HashMap<String, Extension>>>,
}

impl ExtensionRegistry {
    /// Create a new extension registry
    #[must_use]
    pub fn new() -> Self {
        Self {
            extensions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register an extension
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned or extension initialization fails.
    pub fn register(&self, extension: Extension) -> crate::Result<()> {
        let mut extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;

        // Initialize the extension
        extension.initialize()?;

        extensions.insert(extension.name.clone(), extension);
        Ok(())
    }

    /// Unregister an extension
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned or extension cleanup fails.
    pub fn unregister(&self, name: &str) -> crate::Result<()> {
        let mut extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;

        if let Some(extension) = extensions.remove(name) {
            extension.cleanup()?;
        }

        Ok(())
    }

    /// Get an extension by name
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn get_extension(&self, name: &str) -> crate::Result<Option<Extension>> {
        let extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;
        Ok(extensions.get(name).cloned())
    }

    /// Check if extension exists
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn has_extension(&self, name: &str) -> crate::Result<bool> {
        let extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;
        Ok(extensions.contains_key(name))
    }

    /// Call a function from an extension
    ///
    /// # Errors
    ///
    /// Returns an error if the extension/function is missing, lock acquisition fails,
    /// or the extension function itself fails.
    pub fn call(
        &self,
        extension_name: &str,
        function_name: &str,
        args: &[Value],
    ) -> crate::Result<Value> {
        let extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;

        let extension = extensions.get(extension_name).ok_or_else(|| {
            crate::Error::Mapping(format!("Extension '{extension_name}' not found"))
        })?;

        let func = extension.get_function(function_name).ok_or_else(|| {
            crate::Error::Mapping(format!(
                "Function '{}' not found in extension '{}', available functions: {:?}",
                function_name,
                extension_name,
                extension.function_names()
            ))
        })?;

        func(args)
    }

    /// Get all extension names
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn extension_names(&self) -> crate::Result<Vec<String>> {
        let extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;
        Ok(extensions.keys().cloned().collect())
    }

    /// Cleanup all extensions
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned or any extension cleanup fails.
    pub fn cleanup_all(&self) -> crate::Result<()> {
        let mut extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;

        for (_, extension) in extensions.drain() {
            extension.cleanup()?;
        }

        Ok(())
    }

    /// Get number of registered extensions
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn len(&self) -> crate::Result<usize> {
        let extensions = self
            .extensions
            .lock()
            .map_err(|_| crate::Error::Mapping("Failed to lock extension registry".to_string()))?;
        Ok(extensions.len())
    }

    /// Check if registry is empty
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn is_empty(&self) -> crate::Result<bool> {
        Ok(self.len()? == 0)
    }
}

/// Built-in string utilities extension
#[must_use]
pub fn create_string_utils_extension() -> Extension {
    let mut ext = Extension::new("string_utils", "1.0.0");

    ext.register_function("reverse", |args| {
        if args.is_empty() {
            return Err(crate::Error::Transform(
                "reverse requires 1 argument".to_string(),
            ));
        }
        match &args[0] {
            Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
            Value::Null => Ok(Value::Null),
            _ => args[0]
                .as_string()
                .map(|s| Value::String(s.chars().rev().collect()))
                .ok_or_else(|| crate::Error::Transform("Cannot reverse value".to_string())),
        }
    })
    .register_function("replace", |args| {
        if args.len() < 3 {
            return Err(crate::Error::Transform(
                "replace requires 3 arguments".to_string(),
            ));
        }
        let input = args[0]
            .as_string()
            .ok_or_else(|| crate::Error::Transform("First argument must be string".to_string()))?;
        let from = args[1]
            .as_string()
            .ok_or_else(|| crate::Error::Transform("Second argument must be string".to_string()))?;
        let to = args[2]
            .as_string()
            .ok_or_else(|| crate::Error::Transform("Third argument must be string".to_string()))?;
        Ok(Value::String(input.replace(&from, &to)))
    })
    .register_function("substring", |args| {
        if args.len() < 3 {
            return Err(crate::Error::Transform(
                "substring requires 3 arguments".to_string(),
            ));
        }
        let input = args[0]
            .as_string()
            .ok_or_else(|| crate::Error::Transform("First argument must be string".to_string()))?;
        let start = args[1]
            .as_string()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| {
                crate::Error::Transform("Second argument must be integer".to_string())
            })?;
        let end = args[2]
            .as_string()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| crate::Error::Transform("Third argument must be integer".to_string()))?;

        if start >= input.len() || end > input.len() || start > end {
            return Err(crate::Error::Transform(
                "Invalid substring range".to_string(),
            ));
        }

        Ok(Value::String(input[start..end].to_string()))
    })
    .on_init(|| {
        tracing::debug!("String utils extension initialized");
        Ok(())
    })
    .on_cleanup(|| {
        tracing::debug!("String utils extension cleaned up");
        Ok(())
    });

    ext
}

/// Built-in math utilities extension
#[must_use]
pub fn create_math_utils_extension() -> Extension {
    let mut ext = Extension::new("math_utils", "1.0.0");

    ext.register_function("add", |args| {
        if args.len() < 2 {
            return Err(crate::Error::Transform(
                "add requires 2 arguments".to_string(),
            ));
        }
        let a = value_to_f64(&args[0], "first")?;
        let b = value_to_f64(&args[1], "second")?;
        Ok(Value::Decimal(a + b))
    })
    .register_function("multiply", |args| {
        if args.len() < 2 {
            return Err(crate::Error::Transform(
                "multiply requires 2 arguments".to_string(),
            ));
        }
        let a = value_to_f64(&args[0], "first")?;
        let b = value_to_f64(&args[1], "second")?;
        Ok(Value::Decimal(a * b))
    });

    ext
}

#[cfg(test)]
mod tests {
    use super::*;

    // Register extension tests
    #[test]
    fn test_register_extension() {
        let registry = ExtensionRegistry::new();
        let ext = Extension::new("test_ext", "1.0.0");

        assert!(registry.register(ext).is_ok());
        assert!(registry.has_extension("test_ext").unwrap());
        assert_eq!(registry.len().unwrap(), 1);
    }

    #[test]
    fn test_register_multiple_extensions() {
        let registry = ExtensionRegistry::new();
        let ext1 = Extension::new("ext1", "1.0.0");
        let ext2 = Extension::new("ext2", "2.0.0");

        assert!(registry.register(ext1).is_ok());
        assert!(registry.register(ext2).is_ok());
        assert_eq!(registry.len().unwrap(), 2);

        let names = registry.extension_names().unwrap();
        assert!(names.contains(&"ext1".to_string()));
        assert!(names.contains(&"ext2".to_string()));
    }

    #[test]
    fn test_register_duplicate_extension() {
        let registry = ExtensionRegistry::new();
        let ext1 = Extension::new("duplicate", "1.0.0");
        let ext2 = Extension::new("duplicate", "2.0.0");

        assert!(registry.register(ext1).is_ok());
        assert!(registry.register(ext2).is_ok()); // Overwrites

        let ext = registry.get_extension("duplicate").unwrap().unwrap();
        assert_eq!(ext.version, "2.0.0");
    }

    // Call extension tests
    #[test]
    fn test_call_extension() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("string_ops", "1.0.0");

        ext.register_function("to_upper", |args| {
            if args.is_empty() {
                return Err(crate::Error::Transform(
                    "to_upper requires 1 argument".to_string(),
                ));
            }
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_uppercase())),
                _ => Err(crate::Error::Transform("Invalid argument type".to_string())),
            }
        });

        registry.register(ext).unwrap();

        let result = registry
            .call(
                "string_ops",
                "to_upper",
                &[Value::String("hello".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_call_extension_multiple_args() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("math_ops", "1.0.0");

        ext.register_function("sum", |args| {
            let sum: f64 = args
                .iter()
                .filter_map(|v| match v {
                    Value::Integer(i) => i.to_string().parse::<f64>().ok(),
                    Value::Decimal(d) => Some(*d),
                    _ => None,
                })
                .sum();
            Ok(Value::Decimal(sum))
        });

        registry.register(ext).unwrap();

        let result = registry
            .call(
                "math_ops",
                "sum",
                &[Value::Integer(10), Value::Integer(20), Value::Decimal(5.5)],
            )
            .unwrap();
        assert_eq!(result, Value::Decimal(35.5));
    }

    #[test]
    fn test_call_extension_not_found() {
        let registry = ExtensionRegistry::new();

        let result = registry.call("nonexistent", "func", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Extension 'nonexistent' not found")
        );
    }

    #[test]
    fn test_call_function_not_found() {
        let registry = ExtensionRegistry::new();
        let ext = Extension::new("test_ext", "1.0.0");
        registry.register(ext).unwrap();

        let result = registry.call("test_ext", "nonexistent_func", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Function 'nonexistent_func' not found")
        );
    }

    // Error handling tests
    #[test]
    fn test_extension_error_handling() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("error_ext", "1.0.0");

        ext.register_function("always_fail", |_args| {
            Err(crate::Error::Transform("Intentional failure".to_string()))
        });

        registry.register(ext).unwrap();

        let result = registry.call("error_ext", "always_fail", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Intentional failure")
        );
    }

    #[test]
    fn test_extension_error_invalid_args() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("strict_ext", "1.0.0");

        ext.register_function("requires_two", |args| {
            if args.len() != 2 {
                return Err(crate::Error::Transform(format!(
                    "Expected 2 arguments, got {}",
                    args.len()
                )));
            }
            Ok(Value::Null)
        });

        registry.register(ext).unwrap();

        let result = registry.call("strict_ext", "requires_two", &[Value::Integer(1)]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected 2 arguments")
        );
    }

    #[test]
    fn test_extension_error_type_mismatch() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("type_check_ext", "1.0.0");

        ext.register_function("requires_string", |args| match &args[0] {
            Value::String(_) => Ok(Value::Boolean(true)),
            _ => Err(crate::Error::Transform(
                "First argument must be string".to_string(),
            )),
        });

        registry.register(ext).unwrap();

        let result = registry.call("type_check_ext", "requires_string", &[Value::Integer(42)]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be string"));
    }

    // Extension lifecycle tests
    #[test]
    fn test_extension_lifecycle_init() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let initialized = Arc::new(AtomicBool::new(false));
        let initialized_clone = initialized.clone();

        let mut ext = Extension::new("lifecycle_ext", "1.0.0");
        ext.on_init(move || {
            initialized_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let registry = ExtensionRegistry::new();
        registry.register(ext).unwrap();

        assert!(initialized.load(Ordering::SeqCst));
    }

    #[test]
    fn test_extension_lifecycle_cleanup() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_up_clone = cleaned_up.clone();

        let mut ext = Extension::new("cleanup_ext", "1.0.0");
        ext.on_cleanup(move || {
            cleaned_up_clone.store(true, Ordering::SeqCst);
            Ok(())
        });

        let registry = ExtensionRegistry::new();
        registry.register(ext).unwrap();
        registry.unregister("cleanup_ext").unwrap();

        assert!(cleaned_up.load(Ordering::SeqCst));
    }

    #[test]
    fn test_extension_cleanup_all() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let cleanup_count = Arc::new(AtomicUsize::new(0));

        let registry = ExtensionRegistry::new();

        for i in 0..3 {
            let count = cleanup_count.clone();
            let mut ext = Extension::new(format!("ext_{i}"), "1.0.0");
            ext.on_cleanup(move || {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            registry.register(ext).unwrap();
        }

        assert_eq!(cleanup_count.load(Ordering::SeqCst), 0);
        registry.cleanup_all().unwrap();
        assert_eq!(cleanup_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_extension_lifecycle_error_in_init() {
        let mut ext = Extension::new("failing_init_ext", "1.0.0");
        ext.on_init(|| Err(crate::Error::Transform("Init failed".to_string())));

        let registry = ExtensionRegistry::new();
        let result = registry.register(ext);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Init failed"));
        assert!(!registry.has_extension("failing_init_ext").unwrap());
    }

    #[test]
    fn test_extension_lifecycle_error_in_cleanup() {
        let mut ext = Extension::new("failing_cleanup_ext", "1.0.0");
        ext.on_cleanup(|| Err(crate::Error::Transform("Cleanup failed".to_string())));

        let registry = ExtensionRegistry::new();
        registry.register(ext).unwrap();

        let result = registry.unregister("failing_cleanup_ext");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cleanup failed"));
    }

    // Built-in extension tests
    #[test]
    fn test_string_utils_extension() {
        let registry = ExtensionRegistry::new();
        let ext = create_string_utils_extension();
        registry.register(ext).unwrap();

        // Test reverse
        let result = registry
            .call(
                "string_utils",
                "reverse",
                &[Value::String("hello".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::String("olleh".to_string()));

        // Test replace
        let result = registry
            .call(
                "string_utils",
                "replace",
                &[
                    Value::String("hello world".to_string()),
                    Value::String("world".to_string()),
                    Value::String("Rust".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::String("hello Rust".to_string()));

        // Test substring
        let result = registry
            .call(
                "string_utils",
                "substring",
                &[
                    Value::String("hello".to_string()),
                    Value::String("1".to_string()),
                    Value::String("4".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::String("ell".to_string()));
    }

    #[test]
    fn test_math_utils_extension() {
        let registry = ExtensionRegistry::new();
        let ext = create_math_utils_extension();
        registry.register(ext).unwrap();

        // Test add
        let result = registry
            .call(
                "math_utils",
                "add",
                &[Value::Integer(10), Value::Integer(20)],
            )
            .unwrap();
        assert_eq!(result, Value::Decimal(30.0));

        // Test multiply
        let result = registry
            .call(
                "math_utils",
                "multiply",
                &[Value::Decimal(5.5), Value::Integer(2)],
            )
            .unwrap();
        assert_eq!(result, Value::Decimal(11.0));
    }

    // Edge case tests
    #[test]
    fn test_extension_empty_args() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("no_args_ext", "1.0.0");

        ext.register_function("get_constant", |_args| {
            Ok(Value::String("constant".to_string()))
        });

        registry.register(ext).unwrap();

        let result = registry.call("no_args_ext", "get_constant", &[]).unwrap();
        assert_eq!(result, Value::String("constant".to_string()));
    }

    #[test]
    fn test_extension_null_handling() {
        let registry = ExtensionRegistry::new();
        let mut ext = Extension::new("null_handler", "1.0.0");

        ext.register_function("is_null", |args| Ok(Value::Boolean(args[0].is_null())));

        registry.register(ext).unwrap();

        let result = registry
            .call("null_handler", "is_null", &[Value::Null])
            .unwrap();
        assert_eq!(result, Value::Boolean(true));

        let result = registry
            .call(
                "null_handler",
                "is_null",
                &[Value::String("test".to_string())],
            )
            .unwrap();
        assert_eq!(result, Value::Boolean(false));
    }

    #[test]
    fn test_extension_function_names() {
        let mut ext = Extension::new("multi_func", "1.0.0");
        ext.register_function("func1", |_args| Ok(Value::Null));
        ext.register_function("func2", |_args| Ok(Value::Null));
        ext.register_function("func3", |_args| Ok(Value::Null));

        let names = ext.function_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"func1".to_string()));
        assert!(names.contains(&"func2".to_string()));
        assert!(names.contains(&"func3".to_string()));
    }

    #[test]
    fn test_extension_has_function() {
        let mut ext = Extension::new("check_func", "1.0.0");
        ext.register_function("exists", |_args| Ok(Value::Null));

        assert!(ext.has_function("exists"));
        assert!(!ext.has_function("does_not_exist"));
    }

    #[test]
    fn test_extension_registry_is_empty() {
        let registry = ExtensionRegistry::new();
        assert!(registry.is_empty().unwrap());

        let ext = Extension::new("test", "1.0.0");
        registry.register(ext).unwrap();
        assert!(!registry.is_empty().unwrap());
    }

    #[test]
    fn test_extension_debug_format() {
        let mut ext = Extension::new("debug_test", "1.0.0");
        ext.register_function("test_func", |_args| Ok(Value::Null))
            .on_init(|| Ok(()))
            .on_cleanup(|| Ok(()));

        let debug_str = format!("{ext:?}");
        assert!(debug_str.contains("debug_test"));
        assert!(debug_str.contains("1.0.0"));
        assert!(debug_str.contains("test_func"));
        assert!(debug_str.contains("has_init: true"));
        assert!(debug_str.contains("has_cleanup: true"));
    }

    #[test]
    fn test_string_utils_reverse_null() {
        let registry = ExtensionRegistry::new();
        let ext = create_string_utils_extension();
        registry.register(ext).unwrap();

        let result = registry
            .call("string_utils", "reverse", &[Value::Null])
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_string_utils_substring_invalid_range() {
        let registry = ExtensionRegistry::new();
        let ext = create_string_utils_extension();
        registry.register(ext).unwrap();

        let result = registry.call(
            "string_utils",
            "substring",
            &[
                Value::String("hi".to_string()),
                Value::String("5".to_string()),
                Value::String("10".to_string()),
            ],
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid substring range")
        );
    }

    #[test]
    fn test_math_utils_add_strings() {
        let registry = ExtensionRegistry::new();
        let ext = create_math_utils_extension();
        registry.register(ext).unwrap();

        let result = registry
            .call(
                "math_utils",
                "add",
                &[
                    Value::String("10.5".to_string()),
                    Value::String("20.5".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(result, Value::Decimal(31.0));
    }

    #[test]
    fn test_unregister_nonexistent_extension() {
        let registry = ExtensionRegistry::new();
        // Should not error
        assert!(registry.unregister("nonexistent").is_ok());
    }
}

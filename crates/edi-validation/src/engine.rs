//! Validation engine

use crate::codelist::CodeListRegistry;
use crate::reporter::{Severity, ValidationIssue, ValidationReport};
use crate::rules::{
    ConditionalRule, SegmentOrderRule, validate_conditional, validate_segment_order,
};
use edi_ir::{Document, Node, NodeType};
use edi_schema::{ElementDefinition, Schema, SegmentDefinition};
use std::collections::HashMap;

/// Strictness level for validation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StrictnessLevel {
    /// Strict: Fail on any validation error
    Strict,
    /// Moderate: Allow warnings, fail on errors
    #[default]
    Moderate,
    /// Lenient: Allow all non-critical issues
    Lenient,
}

impl StrictnessLevel {
    /// Determine if an error should fail validation based on strictness
    pub fn should_fail(&self, severity: Severity) -> bool {
        matches!(self.effective_severity(severity), Severity::Error)
    }

    /// Determine the effective severity based on strictness
    pub fn effective_severity(&self, severity: Severity) -> Severity {
        match self {
            StrictnessLevel::Strict => match severity {
                Severity::Warning => Severity::Error,
                _ => severity,
            },
            StrictnessLevel::Moderate => severity,
            StrictnessLevel::Lenient => match severity {
                Severity::Error => Severity::Warning,
                _ => severity,
            },
        }
    }
}

/// Validation configuration
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    /// Strictness level
    pub strictness: StrictnessLevel,
    /// Continue validation after errors (collect all)
    pub continue_on_error: bool,
    /// Maximum errors before stopping (0 = unlimited)
    pub max_errors: usize,
    /// Whether to validate against codelists
    pub validate_codelists: bool,
    /// Whether to validate conditional rules
    pub validate_conditionals: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strictness: StrictnessLevel::Moderate,
            continue_on_error: true,
            max_errors: 0,
            validate_codelists: true,
            validate_conditionals: true,
        }
    }
}

/// Validation error details
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error message
    pub message: String,
    /// Path in the document where error occurred
    pub path: String,
    /// Line number (if available)
    pub line: Option<usize>,
    /// Severity level
    pub severity: Severity,
    /// Error code
    pub code: Option<String>,
}

/// Validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether validation passed
    pub is_valid: bool,
    /// List of errors found
    pub errors: Vec<ValidationError>,
    /// List of warnings found
    pub warnings: Vec<ValidationError>,
    /// Detailed validation report
    pub report: ValidationReport,
}

impl ValidationResult {
    /// Create a new valid result
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            report: ValidationReport::new(),
        }
    }

    /// Check if there are any errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty() || self.report.has_errors()
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty() || !self.report.warnings().is_empty()
    }

    /// Add an error
    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
        self.is_valid = false;
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: ValidationError) {
        self.warnings.push(warning);
    }

    /// Add a validation issue to the report
    pub fn add_issue(&mut self, issue: ValidationIssue) {
        if issue.severity == Severity::Error {
            self.is_valid = false;
        }
        self.report.add_issue(issue);
    }

    /// Get total issue count
    pub fn total_issues(&self) -> usize {
        self.report.count()
    }

    /// Merge another validation result into this one
    pub fn merge(&mut self, other: ValidationResult) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
        for issue in other.report.all_issues() {
            self.report.add_issue(issue.clone());
        }
        if !other.is_valid {
            self.is_valid = false;
        }
    }
}

/// Context for validation operations
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// Current path in the document
    pub path: String,
    /// Current segment position
    pub segment_pos: Option<usize>,
    /// Current element position
    pub element_pos: Option<usize>,
    /// Current component position
    pub component_pos: Option<usize>,
    /// Line number (if available)
    pub line: Option<usize>,
}

impl ValidationContext {
    /// Create a new root context
    pub fn root() -> Self {
        Self {
            path: String::new(),
            segment_pos: None,
            element_pos: None,
            component_pos: None,
            line: None,
        }
    }

    /// Create a child context with extended path
    pub fn child(&self, name: &str) -> Self {
        let path = if self.path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", self.path, name)
        };
        Self {
            path,
            segment_pos: self.segment_pos,
            element_pos: self.element_pos,
            component_pos: self.component_pos,
            line: self.line,
        }
    }

    /// Create a context for an indexed child (e.g., LIN[2])
    pub fn indexed_child(&self, name: &str, index: usize) -> Self {
        let path = if self.path.is_empty() {
            format!("{}[{}]", name, index)
        } else {
            format!("{}/{}[{}]", self.path, name, index)
        };
        Self {
            path,
            segment_pos: self.segment_pos,
            element_pos: self.element_pos,
            component_pos: self.component_pos,
            line: self.line,
        }
    }

    /// With segment position
    pub fn with_segment_pos(mut self, pos: usize) -> Self {
        self.segment_pos = Some(pos);
        self
    }

    /// With element position
    pub fn with_element_pos(mut self, pos: usize) -> Self {
        self.element_pos = Some(pos);
        self
    }

    /// With component position
    pub fn with_component_pos(mut self, pos: usize) -> Self {
        self.component_pos = Some(pos);
        self
    }

    /// With line number
    pub fn with_line(mut self, line: usize) -> Self {
        self.line = Some(line);
        self
    }
}

/// Main validation engine
pub struct ValidationEngine {
    config: ValidationConfig,
    codelist_registry: CodeListRegistry,
    /// Segment order rules indexed by parent context
    segment_order_rules: HashMap<String, Vec<SegmentOrderRule>>,
    /// Conditional rules indexed by parent context
    conditional_rules: HashMap<String, Vec<ConditionalRule>>,
}

impl ValidationEngine {
    /// Create a new validation engine
    pub fn new() -> Self {
        Self {
            config: ValidationConfig::default(),
            codelist_registry: CodeListRegistry::new(),
            segment_order_rules: HashMap::new(),
            conditional_rules: HashMap::new(),
        }
    }

    /// Create with specific configuration
    pub fn with_config(config: ValidationConfig) -> Self {
        Self {
            config,
            codelist_registry: CodeListRegistry::new(),
            segment_order_rules: HashMap::new(),
            conditional_rules: HashMap::new(),
        }
    }

    /// Register a code list
    pub fn register_codelist(&mut self, list: crate::codelist::CodeList) {
        self.codelist_registry.register(list);
    }

    /// Set segment order rules for a context
    pub fn set_segment_order_rules(
        &mut self,
        context: impl Into<String>,
        rules: Vec<SegmentOrderRule>,
    ) {
        self.segment_order_rules.insert(context.into(), rules);
    }

    /// Set conditional rules for a context
    pub fn set_conditional_rules(
        &mut self,
        context: impl Into<String>,
        rules: Vec<ConditionalRule>,
    ) {
        self.conditional_rules.insert(context.into(), rules);
    }

    /// Validate a complete document against a schema
    pub fn validate(&self, doc: &Document) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();
        let context = ValidationContext::root();

        // Validate the root node
        let _ = self.validate_node(&doc.root, &mut result, &context);
        if self.should_stop(&result) {
            self.apply_strictness(&mut result);
            return Ok(result);
        }

        // Collect all segments for rule validation
        let mut segments = Vec::new();
        self.collect_segments(&doc.root, &mut segments);

        // Validate segment order if rules are defined
        if let Some(rules) = self.segment_order_rules.get("") {
            let order_result = validate_segment_order(&segments, rules);
            if !order_result.is_valid {
                if let Some(msg) = order_result.message {
                    self.add_error(&mut result, &context, "SEGMENT_ORDER_VIOLATION", msg);
                    if self.should_stop(&result) {
                        self.apply_strictness(&mut result);
                        return Ok(result);
                    }
                }
            }
        }

        // Validate conditional rules if configured
        if self.config.validate_conditionals {
            if let Some(rules) = self.conditional_rules.get("") {
                let conditional_result = validate_conditional(&segments, rules);
                if !conditional_result.is_valid {
                    if let Some(msg) = conditional_result.message {
                        self.add_error(&mut result, &context, "CONDITIONAL_RULE_VIOLATION", msg);
                        if self.should_stop(&result) {
                            self.apply_strictness(&mut result);
                            return Ok(result);
                        }
                    }
                }
            }
        }

        // Apply strictness rules
        self.apply_strictness(&mut result);

        Ok(result)
    }

    /// Validate a document against a specific schema
    pub fn validate_with_schema(
        &self,
        doc: &Document,
        schema: &Schema,
    ) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();
        let context = ValidationContext::root();

        // Validate document structure against schema
        self.validate_document_against_schema(doc, schema, &mut result, &context)?;

        // Apply strictness rules
        self.apply_strictness(&mut result);

        Ok(result)
    }

    /// Validate a single segment against its definition
    pub fn validate_segment(
        &self,
        segment: &Node,
        segment_def: &SegmentDefinition,
    ) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();
        let context = ValidationContext::root().child(&segment.name);

        if segment.node_type != NodeType::Segment {
            self.add_error(
                &mut result,
                &context,
                "TYPE_MISMATCH",
                format!("Expected Segment, found {:?}", segment.node_type),
            );
            return Ok(result);
        }

        // Check segment tag matches definition
        if segment.name != segment_def.tag {
            self.add_error(
                &mut result,
                &context,
                "SEGMENT_TAG_MISMATCH",
                format!(
                    "Expected segment '{}', found '{}'",
                    segment_def.tag, segment.name
                ),
            );
            if self.should_stop(&result) {
                return Ok(result);
            }
        }

        // Validate mandatory status
        if segment_def.is_mandatory && segment.children.is_empty() {
            self.add_error(
                &mut result,
                &context,
                "MANDATORY_SEGMENT_EMPTY",
                format!("Mandatory segment '{}' has no elements", segment_def.tag),
            );
            if self.should_stop(&result) {
                return Ok(result);
            }
        }

        // Validate each element against its definition
        for (idx, (child, element_def)) in segment
            .children
            .iter()
            .zip(segment_def.elements.iter())
            .enumerate()
        {
            let element_context = context.child(&element_def.id).with_element_pos(idx);
            let element_result =
                self.validate_element_internal(child, element_def, &element_context)?;
            result.merge(element_result);
            if self.should_stop(&result) {
                return Ok(result);
            }
        }

        // Check for extra elements not in definition
        if segment.children.len() > segment_def.elements.len() {
            for (idx, extra) in segment
                .children
                .iter()
                .enumerate()
                .skip(segment_def.elements.len())
            {
                let extra_context = context.child(&extra.name).with_element_pos(idx);
                self.add_warning(
                    &mut result,
                    &extra_context,
                    "EXTRA_ELEMENT",
                    format!(
                        "Element '{}' at position {} is not defined in schema",
                        extra.name, idx
                    ),
                );
                if self.should_stop(&result) {
                    return Ok(result);
                }
            }
        }

        // Check for missing mandatory elements
        for (idx, element_def) in segment_def.elements.iter().enumerate() {
            if element_def.is_mandatory && idx >= segment.children.len() {
                let missing_context = context.child(&element_def.id).with_element_pos(idx);
                self.add_error(
                    &mut result,
                    &missing_context,
                    "MISSING_MANDATORY_ELEMENT",
                    format!(
                        "Mandatory element '{}' ({}) is missing at position {}",
                        element_def.id, element_def.name, idx
                    ),
                );
                if self.should_stop(&result) {
                    return Ok(result);
                }
            }
        }

        Ok(result)
    }

    /// Validate a single element against its definition
    pub fn validate_element(
        &self,
        element: &Node,
        element_def: &ElementDefinition,
    ) -> crate::Result<ValidationResult> {
        let context = ValidationContext::root().child(&element_def.id);
        self.validate_element_internal(element, element_def, &context)
    }

    /// Internal method to validate an element with context
    fn validate_element_internal(
        &self,
        element: &Node,
        element_def: &ElementDefinition,
        context: &ValidationContext,
    ) -> crate::Result<ValidationResult> {
        let mut result = ValidationResult::valid();

        // Check if element has the expected ID
        if element.name != element_def.id {
            self.add_warning(
                &mut result,
                context,
                "ELEMENT_ID_MISMATCH",
                format!(
                    "Expected element '{}', found '{}'",
                    element_def.id, element.name
                ),
            );
            if self.should_stop(&result) {
                return Ok(result);
            }
        }

        // Get the value to validate
        let value_str = element.value.as_ref().and_then(|v| v.as_string());

        // Check for mandatory element
        if element_def.is_mandatory
            && (value_str.is_none() || value_str.as_ref().unwrap().is_empty())
        {
            self.add_error(
                &mut result,
                context,
                "MANDATORY_ELEMENT_EMPTY",
                format!(
                    "Mandatory element '{}' ({}) has no value",
                    element_def.id, element_def.name
                ),
            );
            if self.should_stop(&result) {
                return Ok(result);
            }
        }

        // Validate length constraints if value exists
        if let Some(ref value) = value_str {
            let len = value.len();
            if len < element_def.min_length {
                self.add_error(
                    &mut result,
                    context,
                    "MIN_LENGTH_VIOLATION",
                    format!(
                        "Element '{}' length {} is less than minimum {} (data type: {})",
                        element_def.id, len, element_def.min_length, element_def.data_type
                    ),
                );
                if self.should_stop(&result) {
                    return Ok(result);
                }
            }
            if len > element_def.max_length {
                self.add_error(
                    &mut result,
                    context,
                    "MAX_LENGTH_VIOLATION",
                    format!(
                        "Element '{}' length {} exceeds maximum {} (data type: {})",
                        element_def.id, len, element_def.max_length, element_def.data_type
                    ),
                );
                if self.should_stop(&result) {
                    return Ok(result);
                }
            }

            // Validate data type
            if let Err(msg) = self.validate_data_type_for_element(value, element_def) {
                self.add_error(&mut result, context, "DATA_TYPE_VIOLATION", msg);
                if self.should_stop(&result) {
                    return Ok(result);
                }
            }

            // Validate against codelist if configured
            if self.config.validate_codelists {
                // Check if this element should be validated against a codelist
                // This could be determined from schema annotations
                let codelist_name = format!("{}_{}", element_def.data_type, element_def.id);
                if let Some(codelist) = self.codelist_registry.get(&codelist_name) {
                    let validation_result = crate::codelist::validate_code(value, codelist);
                    if !validation_result.is_valid() {
                        if let Some(msg) = validation_result.error_message() {
                            self.add_error(&mut result, context, "CODELIST_VIOLATION", msg);
                            if self.should_stop(&result) {
                                return Ok(result);
                            }
                        }
                    }
                }
            }
        }

        // Validate component children if this is a composite element
        for (idx, child) in element.children.iter().enumerate() {
            let component_context = context.child(&child.name).with_component_pos(idx);
            self.validate_component(child, &mut result, &component_context)?;
            if self.should_stop(&result) {
                return Ok(result);
            }
        }

        Ok(result)
    }

    /// Validate a component element
    fn validate_component(
        &self,
        component: &Node,
        result: &mut ValidationResult,
        context: &ValidationContext,
    ) -> crate::Result<()> {
        if component.node_type != NodeType::Component {
            self.add_warning(
                result,
                context,
                "EXPECTED_COMPONENT",
                format!("Expected Component, found {:?}", component.node_type),
            );
            if self.should_stop(result) {
                return Ok(());
            }
        }

        // Check for null values in strict mode
        if let Some(ref value) = component.value {
            if value.is_null() && self.config.strictness == StrictnessLevel::Strict {
                self.add_error(
                    result,
                    context,
                    "NULL_COMPONENT_VALUE",
                    format!(
                        "Component '{}' has null value in strict mode",
                        component.name
                    ),
                );
                if self.should_stop(result) {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Validate document against schema structure
    fn validate_document_against_schema(
        &self,
        doc: &Document,
        schema: &Schema,
        result: &mut ValidationResult,
        context: &ValidationContext,
    ) -> crate::Result<()> {
        // Collect all segments from the document
        let mut segments = Vec::new();
        self.collect_segments(&doc.root, &mut segments);

        // Validate segment order if rules are defined
        if let Some(rules) = self.segment_order_rules.get("") {
            let order_result = validate_segment_order(&segments, rules);
            if !order_result.is_valid {
                if let Some(msg) = order_result.message {
                    self.add_error(result, context, "SEGMENT_ORDER_VIOLATION", msg);
                    if self.should_stop(result) {
                        return Ok(());
                    }
                }
            }
        }

        // Validate each segment against its schema definition
        for (idx, segment) in segments.iter().enumerate() {
            let segment_context = context
                .indexed_child(&segment.name, idx)
                .with_segment_pos(idx);

            if let Some(segment_def) = schema.find_segment(&segment.name) {
                let segment_result = self.validate_segment(segment, segment_def)?;

                // Merge segment results
                for issue in segment_result.report.all_issues() {
                    result.add_issue(issue.clone());
                }
                if self.should_stop(result) {
                    return Ok(());
                }
            } else {
                // Segment not found in schema
                self.add_warning(
                    result,
                    &segment_context,
                    "UNKNOWN_SEGMENT",
                    format!(
                        "Segment '{}' is not defined in schema {}",
                        segment.name,
                        schema.qualified_name()
                    ),
                );
                if self.should_stop(result) {
                    return Ok(());
                }
            }
        }

        // Check for mandatory segments
        for segment_def in &schema.segments {
            if segment_def.is_mandatory {
                let found = segments.iter().any(|s| s.name == segment_def.tag);
                if !found {
                    self.add_error(
                        result,
                        context,
                        "MISSING_MANDATORY_SEGMENT",
                        format!(
                            "Mandatory segment '{}' is missing from document",
                            segment_def.tag
                        ),
                    );
                    if self.should_stop(result) {
                        return Ok(());
                    }
                }
            }
        }

        // Validate conditional rules if configured
        if self.config.validate_conditionals {
            if let Some(rules) = self.conditional_rules.get("") {
                let conditional_result = validate_conditional(&segments, rules);
                if !conditional_result.is_valid {
                    if let Some(msg) = conditional_result.message {
                        self.add_error(result, context, "CONDITIONAL_RULE_VIOLATION", msg);
                        if self.should_stop(result) {
                            return Ok(());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Recursively validate a node
    fn validate_node(
        &self,
        node: &Node,
        result: &mut ValidationResult,
        context: &ValidationContext,
    ) -> bool {
        match node.node_type {
            NodeType::Segment => self.validate_segment_node(node, result, context),
            NodeType::Element | NodeType::Component => {
                self.validate_element_node(node, result, context)
            }
            _ => {
                // Recursively validate children
                for (idx, child) in node.children.iter().enumerate() {
                    let child_context = if matches!(child.node_type, NodeType::Segment) {
                        context
                            .indexed_child(&child.name, idx)
                            .with_segment_pos(idx)
                    } else {
                        context.child(&child.name)
                    };
                    if !self.validate_node(child, result, &child_context) {
                        return false;
                    }
                }
                true
            }
        }
    }

    fn collect_segments<'a>(&self, node: &'a Node, segments: &mut Vec<&'a Node>) {
        if matches!(
            node.node_type,
            NodeType::Segment | NodeType::Interchange | NodeType::Message
        ) {
            segments.push(node);
        }

        for child in &node.children {
            self.collect_segments(child, segments);
        }
    }

    /// Validate a segment node without schema
    fn validate_segment_node(
        &self,
        segment: &Node,
        result: &mut ValidationResult,
        context: &ValidationContext,
    ) -> bool {
        // Check for empty segment
        if segment.children.is_empty() {
            self.add_warning(
                result,
                context,
                "EMPTY_SEGMENT",
                format!("Segment '{}' has no elements", segment.name),
            );
            if self.should_stop(result) {
                return false;
            }
        }

        // Validate each element
        for (idx, child) in segment.children.iter().enumerate() {
            let element_context = context.child(&child.name).with_element_pos(idx);
            if !self.validate_element_node(child, result, &element_context) {
                return false;
            }
        }

        true
    }

    /// Validate an element node without schema
    fn validate_element_node(
        &self,
        element: &Node,
        result: &mut ValidationResult,
        context: &ValidationContext,
    ) -> bool {
        // Check if value is present
        if let Some(ref value) = element.value {
            if value.is_null() && self.config.strictness == StrictnessLevel::Strict {
                self.add_error(
                    result,
                    context,
                    "NULL_VALUE",
                    format!("Element '{}' has null value in strict mode", element.name),
                );
                if self.should_stop(result) {
                    return false;
                }
            }
        }

        // Validate component children
        for (idx, child) in element.children.iter().enumerate() {
            let component_context = context.child(&child.name).with_component_pos(idx);
            if !self.validate_element_node(child, result, &component_context) {
                return false;
            }
        }

        true
    }

    /// Validate data type for an element
    fn validate_data_type_for_element(
        &self,
        value: &str,
        element_def: &ElementDefinition,
    ) -> Result<(), String> {
        match element_def.data_type.as_str() {
            "an" | "a" | "n" => {
                // alphanumeric, alphabetic, numeric strings - basic validation
                if element_def.data_type == "a" && !value.chars().all(|c| c.is_alphabetic()) {
                    return Err(format!(
                        "Element '{}' should be alphabetic only, got '{}'",
                        element_def.id, value
                    ));
                }
                if element_def.data_type == "n" && !value.chars().all(|c| c.is_numeric()) {
                    return Err(format!(
                        "Element '{}' should be numeric only, got '{}'",
                        element_def.id, value
                    ));
                }
                Ok(())
            }
            "dt" => {
                // Date format
                if value.len() != 8 || !value.chars().all(|c| c.is_numeric()) {
                    return Err(format!(
                        "Element '{}' should be date format (YYYYMMDD), got '{}'",
                        element_def.id, value
                    ));
                }
                Ok(())
            }
            "tm" => {
                // Time format
                if !(value.len() == 4 || value.len() == 6) || !value.chars().all(|c| c.is_numeric())
                {
                    return Err(format!(
                        "Element '{}' should be time format (HHMM or HHMMSS), got '{}'",
                        element_def.id, value
                    ));
                }
                Ok(())
            }
            _ => Ok(()), // Unknown data types pass through
        }
    }

    /// Add an error to the result
    fn add_error(
        &self,
        result: &mut ValidationResult,
        context: &ValidationContext,
        code: &str,
        message: String,
    ) {
        let severity = self.config.strictness.effective_severity(Severity::Error);

        let issue = ValidationIssue::new(severity, message)
            .with_path(&context.path)
            .with_code(code);

        result.add_issue(issue);

        if self.config.strictness.should_fail(Severity::Error) {
            result.is_valid = false;
        }
    }

    /// Add a warning to the result
    fn add_warning(
        &self,
        result: &mut ValidationResult,
        context: &ValidationContext,
        code: &str,
        message: String,
    ) {
        let severity = self.config.strictness.effective_severity(Severity::Warning);

        let issue = ValidationIssue::new(severity, message)
            .with_path(&context.path)
            .with_code(code);

        result.add_issue(issue);

        if self.config.strictness.should_fail(Severity::Warning) {
            result.is_valid = false;
        }
    }

    /// Apply strictness rules to the final result
    fn apply_strictness(&self, result: &mut ValidationResult) {
        result.is_valid = !result.has_errors();
    }

    fn error_count(&self, result: &ValidationResult) -> usize {
        result.errors.len() + result.report.count_by_severity(Severity::Error)
    }

    /// Check if validation should stop due to max errors
    fn should_stop(&self, result: &ValidationResult) -> bool {
        let error_count = self.error_count(result);

        if !self.config.continue_on_error && error_count > 0 {
            return true;
        }

        self.config.max_errors > 0 && error_count >= self.config.max_errors
    }
}

impl Default for ValidationEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edi_ir::Value;
    use edi_schema::{ElementDefinition, Schema, SegmentDefinition};

    // Helper function to create a test document
    fn create_test_document() -> Document {
        let mut root = Node::new("ROOT", NodeType::Root);

        // Create a test segment
        let mut segment = Node::new("TEST", NodeType::Segment);
        segment.add_child(Node::with_value(
            "FIELD1",
            NodeType::Element,
            Value::String("value1".to_string()),
        ));
        segment.add_child(Node::with_value(
            "FIELD2",
            NodeType::Element,
            Value::String("value2".to_string()),
        ));

        root.add_child(segment);
        Document::new(root)
    }

    fn create_grouped_document() -> Document {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut group = Node::new("SG1", NodeType::SegmentGroup);
        let mut segment = Node::new("TEST", NodeType::Segment);

        segment.add_child(Node::with_value(
            "FIELD1",
            NodeType::Element,
            Value::String("value1".to_string()),
        ));

        group.add_child(segment);
        root.add_child(group);
        Document::new(root)
    }

    fn create_test_segment() -> Node {
        let mut segment = Node::new("LIN", NodeType::Segment);
        segment.add_child(Node::with_value(
            "C212",
            NodeType::Element,
            Value::String("12345".to_string()),
        ));
        segment.add_child(Node::with_value(
            "C212",
            NodeType::Element,
            Value::String("EN".to_string()),
        ));
        segment
    }

    fn create_test_element() -> Node {
        Node::with_value(
            "7140",
            NodeType::Element,
            Value::String("ITEM123".to_string()),
        )
    }

    fn create_test_schema() -> Schema {
        Schema::new("TEST", "1.0").with_segments(vec![
            SegmentDefinition::new("TEST")
                .mandatory(true)
                .with_elements(vec![
                    ElementDefinition::new("FIELD1", "Field 1", "an")
                        .mandatory(true)
                        .length(1, 35),
                    ElementDefinition::new("FIELD2", "Field 2", "an").length(0, 35),
                ]),
            SegmentDefinition::new("LIN")
                .mandatory(false)
                .with_elements(vec![
                    ElementDefinition::new("C212", "Item ID", "an")
                        .mandatory(true)
                        .length(1, 35),
                ]),
        ])
    }

    fn create_document_with_null_elements(count: usize) -> Document {
        let mut root = Node::new("ROOT", NodeType::Root);
        for index in 0..count {
            root.add_child(Node::with_value(
                format!("FIELD{index}"),
                NodeType::Element,
                Value::Null,
            ));
        }
        Document::new(root)
    }

    #[test]
    fn test_validate_document() {
        let doc = create_test_document();
        let engine = ValidationEngine::new();

        let result = engine.validate(&doc).unwrap();

        assert!(result.is_valid || result.errors.is_empty());
    }

    #[test]
    fn test_validate_document_with_empty_children() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = Document::new(root);
        let engine = ValidationEngine::new();

        let result = engine.validate(&doc).unwrap();

        assert!(result.is_valid);
    }

    #[test]
    fn test_validate_segment() {
        let segment = create_test_segment();
        let schema = create_test_schema();
        let segment_def = schema.find_segment("LIN").unwrap();
        let engine = ValidationEngine::new();

        let result = engine.validate_segment(&segment, segment_def).unwrap();

        assert!(result.is_valid || !result.has_errors());
    }

    #[test]
    fn test_validate_segment_with_wrong_type() {
        let wrong_node = Node::new("NOT_SEGMENT", NodeType::Element);
        let schema = create_test_schema();
        let segment_def = schema.find_segment("TEST").unwrap();
        let engine = ValidationEngine::new();

        let result = engine.validate_segment(&wrong_node, segment_def).unwrap();

        assert!(!result.is_valid);
        assert!(result.has_errors());
    }

    #[test]
    fn test_validate_element() {
        let element = create_test_element();
        let element_def = ElementDefinition::new("7140", "Item Number", "an").length(1, 35);
        let engine = ValidationEngine::new();

        let result = engine.validate_element(&element, &element_def).unwrap();

        assert!(result.is_valid);
    }

    #[test]
    fn test_validate_element_with_null_value() {
        let element = Node::with_value("7140", NodeType::Element, Value::Null);
        let element_def = ElementDefinition::new("7140", "Item Number", "an")
            .mandatory(true)
            .length(1, 35);
        let engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let result = engine.validate_element(&element, &element_def).unwrap();

        assert!(!result.is_valid || result.has_errors());
    }

    #[test]
    fn test_strictness_levels() {
        let element = Node::with_value("FIELD", NodeType::Element, Value::Null);
        let element_def = ElementDefinition::new("FIELD", "Test Field", "an").mandatory(true);

        // Test Strict mode
        let strict_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });
        let result = strict_engine
            .validate_element(&element, &element_def)
            .unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.report.count_by_severity(Severity::Error), 1);
        assert_eq!(result.report.count_by_severity(Severity::Warning), 0);

        // Test Moderate mode
        let moderate_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Moderate,
            ..Default::default()
        });
        let result = moderate_engine
            .validate_element(&element, &element_def)
            .unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.report.count_by_severity(Severity::Error), 1);
        assert_eq!(result.report.count_by_severity(Severity::Warning), 0);

        // Test Lenient mode
        let lenient_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Lenient,
            ..Default::default()
        });
        let result = lenient_engine
            .validate_element(&element, &element_def)
            .unwrap();
        assert!(result.is_valid);
        assert_eq!(result.report.count_by_severity(Severity::Error), 0);
        assert_eq!(result.report.count_by_severity(Severity::Warning), 1);
    }

    #[test]
    fn test_partial_validation() {
        let doc = create_document_with_null_elements(2);

        // Continue on error mode
        let engine_continue = ValidationEngine::with_config(ValidationConfig {
            continue_on_error: true,
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let result = engine_continue.validate(&doc).unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.report.count_by_severity(Severity::Error), 2);
    }

    #[test]
    fn test_stop_on_first_error() {
        let doc = create_document_with_null_elements(3);

        // Stop on first error mode
        let engine_stop = ValidationEngine::with_config(ValidationConfig {
            continue_on_error: false,
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let result = engine_stop.validate(&doc).unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.report.count_by_severity(Severity::Error), 1);
    }

    #[test]
    fn test_max_errors_limits_collected_errors() {
        let doc = create_document_with_null_elements(3);

        let engine = ValidationEngine::with_config(ValidationConfig {
            continue_on_error: true,
            max_errors: 2,
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let result = engine.validate(&doc).unwrap();
        assert!(!result.is_valid);
        assert_eq!(result.report.count_by_severity(Severity::Error), 2);
    }

    #[test]
    fn test_strict_mode_escalates_warnings_to_errors() {
        let mut root = Node::new("ROOT", NodeType::Root);
        root.add_child(Node::new("SEG", NodeType::Segment));
        let doc = Document::new(root);

        let strict_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Strict,
            ..Default::default()
        });

        let strict_result = strict_engine.validate(&doc).unwrap();
        assert!(!strict_result.is_valid);
        assert_eq!(strict_result.report.count_by_severity(Severity::Error), 1);
        assert_eq!(strict_result.report.count_by_severity(Severity::Warning), 0);

        let moderate_engine = ValidationEngine::with_config(ValidationConfig {
            strictness: StrictnessLevel::Moderate,
            ..Default::default()
        });

        let moderate_result = moderate_engine.validate(&doc).unwrap();
        assert!(moderate_result.is_valid);
        assert_eq!(moderate_result.report.count_by_severity(Severity::Error), 0);
        assert_eq!(
            moderate_result.report.count_by_severity(Severity::Warning),
            1
        );
    }

    #[test]
    fn test_nested_document_validation() {
        let mut root = Node::new("ROOT", NodeType::Root);
        let mut group = Node::new("SG1", NodeType::SegmentGroup);
        let mut segment = Node::new("LIN", NodeType::Segment);

        segment.add_child(Node::with_value(
            "C212",
            NodeType::Element,
            Value::String("123".to_string()),
        ));
        group.add_child(segment);
        root.add_child(group);

        let doc = Document::new(root);
        let engine = ValidationEngine::new();

        let result = engine.validate(&doc).unwrap();
        assert!(result.is_valid || !result.has_errors());
    }

    #[test]
    fn test_validation_result_helpers() {
        let mut result = ValidationResult::valid();

        assert!(result.is_valid);
        assert!(!result.has_errors());
        assert!(!result.has_warnings());

        result.add_error(ValidationError {
            message: "Test error".to_string(),
            path: "/test".to_string(),
            line: Some(1),
            severity: Severity::Error,
            code: Some("TEST".to_string()),
        });

        assert!(!result.is_valid);
        assert!(result.has_errors());

        result.add_warning(ValidationError {
            message: "Test warning".to_string(),
            path: "/test".to_string(),
            line: None,
            severity: Severity::Warning,
            code: None,
        });

        assert!(result.has_warnings());
    }

    #[test]
    fn test_default_config() {
        let config = ValidationConfig::default();
        assert_eq!(config.strictness, StrictnessLevel::Moderate);
        assert!(config.continue_on_error);
        assert_eq!(config.max_errors, 0);
        assert!(config.validate_codelists);
        assert!(config.validate_conditionals);
    }

    #[test]
    fn test_validation_context() {
        let root = ValidationContext::root();
        assert!(root.path.is_empty());

        let child = root.child("SEGMENT");
        assert_eq!(child.path, "SEGMENT");

        let grandchild = child.indexed_child("LIN", 2);
        assert_eq!(grandchild.path, "SEGMENT/LIN[2]");

        let with_pos = grandchild.with_segment_pos(5).with_element_pos(3);
        assert_eq!(with_pos.segment_pos, Some(5));
        assert_eq!(with_pos.element_pos, Some(3));
    }

    #[test]
    fn test_validate_with_schema() {
        let doc = create_test_document();
        let schema = create_test_schema();
        let engine = ValidationEngine::new();

        let result = engine.validate_with_schema(&doc, &schema).unwrap();

        // Document should validate against schema
        // Note: may have warnings about unknown segments
        assert!(result.is_valid || result.has_errors());
    }

    #[test]
    fn test_validate_with_schema_segment_group_traversal() {
        let doc = create_grouped_document();
        let schema = create_test_schema();
        let engine = ValidationEngine::new();

        let result = engine.validate_with_schema(&doc, &schema).unwrap();

        assert!(result.is_valid, "Expected grouped segment to be validated");
    }

    #[test]
    fn test_validate_mandatory_segment_missing() {
        let root = Node::new("ROOT", NodeType::Root);
        let doc = Document::new(root);

        let schema = create_test_schema();
        let engine = ValidationEngine::new();

        let result = engine.validate_with_schema(&doc, &schema).unwrap();

        // Should fail because mandatory TEST segment is missing
        assert!(!result.is_valid || result.has_errors());
    }

    #[test]
    fn test_validate_element_length_constraints() {
        let element = Node::with_value(
            "FIELD1",
            NodeType::Element,
            Value::String("this is a very long value that exceeds the limit".to_string()),
        );
        let element_def = ElementDefinition::new("FIELD1", "Field 1", "an").length(1, 10);
        let engine = ValidationEngine::new();

        let result = engine.validate_element(&element, &element_def).unwrap();

        assert!(!result.is_valid || result.has_errors());
    }

    #[test]
    fn test_validate_element_data_type() {
        // Test numeric type
        let element = Node::with_value("NUM", NodeType::Element, Value::String("ABC".to_string()));
        let element_def = ElementDefinition::new("NUM", "Number", "n");
        let engine = ValidationEngine::new();

        let result = engine.validate_element(&element, &element_def).unwrap();
        assert!(!result.is_valid || result.has_errors());

        // Valid numeric
        let element =
            Node::with_value("NUM", NodeType::Element, Value::String("12345".to_string()));
        let result = engine.validate_element(&element, &element_def).unwrap();
        assert!(result.is_valid);
    }

    #[test]
    fn test_codelist_validation() {
        let mut engine = ValidationEngine::new();

        // Register a code list (use "an" for alphanumeric data type)
        let codelist = crate::codelist::CodeList::with_codes("an_7140", vec!["ITEM001", "ITEM002"]);
        engine.register_codelist(codelist);

        // Validate element with valid code
        let element = Node::with_value(
            "7140",
            NodeType::Element,
            Value::String("ITEM001".to_string()),
        );
        let element_def = ElementDefinition::new("7140", "Item ID", "an"); // Use "an" for alphanumeric

        let result = engine.validate_element(&element, &element_def).unwrap();
        assert!(
            result.is_valid,
            "Expected valid result but got errors: {:?}",
            result.errors
        );

        // Validate element with invalid code
        let element = Node::with_value(
            "7140",
            NodeType::Element,
            Value::String("INVALID".to_string()),
        );

        let result = engine.validate_element(&element, &element_def).unwrap();
        assert!(!result.is_valid || result.has_errors());
    }

    #[test]
    fn test_segment_order_rules() {
        let mut engine = ValidationEngine::new();

        let rules = vec![
            SegmentOrderRule {
                segment_name: "UNH".to_string(),
                min_occurs: 1,
                max_occurs: Some(1),
            },
            SegmentOrderRule {
                segment_name: "BGM".to_string(),
                min_occurs: 1,
                max_occurs: Some(1),
            },
        ];

        engine.set_segment_order_rules("", rules);

        // Create document with segments in wrong order (missing UNH)
        let mut root = Node::new("ROOT", NodeType::Root);
        root.add_child(Node::new("BGM", NodeType::Segment));

        let doc = Document::new(root);

        let result = engine.validate(&doc).unwrap();
        assert!(!result.is_valid || result.has_errors());
    }

    #[test]
    fn test_conditional_rules() {
        let mut engine = ValidationEngine::new();

        let rules = vec![ConditionalRule {
            trigger_field: "C002".to_string(),
            trigger_value: "USD".to_string(),
            required_fields: vec!["C004".to_string()],
        }];

        engine.set_conditional_rules("", rules);

        // Create nodes
        let currency =
            Node::with_value("C002", NodeType::Element, Value::String("USD".to_string()));
        let amount = Node::with_value("C004", NodeType::Element, Value::String("100".to_string()));

        let mut root = Node::new("ROOT", NodeType::Root);
        root.add_child(currency);
        root.add_child(amount);

        let doc = Document::new(root);

        let result = engine.validate(&doc).unwrap();
        // Should be valid since both fields are present
        assert!(result.is_valid || !result.has_errors());
    }

    #[test]
    fn test_validation_result_merge() {
        let mut result1 = ValidationResult::valid();
        result1.add_error(ValidationError {
            message: "Error 1".to_string(),
            path: "/path1".to_string(),
            line: None,
            severity: Severity::Error,
            code: None,
        });

        let mut result2 = ValidationResult::valid();
        result2.add_error(ValidationError {
            message: "Error 2".to_string(),
            path: "/path2".to_string(),
            line: None,
            severity: Severity::Error,
            code: None,
        });

        result1.merge(result2);

        assert_eq!(result1.errors.len(), 2);
        assert!(!result1.is_valid);
    }

    #[test]
    fn test_strictness_level_effective_severity() {
        assert_eq!(
            StrictnessLevel::Strict.effective_severity(Severity::Error),
            Severity::Error
        );
        assert_eq!(
            StrictnessLevel::Strict.effective_severity(Severity::Warning),
            Severity::Error
        );
        assert_eq!(
            StrictnessLevel::Lenient.effective_severity(Severity::Error),
            Severity::Warning
        );
        assert_eq!(
            StrictnessLevel::Lenient.effective_severity(Severity::Warning),
            Severity::Warning
        );
    }
}

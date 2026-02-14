# X12 Adapter Strategy

## Purpose

Define a production-ready plan for adding ANSI X12 support while preserving the existing EDIFACT-first architecture.

## 1. X12 Syntax Differences

### Delimiters and Segment Terminators

- X12 uses ISA-defined separators.
- Element separator comes from the ISA04 position.
- Repetition separator (when present) is ISA11.
- Component separator is ISA16.
- Segment terminator is the single character immediately following ISA16 in the ISA interchange header.

### Envelope Model

- Interchange: `ISA`/`IEA`
- Functional group: `GS`/`GE`
- Transaction set: `ST`/`SE`

This differs from EDIFACT `UNB/UNZ` and `UNH/UNT`, but maps cleanly to the same hierarchy in IR.

### Character Set and Padding

- ISA is fixed-width and space-padded in multiple elements.
- EDIFACT parser assumptions about free-form segment headers must not be reused directly.

### Acknowledgment and Control Numbers

- Control numbers are distributed across ISA13, GS06, ST02, and matching trailers.
- Validation must include cross-envelope consistency checks.

## 2. Architecture Decision

### Decision

Create a dedicated crate: `crates/edi-adapter-x12`.

### Rationale

- EDIFACT and X12 syntax/tokenization differ enough to justify separate parsers.
- A separate crate avoids growing EDIFACT-specific branches in one parser.
- Shared abstractions remain in IR/schema/validation/pipeline layers.

### Rejected Alternative

Generalize `edi-adapter-edifact` into one multi-standard parser now.

- Rejected because it increases short-term complexity and regression risk.
- Can be revisited after X12 adapter reaches parity and shared parser traits stabilize.

## 3. Message Mapping Analysis

### Scope Pairing

- X12 `850` Purchase Order -> EANCOM `ORDERS`
- X12 `856` ASN -> EANCOM `DESADV`
- X12 `810` Invoice -> EANCOM `INVOIC`

### Mapping Observations

- Core business entities align: parties, dates, references, lines, quantities, prices.
- Segment-level semantics differ and qualifiers require lookup tables.
- Some X12 code values do not have 1:1 EANCOM equivalents and require normalization rules.

### IR Strategy

- Keep message-type-neutral IR node model.
- Adapter-specific mapping layers populate standardized IR paths for downstream mapping DSL.

## 4. Proposed Crate and Interfaces

### New Crate

`crates/edi-adapter-x12`

### Initial Public Surface

- `X12Parser`
- `X12Serializer` (phase 2+)
- Envelope/control validation helpers

### Error Model

Use structured typed errors with full operational context:

- envelope context: `isa_control_number`, `gs_control_number`, `st_control_number`
- positional context: `messageIndex`, `segmentIndex`, `elementIndex`, `componentIndex`
- IR context: `irPath`
- source context: `sourcePosition` (`line`, `column`)
- actionable diagnostics: `expectedValue`, `actualValue`, `allowedCodes`

Errors should be emitted in both machine-readable form (for automation) and human-readable form (for operators).

## 5. Phased Implementation Plan

### Phase 0: Foundations

- Create crate skeleton with parser entrypoints and typed error model.
- Add baseline fixtures for 850/856/810 valid+invalid inputs.

### Phase 1: Parsing and IR Conversion

- Parse ISA/GS/ST envelopes and transaction sets.
- Convert 850/856/810 core segments into IR.
- Implement streaming as a first-class path:
  - message-level streaming by default
  - segment-level streaming for very large messages
- Add deterministic unit/integration tests and fixtures for both normal and streaming execution paths.

### Phase 2: Validation Integration

- Add runtime schemas for X12 transaction sets.
- Validate structural constraints and control-number consistency.
- Add configurable strictness levels:
  - `strict`
  - `accept-with-warnings` (default)
  - `lenient`
- Support per-partner strictness override and ensure warnings/errors are logged with stable identifiers.

### Phase 3: Pipeline and CLI Integration

- Add `--format x12` pipeline/CLI paths.
- Support parsing and validation commands for X12 inputs.

### Phase 4: Serialization and Round-Trip

- Add IR -> X12 serialization for selected messages.
- Add round-trip and compatibility tests.

## 6. Open Questions

- Minimum X12 versions for first release (4010 only vs 5010 support).
- How strict to be on ISA padding and non-compliant partner variations.
- Whether to ship 997/999 acknowledgments in initial release.
- Shared schema abstraction for EDIFACT and X12: unified model now or adapter-specific schema loaders first.
- Streaming buffer limits: maximum interchange/message size for in-memory vs streaming mode, plus backpressure/chunking requirements.
- Performance targets: baseline throughput goals (messages/sec or bytes/sec) and acceptable latency by transaction-set size for Phase 1.

## 7. Delivery Order Recommendation

1. Parse-only read path for 850.
2. Extend to 856 and 810.
3. Add schema-driven validation and strictness profiles.
4. Add serialization and acknowledgment support.

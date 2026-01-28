# Product Specification: Rust EDI Integration Engine (EANCOM-first)

## 1. Summary

Build a Rust-based application (including a reusable library) that reads and writes
UN/EDIFACT EDI files—especially EANCOM—and integrates them with custom
implementations, databases, and CSV files. The product provides a runtime-configurable
mapping layer (DSL) to transform data between EDI and custom formats via a
schema-aware Intermediate Representation (IR).

This specification captures the content discussed so far and identifies open
decisions needed to finalize an MVP.

---

## 2. Goals

### 2.1 Primary goals
- **Read and write EDIFACT/EANCOM** (EANCOM is the most important standard).
- Act as an **integration layer** between:
  - EDI ↔ custom implementation formats
  - EDI ↔ databases
  - EDI ↔ CSV files
- Provide a **mapping engine** to transform between models:
  - **EDI → IR**
  - **IR → EDI**
  - With different profiles/schemas per direction if needed.
- Support **typed information and validation** without redeploying the application:
  - Schemas and mappings should be **loaded at runtime**.
- Support **profiles hierarchy** (general → version → partner custom), per EDI business
  practice.

### 2.2 Secondary goals
- Streaming processing (do not load entire files into memory).
- High-quality error reporting (precise locations, actionable messages).
- Configurable behavior for partial acceptance of files containing multiple messages.

---

## 3. Non-goals (initial)
- X12 support (planned later; architecture should keep the door open).
- Transport protocols like AS2/PGP/signing/encryption (out of scope for now).
- Lossless round-tripping of original EDI formatting (not required for MVP).

---

## 4. Target standards and scope (current decisions)

### 4.1 Standards
- **Initial**: UN/EDIFACT only
- **Primary subset**: EANCOM

### 4.2 Message types (MVP candidate set)
- ORDERS
- DESADV
- INVOIC
- SLSRPT
- ORDRSP

### 4.3 Versions / profiles
- EANCOM **D96A**, **D01B**
- Always include **partner custom profiles**

---

## 5. Conceptual model (contracts, not strict layers)

We treat the system as a set of well-defined contracts between components rather than
a strictly layered implementation.

- **Interchange Syntax**: format-specific parsing/serialization rules (e.g., EDIFACT
  separators, envelopes).
- **Message Model**: schema-driven semantic structure (e.g., ORDERS with segment groups,
  mandatory constraints).
- **Intermediate Representation (IR)**: canonical, schema-aware, runtime-typed structure
  used for mapping and validation.
- **Mapping Engine**: DSL that transforms Source(IR+Schema A) → Target(IR+Schema B).
- **Adapters**: implementations for EDIFACT, CSV, DB, etc., each with their own syntax
  and message model concepts.

Business logic predominantly lives in mapping rules operating over the IR, but the
system must support returning to message models and interchange syntax for output.

---

## 6. Architecture

### 6.1 Diagram

┌─────────────────────────────────────────────────────────────────────────────┐

│                              SCHEMA REGISTRY                                │

│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │

│  │ EDIFACT     │  │ EANCOM      │  │ Partner     │  │ Custom Schemas      │ │

│  │ Base        │  │ D96A/D01B   │  │ Profiles    │  │ (CSV, DB, JSON)     │ │

│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────────┘ │

│                         ▲                                   ▲               │

│                         │ inherits / extends                │               │

└─────────────────────────┼───────────────────────────────────┼───────────────┘

│                                   │

▼                                   ▼

┌───────────────────────┐           ┌───────────────────────┐

│  VALIDATION ENGINE    │           │  MAPPING ENGINE (DSL) │

│  - structural rules   │           │  - query + transform  │

│  - codelists          │           │  - bidirectional use  │

│  - warnings/errors    │           │  - extensible funcs   │

└───────────┬───────────┘           └───────────┬───────────┘

│                                   │

▼                                   ▼

┌──────────────────────────────────────────────────────────────────────────────┐

│                        INTERMEDIATE REPRESENTATION (IR)                      │

│  - generic document tree                                                     │

│  - schema-aware typing                                                      │

│  - streamable (subtrees/messages)                                            │

│  - metadata for source position + validation state                           │

└──────────────────────────────────────────────────────────────────────────────┘

▲                                              ▲

│                                              │

┌─────┴────────┐                              ┌──────┴───────┐

│ ADAPTER:      │                              │ ADAPTER:      │

│ EDIFACT/EANCOM│                              │ CSV / DB       │

│ - parser/ser  │                              │ - parser/writer│

│ - envelopes   │                              │ - batching     │

└─────┬─────────┘                              └──────┬────────┘

│                                              │

▼                                              ▼

┌───────────────┐                            ┌─────────────────┐

│ Transport: FS  │                            │ Transport: FS/DB │

└───────────────┘                            └─────────────────┘

### 6.2 Key component responsibilities
- **EDIFACT Adapter**
  - Streaming parser and serializer.
  - Envelope handling: UNB/UNZ, UNH/UNT, control numbers (generation/validation).
  - Produces/consumes IR nodes based on schemas.

- **CSV Adapter**
  - Reads/writes CSV based on runtime schema (columns, types, rules).
  - Supports streaming rows.

- **DB Adapter**
  - Reads/writes via runtime schema describing tables/relations or query mapping.
  - Supports batching and transaction configuration.

- **IR**
  - Canonical structure for mapping and validation across all formats.

- **Schema Registry**
  - Loads schemas at runtime.
  - Supports inheritance: EDIFACT base → EANCOM version → message → partner profile.

- **Validation Engine**
  - Runs on input and output sides.
  - Distinguishes warnings vs errors; strictness configurable.

- **Mapping Engine (DSL)**
  - Query/select nodes, apply transforms, write into target structure.
  - Supports custom transformers/extensions.

---

## 7. Intermediate Representation (IR)

### 7.1 Desired characteristics
- Generic enough to represent EDIFACT, CSV, and DB-derived models.
- Schema-aware typing for validation and better end-user mapping experience.
- Streamable (operate message-by-message or subtree-by-subtree).
- Includes metadata needed for error reporting.

### 7.2 Hybrid approach (current direction)
- Avoid purely generic “stringly typed” tree due to poor mapping validation.
- Avoid purely compile-time Rust typed models due to need for runtime schema loading.
- **Hybrid**: generic tree + runtime schemas that enable typed access and validation.

### 7.3 Serialization formats
- IR may be exposed as JSON (likely), but the internal IR is a Rust data model.
- Schema format may be JSON Schema-like, but must cover EDI-specific constraints.

---

## 8. Schema management

### 8.1 Requirements
- Schemas are loaded at runtime (no redeploy).
- Support hierarchical profiles:
  - EDIFACT base definitions (segments/elements types)
  - EANCOM version specifics (D96A, D01B)
  - Message definitions (ORDERS/DESADV/INVOIC/...)
  - Partner customizations (constraints/optionalities/extensions)

### 8.2 What schemas must express
- Structural rules: required/optional, min/max repetitions, ordering/segment groups.
- Field types: string/decimal/date/etc.
- Constraints: length, pattern, numeric precision.
- Code lists (UNCL/EANCOM + partner-specific lists).
- Conditional requirements (“if X then Y”).

---

## 9. Mapping (DSL)

### 9.1 Requirements
- End users author mappings (not only developers).
- Bidirectional mappings supported:
  - EDI → IR/custom
  - IR/custom → EDI
- DSL supports:
  - Selecting nodes by query (XPath/jq-like semantics).
  - Built-in transformations (type coercion, date parsing, string ops, lookups).
  - Custom transformer extensions.

### 9.2 Expected mapping workflow
1. Load source document into IR (streaming).
2. Validate against source schema (warnings/errors).
3. Execute mapping DSL to produce target IR.
4. Validate target IR against target schema.
5. Serialize target IR through the appropriate adapter.

---

## 10. Validation and error reporting

### 10.1 Validation stages
- Validate **after parse** (source).
- Validate **after mapping** (target).
- Optional validations at intermediate steps (configurable).

### 10.2 Error reporting requirements
- Include precise context:
  - Message index / reference number
  - Segment position and element/component index
  - Path in IR
- Provide actionable messages (expected vs actual, allowed codes, etc.)

### 10.3 Strictness
- Parser/validator should **accept with warnings** by default (real-world EDI).
- Configurable strictness per partner/profile or per pipeline.

---

## 11. Partial acceptance and quarantine behavior

Files may contain multiple messages. Behavior must be configurable:
- **Always accept** and report damaged messages (continue processing others).
- **Never accept** if damaged (fail the entire file/run).
- **Quarantine** damaged messages for later review/processing; continue with valid ones.

This applies both to syntactical and schema/semantic errors (policy-configurable).

---

## 12. Streaming and performance

### 12.1 Streaming requirement
- Streaming is mandatory.
- Only subsections of the document tree should be loaded into memory (e.g., per-message
  processing for batch files).

### 12.2 Practical minimum
- Message-level streaming is likely the first milestone:
  - Parse interchange envelope
  - Iterate messages
  - Emit mapped output incrementally

---

## 13. Integrations (initial)

### 13.1 Required connectors for initial release
- Filesystem input/output
- CSV input/output
- Database read/write

### 13.2 Batch support
- Must support multiple messages per file and batch runs.

---

## 14. Output requirements (EDIFACT)
- Must generate envelopes and manage control numbers (UNB/UNZ and UNH/UNT).
- Control number strategy (sequence, persistence, partner rules) is a design decision
  (see Open Questions).

---

## 15. Implementation approach (Rust)

### 15.1 Packaging
- Provide a **reusable library** (core engine) plus an **application** (CLI/service) that
  orchestrates pipelines.

### 15.2 Proposed crate/module boundaries (draft)
- `ir`: IR structures, traversal/cursor APIs, metadata.
- `schema`: schema model, loader, inheritance/merge.
- `validation`: validation engine.
- `mapping`: DSL parser/runtime + transform library + extension API.
- `adapter-edifact`: EDIFACT/EANCOM parsing/serialization + envelope utilities.
- `adapter-csv`: CSV integration.
- `adapter-db`: DB integration.
- `pipeline`: orchestration for streaming, batching, partial acceptance policies.
- `cli`: configuration + execution.

(Exact crate splitting can be deferred until boundaries harden.)

---

## 16. MVP proposal (vertical slice)

### 16.1 MVP outcome
Demonstrate end-to-end processing with runtime schemas and runtime mappings:

- Parse EDIFACT/EANCOM input file (streaming) → IR
- Validate (structural + basic codelists)
- Map using end-user DSL → target IR
- Validate output
- Serialize output (to EDI and/or CSV/JSON)

### 16.2 Suggested MVP scope
- Support at least one message type end-to-end first (likely ORDERS).
- Runtime schema loading for:
  - EANCOM D96A ORDERS baseline
  - One partner customization
  - One custom target schema (CSV or internal JSON)
- Mapping DSL supports:
  - field copy, constant, conditional, simple transforms
  - iteration over repeating groups (line items)

---

## 17. Roadmap (high-level)

1. EDIFACT syntax parser/serializer + envelope support (streaming).
2. IR core + schema loader + inheritance.
3. Validation engine (structural + codelists).
4. Mapping DSL MVP + built-in transforms.
5. CSV adapter + FS pipeline.
6. DB adapter.
7. Expand message coverage (DESADV, INVOIC, SLSRPT, ORDRSP).
8. Partner tooling: mapping tester, schema linter, sample generator.
9. X12 adapter (later).

---

## 18. Open questions / decisions to make next

### 18.1 Schema definition format
- Use JSON Schema directly?
- Use a custom YAML/TOML schema DSL that is EDI-aware?
- How to represent segment groups, conditional rules, and partner overrides cleanly?

### 18.2 Mapping DSL shape
- YAML-based declarative mappings vs a custom text DSL.
- Required control structures:
  - foreach/repeat
  - conditions
  - lookup tables
  - error handling per rule (fail/warn/default)

### 18.3 Query/path language for IR selection
- XPath-like subset, jq-like, or custom?
- Must support EDIFACT-specific addressing (segment qualifiers, composite elements).

### 18.4 Extension mechanism for custom transforms
- Rust dynamic plugins (shared libs)?
- WASM modules (portable + sandboxed)?
- Embedded scripting language (e.g., Rhai/Lua)?
- Operational constraints (deployment, security) will influence this.

### 18.5 Streaming granularity targets
- Is message-level streaming sufficient for expected max sizes?
- Any requirement for segment-level streaming within a single huge message?

### 18.6 Control number management
- Persisted sequences (DB-backed) vs caller-provided vs file-based state.
- Partner-specific rules for control references.

---

## 19. Risks and mitigations (early)

- **Schema complexity** (EDI-specific constraints): mitigate by defining a purpose-built
  schema model rather than forcing generic JSON Schema if it becomes awkward.
- **DSL scope creep**: mitigate by shipping a minimal, composable DSL with a stable
  extension mechanism.
- **Partner variability**: mitigate via profile inheritance + override semantics, plus
  robust warning system.
- **Operational correctness** (control numbers, batching, quarantine): mitigate via
  explicit policy configuration and audit logs.

---

## 20. Appendix: Current requirement checklist

- [x] EDIFACT/EANCOM read + write
- [x] Runtime schemas (no redeploy)
- [x] Runtime mappings (end users)
- [x] Bidirectional mapping support
- [x] Streaming processing
- [x] Validation (structural + codelists)
- [x] Configurable partial acceptance/quarantine
- [x] Integrations: filesystem, CSV, DB
- [x] Envelope/control number handling
- [ ] Schema format finalized
- [ ] Mapping DSL format finalized
- [ ] Extension mechanism finalized


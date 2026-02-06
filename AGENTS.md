# Production-Grade Rust Best Practices

This document describes standards and expectations for writing and changing
production Rust code in this repository.

## Priorities (in order)
1. Correctness and safety
2. Clarity and maintainability
3. Observability (logging/metrics/tracing)
4. Performance (only after measuring)
5. Minimal, well-justified dependencies

---

## Baseline Tooling & Policy

### Rust version / edition
- Use the newest stable edition allowed by the repo (prefer 2024 if possible).
- Define and respect an MSRV (Minimum Supported Rust Version) via `rust-version`
  in `Cargo.toml` and ensure CI tests it.
- Avoid relying on nightly features unless explicitly approved and gated.

Example:
```toml
[package]
edition = "2024"
rust-version = "1.78"
```

### Formatting, linting, and warnings
- Code must be formatted with `rustfmt`.
- Code must be clean under `clippy` (no ignored warnings without justification).
- Treat warnings as errors in CI.

Recommended commands:
```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Optional (but recommended) lint policy for libraries/binaries:
```rust
#![deny(warnings)]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
// Allow pedantic lints explicitly where needed, with a short comment.
```

---

## Crate/Workspace Structure
- Prefer a Cargo workspace for multi-crate repos; keep crate boundaries meaningful.
- Separate:
  - `lib` crate for core domain logic
  - `bin` crate(s) for entrypoints/CLI/service
  - integration tests in `tests/`
- Keep modules small and cohesive; avoid “god modules”.
- Prefer explicit `pub(crate)` over `pub` until an API is intentionally public.

---

## Dependency Management
- Keep dependencies minimal and well-reviewed.
- Pin features deliberately; avoid `default-features = true` unless you intend them.
- Prefer mature crates with good maintenance signals and security posture.
- Run security and license checks in CI:
  - `cargo audit` (RustSec)
  - `cargo deny` (licenses/advisories/source)
- Remove unused dependencies (`cargo udeps` where applicable).

Example:
```toml
anyhow = { version = "1", default-features = false }
tracing = "0.1"
thiserror = "2"
serde = { version = "1", features = ["derive"] }
```

---

## Error Handling
- Use `Result<T, E>` pervasively; avoid `unwrap()`/`expect()` in production paths.
- Choose error strategy intentionally:
  - Libraries: define typed errors (`thiserror`) and avoid losing context.
  - Binaries/services: use `anyhow` at boundaries for ergonomic context.
- Add context at boundaries (I/O, parsing, network):
  - `anyhow::Context` or custom error variants with fields.
- Prefer error enums that model actionable causes (not stringly-typed errors).

Guideline:
- “Leaf” code returns specific errors.
- “Boundary” code logs and maps errors into user-facing responses/status codes.

---

## Observability (Logging/Tracing/Metrics)
- Use structured logging (prefer `tracing`).
- Include stable identifiers: request IDs, user IDs (where safe), correlation IDs.
- Use spans for request/task lifetimes; add fields early.
- Don’t log secrets/credentials/PII. Redact or avoid entirely.
- Emit metrics for:
  - request rate, error rate, latency
  - queue depths / backpressure
  - resource usage if relevant

Example:
```rust
use tracing::{info, instrument};

#[instrument(skip(input), fields(item_id = %id))]
fn handle(id: u64, input: &[u8]) {
    info!("handling item");
}
```

---

## Concurrency & Async (if applicable)
- Prefer structured concurrency: tasks should have clear ownership/lifetimes.
- Ensure cancellation safety: handle dropped futures and shutdown paths cleanly.
- Avoid blocking in async contexts; use `spawn_blocking` or a dedicated threadpool.
- Limit unbounded concurrency; use semaphores/buffers/backpressure.
- Timeouts are required for network calls and external dependencies.

---

## Performance: Measure, Then Optimize
- Don’t micro-optimize preemptively. Start with clear, correct code.
- When optimizing:
  - add benchmarks (`criterion`) and/or profiling data
  - document the reason and expected improvement
- Avoid unnecessary allocations:
  - prefer borrowing (`&str`, `&[u8]`) over cloning
  - use `Cow` where it materially helps clarity/perf
- For hot paths, consider:
  - reducing dynamic dispatch
  - batching I/O
  - careful use of `Bytes`/`Arc<[u8]>` for shared buffers

---

## Safety and `unsafe`
- Default rule: **no `unsafe`**.
- If `unsafe` is necessary:
  - isolate it in a small module with a safe API
  - document invariants and why they hold
  - add targeted tests (including property tests if appropriate)
  - enable `#![deny(unsafe_op_in_unsafe_fn)]`
  - consider Miri on relevant code paths

---

## API Design Guidelines
- Make invalid states unrepresentable (types > runtime checks).
- Prefer small, explicit types over “stringly typed” parameters.
- Use `&self` / `&mut self` methods when logical; keep ownership clear.
- Return iterators when useful; don’t overexpose internal collections.
- Avoid `pub` fields on structs unless they are stable and intentionally part of API.
- Document panic conditions; ideally avoid panics in library code.

---

## Testing Strategy
- Unit tests for pure logic; integration tests for public behavior.
- Use table-driven tests for many cases.
- Add regression tests for bugs.
- Consider:
  - property-based tests (`proptest`) for invariants
  - fuzzing (`cargo fuzz`) for parsers and complex input handling
- Keep tests deterministic; avoid timing-based flakiness.
- Prefer `cargo nextest` for speed and isolation if used in the repo.

Recommended commands:
```bash
cargo test --all-targets --all-features
cargo test --doc
```

Coverage (optional):
```bash
cargo llvm-cov --all-features --workspace
```

---

## Documentation
- Public items must have doc comments explaining:
  - what it does
  - invariants and edge cases
  - examples where helpful
- Keep module-level docs for non-trivial modules.
- Keep README(s) accurate; update when behavior/config changes.
- Add `#[doc = include_str!("../README.md")]` only when it improves discoverability.

---

## Configuration, Secrets, and Security
- Never hardcode secrets; load from env/secret store at runtime.
- Validate configuration early with clear error messages.
- Sanitize all external inputs; treat files, network, and env as untrusted.
- Prefer constant-time comparisons for secrets when applicable.
- Consider DOS vectors: unbounded memory growth, pathological inputs, regex bombs.
- Review serialization formats for backward compatibility and security implications.

---

## Build Profiles & Release Settings
- Be explicit about release settings for services/CLIs.
- Consider (case-by-case) in `Cargo.toml`:
  - `lto = "thin"` for better perf
  - `panic = "abort"` for small binaries (only if acceptable)
  - `codegen-units = 1` for peak perf (slower builds)
- Keep debug symbols policy explicit (useful for production debugging).

Example:
```toml
[profile.release]
lto = "thin"
codegen-units = 1
```

---

## CI Expectations (minimum)
CI should run:
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- MSRV build/test (if defined)
- `cargo audit` and/or `cargo deny` (as configured)

---

## Review Checklist (for any change)
- [ ] Correctness: edge cases handled, no silent failures
- [ ] No `unwrap()`/`expect()` in production paths (tests OK with justification)
- [ ] Errors have context and are actionable
- [ ] Logging is structured and avoids sensitive data
- [ ] Concurrency is bounded and shutdown/cancellation safe
- [ ] Tests added/updated; regression tests for bug fixes
- [ ] Docs updated for behavior or API changes
- [ ] `cargo fmt`, `cargo clippy`, and `cargo test` pass

---

## If You’re an Automated Agent Making Changes
- Prefer small, reviewable diffs.
- Don’t introduce new dependencies without explaining why and alternatives considered.
- If changing public APIs, document migration notes and SemVer impact.
- If you can’t run commands locally, still ensure changes are consistent with the
  commands listed above and explain assumptions.

---
# EDI Integration Application 

## Overview

Rust-based EDI integration engine for reading/writing UN/EDIFACT files (EANCOM-first), with runtime-configurable schemas and mappings. Supports EDI ↔ custom implementations, databases, and CSV files.

**Primary Standards:** EANCOM D96A, D01B (ORDERS, DESADV, INVOIC, SLSRPT, ORDRSP)

## Architecture

```
Schema Registry (Hierarchical: EDIFACT → EANCOM → Partner)
         ↓
    Validation Engine ← → Mapping Engine (DSL)
         ↓                      ↓
    Intermediate Representation (IR)
         ↓
    Adapters (EDIFACT | CSV | DB)
         ↓
    Transport (FS | DB)
```

## Crate Structure

Workspace with the following crates:

- `edi-ir` - Intermediate Representation structures and traversal APIs
- `edi-schema` - Schema model, loader, inheritance/merge logic
- `edi-validation` - Validation engine (structural, codelists)
- `edi-mapping` - DSL parser/runtime, transforms, extension API
- `edi-adapter-edifact` - EDIFACT/EANCOM parser/serializer, envelopes
- `edi-adapter-csv` - CSV integration
- `edi-adapter-db` - Database integration
- `edi-pipeline` - Streaming orchestration, batching, partial acceptance
- `edi-cli` - CLI application and configuration

## Key Design Decisions

1. **Streaming Required** - Message-level streaming minimum; segment-level for huge messages
2. **Runtime Schemas** - No redeployment for schema/mapping changes
3. **Profile Inheritance** - EDIFACT base → EANCOM version → Message → Partner
4. **Hybrid IR** - Generic tree + runtime schema typing (not purely stringly-typed or compile-time typed)
5. **Partial Acceptance** - Configurable: accept-all-with-warnings, fail-all, or quarantine damaged messages
6. **Strictness Levels** - Accept with warnings by default (real-world EDI), configurable per partner

## Open Questions (Need Decisions)

### 1. Schema Format
- [ ] JSON Schema directly
- [ ] Custom YAML/TOML DSL (EDI-aware)
- [ ] How to represent segment groups, conditional rules, partner overrides

### 2. Mapping DSL
- [ ] YAML-based declarative vs custom text DSL
- [ ] Required constructs: foreach/repeat, conditions, lookups, error handling

### 3. Query Language for IR
- [ ] XPath-like subset
- [ ] jq-like
- [ ] Custom (must support segment qualifiers, composite elements)

### 4. Extension Mechanism
- [ ] Rust dynamic plugins (shared libs)
- [ ] WASM modules (portable + sandboxed)
- [ ] Embedded scripting (Rhai/Lua)

### 5. Control Number Management
- [ ] Persisted sequences (DB-backed)
- [ ] Caller-provided
- [ ] File-based state
- [ ] Partner-specific rules

## Development Commands

```bash
# Build entire workspace
cargo build --workspace

# Run tests
cargo test --workspace

# Run specific crate tests
cargo test -p edi-ir

# Build release
cargo build --release --workspace

# Run CLI
cargo run -p edi-cli -- <args>

# Linting
cargo clippy --workspace --all-targets

# Formatting
cargo fmt --all
```

## Testing Strategy

1. **Unit tests** - Per crate in `src/` alongside code
2. **Integration tests** - In `tests/` directories with sample EDI files
3. **Test data** - Sample EDI files in `testdata/`:
   - `edi/` - EDIFACT/EANCOM samples (valid, invalid, edge cases)
   - `csv/` - CSV schemas and sample files
   - `schemas/` - Runtime schema files for testing
   - `mappings/` - DSL mapping files for testing

## Project Structure

```
/
├── Cargo.toml              # Workspace root
├── AGENTS.md               # This file
├── product_specification.md # Requirements
├── crates/
│   ├── edi-ir/
│   ├── edi-schema/
│   ├── edi-validation/
│   ├── edi-mapping/
│   ├── edi-adapter-edifact/
│   ├── edi-adapter-csv/
│   ├── edi-adapter-db/
│   ├── edi-pipeline/
│   └── edi-cli/
├── testdata/
│   ├── edi/
│   ├── csv/
│   ├── schemas/
│   └── mappings/
└── docs/
    ├── architecture/
    └── examples/
```

## MVP Scope

Demonstrate end-to-end with ORDERS (EANCOM D96A):
1. Parse EDIFACT file (streaming)
2. Validate against runtime schema
3. Map using DSL to target IR
4. Validate output
5. Serialize to CSV/JSON

## Code Conventions

- Rust 2021 edition
- Comprehensive error types with `thiserror` or `snafu`
- Async where needed (likely for DB adapter)
- Streaming iterators for large files
- Strict typing; avoid stringly-typed where possible
- Document public APIs with rustdoc

## Performance Targets

- Message-level streaming (parse envelope → iterate messages → emit incrementally)
- Minimal memory footprint per message
- Support for batch files with thousands of messages

## Error Reporting Requirements

- Message index/reference number
- Segment position and element/component index
- Path in IR
- Actionable messages (expected vs actual, allowed codes)
- Source position metadata

## Dependencies to Consider

- `serde` + `serde_json`/`serde_yaml` - Serialization
- `nom` or `winnow` - Parser combinators for EDIFACT
- `csv` - CSV reading/writing
- `sqlx` or `tokio-postgres` - Database (async)
- `clap` - CLI parsing
- `tracing` - Logging/observability
- `thiserror` - Error handling
- `dashmap` or similar - Concurrent schema cache

## Next Steps

1. Finalize open questions (schema format, DSL syntax)
2. Initialize Rust workspace with crate structure
3. Define IR core structures
4. Implement EDIFACT streaming parser
5. Build schema loader with inheritance

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync --status
   git push origin beads-sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- bv-agent-instructions-v1 -->

---

## Beads Workflow Integration

This project uses [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) for issue tracking. Issues are stored in `.beads/` and tracked in git.

### Protected Branch Setup (Current Repo Configuration)

- Metadata sync branch: `beads-sync`
- Configure once per clone: `bd config set sync.branch beads-sync`
- Start daemon for automatic metadata sync:
  - `bd daemon start --auto-commit --auto-push --auto-pull`
- Keep merge support configured:
  - `.gitattributes` must include `.beads/issues.jsonl merge=beads`
  - local git config must include:
    - `git config merge.beads.driver "bd merge %A %O %A %B"`
    - `git config merge.beads.name "bd JSONL merge driver"`

### Essential Commands

```bash
# View issues (launches TUI - avoid in automated sessions)
bv

# CLI commands for agents (use these instead)
bd ready              # Show issues ready to work (no blockers)
bd list --status=open # All open issues
bd show <id>          # Full issue details with dependencies
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id> --reason="Completed"
bd close <id1> <id2>  # Close multiple issues at once
bd sync --status      # Show diff/status between current branch and beads-sync
bd sync --flush-only  # Manual metadata commit to beads-sync (if daemon is not running)
```

### Workflow Pattern

1. **Start**: Run `bd ready` to find actionable work
2. **Claim**: Use `bd update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `bd close <id>`
5. **Sync**: Ensure daemon is running and verify with `bd sync --status` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `bd ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
- **Blocking**: `bd dep add <issue> <depends-on>` to add dependencies

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
bd sync --status        # Verify metadata sync state
git commit -m "..."     # Commit code
git push origin beads-sync  # Ensure metadata branch is pushed
git push                # Push to remote
```

### Best Practices

- Check `bd ready` at session start to find available work
- Update status as you work (in_progress → closed)
- Create new issues with `bd create` when you discover tasks
- Use descriptive titles and set appropriate priority/type
- Keep `bd daemon` running with auto-commit/push/pull in this repo
- Periodically merge `beads-sync` -> `main` via PR to publish metadata history

<!-- end-bv-agent-instructions -->

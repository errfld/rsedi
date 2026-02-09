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

---

## GitHub Issues Workflow Integration

This project now uses GitHub Issues as the source of truth for task tracking.

### Essential Commands

```bash
# List open issues
gh issue list --state open --limit 200

# Show details for one issue
gh issue view <number>

# Create issue
gh issue create --title "..." --body-file /tmp/body.md --label "type:task" --label "priority:P2"

# Mark in progress
gh issue edit <number> --add-label "status:in-progress"

# Close as completed (only after PR merge and full scope resolution)
gh issue close <number> --comment "Completed"

# Optional: mark partially resolved work that needs follow-up
gh issue edit <number> --add-label "needs-follow-up"
```

### Dependencies and Hierarchy

Use GitHub API for graph links:

```bash
# Mark issue <child> as blocked by <blocker>
gh api -X POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -f issue_id=<blocker_issue_id>

# Add <child_issue_id> as sub-issue under parent issue number <parent>
gh api -X POST repos/<owner>/<repo>/issues/<parent>/sub_issues -f sub_issue_id=<child_issue_id>
```

---

## Workflow Pattern (GitHub-Issues-First)

1. **Pick issue**: determine the best open issue to start based on priority, dependencies, and unblocking impact.
2. **Mark in progress**: add `status:in-progress` on the selected issue.
3. **Create worktree and branch**: create a dedicated branch named `gh-<issue-number>/<short-description>` and a matching worktree directory name that mirrors it (replace `/` with `-`). Sanitize `<short-description>` as follows: lowercase; convert spaces/consecutive whitespace to a single `-`; replace `/` and `\` with `-`; remove invalid Git ref characters (such as `:`, `?`, `*`, `[`, `]`, `~`, `^`, `@{`); remove consecutive dots (`..`) and leading `.` components; reject or strip path components ending with `.lock`; collapse repeated `-`; trim leading/trailing `.`, `/`, and `-`; and cap length to 50 characters. After sanitization, validate the full branch name with `git check-ref-format --branch`; if validation fails, fall back to a safe sanitized alternative. Apply the same sanitized token when generating the worktree folder name. Example: branch `gh-123/fix-parser` with worktree folder `gh-123-fix-parser`.
4. **Implement**:
   - Record notable operational/implementation improvements in `AGENTS.md`.
   - If you identify follow-up improvements/reworks, create GitHub issues with full context for someone with no prior project knowledge.
   - Verify behavior with appropriate tests and quality checks (`cargo fmt`, `cargo clippy`, `cargo test`), and ensure production-ready quality.
   - Review your own changes before opening a PR.
5. **Push and open PR**: push branch to remote and create a pull request linked to the issue.
6. **Review cycle**: wait for review, then address/resolve all PR comments.
7. **Track Learning**: keep a record of what you learned during the implementation process, including any challenges you faced and how you overcame them in AGENT.md under `## Learnings`.
8. **Merge and close**: after PR merge (performed by the user/maintainer), the agent may close the related issue automatically only if the merge fully resolves the issue scope.
   - Supported merge-detection methods:
     - periodic polling of PR status via API/CLI (e.g. `gh pr view <number> --json state,mergedAt`)
     - repository webhook events for merged PRs
     - explicit maintainer trigger (comment, label, or command)
   - Default behavior: wait for confirmed merged state before closing the issue.
   - Optional stricter behavior: require a maintainer confirmation signal in addition to merged state before closing.
   - Partial resolution: leave the issue open, add a detailed comment describing remaining tasks, and optionally add a `needs-follow-up` label.

--- 

## Learnings
- 2026-02-09 (`#55`): CSV-to-IR conversion now validates row/header column counts before zipping values. This prevents silent truncation/misalignment when rows contain missing or extra columns and returns a line-specific validation error instead.
- 2026-02-09: For `edi-adapter-db`, IR write behavior needed explicit mode semantics (`insert`/`update`/`upsert`) plus batch controls in a single API. Adding `WriteMode` + `WriteOptions` avoided duplicated call-site logic and made transactional chunking deterministic for large payloads.
- 2026-02-09: In-memory transaction paths must validate against applied schema just like direct writes; otherwise tests can pass on libsql while memory-mode allows invalid rows. Capturing schema snapshot in `DbTransaction` fixed parity.
- 2026-02-09: `edi generate` is most reliable when CSV/JSON inputs are normalized into a stable IR shape (`/rows/row` for CSV, `/rows/item` for JSON arrays), because current mapping runtime path resolution does not support index syntax like `[0]` or wildcards.

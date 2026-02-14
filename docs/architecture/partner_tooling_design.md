# Partner Tooling Design

## Scope
Design three partner-facing tools and their CLI integration:
- Mapping Tester
- Schema Linter
- Sample Generator

Optional future extension:
- Schema Visualizer

## Goals
- Reduce onboarding time for new partners.
- Catch mapping/schema mistakes before runtime.
- Provide deterministic examples and CI-friendly outputs.

## Non-Goals (Initial)
- Full IDE plugin support.
- GUI tooling.
- Auto-remediation of all lint findings.

## CLI Command Structure

## Proposed Top-Level Group
`edi partner <subcommand>`

## Mapping Tester
`edi partner mapping-test --mapping <file> --input <file> --expected <file> [--format json|csv|edi] [--strict]`

Behavior:
- Loads mapping and input document.
- Executes mapping runtime.
- Compares produced output with expected output.
- Supports structural diff output for JSON and line diff for CSV/EDI.

Exit codes:
- `0` pass
- `1` pass with warnings (non-fatal normalization differences when allowed)
- `2` mismatch/failure
- `3` fatal execution error

## Schema Linter
`edi partner schema-lint --schema <file> [--schema-dir <dir>] [--format text|json] [--strict]`

Checks:
- required top-level fields (name/version/segments)
- duplicate segment tags with conflicting definitions
- invalid element types/length ranges
- orphan references and parent schema resolution
- codelist reference integrity

Exit codes:
- `0` no findings
- `1` warnings only
- `2` errors found
- `3` fatal error (I/O/parse/runtime)

## Sample Generator
`edi partner sample-generate --schema <file> --message-type <type> --count <n> --output <file> [--seed <u64>] [--minimal|--full]`

Behavior:
- Generates valid sample documents conforming to schema constraints.
- Uses deterministic RNG when `--seed` is supplied.
- Can generate minimal required-only payloads or fuller samples.

Exit codes:
- `0` success
- `2` generation constraint failure
- `3` fatal error

## Optional Schema Visualizer (Future)
`edi partner schema-visualize --schema <file> --output <file> --format mermaid|dot`

## Tool Design Details

## Mapping Tester
Inputs:
- source input payload (EDI/JSON/CSV)
- mapping DSL file
- optional schema and expected output

Outputs:
- summary (pass/fail, rule counts, elapsed time)
- structured diff report (`json`) and human-readable diff (`text`)

Key implementation pieces:
- reuse `edi-mapping` runtime
- normalize output before comparison (optional strict mode)
- provide path-level mismatch diagnostics

## Schema Linter
Inputs:
- schema file or directory

Outputs:
- findings list with severity/code/path/message

Finding taxonomy:
- `LINT001` malformed metadata
- `LINT1xx` structural issues
- `LINT2xx` element/codelist issues
- `LINT3xx` inheritance/reference issues

## Sample Generator
Inputs:
- schema file
- generation profile (`minimal`/`full`)

Outputs:
- generated sample EDI/JSON/CSV depending on target schema type

Generation rules:
- mandatory segments always present
- optional segments included based on profile and deterministic RNG
- lengths and primitive types obey schema bounds

## Workflow Examples

## Partner Onboarding
1. Run `schema-lint` on incoming partner schema.
2. Run `sample-generate` to create baseline fixtures.
3. Build mapping and verify with `mapping-test` in CI.

## Regression Workflow
1. Update schema or mapping.
2. Re-run `schema-lint` and `mapping-test` against golden files.
3. Regenerate samples only when schema semantics change.

## Implementation Priorities
1. Schema Linter (highest leverage, fast feedback).
2. Mapping Tester (critical for safe mapping changes).
3. Sample Generator (accelerates test data and onboarding).
4. Visualizer (optional enhancement).

## Delivery Plan

## Phase 1
- Define finding model and CLI contracts.
- Implement schema-lint MVP and JSON output mode.

## Phase 2
- Implement mapping-test with compare and diff modes.
- Add CI-ready exit code behavior.

## Phase 3
- Implement sample-generate minimal/full profiles.
- Add deterministic seed support.

## Phase 4
- Optional visualizer and richer reports.

## Risks and Mitigations
- Risk: false-positive lint findings.
  Mitigation: severity tuning and suppressions with justification.
- Risk: mapping diff noise.
  Mitigation: canonicalization mode and strict toggle.
- Risk: non-deterministic sample generation.
  Mitigation: mandatory seed support and snapshot tests.

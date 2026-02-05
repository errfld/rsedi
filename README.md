# EDI Integration Engine (MVP)

Rust workspace for parsing, validating, and transforming UN/EDIFACT EDI (EANCOM-first) into a schema-aware Intermediate Representation (IR). The current MVP focuses on ORDERS (EANCOM D96A) with runtime-loaded schemas and a YAML mapping DSL.

## Status
- MVP focus: ORDERS (EANCOM D96A) parse, validate, map to IR/JSON.
- CLI supports `transform` and `validate` subcommands. `generate` and config-file support are placeholders.
- CSV adapter and pipeline logic exist as building blocks; DB adapter types are present but not wired to a driver.
- Streaming at message boundaries is a design goal; current CLI reads full input files into memory.

## Architecture (Conceptual)

```
Schema Registry (EDIFACT -> EANCOM -> Partner)
         |
         v
    Validation Engine <-> Mapping Engine (DSL)
         |                     |
         v                     v
    Intermediate Representation (IR)
         |
         v
    Adapters (EDIFACT | CSV | DB)
         |
         v
    Transport (FS | DB)
```

## Workspace Crates
- `crates/edi-ir`: IR node model, metadata, traversal utilities.
- `crates/edi-schema`: Schema model, loader, inheritance/merge logic.
- `crates/edi-validation`: Validation engine and reporter for schema-driven rules.
- `crates/edi-mapping`: YAML mapping DSL parser, runtime, and transforms.
- `crates/edi-adapter-edifact`: EDIFACT parser, syntax handling, envelopes.
- `crates/edi-adapter-csv`: CSV schema, reader, writer utilities.
- `crates/edi-adapter-db`: DB schema mapping types and stubs for future integration.
- `crates/edi-pipeline`: Pipeline policies, batching, quarantine flows.
- `crates/edi-cli`: CLI entrypoint used for quickstart commands.

## Quickstart

Build the workspace:

```bash
cargo build --workspace
```

Run an ORDERS transform to JSON IR output:

```bash
cargo run -p edi-cli -- transform testdata/edi/valid_orders_d96a_minimal.edi /tmp/orders.json -m testdata/mappings/orders_to_json.yaml
cat /tmp/orders.json
```

Validate a valid ORDERS message:

```bash
cargo run -p edi-cli -- validate testdata/edi/valid_orders_d96a_minimal.edi -s testdata/schemas/eancom_orders_d96a.yaml
```

Validate an invalid ORDERS message (missing BGM):

```bash
cargo run -p edi-cli -- validate testdata/edi/invalid_orders_missing_bgm.edi -s testdata/schemas/eancom_orders_d96a.yaml
```

## CLI Usage

Binary name: `edi`

Transform an EDI file into JSON IR:

```bash
edi transform <input.edi> <output.json> -m <mapping.yaml> [-s <schema.yaml>]
```

Validate an EDI file against a schema:

```bash
edi validate <input.edi> -s <schema.yaml>
```

Generate a sample EDI file (placeholder):

```bash
edi generate <output.edi> --message-type ORDERS --version D96A
```

Config file flag (currently ignored in MVP):

```bash
edi --config <path> <subcommand>
```

### Exit Codes
- `0`: no warnings or errors
- `1`: warnings only
- `2`: validation errors

## Schemas and Mapping DSL

Schema and mapping examples live in `testdata/`.

- Schema files: `testdata/schemas/`
- Mapping DSL examples: `testdata/mappings/`
- EDI samples: `testdata/edi/`

Mapping DSL notes and examples:
- `testdata/mappings/README.md`

## Test Data

Sample EDIFACT files and their expected behavior are documented in:
- `testdata/edi/README.md`

## Development

Formatting, linting, and tests:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

## Known MVP Limitations
- `edi --config` is accepted but ignored.
- `edi generate` is not implemented yet.
- `edi transform` ignores the optional schema flag for now.
- CLI runs on full in-memory files rather than streaming chunks.

## References
- `product_specification.md`
- `AGENTS.md`

# EDI Integration Engine (MVP)

Rust workspace for parsing, validating, and transforming EDI (EDIFACT/EANCOM) messages.

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

Validation exit codes:

- `0`: no warnings or errors
- `1`: warnings only
- `2`: validation errors

## Issue Tracking Migration

Task tracking has moved from Beads to GitHub Issues. Migration tooling and runbook:

- `scripts/github-migration/migrate-beads-to-github.sh`
- `scripts/github-migration/validate-beads-github-migration.sh`
- `docs/operations/github-issues-migration.md`

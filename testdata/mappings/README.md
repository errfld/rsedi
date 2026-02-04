# Mapping DSL Examples

This directory contains ORDERS mapping examples used as reference templates and parser/runtime test fixtures.

## Schema references

- Source EDI schema: `testdata/schemas/eancom_orders_d96a.yaml`
- CSV target schema: `testdata/schemas/csv_orders_target.yaml`

## Files

- `orders_to_csv.yaml`
  - EANCOM D96A ORDERS -> flattened CSV rows.
  - Emits one row per line item and repeats key header values.
  - Demonstrates `foreach`, qualifier-aware paths, date/number transforms, lookups, and defaults.

- `orders_to_json.yaml`
  - EANCOM D96A ORDERS -> JSON-style hierarchical IR.
  - Preserves message structure (`parties`, `references`, `lines`).
  - Demonstrates nested arrays, role-based conditionals (BY/SU/DP), defaults, and lookups.

- `csv_to_orders.yaml`
  - CSV rows -> EANCOM D96A ORDERS-style IR.
  - Rebuilds BGM/DTM/NAD/LIN-oriented structures from flat records.
  - Demonstrates reverse direction mapping, constants via defaults, row iteration, and conditional line-level output.

## Notes

- Paths intentionally use the jq-like qualifier style agreed for `edi-62b` (for example `NAD[3035='BY']`).
- These files are examples first; adapter-specific path evaluators may support subsets while implementation evolves.

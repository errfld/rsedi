# Changelog

## Unreleased

### Changed

- `edi-pipeline`: added public re-exports for pipeline orchestration types from
  `batch`, `pipeline`, `policies`, `quarantine`, and `streaming`.
  SemVer impact: **minor** (additive API surface).
  Migration note: downstream crates may now import these names directly from
  `edi_pipeline`; if you already define similarly named symbols, prefer explicit
  imports or aliases to avoid name collisions.

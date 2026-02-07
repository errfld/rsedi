# Beads -> GitHub Issues Migration

This directory contains the migration tooling that moves issue tracking from Beads to GitHub Issues while preserving:

- task hierarchy (`parent-child` -> GitHub sub-issues)
- task dependencies (`blocks` -> GitHub issue dependencies)
- status/type/priority metadata

## Prerequisites

- `gh` CLI authenticated with `repo` scope
- `jq`
- source file at `.beads/issues.jsonl`

Optional for Project setup:
- `project` scope on the GitHub token (`gh auth refresh -s project`)

## 1) Dry run

```bash
scripts/github-migration/migrate-beads-to-github.sh \
  --source .beads/issues.jsonl \
  --repo <owner>/<repo> \
  --dry-run
```

## 2) Run migration

```bash
scripts/github-migration/migrate-beads-to-github.sh \
  --source .beads/issues.jsonl \
  --repo <owner>/<repo>
```

The script creates a run directory under:

```text
.migration/github-issues/<timestamp>/
```

with:

- `issues.snapshot.jsonl` - immutable snapshot used by the run
- `id-map.jsonl` - Beads ID to GitHub issue mapping
- `migration-report.json` - import + relationship summary

## 3) Validate parity

```bash
scripts/github-migration/validate-beads-github-migration.sh \
  --source .beads/issues.jsonl \
  --repo <owner>/<repo> \
  --map .migration/github-issues/<timestamp>/id-map.jsonl
```

## Notes on idempotency

- The importer searches GitHub for `Legacy-Beads-ID: <id>` before creating a new issue.
- Re-running the migration updates labels/state and reuses existing migrated issues.
- Relationship creation is safe to rerun; duplicate links are treated as skipped.

## Labels created by migration

- `source:beads`
- `status:in-progress`
- `type:task`, `type:bug`, `type:feature`
- `priority:P0` ... `priority:P4`

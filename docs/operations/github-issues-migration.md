# Migration Runbook: Beads to GitHub Issues

## Goal

Replace Beads with GitHub-native tracking while preserving:

- dependencies between tasks
- hierarchy (parent/child)
- issue lifecycle metadata for agent memory

## Current Source Baseline

Source file: `.beads/issues.jsonl`

Baseline (captured from current source snapshot on 2026-02-07):

- total issues: `65`
- open: `20`
- in progress: `0`
- closed: `45`
- dependency links (`blocks`): `32`
- hierarchy links (`parent-child`): `0`

## Canonical Mapping

- Beads issue -> GitHub Issue
- Beads `blocks` -> GitHub Issue Dependencies (`blocked_by`)
- Beads `parent-child` -> GitHub Sub-issues
- Beads `priority` -> labels `priority:P0..P4`
- Beads `issue_type` -> labels `type:task|bug|feature`
- Beads `in_progress` -> label `status:in-progress`
- Source provenance -> `source:beads`

Each migrated issue body includes:

- `Legacy-Beads-ID`
- `Legacy-Status`
- `Legacy-Priority`
- `Legacy-Type`
- `Legacy-Owner`
- source timestamps

This allows reliable re-runs and robust agent lookups.

## Migration Steps

1. Freeze Beads updates.
2. Run importer in dry-run mode.
3. Run importer for real.
4. Run parity validator.
5. Update agent workflow to use GitHub Issues only.

## Commands

### Dry run

```bash
scripts/github-migration/migrate-beads-to-github.sh \
  --source .beads/issues.jsonl \
  --repo <owner>/<repo> \
  --dry-run
```

### Execute migration

```bash
scripts/github-migration/migrate-beads-to-github.sh \
  --source .beads/issues.jsonl \
  --repo <owner>/<repo>
```

### Validate parity

```bash
scripts/github-migration/validate-beads-github-migration.sh \
  --source .beads/issues.jsonl \
  --repo <owner>/<repo> \
  --map .migration/github-issues/<timestamp>/id-map.jsonl
```

## Agent Memory Model on GitHub

Store durable context in issue bodies with stable sections:

- `## Context`
- `## Decisions`
- `## Next Actions`
- `## Risks`
- `## Agent Notes`

Store temporal run history in comments:

- one comment per agent run
- include date/time, command/result summary, next action

Suggested compact machine-readable comment footer:

```text
<!-- agent-memory
run_at: 2026-02-07T14:00:00Z
agent: codex
status: in_progress
next: "Implement CSV header error propagation"
-->
```

## Recommended TUI

Use `gh-dash` as the primary terminal UI for issues/PRs.

- install: `gh extension install dlvhdr/gh-dash`
- launch: `gh dash`

Why this choice:

- keyboard-first issue triage
- configurable panes/filters for open, blocked, in-progress
- works cleanly with `gh api` for dependency/sub-issue actions

## Rollback and Safety

- keep `.beads/issues.jsonl` unchanged until parity checks pass
- migration is idempotent via `Legacy-Beads-ID`
- rerun importer safely after partial failures
- validate before deleting or archiving Beads artifacts

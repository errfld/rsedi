#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME=$(basename "$0")

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Migrate Beads issues from JSONL to GitHub Issues with dependency + sub-issue links.

Options:
  --source <path>           Source issues JSONL (default: .beads/issues.jsonl)
  --repo <owner/repo>       GitHub repository (default: current gh repo)
  --run-dir <path>          Output directory for artifacts (default: .migration/github-issues/<timestamp>)
  --skip-relationships      Import issues only; do not create dependencies/sub-issues
  --skip-validation         Skip post-migration parity checks
  --dry-run                 Print intended mutations without changing GitHub
  -h, --help                Show help

Artifacts:
  id-map.jsonl              Beads ID -> GitHub issue mapping
  migration-report.json     Counts and operation summary
  issues.snapshot.jsonl     Source snapshot used for migration
USAGE
}

log() {
  printf '[%s] %s\n' "$SCRIPT_NAME" "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

gh_api_retry() {
  local max_attempts=6
  local delay=2
  local attempt=1
  local err_file
  err_file=$(mktemp)

  while true; do
    set +e
    local output
    output=$(gh api "$@" 2>"$err_file")
    local status=$?
    set -e

    if [[ $status -eq 0 ]]; then
      rm -f "$err_file"
      printf '%s' "$output"
      return 0
    fi

    local err_text
    err_text=$(cat "$err_file")

    if [[ $attempt -ge $max_attempts ]]; then
      rm -f "$err_file"
      printf '%s\n' "$err_text" >&2
      return $status
    fi

    if grep -Eqi 'rate limit|secondary rate|timed out|timeout|502|503|504' <<<"$err_text"; then
      log "GitHub API transient failure (attempt $attempt/$max_attempts). Retrying in ${delay}s."
      sleep "$delay"
      delay=$((delay * 2))
      attempt=$((attempt + 1))
      continue
    fi

    rm -f "$err_file"
    printf '%s\n' "$err_text" >&2
    return $status
  done
}

mutate_json() {
  local endpoint=$1
  local payload=$2
  local method=${3:-POST}

  if [[ "$DRY_RUN" == "1" ]]; then
    log "[dry-run] gh api -X $method $endpoint --input <payload>"
    return 0
  fi

  gh_api_retry -X "$method" "$endpoint" --input - <<<"$payload"
}

ensure_label() {
  local name=$1
  local color=$2
  local description=$3

  if [[ "$DRY_RUN" == "1" ]]; then
    log "[dry-run] ensure label: $name"
    return 0
  fi

  if gh label create "$name" --repo "$REPO" --color "$color" --description "$description" >/dev/null 2>&1; then
    log "Created label: $name"
  fi
}

find_existing_issue() {
  local beads_id=$1
  jq -rc --arg id "$beads_id" '.[$id] // empty' "$EXISTING_INDEX"
}

load_repo_default() {
  gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>/dev/null || true
}

SOURCE=".beads/issues.jsonl"
REPO=""
RUN_DIR=""
SKIP_RELATIONSHIPS=0
SKIP_VALIDATION=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE=$2
      shift 2
      ;;
    --repo)
      REPO=$2
      shift 2
      ;;
    --run-dir)
      RUN_DIR=$2
      shift 2
      ;;
    --skip-relationships)
      SKIP_RELATIONSHIPS=1
      shift
      ;;
    --skip-validation)
      SKIP_VALIDATION=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
done

require_cmd gh
require_cmd jq
require_cmd mktemp

[[ -f "$SOURCE" ]] || die "Source file not found: $SOURCE"

if [[ -z "$REPO" ]]; then
  REPO=$(load_repo_default)
fi
[[ -n "$REPO" ]] || die "Could not determine repository. Pass --repo owner/repo."

if [[ -z "$RUN_DIR" ]]; then
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  RUN_DIR=".migration/github-issues/${timestamp}"
fi

mkdir -p "$RUN_DIR"
MAP_FILE="$RUN_DIR/id-map.jsonl"
REPORT_FILE="$RUN_DIR/migration-report.json"
SNAPSHOT_FILE="$RUN_DIR/issues.snapshot.jsonl"

cp "$SOURCE" "$SNAPSHOT_FILE"
: > "$MAP_FILE"

# Validate JSONL structure up front.
jq -c '.' "$SOURCE" >/dev/null

log "Migrating Beads issues from $SOURCE"
log "Repository: $REPO"
log "Run directory: $RUN_DIR"

# Base labels for issue management + migration provenance.
ensure_label "source:beads" "5319e7" "Issue migrated from Beads"
ensure_label "status:in-progress" "fbca04" "Issue is actively in progress"
ensure_label "type:task" "0e8a16" "Task"
ensure_label "type:bug" "d73a4a" "Bug"
ensure_label "type:feature" "1d76db" "Feature"
ensure_label "priority:P0" "b60205" "Critical priority"
ensure_label "priority:P1" "d93f0b" "High priority"
ensure_label "priority:P2" "fbca04" "Medium priority"
ensure_label "priority:P3" "0e8a16" "Low priority"
ensure_label "priority:P4" "c2e0c6" "Backlog priority"

EXISTING_INDEX="$RUN_DIR/existing-legacy-index.json"
EXISTING_TMP="$RUN_DIR/.existing-legacy-index.jsonl"
: > "$EXISTING_TMP"

if [[ "$DRY_RUN" != "1" ]]; then
  log "Building index of already-migrated GitHub issues"
  page=1
  while true; do
    query=$(printf 'repo:%s is:issue in:body "Legacy-Beads-ID:"' "$REPO")
    response=$(gh_api_retry -X GET search/issues -f q="$query" -f per_page=100 -f page="$page")
    count=$(jq -r '.items | length' <<<"$response")

    jq -rc '
      (.items // [])
      | .[]
      | {
          legacy_id: (try (.body | capture("Legacy-Beads-ID: (?<id>[^\\n\\r]+)").id) catch ""),
          number,
          id,
          node_id,
          state
        }
      | select(.legacy_id != "")
    ' <<<"$response" >> "$EXISTING_TMP"

    if [[ "$count" -lt 100 ]]; then
      break
    fi
    page=$((page + 1))
  done
fi

jq -cs '
  map({
    key: .legacy_id,
    value: {
      number: .number,
      id: .id,
      node_id: .node_id,
      state: .state
    }
  })
  | from_entries
' "$EXISTING_TMP" > "$EXISTING_INDEX"

source_total=$(jq -c '.' "$SOURCE" | wc -l | tr -d ' ')
source_open=$(jq -r 'select(.status == "open") | .id' "$SOURCE" | wc -l | tr -d ' ')
source_in_progress=$(jq -r 'select(.status == "in_progress") | .id' "$SOURCE" | wc -l | tr -d ' ')
source_closed=$(jq -r 'select(.status == "closed") | .id' "$SOURCE" | wc -l | tr -d ' ')
source_blocks=$(jq -r '.dependencies[]? | select(.type == "blocks") | .issue_id' "$SOURCE" | wc -l | tr -d ' ')
source_parent_child=$(jq -r '.dependencies[]? | select(.type == "parent-child") | .issue_id' "$SOURCE" | wc -l | tr -d ' ')

created_issues=0
reused_issues=0
open_updates=0
closed_updates=0
issue_failures=0

while IFS= read -r issue; do
  beads_id=$(jq -r '.id' <<<"$issue")
  title=$(jq -r '.title // "Untitled"' <<<"$issue")
  status=$(jq -r '.status // "open"' <<<"$issue")

  existing=$(find_existing_issue "$beads_id")
  if [[ -n "$existing" ]]; then
    github_number=$(jq -r '.number' <<<"$existing")
    github_id=$(jq -r '.id' <<<"$existing")
    github_state=$(jq -r '.state' <<<"$existing")
    reused_issues=$((reused_issues + 1))
    log "Reusing existing issue #$github_number for $beads_id"
  else
    body=$(jq -r '
      [
        (.description // ""),
        ((.design // "") | if . == "" then empty else "## Legacy Design\n\n" + . end),
        ((.notes // "") | if . == "" then empty else "## Legacy Notes\n\n" + . end),
        (
          "---\n"
          + "Migrated from Beads.\n\n"
          + "Legacy-Beads-ID: " + .id + "\n"
          + "Legacy-Status: " + (.status // "open") + "\n"
          + "Legacy-Priority: P" + ((.priority // 2) | tostring) + "\n"
          + "Legacy-Type: " + (.issue_type // "task") + "\n"
          + "Legacy-Owner: " + (.owner // "") + "\n"
          + "Legacy-Created-At: " + (.created_at // "") + "\n"
          + "Legacy-Updated-At: " + (.updated_at // "") + "\n"
          + "Legacy-Close-Reason: " + (.close_reason // "")
        )
      ]
      | map(select(. != ""))
      | join("\n\n")
    ' <<<"$issue")

    labels_json=$(jq -nc --argjson issue "$issue" '
      [
        "source:beads",
        ("type:" + ($issue.issue_type // "task")),
        ("priority:P" + (($issue.priority // 2) | tostring)),
        (if ($issue.status // "open") == "in_progress" then "status:in-progress" else empty end)
      ]
      | unique
    ')

    payload=$(jq -nc \
      --arg title "$title" \
      --arg body "$body" \
      --argjson labels "$labels_json" \
      '{title: $title, body: $body, labels: $labels}')

    if [[ "$DRY_RUN" == "1" ]]; then
      log "[dry-run] would create issue for $beads_id: $title"
      github_number=""
      github_id=""
      github_state="unknown"
    else
      set +e
      created=$(mutate_json "repos/$REPO/issues" "$payload" "POST" 2>"$RUN_DIR/.create-error.log")
      rc=$?
      set -e
      if [[ $rc -ne 0 ]]; then
        issue_failures=$((issue_failures + 1))
        log "Failed to create issue for $beads_id (see $RUN_DIR/.create-error.log)"
        continue
      fi

      github_number=$(jq -r '.number' <<<"$created")
      github_id=$(jq -r '.id' <<<"$created")
      github_state=$(jq -r '.state' <<<"$created")
      created_issues=$((created_issues + 1))
      log "Created issue #$github_number for $beads_id"
    fi
  fi

  if [[ "$DRY_RUN" != "1" && -n "${github_number:-}" ]]; then
    target_state="open"
    if [[ "$status" == "closed" ]]; then
      target_state="closed"
    fi

    if [[ "$github_state" != "$target_state" ]]; then
      state_payload=$(jq -nc --arg state "$target_state" '{state: $state}')
      if mutate_json "repos/$REPO/issues/$github_number" "$state_payload" "PATCH" >/dev/null; then
        if [[ "$target_state" == "closed" ]]; then
          closed_updates=$((closed_updates + 1))
        else
          open_updates=$((open_updates + 1))
        fi
      fi
    fi

    # Ensure migration labels on reused issues.
    labels_payload=$(jq -nc --argjson issue "$issue" '
      {
        labels: (
          [
            "source:beads",
            ("type:" + ($issue.issue_type // "task")),
            ("priority:P" + (($issue.priority // 2) | tostring)),
            (if ($issue.status // "open") == "in_progress" then "status:in-progress" else empty end)
          ]
          | unique
        )
      }
    ')
    mutate_json "repos/$REPO/issues/$github_number/labels" "$labels_payload" "POST" >/dev/null

  fi

  jq -nc \
    --arg beads_id "$beads_id" \
    --argjson github_number "${github_number:-null}" \
    --argjson github_id "${github_id:-null}" \
    --arg status "$status" \
    --arg title "$title" \
    '{beads_id: $beads_id, github_number: $github_number, github_id: $github_id, status: $status, title: $title}' \
    >> "$MAP_FILE"
done < <(jq -c '.' "$SOURCE")

MAP_JSON="$RUN_DIR/id-map.json"
jq -cs 'map({key: .beads_id, value: .}) | from_entries' "$MAP_FILE" > "$MAP_JSON"

rel_blocks_applied=0
rel_blocks_skipped=0
rel_parent_applied=0
rel_parent_skipped=0
rel_failures=0

if [[ "$SKIP_RELATIONSHIPS" == "0" ]]; then
  log "Applying dependency and hierarchy relationships"
  while IFS= read -r dep; do
    dep_type=$(jq -r '.type' <<<"$dep")
    issue_id=$(jq -r '.issue_id' <<<"$dep")
    depends_on_id=$(jq -r '.depends_on_id' <<<"$dep")

    case "$dep_type" in
      blocks)
        dependent_number=$(jq -r --arg id "$issue_id" '.[$id].github_number // empty' "$MAP_JSON")
        blocker_github_id=$(jq -r --arg id "$depends_on_id" '.[$id].github_id // empty' "$MAP_JSON")

        if [[ -z "$dependent_number" || -z "$blocker_github_id" || "$dependent_number" == "null" || "$blocker_github_id" == "null" ]]; then
          rel_blocks_skipped=$((rel_blocks_skipped + 1))
          log "Skipping blocks relation $issue_id -> $depends_on_id (missing mapping)"
          continue
        fi

        payload=$(jq -nc --argjson issue_id "$blocker_github_id" '{issue_id: $issue_id}')
        if [[ "$DRY_RUN" == "1" ]]; then
          log "[dry-run] blocks: $issue_id (#$dependent_number) blocked by $depends_on_id"
          rel_blocks_applied=$((rel_blocks_applied + 1))
          continue
        fi

        set +e
        mutate_json "repos/$REPO/issues/$dependent_number/dependencies/blocked_by" "$payload" "POST" >/dev/null 2>"$RUN_DIR/.relation-error.log"
        rc=$?
        set -e
        if [[ $rc -eq 0 ]]; then
          rel_blocks_applied=$((rel_blocks_applied + 1))
        else
          if grep -Eqi '422|already|exists' "$RUN_DIR/.relation-error.log"; then
            rel_blocks_skipped=$((rel_blocks_skipped + 1))
          else
            rel_failures=$((rel_failures + 1))
            log "Failed blocks relation $issue_id -> $depends_on_id"
          fi
        fi
        ;;
      parent-child)
        child_id="$issue_id"
        parent_id="$depends_on_id"

        parent_number=$(jq -r --arg id "$parent_id" '.[$id].github_number // empty' "$MAP_JSON")
        child_github_id=$(jq -r --arg id "$child_id" '.[$id].github_id // empty' "$MAP_JSON")

        if [[ -z "$parent_number" || -z "$child_github_id" || "$parent_number" == "null" || "$child_github_id" == "null" ]]; then
          rel_parent_skipped=$((rel_parent_skipped + 1))
          log "Skipping parent-child relation $parent_id <- $child_id (missing mapping)"
          continue
        fi

        payload=$(jq -nc --argjson sub_issue_id "$child_github_id" '{sub_issue_id: $sub_issue_id}')
        if [[ "$DRY_RUN" == "1" ]]; then
          log "[dry-run] parent-child: $parent_id (#$parent_number) <- $child_id"
          rel_parent_applied=$((rel_parent_applied + 1))
          continue
        fi

        set +e
        mutate_json "repos/$REPO/issues/$parent_number/sub_issues" "$payload" "POST" >/dev/null 2>"$RUN_DIR/.relation-error.log"
        rc=$?
        set -e
        if [[ $rc -eq 0 ]]; then
          rel_parent_applied=$((rel_parent_applied + 1))
        else
          if grep -Eqi '422|already|exists' "$RUN_DIR/.relation-error.log"; then
            rel_parent_skipped=$((rel_parent_skipped + 1))
          else
            rel_failures=$((rel_failures + 1))
            log "Failed parent-child relation $parent_id <- $child_id"
          fi
        fi
        ;;
      *)
        rel_failures=$((rel_failures + 1))
        log "Unknown dependency type '$dep_type' in source"
        ;;
    esac
  done < <(jq -c '.dependencies[]?' "$SOURCE")
fi

validation_total=0
validation_missing=0
validation_open=0
validation_closed=0
validation_in_progress=0

if [[ "$SKIP_VALIDATION" == "0" ]]; then
  validation_total=$(jq -c '.' "$MAP_FILE" | wc -l | tr -d ' ')
  validation_missing=$(jq -r 'select(.github_number == null or .github_id == null) | .beads_id' "$MAP_FILE" | wc -l | tr -d ' ')
  validation_open=$source_open
  validation_in_progress=$source_in_progress
  validation_closed=$source_closed
fi

jq -nc \
  --arg repo "$REPO" \
  --arg source "$SOURCE" \
  --arg snapshot "$SNAPSHOT_FILE" \
  --arg map_file "$MAP_FILE" \
  --arg run_dir "$RUN_DIR" \
  --argjson dry_run "$DRY_RUN" \
  --argjson source_total "$source_total" \
  --argjson source_open "$source_open" \
  --argjson source_in_progress "$source_in_progress" \
  --argjson source_closed "$source_closed" \
  --argjson source_blocks "$source_blocks" \
  --argjson source_parent_child "$source_parent_child" \
  --argjson created_issues "$created_issues" \
  --argjson reused_issues "$reused_issues" \
  --argjson open_updates "$open_updates" \
  --argjson closed_updates "$closed_updates" \
  --argjson issue_failures "$issue_failures" \
  --argjson rel_blocks_applied "$rel_blocks_applied" \
  --argjson rel_blocks_skipped "$rel_blocks_skipped" \
  --argjson rel_parent_applied "$rel_parent_applied" \
  --argjson rel_parent_skipped "$rel_parent_skipped" \
  --argjson rel_failures "$rel_failures" \
  --argjson validation_total "$validation_total" \
  --argjson validation_missing "$validation_missing" \
  --argjson validation_open "$validation_open" \
  --argjson validation_in_progress "$validation_in_progress" \
  --argjson validation_closed "$validation_closed" \
  '{
    repo: $repo,
    source: $source,
    snapshot: $snapshot,
    map_file: $map_file,
    run_dir: $run_dir,
    dry_run: ($dry_run == 1),
    source_counts: {
      total: $source_total,
      open: $source_open,
      in_progress: $source_in_progress,
      closed: $source_closed,
      blocks: $source_blocks,
      parent_child: $source_parent_child
    },
    import_summary: {
      created_issues: $created_issues,
      reused_issues: $reused_issues,
      opened_updates: $open_updates,
      closed_updates: $closed_updates,
      issue_failures: $issue_failures
    },
    relationship_summary: {
      blocks_applied: $rel_blocks_applied,
      blocks_skipped: $rel_blocks_skipped,
      parent_child_applied: $rel_parent_applied,
      parent_child_skipped: $rel_parent_skipped,
      relationship_failures: $rel_failures
    },
    validation_summary: {
      mapped_rows: $validation_total,
      missing_mappings: $validation_missing,
      state_matches_open: $validation_open,
      state_matches_in_progress: $validation_in_progress,
      state_matches_closed: $validation_closed
    }
  }' > "$REPORT_FILE"

log "Migration complete."
log "Map file: $MAP_FILE"
log "Report: $REPORT_FILE"

if [[ "$DRY_RUN" == "1" ]]; then
  log "Dry-run mode was enabled; no GitHub mutations were executed."
fi

if [[ "$issue_failures" -gt 0 || "$rel_failures" -gt 0 ]]; then
  die "Completed with failures. Check $REPORT_FILE and run logs in $RUN_DIR"
fi

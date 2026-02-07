#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME=$(basename "$0")

usage() {
  cat <<USAGE
Usage: $SCRIPT_NAME [options]

Validate parity between Beads source data and migrated GitHub Issues.

Options:
  --source <path>        Source Beads JSONL (default: .beads/issues.jsonl)
  --repo <owner/repo>    GitHub repository (default: current gh repo)
  --map <path>           Migration map JSONL (required)
  --report <path>        Validation report output (default: <map_dir>/validation-report.json)
  --no-relationships     Skip dependency/sub-issue parity checks
  -h, --help             Show help
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

load_repo_default() {
  gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>/dev/null || true
}

SOURCE=".beads/issues.jsonl"
REPO=""
MAP_FILE=""
REPORT_FILE=""
CHECK_RELATIONSHIPS=1

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
    --map)
      MAP_FILE=$2
      shift 2
      ;;
    --report)
      REPORT_FILE=$2
      shift 2
      ;;
    --no-relationships)
      CHECK_RELATIONSHIPS=0
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

[[ -f "$SOURCE" ]] || die "Source file not found: $SOURCE"
[[ -n "$MAP_FILE" ]] || die "--map is required"
[[ -f "$MAP_FILE" ]] || die "Map file not found: $MAP_FILE"

if [[ -z "$REPO" ]]; then
  REPO=$(load_repo_default)
fi
[[ -n "$REPO" ]] || die "Could not determine repository. Pass --repo owner/repo."

if [[ -z "$REPORT_FILE" ]]; then
  map_dir=$(cd "$(dirname "$MAP_FILE")" && pwd)
  REPORT_FILE="$map_dir/validation-report.json"
fi

jq -c '.' "$SOURCE" >/dev/null
jq -c '.' "$MAP_FILE" >/dev/null

source_total=$(jq -c '.' "$SOURCE" | wc -l | tr -d ' ')
expected_blocks=$(jq -r '.dependencies[]? | select(.type == "blocks") | .issue_id' "$SOURCE" | wc -l | tr -d ' ')
expected_parent_child=$(jq -r '.dependencies[]? | select(.type == "parent-child") | .issue_id' "$SOURCE" | wc -l | tr -d ' ')

map_total=$(jq -c '.' "$MAP_FILE" | wc -l | tr -d ' ')
map_missing=$(jq -r 'select(.github_number == null or .github_id == null) | .beads_id' "$MAP_FILE" | wc -l | tr -d ' ')

missing_in_map=0
while IFS= read -r beads_id; do
  if ! jq -e --arg id "$beads_id" 'select(.beads_id == $id)' "$MAP_FILE" >/dev/null; then
    missing_in_map=$((missing_in_map + 1))
  fi
done < <(jq -r '.id' "$SOURCE")

state_match_open=0
state_match_in_progress=0
state_match_closed=0
label_match_in_progress=0
state_mismatch=0
missing_issues=0
legacy_marker_mismatch=0

while IFS= read -r row; do
  beads_id=$(jq -r '.beads_id' <<<"$row")
  expected_status=$(jq -r '.status' <<<"$row")
  github_number=$(jq -r '.github_number // empty' <<<"$row")

  if [[ -z "$github_number" || "$github_number" == "null" ]]; then
    missing_issues=$((missing_issues + 1))
    continue
  fi

  set +e
  issue_json=$(gh_api_retry "repos/$REPO/issues/$github_number")
  rc=$?
  set -e
  if [[ $rc -ne 0 || -z "$issue_json" ]]; then
    missing_issues=$((missing_issues + 1))
    continue
  fi

  state=$(jq -r '.state' <<<"$issue_json")
  has_in_progress=$(jq -r '([.labels[].name] | index("status:in-progress") != null)' <<<"$issue_json")
  body_legacy=$(jq -r 'try (.body | capture("Legacy-Beads-ID: (?<id>[^\\n\\r]+)").id) catch ""' <<<"$issue_json")

  if [[ "$body_legacy" != "$beads_id" ]]; then
    legacy_marker_mismatch=$((legacy_marker_mismatch + 1))
  fi

  case "$expected_status" in
    open)
      if [[ "$state" == "open" ]]; then
        state_match_open=$((state_match_open + 1))
      else
        state_mismatch=$((state_mismatch + 1))
      fi
      ;;
    in_progress)
      if [[ "$state" == "open" ]]; then
        state_match_in_progress=$((state_match_in_progress + 1))
      else
        state_mismatch=$((state_mismatch + 1))
      fi
      if [[ "$has_in_progress" == "true" ]]; then
        label_match_in_progress=$((label_match_in_progress + 1))
      fi
      ;;
    closed)
      if [[ "$state" == "closed" ]]; then
        state_match_closed=$((state_match_closed + 1))
      else
        state_mismatch=$((state_mismatch + 1))
      fi
      ;;
    *)
      state_mismatch=$((state_mismatch + 1))
      ;;
  esac
done < <(jq -c '.' "$MAP_FILE")

validated_blocks=0
matched_blocks=0
validated_parent_child=0
matched_parent_child=0
relationship_validation_errors=0

if [[ "$CHECK_RELATIONSHIPS" == "1" ]]; then
  while IFS= read -r dep; do
    dep_type=$(jq -r '.type' <<<"$dep")
    issue_id=$(jq -r '.issue_id' <<<"$dep")
    depends_on_id=$(jq -r '.depends_on_id' <<<"$dep")

    dependent_number=$(jq -r --arg id "$issue_id" 'select(.beads_id == $id) | .github_number' "$MAP_FILE" | head -n 1)
    dependent_github_id=$(jq -r --arg id "$issue_id" 'select(.beads_id == $id) | .github_id' "$MAP_FILE" | head -n 1)
    depends_on_number=$(jq -r --arg id "$depends_on_id" 'select(.beads_id == $id) | .github_number' "$MAP_FILE" | head -n 1)
    depends_on_github_id=$(jq -r --arg id "$depends_on_id" 'select(.beads_id == $id) | .github_id' "$MAP_FILE" | head -n 1)

    if [[ -z "$dependent_number" || -z "$dependent_github_id" || -z "$depends_on_number" || -z "$depends_on_github_id" ]]; then
      relationship_validation_errors=$((relationship_validation_errors + 1))
      continue
    fi

    case "$dep_type" in
      blocks)
        validated_blocks=$((validated_blocks + 1))
        set +e
        dep_json=$(gh_api_retry "repos/$REPO/issues/$dependent_number/dependencies/blocked_by")
        rc=$?
        set -e

        if [[ $rc -ne 0 || -z "$dep_json" ]]; then
          set +e
          dep_json=$(gh_api_retry "repos/$REPO/issues/$dependent_number/dependencies")
          rc=$?
          set -e
        fi

        if [[ $rc -ne 0 || -z "$dep_json" ]]; then
          relationship_validation_errors=$((relationship_validation_errors + 1))
          continue
        fi

        found=$(jq -r --argjson id "$depends_on_github_id" '
          if type == "array" then
            map(.id) | index($id) != null
          else
            ((.blocked_by // []) | map(.id) | index($id) != null)
          end
        ' <<<"$dep_json")

        if [[ "$found" == "true" ]]; then
          matched_blocks=$((matched_blocks + 1))
        fi
        ;;
      parent-child)
        validated_parent_child=$((validated_parent_child + 1))
        set +e
        sub_json=$(gh_api_retry "repos/$REPO/issues/$depends_on_number/sub_issues")
        rc=$?
        set -e

        if [[ $rc -ne 0 || -z "$sub_json" ]]; then
          relationship_validation_errors=$((relationship_validation_errors + 1))
          continue
        fi

        found=$(jq -r --argjson child_id "$dependent_github_id" '
          if type == "array" then
            map(.id) | index($child_id) != null
          else
            ((.sub_issues // []) | map(.id) | index($child_id) != null)
          end
        ' <<<"$sub_json")

        if [[ "$found" == "true" ]]; then
          matched_parent_child=$((matched_parent_child + 1))
        fi
        ;;
      *)
        relationship_validation_errors=$((relationship_validation_errors + 1))
        ;;
    esac
  done < <(jq -c '.dependencies[]?' "$SOURCE")
fi

jq -nc \
  --arg repo "$REPO" \
  --arg source "$SOURCE" \
  --arg map_file "$MAP_FILE" \
  --argjson source_total "$source_total" \
  --argjson map_total "$map_total" \
  --argjson map_missing "$map_missing" \
  --argjson missing_in_map "$missing_in_map" \
  --argjson state_match_open "$state_match_open" \
  --argjson state_match_in_progress "$state_match_in_progress" \
  --argjson state_match_closed "$state_match_closed" \
  --argjson label_match_in_progress "$label_match_in_progress" \
  --argjson state_mismatch "$state_mismatch" \
  --argjson missing_issues "$missing_issues" \
  --argjson legacy_marker_mismatch "$legacy_marker_mismatch" \
  --argjson expected_blocks "$expected_blocks" \
  --argjson expected_parent_child "$expected_parent_child" \
  --argjson validated_blocks "$validated_blocks" \
  --argjson matched_blocks "$matched_blocks" \
  --argjson validated_parent_child "$validated_parent_child" \
  --argjson matched_parent_child "$matched_parent_child" \
  --argjson relationship_validation_errors "$relationship_validation_errors" \
  '{
    repo: $repo,
    source: $source,
    map_file: $map_file,
    issue_parity: {
      source_total: $source_total,
      map_total: $map_total,
      map_missing_fields: $map_missing,
      missing_ids_in_map: $missing_in_map,
      missing_issues_on_github: $missing_issues,
      legacy_marker_mismatch: $legacy_marker_mismatch,
      state_matches: {
        open: $state_match_open,
        in_progress: $state_match_in_progress,
        closed: $state_match_closed,
        in_progress_label_matches: $label_match_in_progress
      },
      state_mismatches: $state_mismatch
    },
    relationship_parity: {
      checked: ($validated_blocks + $validated_parent_child),
      expected: ($expected_blocks + $expected_parent_child),
      blocks: {
        expected: $expected_blocks,
        checked: $validated_blocks,
        matched: $matched_blocks
      },
      parent_child: {
        expected: $expected_parent_child,
        checked: $validated_parent_child,
        matched: $matched_parent_child
      },
      errors: $relationship_validation_errors
    }
  }' > "$REPORT_FILE"

log "Validation report written to: $REPORT_FILE"

hard_fail=0
if [[ "$missing_in_map" -gt 0 || "$missing_issues" -gt 0 || "$state_mismatch" -gt 0 ]]; then
  hard_fail=1
fi

if [[ "$CHECK_RELATIONSHIPS" == "1" ]]; then
  if [[ "$matched_blocks" -lt "$expected_blocks" || "$matched_parent_child" -lt "$expected_parent_child" || "$relationship_validation_errors" -gt 0 ]]; then
    hard_fail=1
  fi
fi

if [[ "$hard_fail" -eq 1 ]]; then
  die "Parity validation failed. See $REPORT_FILE"
fi

log "Parity validation passed."

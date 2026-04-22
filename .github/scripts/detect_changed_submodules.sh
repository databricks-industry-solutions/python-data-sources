#!/usr/bin/env bash
# detect_changed_submodules.sh
#
# Extracts unique submodule names from a JSON array of changed directory paths.
# Expects $CHANGED_DIRECTORIES to contain the JSON array from the detect step.
# Writes 'submodules' and 'has-changes' to $GITHUB_OUTPUT.
#
# Only names in the hardcoded ALLOWED_SUBMODULES list below are emitted. This
# safeguards the test matrix against changes to untracked directories or files
# (e.g. a new src/python_data_sources/<newdir>, a top-level README, or anything
# outside src/python_data_sources/** and tests/unit/**): unknown names are
# logged to stderr and dropped rather than producing a matrix entry that would
# fail with `no such hatch env`.
#
# Keep ALLOWED_SUBMODULES in sync with the `[tool.hatch.envs.test-<name>]`
# sections in pyproject.toml.
set -euo pipefail

ALLOWED_SUBMODULES=(common mcap mqtt zipdcm)

CHANGED="${CHANGED_DIRECTORIES:-}"
if [ -z "${CHANGED//[[:space:]]/}" ]; then
  CHANGED="[]"
fi
echo "Changed directories: $CHANGED"

ALLOWED=$(printf '%s\n' "${ALLOWED_SUBMODULES[@]}" | jq -R . | jq -cs .)

# Extract candidates. `try ... catch null` keeps the pipeline from aborting if
# an entry doesn't match the capture pattern, and the `type == "array"` guard
# coerces malformed roots (non-array JSON, missing output) to an empty array.
CANDIDATES=$(echo "$CHANGED" | jq -c '
  (if type == "array" then . else [] end)
  | map(select(type == "string"))
  | [ .[] |
      (try (capture("^src/python_data_sources/(?<name>[^/]+)") | .name) catch null),
      (try (capture("^tests/unit/(?<name>[^/]+)") | .name) catch null)
    ]
  | map(select(. != null and . != ""))
  | unique
')

# Partition candidates into known (emitted) and unknown (warned and dropped).
SUBMODULES=$(jq -cn --argjson c "$CANDIDATES" --argjson a "$ALLOWED" \
  '$c | map(select(. as $x | $a | index($x)))')
REJECTED=$(jq -cn --argjson c "$CANDIDATES" --argjson a "$ALLOWED" \
  '$c | map(select(. as $x | ($a | index($x)) | not))')

if [ "$REJECTED" != "[]" ]; then
  echo "warn: ignoring directories without a matching test-<name> hatch env: $REJECTED" >&2
  echo "      (add an env to pyproject.toml if these should be tested)" >&2
fi

# Changes to src/python_data_sources/common are shared by every submodule,
# so expand to the full allowlist and run the whole matrix. Changes under
# tests/unit/common are scoped and don't trigger expansion.
COMMON_SRC_CHANGED=$(echo "$CHANGED" | jq -r '
  (if type == "array" then . else [] end)
  | map(select(type == "string"))
  | any(. == "src/python_data_sources/common" or startswith("src/python_data_sources/common/"))
')
if [ "$COMMON_SRC_CHANGED" = "true" ]; then
  echo "info: src/python_data_sources/common changed — expanding to all submodules" >&2
  SUBMODULES="$ALLOWED"
fi

if [ -z "$SUBMODULES" ] || [ "$SUBMODULES" = "null" ] || [ "$SUBMODULES" = "[]" ]; then
  SUBMODULES="[]"
fi

COMPACT_SUBMODULES=$(echo "$SUBMODULES" | jq -c '.')
echo "submodules=$COMPACT_SUBMODULES" >> "$GITHUB_OUTPUT"

if [ "$COMPACT_SUBMODULES" = "[]" ]; then
  echo "has-changes=false" >> "$GITHUB_OUTPUT"
else
  echo "has-changes=true" >> "$GITHUB_OUTPUT"
fi

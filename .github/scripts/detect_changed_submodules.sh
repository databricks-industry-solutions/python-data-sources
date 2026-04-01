#!/usr/bin/env bash
# detect_changed_submodules.sh
#
# Extracts unique submodule names from a JSON array of changed directory paths.
# Expects $CHANGED_DIRECTORIES to contain the JSON array from the detect step.
# Writes 'submodules' and 'has-changes' to $GITHUB_OUTPUT.
set -euo pipefail

CHANGED="${CHANGED_DIRECTORIES}"
echo "Changed directories: $CHANGED"

SUBMODULES=$(echo "$CHANGED" | jq -r '
  [.[] |
    (capture("src/python_data_sources/(?<name>[^/]+)") | .name),
    (capture("tests/unit/(?<name>[^/]+)") | .name)
  ] |
  map(select(. != null)) |
  unique |
  map(select(. != "conftest.py" and . != "__pycache__"))
')

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

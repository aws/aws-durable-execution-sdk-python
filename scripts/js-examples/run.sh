#!/usr/bin/env bash
# Run the JS SDK example tests against this checkout's local runner.
#
#   scripts/js-examples/run.sh                 # every example
#   scripts/js-examples/run.sh invoke step/    # tests whose path matches
#   scripts/js-examples/run.sh --help          # all options
#
# This script only prepares Python. It creates a virtual environment in
# scripts/js-examples/.venv, installs the SDK and the testing package from
# this checkout in editable mode, and hands over to run.py. run.py fetches and
# builds the JS SDK, starts the servers, and runs jest. See README.md.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
VENV="$HERE/.venv"

# Pick a Python that the testing package supports (3.11 or newer).
find_python() {
  local candidate
  for candidate in "${PYTHON:-}" python3.14 python3.13 python3.12 python3.11 python3; do
    [ -n "$candidate" ] || continue
    command -v "$candidate" >/dev/null 2>&1 || continue
    if "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

for tool in git node npm; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "[js-examples] error: $tool is not on PATH. See $HERE/README.md." >&2
    exit 2
  fi
done

# The install is repeated only when a pyproject.toml it depends on changes.
# Source edits need no reinstall because the install is editable.
stamp_input() {
  cat "$REPO_ROOT/packages/aws-durable-execution-sdk-python/pyproject.toml" \
      "$REPO_ROOT/packages/aws-durable-execution-sdk-python-testing/pyproject.toml" \
      "$HERE/run.sh"
}
STAMP="$(stamp_input | cksum | cut -d' ' -f1)"

if [ ! -x "$VENV/bin/python" ] || [ "$(cat "$VENV/.stamp" 2>/dev/null)" != "$STAMP" ]; then
  PY="$(find_python)" || {
    echo "[js-examples] error: Python 3.11 or newer is required. Set PYTHON=/path/to/python3." >&2
    exit 2
  }
  echo "[js-examples] creating $VENV with $("$PY" --version)"
  rm -rf "$VENV"
  "$PY" -m venv "$VENV"
  "$VENV/bin/python" -m pip install --quiet --upgrade pip
  # Install the core SDK from this checkout too. Otherwise pip would satisfy
  # the testing package's SDK dependency from PyPI.
  "$VENV/bin/python" -m pip install --quiet pyyaml pytest \
    -e "$REPO_ROOT/packages/aws-durable-execution-sdk-python" \
    -e "$REPO_ROOT/packages/aws-durable-execution-sdk-python-testing"
  echo "$STAMP" > "$VENV/.stamp"
fi

exec "$VENV/bin/python" "$HERE/run.py" "$@"

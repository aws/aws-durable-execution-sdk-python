#!/bin/sh

set -e

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# --- Core SDK checks ---
echo "=========================================="
echo "Running checks for aws-durable-execution-sdk-python"
echo "=========================================="

hatch run dev-core:cov
echo "SUCCESS: tests + coverage (core)"

hatch run dev-core:typecheck
echo "SUCCESS: typings (core)"

# --- OTel SDK checks ---
echo "=========================================="
echo "Running checks for aws-durable-execution-sdk-python-otel"
echo "=========================================="

hatch run dev-otel:cov
echo "SUCCESS: tests + coverage (otel)"

hatch run dev-otel:typecheck
echo "SUCCESS: typings (otel)"

# --- Examples checks ---
echo "=========================================="
echo "Running checks for examples"
echo "=========================================="

hatch run dev-examples:test
echo "SUCCESS: tests (examples)"

# --- Formatting / linting (per package) ---
PACKAGES=(
  "packages/aws-durable-execution-sdk-python"
  "packages/aws-durable-execution-sdk-python-otel"
  "packages/examples"
)

for package_dir in "${PACKAGES[@]}"; do
  full_path="$REPO_ROOT/$package_dir"
  if [ -d "$full_path" ]; then
    echo "=========================================="
    echo "Running formatting/linting for $package_dir"
    echo "=========================================="
    cd "$full_path"
    hatch fmt
    echo "SUCCESS: linting/fmt ($package_dir)"
  else
    echo "WARNING: $package_dir does not exist, skipping fmt"
  fi
done

cd "$REPO_ROOT"

# --- Commit message validation ---
hatch run python .github/scripts/lintcommit.py

#!/bin/sh

set -e

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

PACKAGES=(
  "packages/aws-durable-execution-sdk-python"
  "packages/aws-durable-execution-sdk-python-otel"
)

for package_dir in "${PACKAGES[@]}"; do
  full_path="$REPO_ROOT/$package_dir"
  if [ -d "$full_path" ]; then
    echo "=========================================="
    echo "Running checks for $package_dir"
    echo "=========================================="
    cd "$full_path"

    hatch run test:cov
    echo "SUCCESS: tests + coverage ($package_dir)"

    # type checks
    hatch run types:check
    echo "SUCCESS: typings ($package_dir)"

    # static analysis
    hatch fmt
    echo "SUCCESS: linting/fmt ($package_dir)"
  else
    echo "WARNING: $package_dir does not exist, skipping"
  fi
done

# commit message validation (run once from repo root)
cd "$REPO_ROOT"
hatch run python .github/scripts/lintcommit.py

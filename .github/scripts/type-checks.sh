#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

mypy --install-types --non-interactive \
  packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python \
  packages/aws-durable-execution-sdk-python/tests

mypy --install-types --non-interactive \
  packages/aws-durable-execution-sdk-python-otel/src/aws_durable_execution_sdk_python_otel \
  packages/aws-durable-execution-sdk-python-otel/tests

mypy --install-types --non-interactive \
  packages/aws-durable-execution-sdk-python-insight/src/aws_durable_execution_sdk_python_insight \
  packages/aws-durable-execution-sdk-python-insight/tests

mypy --install-types --non-interactive \
  packages/aws-durable-execution-sdk-python-microvm-worker/src/aws_durable_execution_sdk_python_microvm_worker \
  packages/aws-durable-execution-sdk-python-microvm-worker/tests

# comment out this for now as there are many type check errors in this package
#mypy --install-types --non-interactive \
#  packages/aws-durable-execution-sdk-python-testing/src/aws_durable_execution_sdk_python_testing \
#  packages/aws-durable-execution-sdk-python-testing/tests

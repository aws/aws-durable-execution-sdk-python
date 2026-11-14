#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Python commands for the shared LMI reusable workflow's stage contract.
set -euo pipefail

case "${1:?Expected a workflow stage}" in
  setup)
    uv pip install hatch==1.16.5
    hatch run lmi:shared-setup
    ;;
  harness)
    hatch run lmi:lint
    hatch run lmi:shared-tests --junitxml=lmi_tests/artifacts/unit.xml
    hatch run examples:python - <<'PYTHON'
from pathlib import Path
import aws_durable_execution_sdk_python as sdk

source = Path('packages/aws-durable-execution-sdk-python/src').resolve()
assert Path(sdk.__file__).resolve().is_relative_to(source), sdk.__file__
PYTHON
    ;;
  build) hatch run lmi:build ;;
  deploy)
    hatch run lmi:python -m lmi_tests.deploy deploy \
      --run-id "$LMI_SHARED_RUN_ID" \
      --runtime "$LMI_RUNTIME" --stack-name "$LMI_STACK_NAME"
    ;;
  run) hatch run lmi:shared ;;
  collect) hatch run lmi:python -m lmi_tests.deploy collect ;;
  cleanup) hatch run lmi:shared-cleanup ;;
  summary) hatch run lmi:python -m lmi_tests.summary ;;
  *) printf 'Unknown LMI workflow stage: %s\n' "$1" >&2; exit 2 ;;
esac

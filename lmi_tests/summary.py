# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Render the shared report for the Python fixture's artifact directory."""

from inspect import signature
import json
import os
from pathlib import Path
from aws_durable_execution_conformance_tests.lmi.summary import summarize


def render(directory: Path, expected_run: str | None) -> str:
    if "expected_run" in signature(summarize).parameters:
        return summarize(directory, expected_run=expected_run)
    if expected_run:
        manifest = directory / "manifest.json"
        if (
            manifest.is_file()
            and json.loads(manifest.read_text()).get("run") != expected_run
        ):
            return "Deployment identity does not match this run. Current cloud acceptance is unavailable."
    return summarize(directory)


if __name__ == "__main__":
    print(
        render(Path(__file__).parent / "artifacts", os.environ.get("LMI_SHARED_RUN_ID"))
    )

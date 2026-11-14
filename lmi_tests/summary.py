# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Render the shared report for the Python fixture's artifact directory."""

from pathlib import Path
from aws_durable_execution_conformance_tests.lmi.summary import summarize

if __name__ == "__main__":
    print(summarize(Path(__file__).parent / "artifacts"))

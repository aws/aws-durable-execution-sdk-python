# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Pinned runner and verified deployment identities for the SDK adapter."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

CONFORMANCE_REPOSITORY = "aws/aws-durable-execution-conformance-tests"
# Updated only with the matching reviewed shared evidence contract.
CONFORMANCE_REVISION = "6f899679c7eb6ede836c737acb9816e6fd7072fd"


def configuration_identity(deployment: Mapping[str, Any], key: str) -> str:
    fields = (
        "runtime",
        "role",
        "bucket",
        "commit",
        "run",
        "provider",
        "codeSha256",
        "invocationTimeout",
        "executionTimeout",
    )
    configuration = {name: deployment[name] for name in fields}
    configuration.update(
        function=deployment["functions"][key],
        nativeConcurrency=deployment["concurrencies"][key],
        architecture="arm64",
        memorySize=2048,
        handler="lmi_tests.fixture.handler",
        scaling={"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1},
    )
    return (
        "sha256:"
        + hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()
    )

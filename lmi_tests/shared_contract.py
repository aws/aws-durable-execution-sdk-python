# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Pinned runner and verified deployment identities for the SDK adapter."""

from __future__ import annotations

from typing import Any, Mapping

CONFORMANCE_REPOSITORY = "aws/aws-durable-execution-conformance-tests"
# Updated only with the matching reviewed shared evidence contract.
CONFORMANCE_REVISION = "56e7e444457653775579c120469d931b71c8cfd8"


def configuration_identity(deployment: Mapping[str, Any], key: str) -> str:
    from aws_durable_execution_conformance_tests.lmi.configuration import (
        configuration_identity as identity,
    )
    from lmi_tests.runtime import RUNTIME

    return identity(deployment, key, RUNTIME)

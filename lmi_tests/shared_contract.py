# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Pinned runner and verified deployment identities for the SDK adapter."""

from __future__ import annotations

from typing import Any, Mapping

CONFORMANCE_REPOSITORY = "aws/aws-durable-execution-conformance-tests"
# Updated only with the matching reviewed shared evidence contract.
CONFORMANCE_REVISION = "be07fc5a8f4469e4419a7d33cc4d42bf6bf3966e"


def configuration_identity(deployment: Mapping[str, Any], key: str) -> str:
    from aws_durable_execution_conformance_tests.lmi.configuration import (
        configuration_identity as identity,
    )
    from lmi_tests.runtime import RUNTIME

    return identity(deployment, key, RUNTIME)

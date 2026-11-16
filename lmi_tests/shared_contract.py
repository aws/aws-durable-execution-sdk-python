# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Pinned runner and verified deployment identities for the SDK adapter."""

from __future__ import annotations

from typing import Any, Mapping

CONFORMANCE_REPOSITORY = "aws/aws-durable-execution-conformance-tests"
# Updated only with the matching reviewed shared evidence contract.
CONFORMANCE_REVISION = "76732458015d4437e06de061636a61ca2c9558d2"


def configuration_identity(deployment: Mapping[str, Any], key: str) -> str:
    from aws_durable_execution_conformance_tests.lmi.configuration import (
        configuration_identity as identity,
    )
    from lmi_tests.runtime import RUNTIME

    return identity(deployment, key, RUNTIME)

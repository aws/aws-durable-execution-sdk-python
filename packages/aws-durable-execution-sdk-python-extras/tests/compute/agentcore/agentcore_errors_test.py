# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for rebuilding AgentCore errors from a child context failure."""

from __future__ import annotations

import pytest
from aws_durable_execution_sdk_python.exceptions import ChildContextError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_extras.compute.agentcore import (
    AgentcoreDeliveryError,
    AgentcoreError,
    AgentcoreJobFailedError,
    AgentcoreTimeoutError,
    ComputeError,
)


def child_context_error(raised: Exception) -> ChildContextError:
    """The error that the SDK raises for a child context that raised
    ``raised``, built from the checkpoint fields as the SDK builds it."""
    try:
        ErrorObject.from_exception(raised).raise_as_operation_error(ChildContextError)
    except ChildContextError as error:
        return error
    msg = "unreachable"
    raise AssertionError(msg)


@pytest.mark.parametrize(
    "error_class",
    [
        AgentcoreError,
        AgentcoreDeliveryError,
        AgentcoreJobFailedError,
        AgentcoreTimeoutError,
    ],
)
def test_rebuilds_each_agentcore_error_class(
    error_class: type[AgentcoreError],
) -> None:
    raised = error_class(
        message='AgentCore "review": broken', data='{"k":1}', stack_trace=["at x"]
    )

    rebuilt = AgentcoreError.from_child_context_error(
        "review", child_context_error(raised)
    )

    assert type(rebuilt) is error_class
    assert isinstance(rebuilt, ComputeError)
    assert rebuilt.message == 'AgentCore "review": broken'
    assert rebuilt.data == '{"k":1}'
    assert rebuilt is not child_context_error(raised).__cause__


def test_wraps_another_error_type_in_the_base_class() -> None:
    rebuilt = AgentcoreError.from_child_context_error(
        "review", child_context_error(RuntimeError("boom"))
    )

    assert type(rebuilt) is AgentcoreError
    assert rebuilt.message == 'AgentCore "review": boom'
    assert rebuilt.error_type == "RuntimeError"

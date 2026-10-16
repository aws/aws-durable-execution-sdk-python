# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for rebuilding MicroVM errors from a child context failure."""

from __future__ import annotations

import pytest
from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    DurableOperationError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmDeliveryError,
    MicrovmError,
    MicrovmJobFailedError,
    MicrovmLaunchError,
    MicrovmNotRunningError,
    MicrovmTimeoutError,
)


def child_context_error(raised: Exception) -> ChildContextError:
    """The error that the SDK raises for a child context that raised
    ``raised``, built the way the SDK builds it, from the checkpoint fields."""
    try:
        ErrorObject.from_exception(raised).raise_as_operation_error(ChildContextError)
    except ChildContextError as error:
        return error
    msg = "unreachable"
    raise AssertionError(msg)


@pytest.mark.parametrize(
    "error_class",
    [
        MicrovmError,
        MicrovmLaunchError,
        MicrovmDeliveryError,
        MicrovmJobFailedError,
        MicrovmNotRunningError,
        MicrovmTimeoutError,
    ],
)
def test_rebuilds_each_microvm_error_class(error_class: type[MicrovmError]) -> None:
    raised = error_class(
        message='MicroVM "build": broken', data='{"k":1}', stack_trace=["at x"]
    )

    rebuilt = MicrovmError.from_child_context_error(
        "build", child_context_error(raised)
    )

    assert type(rebuilt) is error_class
    assert rebuilt.message == 'MicroVM "build": broken'
    assert rebuilt.data == '{"k":1}'
    assert rebuilt.stack_trace == ["at x"]
    assert rebuilt.error_type == raised.error_type


def test_wraps_another_error_type_in_a_plain_microvm_error() -> None:
    rebuilt = MicrovmError.from_child_context_error(
        "build", child_context_error(RuntimeError("unexpected"))
    )

    assert type(rebuilt) is MicrovmError
    assert rebuilt.message == 'MicroVM "build": unexpected'
    assert rebuilt.error_type == "RuntimeError"


def test_every_microvm_error_is_a_durable_operation_error() -> None:
    assert issubclass(MicrovmError, DurableOperationError)
    for error_class in (
        MicrovmLaunchError,
        MicrovmDeliveryError,
        MicrovmJobFailedError,
        MicrovmNotRunningError,
        MicrovmTimeoutError,
    ):
        assert issubclass(error_class, MicrovmError)


@pytest.mark.parametrize(
    "error_class",
    [
        MicrovmError,
        MicrovmLaunchError,
        MicrovmDeliveryError,
        MicrovmJobFailedError,
        MicrovmNotRunningError,
        MicrovmTimeoutError,
    ],
)
def test_registers_each_class_with_the_sdk(error_class: type[MicrovmError]) -> None:
    rebuilt = DurableOperationError.from_error_fields(
        error_type=f"{error_class.__module__}.{error_class.__qualname__}",
        message="m",
        data="d",
        stack_trace=["s"],
    )

    assert type(rebuilt) is error_class
    assert (rebuilt.message, rebuilt.data, rebuilt.stack_trace) == ("m", "d", ["s"])

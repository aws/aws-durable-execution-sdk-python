# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the ai errors and the default retry strategy."""

from __future__ import annotations

from typing import Any

import pytest
from aws_durable_execution_sdk_python.exceptions import DurableOperationError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from aws_durable_execution_sdk_python_extras.ai import (
    AiError,
    AiOperationSubType,
    AiTimeoutError,
    DeciderError,
    DeciderInvalidResponseError,
    LlmMaxTokensError,
    LlmStoppedError,
    default_ai_retry_strategy,
)


@pytest.mark.parametrize(
    "error",
    [
        AiError(message="base"),
        DeciderError(message="HTTP 422", status=422),
        DeciderInvalidResponseError(message="bad shape"),
        AiTimeoutError(message="late"),
        LlmStoppedError(stop_reason="guardrail_intervened"),
        LlmMaxTokensError(message="limit"),
    ],
)
def test_the_registry_rebuilds_each_error_with_its_fields(error: AiError) -> None:
    fields = ErrorObject.from_exception(error)

    rebuilt = DurableOperationError.from_error_fields(
        fields.type, fields.message, fields.data, fields.stack_trace
    )

    assert type(rebuilt) is type(error)
    assert rebuilt.message == error.message
    assert getattr(rebuilt, "status", None) == getattr(error, "status", None)
    assert getattr(rebuilt, "stop_reason", None) == getattr(error, "stop_reason", None)


def test_status_and_stop_reason_are_none_without_data() -> None:
    assert DeciderError(message="x").status is None
    assert DeciderError(message="x", data="not json").status is None
    assert DeciderError(message="x", data="[1]").status is None
    assert LlmStoppedError(message="x").stop_reason is None


def test_llm_stopped_error_names_the_reason() -> None:
    error = LlmStoppedError(stop_reason="content_filtered")

    assert '"content_filtered"' in str(error)


def test_the_subtypes_match_the_javascript_sdk() -> None:
    assert {member.value for member in AiOperationSubType} == {
        "DeciderCheck",
        "DeciderClassify",
        "DeciderLabel",
        "DeciderScore",
        "LlmAsk",
    }


def client_error(code: str, status: int) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": "m"},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, "Call")


@pytest.mark.parametrize(
    "error",
    [
        AiTimeoutError(message="late"),
        TimeoutError(),
        ConnectionResetError(),
        EndpointConnectionError(endpoint_url="https://example"),
        ReadTimeoutError(endpoint_url="https://example"),
        DeciderError(message="unreachable"),
        DeciderError(message="x", status=408),
        DeciderError(message="x", status=429),
        DeciderError(message="x", status=502),
        LlmStoppedError(stop_reason="malformed_model_output"),
        client_error("ThrottlingException", 400),
        client_error("ModelTimeoutException", 408),
        client_error("TooManyRequestsException", 429),
        client_error("ServiceUnavailable", 503),
        client_error("SomethingNew", 500),
        client_error("SomethingNew", 429),
    ],
)
def test_retries_a_transient_failure_with_a_growing_delay(error: Exception) -> None:
    first = default_ai_retry_strategy(error, 1)
    fourth = default_ai_retry_strategy(error, 4)

    assert first.should_retry
    assert first.delay_seconds == 2
    assert fourth.delay_seconds == 16
    assert not default_ai_retry_strategy(error, 5).should_retry


@pytest.mark.parametrize(
    "error",
    [
        DeciderError(message="x", status=422),
        DeciderError(message="x", status=401),
        DeciderError(message="x", status=307),
        DeciderInvalidResponseError(message="x"),
        LlmMaxTokensError(message="x"),
        LlmStoppedError(stop_reason="guardrail_intervened"),
        client_error("AccessDeniedException", 403),
        client_error("ValidationException", 400),
        ValueError("x"),
    ],
)
def test_does_not_retry_a_failure_that_repeats(error: Exception) -> None:
    assert not default_ai_retry_strategy(error, 1).should_retry

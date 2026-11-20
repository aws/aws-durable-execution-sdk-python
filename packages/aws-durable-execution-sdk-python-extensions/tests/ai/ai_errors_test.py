# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the ai errors."""

from __future__ import annotations

import json

import pytest
from aws_durable_execution_sdk_python.exceptions import DurableOperationError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_extensions.ai import (
    AiError,
    AiOperationSubType,
    DeciderError,
    DeciderInvalidResponseError,
    LlmMaxTokensError,
    LlmStoppedError,
)


@pytest.mark.parametrize(
    "error",
    [
        AiError(message="base"),
        DeciderError(message="HTTP 422", status=422),
        DeciderInvalidResponseError(message="bad shape"),
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


@pytest.mark.parametrize("data", [None, "{}", '{"other": 1}', "not json", "[1]"])
def test_status_is_kept_when_data_is_also_given(data: str | None) -> None:
    error = DeciderError(message="x", data=data, status=422)

    assert error.status == 422


def test_status_keeps_the_other_data_fields() -> None:
    error = DeciderError(message="x", data='{"other": 1}', status=422)

    assert error.data is not None
    assert json.loads(error.data) == {"other": 1, "status": 422}


@pytest.mark.parametrize("data", [None, "{}", "not json"])
def test_stop_reason_is_kept_when_data_is_also_given(data: str | None) -> None:
    error = LlmStoppedError(data=data, stop_reason="guardrail_intervened")

    assert error.stop_reason == "guardrail_intervened"

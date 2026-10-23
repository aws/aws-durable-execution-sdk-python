# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the request ID helpers of the MicroVM operations."""

from __future__ import annotations

import logging
from typing import Any

import boto3
import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.microvm.aws_errors import (
    add_request_id,
    decide_on_original_error,
    request_id_sentence,
    with_request_id,
)
from aws_durable_execution_sdk_python_extras.microvm.lifecycle import (
    MicrovmStateError,
    microvm_not_found,
)
from aws_durable_execution_sdk_python_extras.microvm.request import (
    SendJobOptions,
    send_job,
)


def response(request_id: Any = "req-1", code: str = "ThrottlingException") -> Any:
    metadata: dict[str, Any] = {"HTTPStatusCode": 400}
    if request_id is not None:
        metadata["RequestId"] = request_id
    return {
        "Error": {"Code": code, "Message": "slow down"},
        "ResponseMetadata": metadata,
    }


@pytest.mark.parametrize(
    ("source", "sentence"),
    [
        ({"ResponseMetadata": {"RequestId": "req-1"}}, "RunMicrovm request ID: req-1."),
        ({"ResponseMetadata": {"RequestId": ""}}, ""),
        ({"ResponseMetadata": {"RequestId": 7}}, ""),
        ({"ResponseMetadata": {}}, ""),
        ({"ResponseMetadata": "req-1"}, ""),
        ({}, ""),
        (None, ""),
        ("req-1", ""),
    ],
)
def test_names_only_a_request_id_that_is_a_non_empty_string(
    source: Any, sentence: str
) -> None:
    assert request_id_sentence("RunMicrovm", source) == sentence


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("bad image", "bad image. RunMicrovm request ID: req-1."),
        ("bad image.", "bad image. RunMicrovm request ID: req-1."),
        ("bad image!", "bad image! RunMicrovm request ID: req-1."),
        ("bad image?", "bad image? RunMicrovm request ID: req-1."),
        ("", "RunMicrovm request ID: req-1."),
    ],
)
def test_ends_the_message_with_the_request_id_sentence(
    message: str, expected: str
) -> None:
    source = {"ResponseMetadata": {"RequestId": "req-1"}}
    assert with_request_id(message, "RunMicrovm", source) == expected


def test_keeps_the_message_without_a_request_id() -> None:
    assert with_request_id("bad image", "RunMicrovm", {}) == "bad image"


def test_copies_a_modeled_error_with_the_request_id_in_its_message() -> None:
    exceptions = boto3.client("lambda-microvms", region_name="us-west-2").exceptions
    error = exceptions.ThrottlingException(response(), "RunMicrovm")
    error.add_note("a note")
    original = str(error)

    labelled = add_request_id("RunMicrovm", error)

    assert labelled is not error
    assert type(labelled) is type(error)
    assert labelled.response is error.response
    assert labelled.operation_name == "RunMicrovm"
    assert labelled.__notes__ == ["a note"]
    assert labelled.__cause__ is error
    assert labelled.__suppress_context__
    assert str(labelled) == f"{original}. RunMicrovm request ID: req-1."
    assert labelled.args == (str(labelled),)
    # The error that botocore raised keeps its message.
    assert str(error) == original


@pytest.mark.parametrize(
    ("error", "operation"),
    [
        # No request ID.
        (ClientError(response(None), "RunMicrovm"), "RunMicrovm"),
        (ClientError(response(""), "RunMicrovm"), "RunMicrovm"),
        # The answer of another operation, such as STS AssumeRole in a
        # credential provider.
        (ClientError(response(), "AssumeRole"), "RunMicrovm"),
        (ClientError(response(), "CreateMicrovmAuthToken"), "TerminateMicrovm"),
    ],
)
def test_returns_the_error_itself_when_it_gets_no_request_id(
    error: ClientError, operation: Any
) -> None:
    original = str(error)

    assert add_request_id(operation, error) is error
    assert str(error) == original
    assert error.__cause__ is None


def test_returns_the_error_itself_when_its_class_builds_str_otherwise() -> None:
    class FixedMessageError(ClientError):
        def __str__(self) -> str:
            return "fixed"

    error = FixedMessageError(response(), "RunMicrovm")

    assert add_request_id("RunMicrovm", error) is error


def test_copies_a_subclass_whose_init_takes_other_arguments() -> None:
    class WrappedError(ClientError):
        def __init__(self, detail: str) -> None:
            super().__init__(response(), "GetMicrovm")
            self.detail = detail

    error = WrappedError("extra")

    labelled = add_request_id("GetMicrovm", error)

    assert type(labelled) is WrappedError
    assert labelled.detail == "extra"  # type: ignore[attr-defined]
    assert str(labelled).endswith(". GetMicrovm request ID: req-1.")


def test_builds_each_copy_from_the_message_of_the_error_that_was_raised() -> None:
    error = ClientError(response("req-1"), "RunMicrovm")
    first = add_request_id("RunMicrovm", error)
    error.response["ResponseMetadata"]["RequestId"] = "req-2"

    second = add_request_id("RunMicrovm", error)

    assert str(first).endswith(" RunMicrovm request ID: req-1.")
    assert str(second).endswith(". RunMicrovm request ID: req-2.")
    assert "req-1" not in str(second)


def test_gives_the_strategy_the_error_that_botocore_raised() -> None:
    error = ClientError(response(), "RunMicrovm")
    labelled = add_request_id("RunMicrovm", error)
    seen: list[tuple[Exception, int]] = []
    decision = RetryDecision.retry(Duration.from_seconds(3))

    def strategy(received: Exception, attempt: int) -> RetryDecision:
        seen.append((received, attempt))
        return decision

    assert decide_on_original_error(strategy)(labelled, 2) is decision
    assert seen == [(error, 2)]
    assert seen[0][0] is error


@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("RunMicrovm returned no microvmId. RunMicrovm request ID: r."),
        MicrovmStateError("MicroVM mvm-1 is TERMINATED."),
        ClientError(response(), "RunMicrovm"),
    ],
    ids=["builtin", "state", "client-error"],
)
def test_passes_any_other_error_unchanged(error: Exception) -> None:
    seen: list[Exception] = []

    def strategy(received: Exception, attempt: int) -> RetryDecision:
        seen.append(received)
        return RetryDecision.no_retry()

    decide_on_original_error(strategy)(error, 1)

    assert seen == [error]
    assert seen[0] is error


def test_raises_what_the_strategy_raises() -> None:
    def strategy(received: Exception, attempt: int) -> RetryDecision:
        msg = "strategy bug"
        raise LookupError(msg)

    labelled = add_request_id("RunMicrovm", ClientError(response(), "RunMicrovm"))

    with pytest.raises(LookupError, match="strategy bug"):
        decide_on_original_error(strategy)(labelled, 1)


@pytest.mark.parametrize(
    ("operation", "error", "sentence"),
    [
        (
            "CreateMicrovmAuthToken",
            ClientError(
                response(code="ResourceNotFoundException"), "CreateMicrovmAuthToken"
            ),
            " CreateMicrovmAuthToken request ID: req-1.",
        ),
        (
            "GetMicrovm",
            ClientError(response(None, "ResourceNotFoundException"), "GetMicrovm"),
            "",
        ),
        # The caller matches the code. An error of another operation with the
        # same code gets no request ID.
        (
            "ResumeMicrovm",
            ClientError(response(code="ResourceNotFoundException"), "AssumeRole"),
            "",
        ),
    ],
)
def test_reports_a_removed_microvm_in_words_that_fit_both_operations(
    operation: Any, error: ClientError, sentence: str
) -> None:
    state_error = microvm_not_found("mvm-1", operation, error)

    assert state_error.state is None
    assert str(state_error) == (
        "MicroVM mvm-1 no longer exists. A MicroVM lives at most its `timeout` "
        "plus 5 minutes, and never longer than 8 hours." + sentence
    )


class _TokenClient:
    """Answers CreateMicrovmAuthToken from a list."""

    def __init__(self, answers: list[Any]) -> None:
        self._answers = answers

    def create_microvm_auth_token(self, **request: Any) -> Any:
        answer = self._answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


@pytest.mark.parametrize(
    ("statuses", "before"),
    [([], []), ([403], [{"authToken": {"x-t": "t"}}])],
    ids=["first-token", "new-token-after-403"],
)
def test_send_job_raises_the_state_error_for_a_removed_microvm(
    fake_endpoint: Any, statuses: list[int], before: list[Any]
) -> None:
    endpoint = fake_endpoint(statuses)
    not_found = ClientError(
        response(code="ResourceNotFoundException"), "CreateMicrovmAuthToken"
    )
    options = SendJobOptions(
        client=_TokenClient([*before, not_found]),  # type: ignore[arg-type]
        microvm_id="mvm-1",
        endpoint=endpoint.url,
        path="/job",
        port=8080,
        body=b"{}",
        retry_window_seconds=60,
        remaining_seconds=lambda: None,
        logger=logging.getLogger("test"),
        sleep=lambda seconds: None,
    )

    with pytest.raises(MicrovmStateError) as raised:
        send_job(options)

    assert raised.value.state is None
    assert str(raised.value).endswith(" CreateMicrovmAuthToken request ID: req-1.")
    assert raised.value.__cause__ is not_found
    assert len(endpoint.requests) == len(statuses)

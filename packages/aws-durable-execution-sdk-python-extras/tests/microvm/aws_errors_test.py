# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the request ID helpers of the MicroVM operations."""

from __future__ import annotations

import gc
import logging
import weakref
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
    assert vars(labelled)["_original_error"] is error
    assert "_original_error" not in vars(error)
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


def test_passes_a_client_error_whose_original_is_not_a_client_error() -> None:
    """Only a ``ClientError`` in ``_original_error`` replaces the error."""
    error = ClientError(response(), "RunMicrovm")
    error.__dict__["_original_error"] = RuntimeError("not botocore")
    seen: list[Exception] = []

    def strategy(received: Exception, attempt: int) -> RetryDecision:
        seen.append(received)
        return RetryDecision.no_retry()

    decide_on_original_error(strategy)(error, 1)

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
@pytest.mark.parametrize(
    "remaining", [None, 600.0], ids=["no-deadline", "in-a-daemon-thread"]
)
def test_send_job_raises_the_not_found_answer_of_the_token_call_unchanged(
    fake_endpoint: Any,
    statuses: list[int],
    before: list[Any],
    remaining: float | None,
) -> None:
    """The request step turns the answer into ``MicrovmStateError``. So
    ``send_job`` raises the error that the client raised, without a copy.
    With a deadline, the call runs in the daemon thread of
    ``_run_with_limit``, which re-raises the same object."""
    endpoint = fake_endpoint(statuses)
    not_found = ClientError(
        response(code="ResourceNotFoundException"), "CreateMicrovmAuthToken"
    )
    original = str(not_found)
    options = SendJobOptions(
        client=_TokenClient([*before, not_found]),  # type: ignore[arg-type]
        microvm_id="mvm-1",
        endpoint=endpoint.url,
        path="/job",
        port=8080,
        body=b"{}",
        retry_window_seconds=60,
        remaining_seconds=lambda: remaining,
        logger=logging.getLogger("test"),
        sleep=lambda seconds: None,
    )

    with pytest.raises(ClientError) as raised:
        send_job(options)

    assert raised.value is not_found
    assert str(not_found) == original
    assert len(endpoint.requests) == len(statuses)


def _frame_names(error: BaseException) -> list[str]:
    names = []
    traceback = error.__traceback__
    while traceback is not None:
        names.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    return names


def _fail_a_token_call_under_a_deadline(
    endpoint_url: str,
) -> tuple[weakref.ref[ClientError], weakref.ref[ClientError]]:
    """Fail one ``CreateMicrovmAuthToken`` call in the daemon thread of
    ``_run_with_limit``, and return weak references to the copy and the
    original.

    The thread function stores the copy in its closure. The traceback of the
    original holds the frame of that function.
    """
    original = ClientError(response("req-1"), "CreateMicrovmAuthToken")
    options = SendJobOptions(
        client=_TokenClient([original]),  # type: ignore[arg-type]
        microvm_id="mvm-1",
        endpoint=endpoint_url,
        path="/job",
        port=8080,
        body=b"{}",
        retry_window_seconds=60,
        remaining_seconds=lambda: 600.0,
        logger=logging.getLogger("test"),
        sleep=lambda seconds: None,
    )
    copy: ClientError | None = None
    try:
        send_job(options)
    except ClientError as error:
        copy = error
    assert copy is not None
    assert copy.__cause__ is original
    assert str(copy).endswith(" CreateMicrovmAuthToken request ID: req-1.")
    # The remaining time sets a deadline. So the call ran in the daemon thread.
    assert "_run_with_limit" in _frame_names(copy)
    return weakref.ref(copy), weakref.ref(original)


def test_keeps_no_error_of_a_token_call_that_failed_under_a_deadline(
    fake_endpoint: Any,
) -> None:
    copy, original = _fail_a_token_call_under_a_deadline(fake_endpoint().url)

    gc.collect()

    assert copy() is None
    assert original() is None


def _keep_the_copy_in_the_frame_of_the_caller() -> tuple[
    weakref.ref[ClientError], weakref.ref[ClientError]
]:
    """Keep the copy in a local variable of the frame that called the failing
    call, as a handler can in a closure. The traceback of the original holds
    that frame."""
    original = ClientError(response(), "RunMicrovm")

    def call() -> None:
        try:
            raise original
        except ClientError as error:
            # The copy already has the error as its __cause__.
            raise add_request_id("RunMicrovm", error)

    held: dict[str, ClientError] = {}
    try:
        call()
    except ClientError as error:
        held["copy"] = error
    assert held["copy"].__cause__ is original
    return weakref.ref(held["copy"]), weakref.ref(original)


def test_keeps_no_error_whose_traceback_reaches_the_copy() -> None:
    copy, original = _keep_the_copy_in_the_frame_of_the_caller()

    gc.collect()

    assert copy() is None
    assert original() is None


def test_gives_the_strategy_the_original_after_code_replaces_the_cause() -> None:
    """Code can replace ``__cause__``. The copy holds the original in
    ``_original_error`` too. So the strategy still receives the original."""
    error = ClientError(response(), "RunMicrovm")
    labelled = add_request_id("RunMicrovm", error)
    labelled.__cause__ = None
    gc.collect()
    seen: list[Exception] = []

    def strategy(received: Exception, attempt: int) -> RetryDecision:
        seen.append(received)
        return RetryDecision.no_retry()

    decide_on_original_error(strategy)(labelled, 1)

    assert seen == [error]
    assert seen[0] is error


def test_keeps_the_original_after_a_raise_from_another_error() -> None:
    """``raise ... from ...`` sets ``__cause__``. It does not change
    ``_original_error``."""
    error = ClientError(response(), "RunMicrovm")
    labelled = add_request_id("RunMicrovm", error)
    other = RuntimeError("other")

    with pytest.raises(ClientError) as raised:
        raise labelled from other

    assert raised.value.__cause__ is other
    seen: list[Exception] = []

    def strategy(received: Exception, attempt: int) -> RetryDecision:
        seen.append(received)
        return RetryDecision.no_retry()

    decide_on_original_error(strategy)(raised.value, 1)

    assert seen[0] is error


def test_names_the_error_it_received_as_the_original_of_a_copy_of_a_copy() -> None:
    """A copy of a copy takes ``__dict__`` of the first copy. The function
    replaces ``_original_error`` with the first copy."""
    error = ClientError(response("req-1"), "RunMicrovm")
    first = add_request_id("RunMicrovm", error)

    second = add_request_id("RunMicrovm", first)

    assert second is not first
    assert vars(first)["_original_error"] is error
    assert vars(second)["_original_error"] is first
    assert second.__cause__ is first

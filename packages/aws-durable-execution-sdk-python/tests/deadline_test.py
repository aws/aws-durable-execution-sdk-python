"""Invocation time budgets preserve acknowledged outcomes and error identity."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import deadline as deadline_module
from aws_durable_execution_sdk_python.deadline import InvocationDeadline
from aws_durable_execution_sdk_python.exceptions import InvocationDeadlineExceeded
from aws_durable_execution_sdk_python.threading import CompletionEvent
from aws_durable_execution_sdk_python.state import ExecutionState
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.lambda_service import StateOutput


def test_budget_is_captured_once_and_shared_by_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [10.0]
    monkeypatch.setattr(
        deadline_module, "time", SimpleNamespace(monotonic=lambda: now[0])
    )
    context = Mock()
    context.get_remaining_time_in_millis.return_value = 2100
    deadline = InvocationDeadline.from_lambda_context(context)
    assert deadline.remaining() == 2.0
    now[0] = 11.5
    assert deadline.remaining(10) == 0.5
    assert deadline.remaining(0.1) == 0.1
    context.get_remaining_time_in_millis.assert_called_once()
    now[0] = 12.0
    with pytest.raises(InvocationDeadlineExceeded) as caught:
        deadline.check()
    assert caught.value.is_retryable()
    assert deadline.remaining() == 0


def test_standalone_state_has_no_implicit_invocation_deadline() -> None:
    deadline = InvocationDeadline()
    assert deadline.remaining() is None
    assert deadline.remaining(0.5) == 0.5
    deadline.check()
    assert not deadline.wait(CompletionEvent(), 0)


def test_acknowledged_event_keeps_success_after_deadline() -> None:
    completed = CompletionEvent()
    completed.set()
    assert InvocationDeadline(0).wait(completed)


def test_event_preserves_original_error_after_deadline() -> None:
    failure = ValueError("original failure")
    completed = CompletionEvent()
    completed.set(failure)
    with pytest.raises(ValueError) as caught:
        InvocationDeadline(0).wait(completed)
    assert caught.value is failure


def test_unacknowledged_event_expires_without_reporting_success() -> None:
    with pytest.raises(InvocationDeadlineExceeded):
        InvocationDeadline(0).wait(CompletionEvent())


def test_shorter_caller_timeout_does_not_expire_the_invocation() -> None:
    assert not InvocationDeadline(float("inf")).wait(CompletionEvent(), 0)


@pytest.mark.parametrize("terminal", [False, True])
def test_page_admission_samples_deadline_once(
    monkeypatch: pytest.MonkeyPatch, terminal: bool
) -> None:
    clock = iter([0.9, 1.1])
    monkeypatch.setattr(
        deadline_module, "time", SimpleNamespace(monotonic=lambda: next(clock))
    )
    client = Mock()
    client.get_execution_state.return_value = StateOutput(
        operations=[], next_marker="next"
    )
    state = ExecutionState(
        "execution",
        "token",
        {},
        client,
        PluginExecutor([]),
        invocation_deadline=InvocationDeadline(1.0),
    )
    if terminal:
        assert (
            state.fetch_paginated_operations(
                [], "token", "first", finish_terminal_checkpoint=True
            )
            == []
        )
    else:
        with pytest.raises(InvocationDeadlineExceeded):
            state.fetch_paginated_operations([], "token", "first")
    client.get_execution_state.assert_called_once_with(
        durable_execution_arn="execution", checkpoint_token="token", next_marker="first"
    )

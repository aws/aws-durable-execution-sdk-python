"""Tests for the observer module: effect collection and application."""

import inspect

import pytest
from aws_durable_execution_sdk_python.lambda_service import CallbackOptions, ErrorObject

from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
    CallbackCreated,
    Completed,
    Failed,
)
from aws_durable_execution_sdk_python_testing.observer import (
    ExecutionNotifier,
    ExecutionObserver,
    apply_effects,
)
from aws_durable_execution_sdk_python_testing.token import CallbackToken


class MockExecutionObserver(ExecutionObserver):
    """Mock implementation of ExecutionObserver for testing."""

    def __init__(self):
        self.on_completed_calls = []
        self.on_failed_calls = []
        self.on_timed_out_calls = []
        self.on_stopped_calls = []
        self.on_callback_created_calls = []

    def on_completed(self, execution_arn: str, result: str | None = None) -> None:
        self.on_completed_calls.append((execution_arn, result))

    def on_failed(self, execution_arn: str, error: ErrorObject) -> None:
        self.on_failed_calls.append((execution_arn, error))

    def on_timed_out(self, execution_arn: str, error: ErrorObject) -> None:
        self.on_timed_out_calls.append((execution_arn, error))

    def on_stopped(self, execution_arn: str, error: ErrorObject) -> None:
        self.on_stopped_calls.append((execution_arn, error))

    def on_callback_created(
        self,
        execution_arn: str,
        operation_id: str,
        callback_options: CallbackOptions | None,
        callback_token: CallbackToken,
    ) -> None:
        self.on_callback_created_calls.append(
            (execution_arn, operation_id, callback_options, callback_token)
        )


# region ExecutionNotifier collects effects


def test_execution_notifier_init_has_no_effects():
    assert ExecutionNotifier().effects == []


def test_notify_completed_records_completed_effect():
    notifier = ExecutionNotifier()
    notifier.notify_completed("test-arn", "test-result")
    assert notifier.effects == [
        Completed(execution_arn="test-arn", result="test-result")
    ]


def test_notify_completed_no_result_records_none_result():
    notifier = ExecutionNotifier()
    notifier.notify_completed("test-arn")
    assert notifier.effects == [Completed(execution_arn="test-arn", result=None)]


def test_notify_failed_records_failed_effect():
    notifier = ExecutionNotifier()
    error = ErrorObject("TestError", "Test error message", "test-data", ["trace"])
    notifier.notify_failed("test-arn", error)
    assert notifier.effects == [Failed(execution_arn="test-arn", error=error)]


def test_notify_callback_created_records_callback_effect():
    notifier = ExecutionNotifier()
    token = CallbackToken(execution_arn="test-arn", operation_id="op-1")
    options = CallbackOptions()
    notifier.notify_callback_created(
        execution_arn="test-arn",
        operation_id="op-1",
        callback_options=options,
        callback_token=token,
    )
    assert notifier.effects == [
        CallbackCreated(
            execution_arn="test-arn",
            operation_id="op-1",
            callback_options=options,
            callback_token=token,
        )
    ]


def test_notify_accumulates_in_order():
    notifier = ExecutionNotifier()
    error = ErrorObject.from_message("boom")
    notifier.notify_completed("arn-1", "r1")
    notifier.notify_failed("arn-2", error)
    assert notifier.effects == [
        Completed(execution_arn="arn-1", result="r1"),
        Failed(execution_arn="arn-2", error=error),
    ]


# endregion ExecutionNotifier collects effects

# region apply_effects drives effects onto an observer


def test_apply_effects_dispatches_completed():
    observer = MockExecutionObserver()
    apply_effects([Completed(execution_arn="arn", result="ok")], observer)
    assert observer.on_completed_calls == [("arn", "ok")]


def test_apply_effects_dispatches_failed():
    observer = MockExecutionObserver()
    error = ErrorObject.from_message("nope")
    apply_effects([Failed(execution_arn="arn", error=error)], observer)
    assert observer.on_failed_calls == [("arn", error)]


def test_apply_effects_dispatches_callback_created():
    observer = MockExecutionObserver()
    token = CallbackToken(execution_arn="arn", operation_id="op-1")
    options = CallbackOptions()
    apply_effects(
        [
            CallbackCreated(
                execution_arn="arn",
                operation_id="op-1",
                callback_options=options,
                callback_token=token,
            )
        ],
        observer,
    )
    assert observer.on_callback_created_calls == [("arn", "op-1", options, token)]


def test_apply_effects_preserves_order_across_observer():
    observer = MockExecutionObserver()
    error = ErrorObject.from_message("boom")
    apply_effects(
        [
            Completed(execution_arn="arn-1", result="r1"),
            Failed(execution_arn="arn-2", error=error),
        ],
        observer,
    )
    assert observer.on_completed_calls == [("arn-1", "r1")]
    assert observer.on_failed_calls == [("arn-2", error)]


def test_apply_effects_empty_is_a_noop():
    observer = MockExecutionObserver()
    apply_effects([], observer)
    assert observer.on_completed_calls == []
    assert observer.on_failed_calls == []
    assert observer.on_callback_created_calls == []


# endregion apply_effects

# region ExecutionObserver interface


def test_execution_observer_is_abstract():
    with pytest.raises(TypeError):
        ExecutionObserver()


def test_mock_execution_observer_implements_all_methods():
    observer = MockExecutionObserver()
    error = ErrorObject("Error", "Message", "data", ["trace"])
    observer.on_completed("arn", "result")
    observer.on_failed("arn", error)
    observer.on_timed_out("arn", error)
    observer.on_stopped("arn", error)

    assert len(observer.on_completed_calls) == 1
    assert len(observer.on_failed_calls) == 1
    assert len(observer.on_timed_out_calls) == 1
    assert len(observer.on_stopped_calls) == 1


def test_execution_observer_declares_expected_methods():
    methods = inspect.getmembers(ExecutionObserver, predicate=inspect.isfunction)
    method_names = [name for name, _ in methods]
    assert "on_completed" in method_names
    assert "on_failed" in method_names
    assert "on_timed_out" in method_names
    assert "on_stopped" in method_names
    assert "on_callback_created" in method_names


# endregion ExecutionObserver interface

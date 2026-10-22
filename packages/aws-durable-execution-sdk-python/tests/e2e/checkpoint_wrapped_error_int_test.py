"""A failed context checkpoints the error it wraps, not the SDK wrapper.

Each scenario runs through the local test runner twice: live, and replayed.
"Replayed" means the handler catches the error, waits so that a new invocation
starts, and rethrows it, so the second invocation rebuilds the error from the
checkpoint. Every scenario asserts:

* the ErrorType checkpointed by each failed operation and by the execution;
* the error user code sees has the same shape live and on replay.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pytest

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    Duration,
    MapConfig,
    ParallelConfig,
    StepConfig,
    WaitForCallbackConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    CallbackSubmitterError,
    CallbackTimeoutError,
    ChildContextError,
    DurableOperationError,
    StepError,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from aws_durable_execution_sdk_python_testing.runner import (
    ContextOperation,
    DurableFunctionTestRunner,
    Operation,
)


MESSAGE = "card declined"


class PaymentError(Exception):
    pass


class FraudError(Exception):
    pass


PAYMENT = f"{PaymentError.__module__}.PaymentError"
FRAUD = f"{FraudError.__module__}.FraudError"


def _sdk(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


NO_RETRY = StepConfig(
    retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=1))
)
NO_RETRY_SUBMITTER = WaitForCallbackConfig(
    retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=1))
)


def _raise(exc: BaseException) -> Callable[..., Any]:
    def fail(*_args: Any) -> Any:
        raise exc

    return fail


def _failing_step(ctx: DurableContext) -> Any:
    return ctx.step(_raise(PaymentError(MESSAGE)), name="charge", config=NO_RETRY)


def _child_step(ctx: DurableContext) -> Any:
    return ctx.run_in_child_context(_failing_step, name="checkout")


def _nested_child_step(ctx: DurableContext) -> Any:
    return ctx.run_in_child_context(_child_step, name="outer")


def _map_item_step(ctx: DurableContext) -> Any:
    def item(c: DurableContext, *_args: Any) -> Any:
        return _failing_step(c)

    ctx.map(["a"], item, name="fanout").throw_if_error()


def _map_two_types(ctx: DurableContext) -> Any:
    def item(_c: DurableContext, value: str, *_args: Any) -> Any:
        raise (PaymentError if value == "a" else FraudError)(MESSAGE)

    # Both items run, one at a time, so item 0's error is the one raised.
    config = MapConfig(
        max_concurrency=1, completion_config=CompletionConfig.all_completed()
    )
    ctx.map(["a", "b"], item, name="fanout", config=config).throw_if_error()


def _parallel_branch_step(ctx: DurableContext) -> Any:
    ctx.parallel([lambda _c: "ok", _failing_step], name="branches").throw_if_error()


def _parallel_two_types(ctx: DurableContext) -> Any:
    config = ParallelConfig(
        max_concurrency=1, completion_config=CompletionConfig.all_completed()
    )
    ctx.parallel(
        [_raise(PaymentError(MESSAGE)), _raise(FraudError(MESSAGE))],
        name="branches",
        config=config,
    ).throw_if_error()


def _child_wait_for_condition(ctx: DurableContext) -> Any:
    def child(c: DurableContext) -> Any:
        return c.wait_for_condition(
            _raise(PaymentError(MESSAGE)),
            WaitForConditionConfig(
                wait_strategy=lambda _s, _n: WaitForConditionDecision.stop_polling(),
                initial_state=0,
            ),
            name="poll",
        )

    return ctx.run_in_child_context(child, name="checkout")


@durable_execution
def _payments_handler(event: Any, context: DurableContext) -> Any:
    raise PaymentError(MESSAGE)


def _child_invoke(ctx: DurableContext) -> Any:
    return ctx.run_in_child_context(
        lambda c: c.invoke("payments", {}, name="pay"), name="checkout"
    )


def _submitter_fails(ctx: DurableContext) -> Any:
    return ctx.wait_for_callback(
        _raise(PaymentError(MESSAGE)), name="approval", config=NO_RETRY_SUBMITTER
    )


def _callback(ctx: DurableContext) -> Any:
    return ctx.wait_for_callback(lambda _id, _c: None, name="approval")


def _callback_times_out(ctx: DurableContext) -> Any:
    return ctx.wait_for_callback(
        lambda _id, _c: None,
        name="approval",
        config=WaitForCallbackConfig(timeout=Duration.from_seconds(1)),
    )


def _fail_callback(error_type: str | None) -> Callable[[Any, str], None]:
    def drive(runner: DurableFunctionTestRunner, arn: str) -> None:
        callback_id = runner.wait_for_callback(arn, timeout=30)
        runner.send_callback_failure(
            callback_id,
            ErrorObject(message=MESSAGE, type=error_type, data=None, stack_trace=None),
        )

    return drive


@dataclass
class Scenario:
    name: str
    body: Callable[[DurableContext], Any]
    # Checkpointed ErrorType by operation name (failed operations only).
    operations: dict[str, str]
    execution: str
    # Class, error_type and cause class of the error user code sees.
    raised: tuple[type, str, type | None]
    message: str = MESSAGE
    drive: Callable[[Any, str], None] | None = None


SCENARIOS = [
    Scenario(
        "child-context-step",
        _child_step,
        {"checkout": PAYMENT, "charge": PAYMENT},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "nested-child-contexts",
        _nested_child_step,
        {"outer": PAYMENT, "checkout": PAYMENT, "charge": PAYMENT},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "map-item-step",
        _map_item_step,
        {"map-item-0": PAYMENT, "charge": PAYMENT},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "map-items-two-types",
        _map_two_types,
        {"map-item-0": PAYMENT, "map-item-1": FRAUD},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "parallel-branch-step",
        _parallel_branch_step,
        {"parallel-branch-1": PAYMENT, "charge": PAYMENT},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "parallel-branches-two-types",
        _parallel_two_types,
        {"parallel-branch-0": PAYMENT, "parallel-branch-1": FRAUD},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "child-context-wait-for-condition",
        _child_wait_for_condition,
        {"checkout": PAYMENT, "poll": PAYMENT},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "child-context-invoke",
        _child_invoke,
        {"checkout": PAYMENT, "pay": PAYMENT},
        PAYMENT,
        (ChildContextError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "wait-for-callback-submitter",
        _submitter_fails,
        {"approval": PAYMENT, "approval submitter": PAYMENT},
        PAYMENT,
        (CallbackSubmitterError, PAYMENT, DurableOperationError),
    ),
    Scenario(
        "wait-for-callback-external-type",
        _callback,
        {"approval": "com.acme.PaymentError"},
        "com.acme.PaymentError",
        (CallbackExternalError, "com.acme.PaymentError", None),
        drive=_fail_callback("com.acme.PaymentError"),
    ),
    Scenario(
        "wait-for-callback-external-error-type",
        _callback,
        {"approval": "Error"},
        "Error",
        (CallbackExternalError, "Error", None),
        drive=_fail_callback("Error"),
    ),
    Scenario(
        "wait-for-callback-external-no-type",
        _callback,
        {"approval": _sdk(CallbackExternalError)},
        _sdk(CallbackExternalError),
        (CallbackExternalError, _sdk(CallbackExternalError), None),
        drive=_fail_callback(None),
    ),
    Scenario(
        "wait-for-callback-external-blank-type",
        _callback,
        {"approval": _sdk(CallbackExternalError)},
        _sdk(CallbackExternalError),
        (CallbackExternalError, _sdk(CallbackExternalError), None),
        drive=_fail_callback(" \t"),
    ),
    Scenario(
        "wait-for-callback-timeout",
        _callback_times_out,
        {"approval": _sdk(CallbackTimeoutError)},
        _sdk(CallbackTimeoutError),
        (CallbackTimeoutError, _sdk(CallbackTimeoutError), None),
        message="Callback timed out",
    ),
    Scenario(
        "step",
        _failing_step,
        {"charge": PAYMENT},
        PAYMENT,
        (StepError, PAYMENT, DurableOperationError),
    ),
]


def _shape(exc: BaseException) -> tuple[Any, ...]:
    cause = exc.__cause__
    return (
        type(exc),
        getattr(exc, "error_type", None),
        str(exc),
        getattr(exc, "data", None),
        getattr(exc, "stack_trace", None),
        type(cause) if cause is not None else None,
        getattr(cause, "error_type", None),
        str(cause) if cause is not None else None,
    )


def _run(scenario: Scenario, *, replay: bool) -> tuple[Any, list[tuple[Any, ...]]]:
    seen: list[tuple[Any, ...]] = []

    @durable_execution
    def handler(event: Any, ctx: DurableContext) -> Any:
        try:
            scenario.body(ctx)
        except Exception as exc:
            seen.append(_shape(exc))
            if not replay:
                raise
            saved = exc
        else:  # pragma: no cover - every scenario fails
            return "no failure"
        ctx.wait(Duration.from_seconds(1), name="pause")
        raise saved

    with DurableFunctionTestRunner(handler=handler, execution_timeout=60) as runner:
        runner.register_durable_function("payments", _payments_handler)
        arn = runner.run_async(input="{}", execution_timeout=60)
        if scenario.drive is not None:
            scenario.drive(runner, arn)
        result = runner.wait_for_result(arn, timeout=60)
    return result, seen


def _failed_operations(operations: list[Operation]) -> dict[str, ErrorObject]:
    failed: dict[str, ErrorObject] = {}
    for op in operations:
        error = getattr(op, "error", None)
        if error is not None and op.name is not None:
            failed[op.name] = error
        if isinstance(op, ContextOperation):
            failed.update(_failed_operations(op.child_operations))
    return failed


@pytest.mark.parametrize("replay", [False, True], ids=["live", "replayed"])
@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.name for s in SCENARIOS])
def test_failed_context_checkpoints_the_wrapped_error(
    scenario: Scenario, replay: bool
) -> None:
    result, seen = _run(scenario, replay=replay)

    assert result.status.value == "FAILED"
    assert result.error is not None
    assert result.error.type == scenario.execution
    assert result.error.message == scenario.message
    failed = _failed_operations(result.operations)
    for name, expected in scenario.operations.items():
        assert failed[name].type == expected, name
        assert failed[name].message == scenario.message, name

    raised_cls, raised_type, cause_cls = scenario.raised
    assert seen, "user code saw no error"
    first = seen[0]
    assert first[0] is raised_cls
    assert first[1] == raised_type
    assert first[5] is cause_cls
    # Live and replay surface the same error shape to user code.
    assert all(shape == first for shape in seen)
    if replay:
        assert len(seen) == 2

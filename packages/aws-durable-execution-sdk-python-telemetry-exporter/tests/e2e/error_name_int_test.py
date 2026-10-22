# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""End-to-end test: the cross-SDK ``error.name`` rule (Table B).

Each scenario runs through the LOCAL durable runner (``DurableFunctionTestRunner``)
twice: live, and replayed. "Replayed" means that the handler catches the error,
waits so that a new invocation starts, and rethrows the error, so the record is
built from the replayed checkpoint. Every row asserts:

* the operation and execution ``error.name`` values of the table;
* ``error.message`` is the original message;
* the live and replayed names are equal;
* each ``operationsByName`` entry has the same name as ``operations[]``.

Row B15 (a JS ``errorType`` field or a Java SDK-exception subclass) runs in its
Python form: a user error class that subclasses an SDK wrapper. B18 uses a class
created with ``type()`` whose name contains ``$``.
"""

from __future__ import annotations

import threading
import time
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
from aws_durable_execution_sdk_python.exceptions import StepError, ValidationError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.retries import (
    JitterStrategy,
    RetryStrategyConfig,
    create_retry_strategy,
)
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)

from aws_durable_execution_sdk_python_telemetry_exporter import (
    DurableExecutionTelemetryConfig,
    LambdaLogExporter,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


MESSAGE = "card declined"


class PaymentError(Exception):
    """The user error type of Table B."""


class FraudError(Exception):
    """B20/B21: a second user error type with the same message."""


class WrappedPaymentError(StepError):
    """B15 (Python form): a user error that subclasses an SDK wrapper."""


# B18: a class whose name holds a separator (legal through ``type()``).
DollarError: type[Exception] = type("Payment$Error", (Exception,), {})

NO_RETRY = StepConfig(
    retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=1))
)
RETRY_ONCE = StepConfig(
    retry_strategy=create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=2,
            initial_delay=Duration.from_seconds(1),
            jitter_strategy=JitterStrategy.NONE,
        )
    )
)
NO_RETRY_SUBMITTER = WaitForCallbackConfig(
    retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=1))
)


def _raise(exc: BaseException) -> Callable[..., Any]:
    def fail(*_args: Any) -> Any:
        raise exc

    return fail


class _Capture:
    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self.records: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        with self.lock:
            self.records.append(record)

    def flush(self) -> None:
        return None


@dataclass
class Scenario:
    row: str
    body: Callable[[DurableContext], Any]
    # Expected error.name per operation name (only failed operations listed).
    operations: dict[str, str]
    execution: str
    message: str = MESSAGE
    setup: Callable[[DurableFunctionTestRunner], None] | None = None
    # Drives an external event (a callback failure) while the execution runs.
    drive: Callable[[DurableFunctionTestRunner, str], None] | None = None
    fresh: Callable[[], Callable[[DurableContext], Any]] | None = None
    # Operation messages that differ from the execution's, by operation name.
    operation_messages: dict[str, str] | None = None


# -- scenario bodies ---------------------------------------------------------------


def _b1(ctx: DurableContext) -> Any:
    return ctx.step(_raise(PaymentError(MESSAGE)), name="charge", config=NO_RETRY)


def _b2() -> Callable[[DurableContext], Any]:
    attempts = {"n": 0}

    def flaky(_s: Any) -> Any:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise TypeError("transient")
        raise PaymentError(MESSAGE)

    def body(ctx: DurableContext) -> Any:
        return ctx.step(flaky, name="charge", config=RETRY_ONCE)

    return body


def _b3(ctx: DurableContext) -> Any:
    return ctx.run_in_child_context(_raise(PaymentError(MESSAGE)), name="checkout")


def _b4(ctx: DurableContext) -> Any:
    def child(c: DurableContext) -> Any:
        return c.step(_raise(PaymentError(MESSAGE)), name="charge", config=NO_RETRY)

    return ctx.run_in_child_context(child, name="checkout")


def _b5(ctx: DurableContext) -> Any:
    ctx.map(["a"], _raise(PaymentError(MESSAGE)), name="fanout").throw_if_error()


def _b6(ctx: DurableContext) -> Any:
    def item(c: DurableContext, *_args: Any) -> Any:
        return c.step(_raise(PaymentError(MESSAGE)), name="charge", config=NO_RETRY)

    ctx.map(["a"], item, name="fanout").throw_if_error()


def _b7(ctx: DurableContext) -> Any:
    ctx.parallel(
        [lambda _c: "ok", _raise(PaymentError(MESSAGE))], name="branches"
    ).throw_if_error()


def _b8(ctx: DurableContext) -> Any:
    return ctx.wait_for_condition(
        _raise(PaymentError(MESSAGE)),
        WaitForConditionConfig(
            wait_strategy=lambda _s, _n: WaitForConditionDecision.stop_polling(),
            initial_state=0,
        ),
        name="poll",
    )


@durable_execution
def _payments_handler(event: Any, context: DurableContext) -> Any:
    raise PaymentError(MESSAGE)


def _b9_setup(runner: DurableFunctionTestRunner) -> None:
    runner.register_durable_function("payments", _payments_handler)


def _b9(ctx: DurableContext) -> Any:
    return ctx.invoke("payments", {}, name="pay")


def _b10(ctx: DurableContext) -> Any:
    return ctx.wait_for_callback(
        _raise(PaymentError(MESSAGE)), name="approval", config=NO_RETRY_SUBMITTER
    )


def _b11(ctx: DurableContext) -> Any:
    return ctx.wait_for_callback(
        lambda _id, _c: None,
        name="approval",
        config=WaitForCallbackConfig(timeout=Duration.from_seconds(1)),
    )


def _b12_b13(ctx: DurableContext) -> Any:
    return ctx.wait_for_callback(lambda _id, _c: None, name="approval")


def _fail_callback(
    error_type: str | None, message: str | None = MESSAGE
) -> Callable[[DurableFunctionTestRunner, str], None]:
    def drive(runner: DurableFunctionTestRunner, arn: str) -> None:
        callback_id = runner.wait_for_callback(arn, timeout=30)
        runner.send_callback_failure(
            callback_id,
            ErrorObject(message=message, type=error_type, data=None, stack_trace=None),
        )

    return drive


def _b10_empty_message(ctx: DurableContext) -> Any:
    # The core re-raises the submitter's failure with its default message.
    return ctx.wait_for_callback(
        _raise(PaymentError()), name="approval", config=NO_RETRY_SUBMITTER
    )


def _b14(ctx: DurableContext) -> Any:
    try:
        raise TypeError("root cause")
    except TypeError as cause:
        raise PaymentError(MESSAGE) from cause


def _b15(ctx: DurableContext) -> Any:
    raise WrappedPaymentError(MESSAGE)


def _b16(ctx: DurableContext) -> Any:
    try:
        ctx.step(_raise(ValueError("x")), name="lookup", config=NO_RETRY)
    except StepError:
        pass
    ctx.run_in_child_context(_raise(ValidationError("x")), name="work")


def _b17(ctx: DurableContext) -> Any:
    # The step result cannot be serialized: an SDK-only failure.
    return ctx.step(lambda _s: object(), name="charge", config=NO_RETRY)


def _b18(ctx: DurableContext) -> Any:
    return ctx.step(_raise(DollarError(MESSAGE)), name="charge", config=NO_RETRY)


def _b20(ctx: DurableContext) -> Any:
    def item(_c: DurableContext, value: str, *_args: Any) -> Any:
        # One raise site, so both items carry the same message and stack trace.
        raise (PaymentError if value == "a" else FraudError)(MESSAGE)

    # Both items run, one at a time, so item 0's error is the one raised.
    config: MapConfig[Any] = MapConfig(
        max_concurrency=1, completion_config=CompletionConfig.all_completed()
    )
    ctx.map(["a", "b"], item, name="fanout", config=config).throw_if_error()


def _b21(ctx: DurableContext) -> Any:
    config = ParallelConfig(
        max_concurrency=1, completion_config=CompletionConfig.all_completed()
    )
    ctx.parallel(
        [_raise(PaymentError(MESSAGE)), _raise(FraudError(MESSAGE))],
        name="branches",
        config=config,
    ).throw_if_error()


def _handled_in_subtree(ctx: DurableContext) -> Any:
    # Review case 5b: a failure handled inside the failing context's subtree.
    def inner(c: DurableContext) -> Any:
        try:
            c.step(_raise(ValueError(MESSAGE)), name="lookup", config=NO_RETRY)
        except StepError:
            return "handled"
        return "unreachable"  # pragma: no cover

    def outer(c: DurableContext) -> Any:
        c.run_in_child_context(inner, name="inner")
        return c.step(_raise(PaymentError(MESSAGE)), name="charge", config=NO_RETRY)

    return ctx.run_in_child_context(outer, name="outer")


def _nested(depth: int) -> Callable[[DurableContext], Any]:
    """A step that throws PaymentError inside ``depth`` nested child contexts."""

    def level(i: int) -> Callable[[DurableContext], Any]:
        def body(c: DurableContext) -> Any:
            if i > depth:
                return c.step(
                    _raise(PaymentError(MESSAGE)), name="charge", config=NO_RETRY
                )
            return c.run_in_child_context(level(i + 1), name=f"ctx-{i:02d}")

        return body

    return level(1)


def _depth_names(depth: int) -> dict[str, str]:
    names = {f"ctx-{i:02d}": "PaymentError" for i in range(1, depth + 1)}
    names["charge"] = "PaymentError"
    return names


def _map_in_map_in_child(ctx: DurableContext) -> Any:
    def inner_item(_c: DurableContext, *_args: Any) -> Any:
        raise PaymentError(MESSAGE)

    def outer_item(c: DurableContext, *_args: Any) -> Any:
        c.map(["b"], inner_item, name="inner").throw_if_error()

    def child(c: DurableContext) -> Any:
        c.map(["a"], outer_item, name="outer").throw_if_error()

    return ctx.run_in_child_context(child, name="checkout")


SCENARIOS = [
    Scenario("B1", _b1, {"charge": "PaymentError"}, "PaymentError"),
    Scenario("B2", _b1, {"charge": "PaymentError"}, "PaymentError", fresh=_b2),
    Scenario("B3", _b3, {"checkout": "PaymentError"}, "PaymentError"),
    Scenario(
        "B4",
        _b4,
        {"checkout": "PaymentError", "charge": "PaymentError"},
        "PaymentError",
    ),
    Scenario("B5", _b5, {"map-item-0": "PaymentError"}, "PaymentError"),
    Scenario(
        "B6",
        _b6,
        {"map-item-0": "PaymentError", "charge": "PaymentError"},
        "PaymentError",
    ),
    Scenario("B7", _b7, {"parallel-branch-1": "PaymentError"}, "PaymentError"),
    Scenario("B8", _b8, {"poll": "PaymentError"}, "PaymentError"),
    Scenario("B9", _b9, {"pay": "PaymentError"}, "PaymentError", setup=_b9_setup),
    Scenario(
        "B10",
        _b10,
        {"approval": "PaymentError", "approval submitter": "PaymentError"},
        "PaymentError",
    ),
    Scenario(
        "B10-empty-message",
        _b10_empty_message,
        {"approval": "PaymentError", "approval submitter": "PaymentError"},
        "PaymentError",
        message="Callback submitter failed",
        # The context and the step keep the submitter's empty message.
        operation_messages={"approval": "", "approval submitter": ""},
    ),
    Scenario(
        "B11",
        _b11,
        {
            "approval": "CallbackTimeoutError",
            "approval create callback id": "CallbackTimeoutError",
        },
        "CallbackTimeoutError",
        message="Callback timed out",
    ),
    Scenario(
        "B12",
        _b12_b13,
        {"approval": "PaymentError", "approval create callback id": "PaymentError"},
        "PaymentError",
        drive=_fail_callback("com.acme.PaymentError"),
    ),
    Scenario(
        "B13",
        _b12_b13,
        {
            "approval": "CallbackExternalError",
            "approval create callback id": "CallbackExternalError",
        },
        "CallbackExternalError",
        drive=_fail_callback(None),
    ),
    Scenario(
        "B13b",
        _b12_b13,
        {"approval": "Error", "approval create callback id": "Error"},
        "Error",
        drive=_fail_callback("Error"),
    ),
    Scenario(
        "B12-empty-message",
        _b12_b13,
        {"approval": "PaymentError", "approval create callback id": "PaymentError"},
        "PaymentError",
        # The core fills in its default message on the context and the
        # execution; the callback operation keeps the sender's empty one.
        message="Callback failed",
        operation_messages={"approval create callback id": ""},
        drive=_fail_callback("com.acme.PaymentError", ""),
    ),
    Scenario("B14", _b14, {}, "PaymentError"),
    Scenario("B15", _b15, {}, "WrappedPaymentError"),
    Scenario(
        "B16",
        _b16,
        {"lookup": "ValueError", "work": "ValidationError"},
        "ValidationError",
        message="x",
    ),
    Scenario(
        "B17",
        _b17,
        {"charge": "SerDesError"},
        "SerDesError",
        message="Unsupported type: <class 'object'>",
    ),
    Scenario("B18", _b18, {"charge": "Error"}, "Error"),
    Scenario(
        "B20",
        _b20,
        {"map-item-0": "PaymentError", "map-item-1": "FraudError"},
        "PaymentError",
    ),
    Scenario(
        "B21",
        _b21,
        {"parallel-branch-0": "PaymentError", "parallel-branch-1": "FraudError"},
        "PaymentError",
    ),
    Scenario(
        "handled-in-subtree",
        _handled_in_subtree,
        {"outer": "PaymentError", "lookup": "ValueError", "charge": "PaymentError"},
        "PaymentError",
    ),
    # The core records the user's type at every level, so the depth of the
    # operation tree is not an unwrap depth (rule 3 counts wrappers passed).
    Scenario("depth-18", _nested(18), _depth_names(18), "PaymentError"),
    Scenario(
        "map-in-map-in-child",
        _map_in_map_in_child,
        {"checkout": "PaymentError", "map-item-0": "PaymentError"},
        "PaymentError",
    ),
]


# -- runner -----------------------------------------------------------------------


def _handler(scenario: Scenario, replay: bool) -> Callable[[Any, DurableContext], Any]:
    body = scenario.fresh() if scenario.fresh is not None else scenario.body

    def handler(event: Any, ctx: DurableContext) -> Any:
        if not replay:
            return body(ctx)
        try:
            body(ctx)
        except Exception as exc:  # noqa: BLE001 - rethrown after the wait
            saved = exc
        else:  # pragma: no cover - every scenario fails
            return "no failure"
        # A new invocation starts after the wait; the failure above replays from
        # its checkpoint there and is rethrown.
        ctx.wait(Duration.from_seconds(1), name="pause")
        raise saved

    return handler


_TERMINAL = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"})


def run_scenario(scenario: Scenario, replay: bool) -> dict[str, Any]:
    capture = _Capture()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[capture], operation_detail="full-tree"
        )
    )
    handler = durable_execution(_handler(scenario, replay), plugins=[plugin])
    with DurableFunctionTestRunner(handler=handler, execution_timeout=60) as runner:
        if scenario.setup is not None:
            scenario.setup(runner)
        arn = runner.run_async(input="{}", execution_timeout=60)
        if scenario.drive is not None:
            scenario.drive(runner, arn)
        runner.wait_for_result(arn, timeout=60)
    # Wait for the terminal record, not just the first one: in on-change mode
    # earlier RUNNING records may arrive first.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with capture.lock:
            terminal = [r for r in capture.records if r["status"] in _TERMINAL]
        if terminal:
            break
        time.sleep(0.01)  # pragma: no cover
    assert terminal, [r["status"] for r in capture.records]
    record = terminal[-1]
    assert record["status"] == "FAILED"
    return record


def failed_operations(record: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        op["name"]: op["error"]
        for op in record["operations"]
        if "error" in op and op["name"] != "pause"
    }


def _assert_record(scenario: Scenario, record: dict[str, Any]) -> None:
    assert record["error"]["name"] == scenario.execution
    errors = failed_operations(record)
    names = {name: err["name"] for name, err in errors.items()}
    for name, expected in scenario.operations.items():
        assert names.get(name) == expected, (name, names)
    assert record["error"]["message"] == scenario.message
    messages = scenario.operation_messages or {}
    for name in scenario.operations:
        assert errors[name]["message"] == messages.get(name, scenario.message), name
    # operationsByName carries the same name as operations[].
    # (A name used by several operations, such as nested map items, is skipped.)
    by_name = LambdaLogExporter().render(record)["operationsByName"]
    used = [op["name"] for op in record["operations"]]
    for op in record["operations"]:
        if "error" in op and used.count(op["name"]) == 1:
            assert by_name[op["name"]]["error"]["name"] == op["error"]["name"]


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.row for s in SCENARIOS])
def test_table_b_live_and_replayed(scenario: Scenario) -> None:
    live = run_scenario(scenario, replay=False)
    replayed = run_scenario(scenario, replay=True)
    _assert_record(scenario, live)
    _assert_record(scenario, replayed)
    assert replayed["error"] == live["error"]
    live_names = {n: e["name"] for n, e in failed_operations(live).items()}
    replayed_names = {n: e["name"] for n, e in failed_operations(replayed).items()}
    assert replayed_names == live_names

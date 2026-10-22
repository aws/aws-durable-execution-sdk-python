# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the cross-SDK ``error.name`` rule.

Table A (type-string reduction) is driven from ``fixtures/error-name-vectors.json``,
which has the same content in the JS, Python and Java exporter repositories.
The operation-tree cases build checkpoint snapshots shaped the way the SDK
writes them; ``tests/e2e/error_name_int_test.py`` runs the same scenarios
through the local runner.
"""

from __future__ import annotations

import datetime
import json
import pathlib
import time
from typing import Any

import pytest
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    CallbackTimeoutError,
    ChildContextError,
    DurableOperationError,
    InvokeError,
    StepError,
    ValidationError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationInfo,
    OperationType,
)

from aws_durable_execution_sdk_python_telemetry_exporter import (
    ContentConfig,
    ContentOperations,
    DurableExecutionTelemetryConfig,
    LambdaLogExporter,
    durable_execution_telemetry,
)
from aws_durable_execution_sdk_python_telemetry_exporter import (
    plugin as plugin_module,
)
from aws_durable_execution_sdk_python_telemetry_exporter.plugin import (
    DurableExecutionTelemetryPlugin,
    _simple_type_name,
)

ARN = "arn:aws:lambda:us-west-2:123456789012:function:my-fn:$LATEST/durable-execution/exec-1/inv-1"
T0 = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
T1 = datetime.datetime(2026, 1, 1, 0, 0, 1, tzinfo=datetime.UTC)

_VECTORS = json.loads(
    (pathlib.Path(__file__).parent / "fixtures" / "error-name-vectors.json").read_text(
        encoding="utf-8"
    )
)


# -- Table A ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "vector", _VECTORS["reduce"], ids=[v["id"] for v in _VECTORS["reduce"]]
)
def test_table_a_reduction(vector: dict[str, Any]) -> None:
    assert _simple_type_name(vector["input"]) == vector["expected"]


@pytest.mark.parametrize(
    "vector", _VECTORS["reduce"], ids=[v["id"] for v in _VECTORS["reduce"]]
)
def test_table_a_applies_to_execution_and_operation_records(
    vector: dict[str, Any],
) -> None:
    err = ErrorObject(message="m", type=vector["input"], data=None, stack_trace=None)
    rec, rendered = _record([_op("op", error=err)], err)
    assert rec["error"] == {"name": vector["expected"], "message": "m"}
    assert rec["operations"][0]["error"] == {"name": vector["expected"], "message": "m"}
    assert rendered["operationsByName"]["op"]["error"]["name"] == vector["expected"]


def test_vectors_file_is_the_shared_version() -> None:
    assert _VECTORS["version"] == 1
    assert [v["id"] for v in _VECTORS["reduce"]][:2] == ["A1", "A2"]


# -- helpers ---------------------------------------------------------------------


class _Capture:
    def __init__(self) -> None:
        self.max_record_size_bytes: int | None = None
        self.records: list[dict[str, Any]] = []

    def render(self, record: dict[str, Any]) -> Any:
        return record

    def export(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def flush(self) -> None:
        return None


def _op(
    name: str,
    *,
    error: ErrorObject | None = None,
    parent: str | None = None,
    op_type: OperationType = OperationType.STEP,
    status: OperationStatus | None = None,
) -> OperationInfo:
    return OperationInfo(
        operation_id=name,
        operation_type=op_type,
        sub_type=None,
        name=name,
        parent_id=parent,
        start_time=T0,
        is_replayed=False,
        status=status
        or (OperationStatus.FAILED if error is not None else OperationStatus.SUCCEEDED),
        end_time=T1,
        error=error,
    )


def _ctx(name: str, **kwargs: Any) -> OperationInfo:
    return _op(name, op_type=OperationType.CONTEXT, **kwargs)


def _err(type_name: str | None, message: str | None = "card declined") -> ErrorObject:
    return ErrorObject(message=message, type=type_name, data=None, stack_trace=None)


def _wrap(error: ErrorObject, wrapper: type[DurableOperationError]) -> ErrorObject:
    """The ErrorObject the SDK checkpoints when ``error`` escapes an operation as
    ``wrapper`` (ErrorObject.raise_as_operation_error + ErrorObject.from_exception),
    on the first run and on replay alike."""
    try:
        error.raise_as_operation_error(wrapper)
    except Exception as exc:  # noqa: BLE001 - captured to build the wire error
        return ErrorObject.from_exception(exc)
    raise AssertionError("raise_as_operation_error must raise")  # pragma: no cover


def _record(
    ops: list[OperationInfo],
    execution_error: ErrorObject | None,
    **config: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    capture = _Capture()
    plugin = durable_execution_telemetry(
        DurableExecutionTelemetryConfig(
            exporters=[capture], operation_detail="full-tree", **config
        )
    )
    operations = {op.operation_id: op for op in ops}
    plugin.on_invocation_start(
        InvocationStartInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=T0,
            execution_input="{}",
            operations=operations,
        )
    )
    plugin.on_invocation_end(
        InvocationEndInfo(
            request_id=None,
            execution_arn=ARN,
            is_first_invocation=True,
            execution_start_time=T0,
            status=InvocationStatus.FAILED,
            error=execution_error,
            execution_result=None,
            operations=operations,
        )
    )
    (rec,) = capture.records
    return rec, LambdaLogExporter().render(rec)


def _names(rec: dict[str, Any]) -> dict[str, Any]:
    return {o["name"]: o["error"]["name"] for o in rec["operations"] if "error" in o}


PAYMENT = _err("__main__.PaymentError")
STEP_WRAPPER = _wrap(PAYMENT, StepError)


# -- which error is named ------------------------------------------------------


def test_step_user_error_names_operation_and_execution() -> None:
    rec, rendered = _record([_op("charge", error=PAYMENT)], STEP_WRAPPER)
    assert rec["error"] == {"name": "PaymentError", "message": "card declined"}
    assert _names(rec) == {"charge": "PaymentError"}
    assert rendered["operationsByName"]["charge"]["error"]["name"] == "PaymentError"


def test_child_context_and_its_step_resolve_to_the_user_error() -> None:
    ops = [
        _ctx("checkout", error=STEP_WRAPPER),
        _op("charge", error=PAYMENT, parent="checkout"),
    ]
    rec, _ = _record(ops, _wrap(STEP_WRAPPER, ChildContextError))
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec) == {"checkout": "PaymentError", "charge": "PaymentError"}


def test_b16_handled_user_error_with_same_message_is_not_the_cause() -> None:
    # A step's ValueError("x") is caught and handled. Then a child context fails
    # with the SDK's ValidationError("x"). The execution error is the child's
    # ChildContextError; only the child's own subtree is searched.
    lookup = _op("lookup", error=ErrorObject.from_exception(ValueError("x")))
    validation = ErrorObject.from_exception(ValidationError("x"))
    work = _ctx("work", error=validation)
    rec, rendered = _record([lookup, work], _wrap(validation, ChildContextError))
    assert rec["error"] == {"name": "ValidationError", "message": "x"}
    assert _names(rec) == {"lookup": "ValueError", "work": "ValidationError"}
    assert rendered["operationsByName"]["work"]["error"]["name"] == "ValidationError"


def test_cause_is_taken_from_the_failing_operation_subtree_only() -> None:
    # Two child contexts fail with the same message but different user errors.
    a_step, b_step = PAYMENT, _err("app.InputError")
    ops = [
        _ctx("a", error=_wrap(a_step, StepError)),
        _op("a-step", error=a_step, parent="a"),
        _ctx("b", error=_wrap(b_step, StepError)),
        _op("b-step", error=b_step, parent="b"),
    ]
    rec, _ = _record(ops, None)
    assert _names(rec) == {
        "a": "PaymentError",
        "a-step": "PaymentError",
        "b": "InputError",
        "b-step": "InputError",
    }


def test_ambiguous_subtree_does_not_guess() -> None:
    # A map whose items failed with the same message and different types, and
    # whose execution error carries those same fields: no single cause.
    item_a, item_b = PAYMENT, _err("app.InputError")
    ops = [
        _ctx("fanout"),
        _ctx("item-0", error=item_a, parent="fanout"),
        _ctx("item-1", error=item_b, parent="fanout"),
        _ctx("outer", error=_wrap(item_a, ChildContextError)),
        _ctx("o-0", error=item_a, parent="outer"),
        _ctx("o-1", error=item_b, parent="outer"),
    ]
    rec, _ = _record(ops, _wrap(_wrap(item_a, ChildContextError), ChildContextError))
    assert _names(rec)["outer"] == "ChildContextError"
    assert rec["error"]["name"] == "ChildContextError"


def test_sdk_non_wrapper_error_is_a_candidate() -> None:
    # A SerDesError at the leaf is an SDK error but not a wrapper: rule 5.
    serdes = _err("aws_durable_execution_sdk_python.exceptions.SerDesError", "bad")
    ops = [
        _ctx("ctx", error=_wrap(serdes, StepError)),
        _op("s", error=serdes, parent="ctx"),
    ]
    rec, _ = _record(ops, _wrap(_wrap(serdes, StepError), ChildContextError))
    assert rec["error"]["name"] == "SerDesError"
    assert _names(rec) == {"ctx": "SerDesError", "s": "SerDesError"}


def test_execution_wrapper_must_match_the_operation_type() -> None:
    # The execution error is a StepError; a failed child context with the same
    # fields is not the operation it propagated from.
    other = _err("app.InputError")
    ops = [_op("charge", error=PAYMENT), _ctx("ctx", error=other)]
    rec, _ = _record(ops, STEP_WRAPPER)
    assert rec["error"]["name"] == "PaymentError"


def test_base_wrapper_matches_any_operation_type() -> None:
    rec, _ = _record(
        [_ctx("ctx", error=PAYMENT)], _wrap(PAYMENT, DurableOperationError)
    )
    assert rec["error"]["name"] == "PaymentError"


def test_invoke_wrapper_resolves_to_the_invoked_error() -> None:
    invoke = _op("pay", op_type=OperationType.CHAINED_INVOKE, error=PAYMENT)
    rec, _ = _record([invoke], _wrap(PAYMENT, InvokeError))
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec) == {"pay": "PaymentError"}


def test_user_error_is_never_unwrapped() -> None:
    # The handler raises PaymentError (whose own cause is a TypeError).
    err = _err("__main__.PaymentError")
    ops = [_op("s", error=_err("TypeError"))]
    rec, _ = _record(ops, err)
    assert rec["error"]["name"] == "PaymentError"


def test_wrapper_without_operation_uses_its_own_simple_name() -> None:
    rec, _ = _record([], STEP_WRAPPER)
    assert rec["error"]["name"] == "StepError"


def _chain(depth: int) -> list[OperationInfo]:
    """``depth`` nested child contexts around a failing step."""
    ops = [_ctx("c0", error=STEP_WRAPPER)]
    ops += [
        _ctx(f"c{i}", error=STEP_WRAPPER, parent=f"c{i - 1}") for i in range(1, depth)
    ]
    ops.append(_op("charge", error=PAYMENT, parent=f"c{depth - 1}"))
    return ops


def test_unwrap_follows_chain_up_to_depth_16() -> None:
    rec, _ = _record(_chain(16), _wrap(STEP_WRAPPER, ChildContextError))
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec)["c0"] == "PaymentError"


def test_unwrap_stops_beyond_depth_16() -> None:
    rec, _ = _record(_chain(17), None)
    names = _names(rec)
    assert names["c0"] == "StepError"
    assert names["c1"] == "PaymentError"


def test_parent_cycle_stops_the_walk() -> None:
    ops = [
        _ctx("a", error=STEP_WRAPPER, parent="b"),
        _ctx("b", error=STEP_WRAPPER, parent="a"),
    ]
    rec, _ = _record(ops, None)
    assert _names(rec) == {"a": "StepError", "b": "StepError"}


# -- callbacks ---------------------------------------------------------------------


def _callback_ops(
    callback_error: ErrorObject | None,
    status: OperationStatus,
    context_error: ErrorObject,
) -> list[OperationInfo]:
    return [
        _ctx("approval", error=context_error),
        _op(
            "approval create callback id",
            op_type=OperationType.CALLBACK,
            parent="approval",
            status=status,
            error=callback_error,
        ),
    ]


def test_b11_callback_timeout_is_named_consistently() -> None:
    service = _err("Callback.Timeout", "Callback timed out")
    timeout = ErrorObject.from_exception(CallbackTimeoutError("Callback timed out"))
    ops = _callback_ops(service, OperationStatus.TIMED_OUT, timeout)
    rec, _ = _record(ops, timeout)
    assert rec["error"]["name"] == "CallbackTimeoutError"
    assert _names(rec) == {
        "approval": "CallbackTimeoutError",
        "approval create callback id": "CallbackTimeoutError",
    }


def test_b12_external_failure_with_type_is_the_senders_type() -> None:
    sender = _err("com.acme.PaymentError")
    external = ErrorObject.from_exception(CallbackExternalError("card declined"))
    rec, _ = _record(_callback_ops(sender, OperationStatus.FAILED, external), external)
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec) == {
        "approval": "PaymentError",
        "approval create callback id": "PaymentError",
    }


def test_b13_external_failure_without_type_is_the_sdk_callback_class() -> None:
    sender = _err(None)
    external = ErrorObject.from_exception(CallbackExternalError("card declined"))
    rec, _ = _record(_callback_ops(sender, OperationStatus.FAILED, external), external)
    assert rec["error"]["name"] == "CallbackExternalError"
    assert _names(rec) == {
        "approval": "CallbackExternalError",
        "approval create callback id": "CallbackExternalError",
    }


def test_external_failure_without_message_matches_the_sdk_default() -> None:
    sender = _err("PaymentError", None)
    external = ErrorObject.from_exception(CallbackExternalError("Callback failed"))
    callback = _op(
        "cb",
        op_type=OperationType.CALLBACK,
        status=OperationStatus.FAILED,
        error=sender,
    )
    rec, _ = _record([callback], external)
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec) == {"cb": "PaymentError"}


# -- options and cost -------------------------------------------------------------


def test_cause_lookup_ignores_include_errors_false() -> None:
    rec, _ = _record(
        [_op("charge", error=PAYMENT)],
        STEP_WRAPPER,
        content=ContentConfig(operations=ContentOperations(include_errors=False)),
    )
    assert rec["error"]["name"] == "PaymentError"
    assert "error" not in rec["operations"][0]


def _failed_map(items: int) -> dict[str, OperationInfo]:
    ops: list[OperationInfo] = [_ctx("fanout")]
    for i in range(items):
        step_error = _err("app.InputError", f"bad {i}")
        ops.append(
            _ctx(f"item-{i}", parent="fanout", error=_wrap(step_error, StepError))
        )
        ops.append(_op(f"step-{i}", parent=f"item-{i}", error=step_error))
    return {op.operation_id: op for op in ops}


def _build(operations: dict[str, OperationInfo]) -> list[dict[str, Any]]:
    plugin = DurableExecutionTelemetryPlugin(
        DurableExecutionTelemetryConfig(
            exporters=[_Capture()], operation_detail="full-tree"
        )
    )
    return plugin._build_operations(operations)  # noqa: SLF001


def test_record_build_is_linear_in_failed_operations(monkeypatch) -> None:
    # Count the per-operation error-field reads: quadratic matching would read
    # every operation once per failed operation.
    calls = 0
    real = plugin_module._operation_error_key  # noqa: SLF001

    def counting(op: OperationInfo) -> Any:
        nonlocal calls
        calls += 1
        return real(op)

    monkeypatch.setattr(plugin_module, "_operation_error_key", counting)
    per_op: list[float] = []
    for items in (500, 2000):
        calls = 0
        operations = _failed_map(items)
        entries = _build(operations)
        assert {e["error"]["name"] for e in entries if "error" in e} == {"InputError"}
        per_op.append(calls / len(operations))
    assert per_op[1] <= per_op[0] * 1.1
    assert per_op[1] < 5


def test_record_build_with_2000_failed_map_items_is_fast() -> None:
    operations = _failed_map(2000)
    start = time.perf_counter()
    _build(operations)
    elapsed = time.perf_counter() - start
    # About 15 ms on a laptop; the bound is generous for shared CI runners.
    assert elapsed < 0.5

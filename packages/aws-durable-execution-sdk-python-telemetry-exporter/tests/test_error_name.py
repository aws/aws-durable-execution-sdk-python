# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the cross-SDK ``error.name`` rule.

Table A (type-string reduction) is driven from ``fixtures/error-name-vectors.json``,
which has the same content in the JS, Python and Java exporter repositories.
The core SDK checkpoints the error a wrapper wraps, so each name is the
reduction of that record's own ErrorType. The snapshot cases below build the
ErrorObjects the core writes (``ErrorObject.from_exception``), plus wrapper
types an older core wrote; ``tests/e2e/error_name_int_test.py`` runs Table B
through the local runner.
"""

from __future__ import annotations

import datetime
import json
import pathlib
from typing import Any

import pytest
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    CallbackSubmitterError,
    CallbackTimeoutError,
    ChildContextError,
    DurableOperationError,
    InvokeError,
    StepError,
    WaitForConditionError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationSubType,
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
from aws_durable_execution_sdk_python_telemetry_exporter.plugin import (
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
    assert _VECTORS["version"] == 2
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
    sub_type: OperationSubType | None = None,
) -> OperationInfo:
    return OperationInfo(
        operation_id=name,
        operation_type=op_type,
        sub_type=sub_type,
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


def _wrapper_type(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _recorded(exc: Exception) -> ErrorObject:
    """The ErrorObject the core checkpoints for ``exc``."""
    return ErrorObject.from_exception(exc)


def _wrapped(error: ErrorObject, wrapper: type[DurableOperationError]) -> Exception:
    """The wrapper the core raises when ``error`` escapes an operation."""
    try:
        error.raise_as_operation_error(wrapper)
    except DurableOperationError as exc:
        return exc
    raise AssertionError("raise_as_operation_error must raise")  # pragma: no cover


# -- which error is named ------------------------------------------------------


def test_step_user_error_names_operation_and_execution() -> None:
    execution = _recorded(_wrapped(PAYMENT, StepError))
    rec, rendered = _record([_op("charge", error=PAYMENT)], execution)
    assert rec["error"] == {"name": "PaymentError", "message": "card declined"}
    assert _names(rec) == {"charge": "PaymentError"}
    assert rendered["operationsByName"]["charge"]["error"]["name"] == "PaymentError"


@pytest.mark.parametrize(
    "wrapper",
    [
        StepError,
        ChildContextError,
        InvokeError,
        WaitForConditionError,
        CallbackSubmitterError,
        CallbackExternalError,
        DurableOperationError,
    ],
)
def test_the_core_records_the_wrapped_type(
    wrapper: type[DurableOperationError],
) -> None:
    # A context the wrapper escaped, and the execution, carry the user's type.
    recorded = _recorded(_wrapped(PAYMENT, wrapper))
    ops = [
        _ctx("checkout", error=recorded),
        _op("charge", parent="checkout", error=PAYMENT),
    ]
    rec, _ = _record(ops, recorded)
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec) == {"checkout": "PaymentError", "charge": "PaymentError"}


def test_each_operation_is_named_by_its_own_type() -> None:
    # B16: no matching by message; each record reduces its own type.
    ops = [
        _op("lookup", error=_err("ValueError", "x")),
        _ctx(
            "work",
            error=_err(
                "aws_durable_execution_sdk_python.exceptions.ValidationError", "x"
            ),
        ),
    ]
    rec, _ = _record(ops, ops[1].error)
    assert _names(rec) == {"lookup": "ValueError", "work": "ValidationError"}
    assert rec["error"]["name"] == "ValidationError"


@pytest.mark.parametrize("sub_type", [OperationSubType.MAP, OperationSubType.PARALLEL])
def test_b20_b21_items_with_the_same_message_keep_their_types(
    sub_type: OperationSubType,
) -> None:
    fraud = _err("__main__.FraudError")
    ops = [
        _ctx("fanout", sub_type=sub_type),
        _ctx("item-0", parent="fanout", error=PAYMENT),
        _ctx("item-1", parent="fanout", error=fraud),
    ]
    # The execution carries the error throw_if_error raised.
    execution = _recorded(_wrapped(PAYMENT, ChildContextError))
    rec, _ = _record(ops, execution)
    assert _names(rec) == {"item-0": "PaymentError", "item-1": "FraudError"}
    assert rec["error"]["name"] == "PaymentError"


def test_user_subclass_of_a_wrapper_is_the_users_name() -> None:
    # B15: the core does not unwrap a user subclass; neither does the exporter.
    err = _err("__main__.WrappedPaymentError")
    rec, _ = _record([], err)
    assert rec["error"]["name"] == "WrappedPaymentError"


# -- checkpoints written by an older core (rule 9) ---------------------------------


@pytest.mark.parametrize(
    "wrapper",
    [StepError, ChildContextError, InvokeError, CallbackSubmitterError],
)
def test_an_old_wrapper_type_reports_the_wrapper(
    wrapper: type[DurableOperationError],
) -> None:
    old = _err(_wrapper_type(wrapper))
    # A child that holds the user's type is never used to guess the cause.
    ops = [_ctx("checkout", error=old), _op("charge", parent="checkout", error=PAYMENT)]
    rec, _ = _record(ops, old)
    assert rec["error"]["name"] == wrapper.__name__
    assert _names(rec) == {"checkout": wrapper.__name__, "charge": "PaymentError"}


# -- callbacks -------------------------------------------------------------------


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


def _external(error_type: str | None) -> ErrorObject:
    """What the core records for a callback failed externally with ``error_type``."""
    return _recorded(CallbackExternalError("card declined", error_type=error_type))


def test_b11_callback_timeout_is_named_consistently() -> None:
    service = _err("Callback.Timeout", "Callback timed out")
    timeout = _recorded(CallbackTimeoutError("Callback timed out"))
    ops = _callback_ops(service, OperationStatus.TIMED_OUT, timeout)
    rec, _ = _record(ops, timeout)
    assert rec["error"]["name"] == "CallbackTimeoutError"
    assert _names(rec) == {
        "approval": "CallbackTimeoutError",
        "approval create callback id": "CallbackTimeoutError",
    }


def test_b12_external_failure_with_type_is_the_senders_type() -> None:
    sender = _err("com.acme.PaymentError")
    recorded = _external("com.acme.PaymentError")
    rec, _ = _record(_callback_ops(sender, OperationStatus.FAILED, recorded), recorded)
    assert rec["error"]["name"] == "PaymentError"
    assert _names(rec) == {
        "approval": "PaymentError",
        "approval create callback id": "PaymentError",
    }


def test_b13_external_failure_without_type_is_the_sdk_callback_class() -> None:
    recorded = _external(None)
    rec, _ = _record(
        _callback_ops(_err(None), OperationStatus.FAILED, recorded), recorded
    )
    assert rec["error"]["name"] == "CallbackExternalError"
    assert _names(rec) == {
        "approval": "CallbackExternalError",
        "approval create callback id": "CallbackExternalError",
    }


def test_b13b_external_failure_with_type_error_is_error() -> None:
    recorded = _external("Error")
    rec, _ = _record(
        _callback_ops(_err("Error"), OperationStatus.FAILED, recorded), recorded
    )
    assert rec["error"]["name"] == "Error"
    assert _names(rec) == {"approval": "Error", "approval create callback id": "Error"}


@pytest.mark.parametrize("blank", ["", "   ", " \t\n\r\f\v"])
def test_external_failure_with_blank_type_is_the_sdk_callback_class(blank: str) -> None:
    recorded = _external(None)
    ops = _callback_ops(_err(blank), OperationStatus.FAILED, recorded)
    rec, _ = _record(ops, recorded)
    assert rec["error"]["name"] == "CallbackExternalError"
    assert _names(rec)["approval create callback id"] == "CallbackExternalError"


def test_external_failure_with_non_ascii_space_type_is_kept() -> None:
    recorded = _external("\u00a0")
    ops = _callback_ops(_err("\u00a0"), OperationStatus.FAILED, recorded)
    rec, _ = _record(ops, recorded)
    assert rec["error"]["name"] == "\u00a0"
    assert _names(rec)["approval create callback id"] == "\u00a0"


# -- options -----------------------------------------------------------------------


def test_execution_name_ignores_include_errors_false() -> None:
    rec, _ = _record(
        [_op("charge", error=PAYMENT)],
        _recorded(_wrapped(PAYMENT, StepError)),
        content=ContentConfig(operations=ContentOperations(include_errors=False)),
    )
    assert rec["error"]["name"] == "PaymentError"
    assert "error" not in rec["operations"][0]

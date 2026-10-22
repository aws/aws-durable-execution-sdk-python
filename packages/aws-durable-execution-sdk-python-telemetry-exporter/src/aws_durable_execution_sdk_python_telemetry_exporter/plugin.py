# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Durable Execution Telemetry plugin for the Durable Execution Python SDK.

Port of the JS ``durableExecutionTelemetry()``
(``aws-durable-execution-sdk-js-telemetry-exporter/src/index.ts``). It listens
to the SDK's instrumentation hooks and emits one curated
``DurableExecutionTelemetry`` record per execution to the configured exporters.
The wire record keeps the JS camelCase field names so records read identically
across SDKs.

Operation-map sourcing:
  The Python SDK invocation hooks now carry the full operation map directly:
  ``InvocationStartInfo.operations`` (a point-in-time snapshot at invocation
  start), ``InvocationEndInfo.operations`` (a fresh snapshot at invocation end),
  and ``OperationChangeInfo.operations`` (the full map at the change). Alongside
  them the invocation hooks carry ``execution_arn``, ``execution_start_time``,
  ``execution_input`` and ``execution_result``. This plugin reads those
  snapshots as the authoritative operation state -- it does NOT reconstruct the
  map by accumulating per-operation ``on_operation_end`` events. Because every
  invocation start re-seeds the map from the snapshot, a cold resume in a fresh
  Lambda environment (a brand-new plugin instance) still reports the prior
  terminal operations.

  The Python SDK has no ``pluginsConfig.childOperationsDepth`` equivalent, so
  ``full-tree`` records rely on the child operations being present in the
  emitting invocation's snapshot (true for single-invocation and warm-resume
  cases).
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import math
import threading
import uuid
from typing import Any, Callable

from aws_durable_execution_sdk_python.concurrency.models import BatchResult
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    CallbackTimeoutError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationInfo,
    OperationType,
)
from aws_durable_execution_sdk_python.serdes import (
    EXTENDED_TYPES_SERDES,
    SerDesContext,
)

from aws_durable_execution_sdk_python_telemetry_exporter._export_scheduler import (
    _ExportScheduler,
)
from aws_durable_execution_sdk_python_telemetry_exporter.exporters.lambda_log_exporter import (
    LambdaLogExporter,
)
from aws_durable_execution_sdk_python_telemetry_exporter.types import (
    ContentConfig,
    EmitMode,
    TelemetryExporter,
    OperationDetail,
    OperationOverride,
    DurableExecutionTelemetryConfig,
)


# Maps the SDK invocation status onto the record status. A durable execution
# suspends (PENDING) while waiting; from the execution's point of view it is
# still in flight, so surface it as RUNNING (mirrors the JS STATUS_MAP).
_STATUS_MAP: dict[InvocationStatus, str] = {
    InvocationStatus.SUCCEEDED: "SUCCEEDED",
    InvocationStatus.FAILED: "FAILED",
    InvocationStatus.PENDING: "RUNNING",
    InvocationStatus.RETRY: "RUNNING",
}


def _parse_execution_arn(execution_arn: str) -> dict[str, str]:
    # arn:<partition>:lambda:<region>:<account>:function:<fn>:<qualifier>/durable-execution/<execName>/<invId>
    parts = execution_arn.split(":")
    last = parts[7] if len(parts) > 7 else ""
    segments = last.split("/")
    return {
        "region": parts[3] if len(parts) > 3 else "",
        "accountId": parts[4] if len(parts) > 4 else "",
        "functionName": parts[6] if len(parts) > 6 else "",
        "qualifier": segments[0] if len(segments) > 0 else "",
        "executionName": segments[2] if len(segments) > 2 else "",
        "invocationId": segments[3] if len(segments) > 3 else "",
    }


def _fnv1a32(value: str) -> int:
    h = 0x811C9DC5
    for ch in value:
        h ^= ord(ch) & 0xFF
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def _should_sample(execution_arn: str, rate: float) -> bool:
    if rate >= 1:
        return True
    if rate <= 0:
        return False
    return _fnv1a32(execution_arn) / 0xFFFFFFFF < rate


def _resolve_sampling_rate(rate: float | None) -> float:
    if rate is None:
        return 1.0
    if not isinstance(rate, (int, float)):
        return 1.0
    if isinstance(rate, float) and math.isnan(rate):
        # Fail open. NaN compares False to everything, so without this guard it
        # would flow through _should_sample (rate >= 1 -> False, rate <= 0 ->
        # False, x < NaN -> False) and silently sample OUT every execution,
        # disabling all instrumentation. Coerce to full sampling instead, which
        # matches the JS plugin's treatment of non-finite/invalid rates.
        return 1.0
    if rate < 0 or rate > 1:
        return max(0.0, min(1.0, float(rate)))
    return float(rate)


def _iso(ts: Any) -> str | None:
    if isinstance(ts, datetime.datetime):
        return ts.astimezone(datetime.UTC).isoformat().replace("+00:00", "Z")
    return None


def _duration_ms(start: Any, end: Any) -> int | None:
    if isinstance(start, datetime.datetime) and isinstance(end, datetime.datetime):
        return int((end - start).total_seconds() * 1000)
    return None


def _apply_data_content(value: Any, setting: Any) -> Any:
    if setting is False:
        return None
    if value is None:
        return None
    if callable(setting):
        try:
            return setting(value)
        except Exception:  # noqa: BLE001 - a failing redactor must never leak the raw value
            return None
    return value


def _apply_result_override(
    transform: Callable[[Any], Any], raw_result: str | None
) -> Any:
    if raw_result is None:
        return None
    parsed = _decode_checkpoint_result(raw_result)
    try:
        return transform(parsed)
    except Exception:  # noqa: BLE001 - untrusted transform must never break emission
        return None


# -- operation results ----------------------------------------------------------


def _is_extended_type_envelope(value: Any) -> bool:
    # ExtendedTypeSerDes wraps every non-primitive value in exactly
    # {"t": <type tag>, "v": <encoded value>} at the root.
    return (
        isinstance(value, dict)
        and value.keys() == {"t", "v"}
        and isinstance(value["t"], str)
    )


def _decode_checkpoint_result(raw_result: str) -> Any:
    """Return an operation's checkpointed result as a plain JSON value.

    The SDK's default serializer (``ExtendedTypeSerDes``) writes primitives and
    primitive lists as plain JSON, and everything else (dicts, nested lists,
    ``Decimal``, ``datetime``, ...) as a ``{"t": ..., "v": ...}`` envelope. The
    JS and Java exporters emit the user's plain JSON value, so an envelope is
    decoded with that default serializer and converted to JSON-compatible
    values (see ``_to_json_compatible``). A user's custom ``SerDes`` is never
    run: a payload that is not an envelope keeps the previous behaviour (parsed
    JSON, or the raw string when it is not JSON), and so does an envelope that
    fails to decode. Never raises.

    The plugin does not know which serializer wrote a result (the hooks do not
    carry it), so the envelope is recognised by shape alone: a JSON object
    with exactly the keys ``t`` and ``v`` where ``t`` is a string. A result
    written by another serializer (``JsonSerDes``, a custom ``SerDes``, or a
    chained invoke's JSON payload) that has this exact shape, and whose ``t``
    is one of the default serializer's tags, is decoded as if the default
    serializer wrote it; for example ``{"t": "s", "v": "x"}`` is reported as
    ``"x"``. Any other ``t`` fails to decode and is passed through unchanged.
    """
    try:
        parsed = json.loads(raw_result)
    except (json.JSONDecodeError, TypeError, RecursionError):
        return raw_result
    try:
        if not _is_extended_type_envelope(parsed):
            # Plain JSON already; only non-finite numbers (json.loads accepts
            # NaN and Infinity) need mapping to null.
            return _to_json_compatible(parsed)
        try:
            decoded = EXTENDED_TYPES_SERDES.deserialize(raw_result, SerDesContext())
            return _to_json_compatible(decoded)
        except Exception:  # noqa: BLE001 - fall back to the parsed payload
            return _to_json_compatible(parsed)
    except Exception:  # noqa: BLE001 - one bad result never breaks the record
        return raw_result


# An integral Decimal becomes a JSON integer only up to this many digits; a
# larger one keeps its exact string form (see _to_json_compatible).
_MAX_INT_DIGITS = 300


def _to_json_compatible(value: Any) -> Any:
    """Convert a value decoded by ``ExtendedTypeSerDes`` into plain JSON values.

    Non-JSON types follow what the Java SDK's default Jackson serializer writes
    for the equivalent Java types, keeping the numeric value (not the scale:
    ``Decimal("1.10")`` -> ``1.1``): an integral ``Decimal`` of at most 300
    digits -> JSON integer; any other finite ``Decimal`` -> JSON number when
    that float converts back to exactly the same ``Decimal``, otherwise its exact
    string form (``str(value)``, e.g. ``"12345678901234567.89"`` or
    ``"1E+5000"``); ``datetime``
    / ``date`` -> ISO-8601 string (``isoformat()``, offset and precision kept),
    ``UUID`` -> string, ``bytes`` -> base64 string, ``tuple`` -> list. A
    ``BatchResult`` becomes its ``to_dict()`` form. A non-finite ``Decimal`` or
    ``float`` (NaN, Infinity) becomes ``None`` (JSON ``null``), like
    ``JSON.stringify(NaN)`` in JS, so the record stays strict JSON.

    Containers are converted with an explicit stack, so a deeply nested result
    does not hit the interpreter's recursion limit.
    """
    root: list[Any] = [None]
    # (value, target container, key in target), processed in source order.
    pending: list[tuple[Any, Any, Any]] = [(value, root, 0)]
    while pending:
        item, target, key = pending.pop()
        if isinstance(item, BatchResult):
            item = item.to_dict()
        if isinstance(item, (list, tuple)):
            converted_list: list[Any] = [None] * len(item)
            target[key] = converted_list
            pending.extend(
                (child, converted_list, index)
                for index, child in reversed(list(enumerate(item)))
            )
        elif isinstance(item, dict):
            converted_dict: dict[str, Any] = {}
            target[key] = converted_dict
            entries = [(str(k), child) for k, child in item.items()]
            for name, _child in entries:
                converted_dict[name] = None
            pending.extend(
                (child, converted_dict, name) for name, child in reversed(entries)
            )
        else:
            target[key] = _scalar_to_json_compatible(item)
    return root[0]


def _scalar_to_json_compatible(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            return None
        # adjusted() is the exponent of the leading digit, so the bound keeps
        # int() to at most _MAX_INT_DIGITS digits: Decimal("1E+1000000") is 46
        # bytes on the wire but a million-digit int, which takes tens of seconds
        # to build and cannot be written by json (the int-to-str digit limit).
        if (
            value.adjusted() < _MAX_INT_DIGITS
            and value == value.to_integral_value()
            and not (value.is_zero() and value.is_signed())
        ):
            return int(value)
        as_float = float(value)
        if decimal.Decimal(repr(as_float)) == value:
            return as_float
        return str(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    msg = f"Unsupported decoded result type: {type(value)!r}"
    raise TypeError(msg)


# -- error names ----------------------------------------------------------------
#
# The cross-SDK rule for ``error.name`` (execution ``error``, ``operations[].error``
# and ``operationsByName.<name>.error``), spec version 3. The core SDK
# checkpoints the error a wrapper wraps, never the wrapper (it unwraps its own
# wrappers, at most 16, stopping at a repeated cause), so every failed
# operation and the execution carry the user's type. The exporter only reduces
# that type:
#
# * An operation is named by its own ErrorType, reduced (Table A). A callback
#   operation that timed out is ``CallbackTimeoutError``; one failed externally
#   with a missing or blank type is ``CallbackExternalError``. An explicit type,
#   ``"Error"`` included, is the sender's.
# * The execution is named by the reduced type of its own ErrorType. A
#   checkpoint written by an older core can still hold an SDK wrapper type; the
#   wrapper's own name is then reported (rule 9). The exporter never infers a
#   cause from other operations.
#
# Reduction: the text after the last ``.`` or ``$``; an empty segment keeps the
# string as it is. A missing type, or one made only of the six ASCII whitespace
# characters, is ``Error``. Names are never trimmed.


def _qualified(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


_CALLBACK_TIMEOUT_TYPE = _qualified(CallbackTimeoutError)
_CALLBACK_EXTERNAL_TYPE = _qualified(CallbackExternalError)

_DEFAULT_ERROR_NAME = "Error"
# Only these six characters are blank (not str.isspace() or strip()).
_BLANK_CHARACTERS = frozenset(" \t\n\r\f\v")


def _is_blank(type_name: str | None) -> bool:
    return type_name is None or all(ch in _BLANK_CHARACTERS for ch in type_name)


def _simple_type_name(type_name: str | None) -> str:
    """Reduce a type string to the reported ``error.name`` (Table A).

    The text after the last ``.`` or ``$``: ``"__main__.PaymentError"`` and
    ``"pkg.Outer$PaymentError"`` -> ``"PaymentError"``. When that segment is
    empty (``"trailing."``, ``"Outer$"``, ``"."``) the string is kept as it is.
    A missing or empty type, or one made only of space, ``\\t``, ``\\n``,
    ``\\r``, ``\\f`` and ``\\v``, is ``"Error"``. Nothing is trimmed, and other
    whitespace (U+00A0, U+2003, U+FEFF, U+001C, ...) is kept.
    """
    if type_name is None or _is_blank(type_name):
        return _DEFAULT_ERROR_NAME
    segment = type_name[max(type_name.rfind("."), type_name.rfind("$")) + 1 :]
    return segment or type_name


def _operation_error_name(op: OperationInfo) -> str:
    """``error.name`` of a failed operation: its own ErrorType, reduced.

    A callback operation's error comes from the service or the external sender:
    a timeout is named by the SDK's CallbackTimeoutError (the service type
    would reduce to ``Timeout``), and an external failure without a type
    (missing or blank) by the SDK's CallbackExternalError, which is what the
    SDK raises for both.
    """
    error_type = op.error.type if op.error is not None else None
    if op.operation_type == OperationType.CALLBACK:
        if op.status == OperationStatus.TIMED_OUT:
            error_type = _CALLBACK_TIMEOUT_TYPE
        elif _is_blank(error_type):
            error_type = _CALLBACK_EXTERNAL_TYPE
    return _simple_type_name(error_type)


def _execution_error_name(error: ErrorObject) -> str:
    """``error.name`` of the execution: its own ErrorType, reduced.

    A wrapper type (from an older core) reduces to the wrapper's name.
    """
    return _simple_type_name(error.type)


class _ExecutionState:
    __slots__ = ("start_time", "parsed_arn", "cached_input", "operations")

    def __init__(self, start_time: Any, parsed_arn: dict[str, str]) -> None:
        self.start_time = start_time
        self.parsed_arn = parsed_arn
        self.cached_input: Any = None
        # operation_id -> OperationInfo, adopted verbatim from the SDK's
        # authoritative snapshot (invocation start/end and operation-change).
        self.operations: dict[str, OperationInfo] = {}


class DurableExecutionTelemetryPlugin(DurableInstrumentationPlugin):
    def __init__(self, config: DurableExecutionTelemetryConfig) -> None:
        self._sampling_rate = _resolve_sampling_rate(config.sampling_rate)
        # config.emit_mode / operation_detail are already normalized to enum
        # members (or None) by DurableExecutionTelemetryConfig.__post_init__;
        # re-wrap to satisfy the static type of the union-typed config fields.
        self._emit_mode: EmitMode = (
            EmitMode(config.emit_mode)
            if config.emit_mode is not None
            else EmitMode.ON_COMPLETE
        )
        detail = (
            OperationDetail(config.operation_detail)
            if config.operation_detail is not None
            else OperationDetail.TOP_LEVEL
        )
        self._top_level_only = detail != OperationDetail.FULL_TREE
        content: ContentConfig | None = config.content
        self._content = content
        ops = content.operations if content and content.operations else None
        self._include_errors = (
            True if ops is None or ops.include_errors is None else ops.include_errors
        )
        self._overrides_by_name: dict[str, OperationOverride] = {}
        if ops is not None:
            for override in ops.overrides:
                self._overrides_by_name[override.operation_name] = override
        # Default-exporter parity with the JS plugin: an omitted OR an explicitly
        # empty exporter list falls back to the Lambda log exporter, so the
        # plugin is never a silent no-op. A non-empty list is used verbatim.
        self._exporters: list[TelemetryExporter] = (
            list(config.exporters) if config.exporters else [LambdaLogExporter()]
        )
        self._scheduler = _ExportScheduler(self._exporters)
        self._state: dict[str, _ExecutionState] = {}
        self._lock = threading.Lock()

    # -- sampling / state -----------------------------------------------------

    def _sampled_in(self, execution_arn: str) -> bool:
        # Deterministic per-ARN, so every hook for one execution agrees without
        # needing to persist the decision in state.
        return _should_sample(execution_arn, self._sampling_rate)

    def _ensure_state(self, execution_arn: str) -> _ExecutionState:
        with self._lock:
            state = self._state.get(execution_arn)
            if state is None:
                state = _ExecutionState(
                    start_time=datetime.datetime.now(datetime.UTC),
                    parsed_arn=_parse_execution_arn(execution_arn),
                )
                self._state[execution_arn] = state
            return state

    def _discard_state(self, execution_arn: str) -> None:
        with self._lock:
            self._state.pop(execution_arn, None)

    def _adopt_operations(
        self, state: _ExecutionState, operations: dict[str, OperationInfo]
    ) -> None:
        # Adopt the authoritative point-in-time snapshot. Copy so plugin state
        # never aliases the SDK-owned map, and rebind the attribute so a
        # concurrent reader holding the prior reference iterates a stable dict.
        with self._lock:
            state.operations = dict(operations)

    # -- hooks ----------------------------------------------------------------

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        arn = info.execution_arn
        if not arn or not self._sampled_in(arn):
            return
        state = self._ensure_state(arn)
        # Always adopt the service-provided execution start time when present,
        # including a cold resume in a fresh environment (never the resume time,
        # which would corrupt duration and the date partition).
        if info.execution_start_time is not None:
            state.start_time = info.execution_start_time
        state.cached_input = info.execution_input
        # Seed the operation map from the full snapshot on every invocation. On a
        # cold resume this rebuilds prior (terminal) operations that a fresh
        # plugin instance never saw via per-operation hooks.
        self._adopt_operations(state, info.operations)
        if self._emit_mode == EmitMode.ON_CHANGE:
            self._emit(
                arn,
                state,
                status="RUNNING",
                end_time=None,
                output_raw=None,
                error=None,
            )

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        arn = info.execution_arn
        if not arn or not self._sampled_in(arn):
            return
        state = self._ensure_state(arn)
        # Replace state with the full operations snapshot carried by the hook.
        self._adopt_operations(state, info.operations)
        # on-change mode exports an updated RUNNING record on each change so
        # mid-invocation progress is observable, not only at start/end.
        if self._emit_mode == EmitMode.ON_CHANGE:
            self._emit(
                arn,
                state,
                status="RUNNING",
                end_time=None,
                output_raw=None,
                error=None,
            )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        arn = info.execution_arn
        if not arn:
            return
        if not self._sampled_in(arn):
            # Sampled-out executions process no operations and retain no state.
            self._discard_state(arn)
            return
        state = self._ensure_state(arn)
        # Refresh from the fresh end-of-invocation snapshot before emitting so
        # the terminal record reflects the final operation map.
        self._adopt_operations(state, info.operations)
        status = _STATUS_MAP.get(info.status, "RUNNING")
        is_terminal = status in ("SUCCEEDED", "FAILED")
        is_failure = status == "FAILED"

        if self._emit_mode == EmitMode.ON_CHANGE:
            should_emit = True
        elif self._emit_mode == EmitMode.ON_FAILURE:
            should_emit = is_failure
        else:  # on-complete
            should_emit = is_terminal

        if should_emit:
            # Every record built at invocation end carries endTime (the emission
            # time) and durationMs, including a PENDING/RETRY end that maps to
            # RUNNING -- the same rule as the JS plugin's onInvocationEnd
            # (``endTime: new Date()`` for every status). Records built at
            # invocation start or on an operation change carry neither. Output
            # and error still belong only to a terminal record.
            self._emit(
                arn,
                state,
                status=status,
                end_time=datetime.datetime.now(datetime.UTC),
                output_raw=info.execution_result if is_terminal else None,
                error=info.error if is_terminal else None,
            )
            self._scheduler.drain()

        # Clear state after EVERY invocation end, including PENDING/RETRY. The
        # next invocation rebuilds it from InvocationStartInfo.operations, so a
        # suspended execution that never resumes in this environment (or that was
        # sampled out) leaks nothing and state stays bounded.
        self._discard_state(arn)

    # -- emission -------------------------------------------------------------

    def _build_operations(
        self, operations: dict[str, OperationInfo]
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for op in operations.values():
            if op.operation_type == OperationType.EXECUTION:
                continue
            if not op.name:
                continue
            if self._top_level_only and op.parent_id:
                continue
            override = self._overrides_by_name.get(op.name)
            if override is not None and override.exclude:
                continue

            entry: dict[str, Any] = {"id": op.operation_id, "name": op.name}
            entry["type"] = op.operation_type.value
            if op.sub_type is not None:
                entry["subType"] = op.sub_type.value
            if op.parent_id is not None:
                entry["parentId"] = op.parent_id
            entry["status"] = op.status.value if op.status is not None else "UNKNOWN"
            start_iso = _iso(op.start_time)
            if start_iso is not None:
                entry["startTime"] = start_iso
            end_iso = _iso(op.end_time)
            if end_iso is not None:
                entry["endTime"] = end_iso
            dur = _duration_ms(op.start_time, op.end_time)
            if dur is not None:
                entry["durationMs"] = dur
            if op.attempt is not None:
                entry["attempt"] = op.attempt
            if self._include_errors and op.error is not None:
                entry["error"] = {
                    "name": _operation_error_name(op),
                    "message": op.error.message,
                }
            if override is not None and override.result is not None:
                value = _apply_result_override(override.result, op.result)
                if value is not None:
                    entry["result"] = value
            records.append(entry)
        return records

    def _emit(
        self,
        execution_arn: str,
        state: _ExecutionState,
        *,
        status: str,
        end_time: Any,
        output_raw: str | None,
        error: Any,
    ) -> None:
        arn = state.parsed_arn
        start_time = state.start_time
        duration = _duration_ms(start_time, end_time)
        # Snapshot the operations reference once so a concurrent adopt() rebind
        # cannot change the map mid-build.
        operations = state.operations

        content = self._content
        record: dict[str, Any] = {
            "recordType": "DurableExecutionTelemetry",
            "schemaVersion": "1.0",
            "emittedAt": datetime.datetime.now(datetime.UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "executionArn": execution_arn,
        }
        if arn.get("executionName"):
            record["executionName"] = arn["executionName"]
        record["functionName"] = arn.get("functionName", "")
        record["functionQualifier"] = arn.get("qualifier", "")
        record["region"] = arn.get("region", "")
        record["accountId"] = arn.get("accountId", "")
        record["status"] = status
        start_iso = _iso(start_time)
        if start_iso is not None:
            record["startTime"] = start_iso
        end_iso = _iso(end_time)
        if end_iso is not None:
            record["endTime"] = end_iso
        if duration is not None:
            record["durationMs"] = duration

        parsed_output: Any = None
        if output_raw is not None and output_raw != "":
            try:
                parsed_output = json.loads(output_raw)
            except (json.JSONDecodeError, TypeError):
                parsed_output = output_raw
        input_value = _apply_data_content(
            state.cached_input, content.input if content else None
        )
        output_value = _apply_data_content(
            parsed_output, content.output if content else None
        )
        if input_value is not None:
            record["input"] = input_value
        if output_value is not None:
            record["output"] = output_value
        if error is not None:
            record["error"] = {
                "name": _execution_error_name(error),
                "message": error.message,
            }
        record["operations"] = self._build_operations(operations)

        self._scheduler.schedule(record)


def durable_execution_telemetry(
    config: DurableExecutionTelemetryConfig,
) -> DurableExecutionTelemetryPlugin:
    """Create a Durable Execution Telemetry plugin.

    Mirrors the JS ``durableExecutionTelemetry()`` factory.
    """
    return DurableExecutionTelemetryPlugin(config)

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
    except (json.JSONDecodeError, TypeError):
        return raw_result
    if not _is_extended_type_envelope(parsed):
        # Plain JSON already; only non-finite numbers (json.loads accepts NaN
        # and Infinity) need mapping to null.
        return _to_json_compatible(parsed)
    try:
        decoded = EXTENDED_TYPES_SERDES.deserialize(raw_result, SerDesContext())
        return _to_json_compatible(decoded)
    except Exception:  # noqa: BLE001 - fall back to the parsed payload, never raise
        return _to_json_compatible(parsed)


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
    """
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
    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_json_compatible(item) for key, item in value.items()}
    if isinstance(value, BatchResult):
        return _to_json_compatible(value.to_dict())
    msg = f"Unsupported decoded result type: {type(value)!r}"
    raise TypeError(msg)


# -- error names ----------------------------------------------------------------
#
# The cross-SDK rule for ``error.name`` (execution ``error``, ``operations[].error``
# and ``operationsByName.<name>.error``), spec version 2:
#
# 1. A record names the error that caused the failure. A wrapper error that the
#    SDK added is never the name, except in the bounded cases of rules 3 and 9.
# 2. Only the SDK's own wrapper types are unwrapped. A user error is never
#    unwrapped, even if it has a cause.
# 3. Unwrapping follows the chain of SDK wrappers. The depth counts unwrap
#    steps, one per wrapper passed, not levels in the operation tree. At most 16
#    unwraps happen; if the error reached after 16 unwraps is still a wrapper,
#    its name is reported. If the chain reaches an error it has already
#    visited, the walk stops and the last error visited before the repeat is
#    reported.
# 4. A chain that ends at a user error is named by that error's type name.
# 5. A chain that ends at an SDK error that is not a wrapper (for example a
#    callback timeout) is named by that SDK error's simple class name.
# 6. A qualified type string is reduced to its last segment after any ``.`` or
#    ``$``; an empty segment keeps the string as it is. A missing type, or one
#    made only of the six ASCII whitespace characters, is ``Error``. Names are
#    never trimmed.
# 7. A failure gets the same name live and from a replayed checkpoint.
# 8. When an operation's error propagates to the execution, the execution name
#    equals that operation's name.
# 9. When the SDK cannot tell which of several failures caused a wrapper, it
#    does not guess and reports the wrapper's name.
#
# Python hooks only carry checkpointed ``ErrorObject`` values, never a live
# exception, so a wrapper's chain is recovered from the operation tree: the SDK
# rebuilds a wrapper from the failed operation's ErrorObject with the same
# message, data and stack trace (``ErrorObject.raise_as_operation_error``), on
# the first run and on replay alike. One wrapper hand-off is therefore a failed
# operation whose error has the same fields: a direct child, or an item or
# branch of a direct map or parallel child (see ``_ErrorNameIndex._links``).


def _qualified(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


_CALLBACK_TIMEOUT_TYPE = _qualified(CallbackTimeoutError)
_CALLBACK_EXTERNAL_TYPE = _qualified(CallbackExternalError)
_CALLBACK_SUBMITTER_TYPE = _qualified(CallbackSubmitterError)

# Messages the core fills in when the error it re-raises has none. The core
# has no constants for them (``context.py``); the e2e tests fail a callback
# and a submitter with an empty message, so a change in the core is caught.
_CALLBACK_TIMEOUT_MESSAGE = "Callback timed out"
_CALLBACK_FAILED_MESSAGE = "Callback failed"
_CALLBACK_SUBMITTER_MESSAGE = "Callback submitter failed"

# SDK wrapper types, mapped to the operation types whose failure they re-raise
# (``None``: any operation type). A wrapper carries the fields of the error that
# escaped that operation; the cause's type lives on an operation in its subtree.
# Every other type (user errors, and SDK errors such as CallbackTimeoutError,
# SerDesError, ValidationError or BatchCompletionError) is never unwrapped.
_WRAPPER_OPERATION_TYPES: dict[str, frozenset[OperationType] | None] = {
    _qualified(DurableOperationError): None,
    _qualified(StepError): frozenset({OperationType.STEP}),
    _qualified(WaitForConditionError): frozenset({OperationType.STEP}),
    _qualified(InvokeError): frozenset({OperationType.CHAINED_INVOKE}),
    _qualified(ChildContextError): frozenset({OperationType.CONTEXT}),
    # wait_for_callback: the submitter step failed inside the callback context.
    _CALLBACK_SUBMITTER_TYPE: frozenset({OperationType.CONTEXT}),
    # The external system failed the callback: raised from the callback
    # operation directly, or from the wait_for_callback context around it.
    _CALLBACK_EXTERNAL_TYPE: frozenset({OperationType.CONTEXT, OperationType.CALLBACK}),
}

# Operations that run items or branches as child contexts. Their own operation
# is not a wrapper link; a failed item or branch below them is.
_BATCH_SUB_TYPES = frozenset({OperationSubType.MAP, OperationSubType.PARALLEL})

_MAX_UNWRAPS = 16
_DEFAULT_ERROR_NAME = "Error"
# Rule 6: only these six characters are blank (not str.isspace() or strip()).
_BLANK_CHARACTERS = frozenset(" \t\n\r\f\v")

_ErrorKey = tuple[str | None, str | None, tuple[str, ...] | None]


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


def _error_key(error: ErrorObject) -> _ErrorKey:
    stack = error.stack_trace
    return (error.message, error.data, tuple(stack) if stack is not None else None)


def _operation_error_type(op: OperationInfo) -> str | None:
    """The type that names ``op``'s own failure before any unwrapping.

    A callback operation's error comes from the service or the external sender:
    a timeout is named by the SDK's CallbackTimeoutError (the service type
    ``Callback.Timeout`` would reduce to ``Timeout``), and an external failure
    without a type (missing or blank) by the SDK's CallbackExternalError, which
    is what the SDK raises for both. An explicit type, ``"Error"`` included, is
    the sender's.
    """
    error_type = op.error.type if op.error is not None else None
    if op.operation_type == OperationType.CALLBACK:
        if op.status == OperationStatus.TIMED_OUT:
            return _CALLBACK_TIMEOUT_TYPE
        if _is_blank(error_type):
            return _CALLBACK_EXTERNAL_TYPE
    return error_type


def _operation_error_key(op: OperationInfo) -> _ErrorKey | None:
    if op.error is None:
        return None
    message, data, stack = _error_key(op.error)
    if op.operation_type == OperationType.CALLBACK and not message:
        # The SDK fills in a default message when the callback error has none.
        is_timeout = op.status == OperationStatus.TIMED_OUT
        message = _CALLBACK_TIMEOUT_MESSAGE if is_timeout else _CALLBACK_FAILED_MESSAGE
    return (message, data, stack)


class _ErrorNameIndex:
    """Resolves ``error.name`` for one record build.

    Built once per record from the operation snapshot in O(n): children by
    parent, error keys by operation, and failed operations by key. An
    operation's name is resolved by following its wrapper links (``_links``)
    with a budget of ``_MAX_UNWRAPS`` unwraps. Each (operation, remaining
    budget) result is cached, so an operation is resolved at most
    ``_MAX_UNWRAPS + 1`` times per record and a record build stays linear in
    the number of operations.
    """

    __slots__ = ("_operations", "_children", "_keys", "_by_key", "_links_of", "_names")

    def __init__(self, operations: dict[str, OperationInfo]) -> None:
        self._operations = operations
        self._children: dict[str, list[OperationInfo]] = {}
        self._keys: dict[str, _ErrorKey | None] = {}
        self._by_key: dict[_ErrorKey, list[OperationInfo]] = {}
        self._links_of: dict[str, list[OperationInfo]] = {}
        self._names: dict[tuple[str, int], str] = {}
        for op in operations.values():
            if op.parent_id is not None:
                self._children.setdefault(op.parent_id, []).append(op)
            key = _operation_error_key(op)
            self._keys[op.operation_id] = key
            if key is not None:
                self._by_key.setdefault(key, []).append(op)

    def _links(self, op: OperationInfo) -> list[OperationInfo]:
        """The operations ``op``'s wrapper hands off to: one unwrap each.

        A failed direct child with the same error fields, or a failed item or
        branch with those fields under a direct map or parallel child (the
        batch operation is a tree level but not a wrapper). Other children are
        not descended into, so a failure that user code handled inside the
        subtree (in a child context that then succeeded) is never a cause.
        """
        links = self._links_of.get(op.operation_id)
        if links is not None:
            return links
        key = self._keys[op.operation_id]
        links = []
        for child in self._children.get(op.operation_id, ()):
            if self._keys[child.operation_id] == key:
                links.append(child)
            elif child.sub_type in _BATCH_SUB_TYPES:
                links.extend(
                    item
                    for item in self._children.get(child.operation_id, ())
                    if self._keys[item.operation_id] == key
                )
        self._links_of[op.operation_id] = links
        return links

    def _resolve(
        self, op: OperationInfo, budget: int, visiting: set[str]
    ) -> tuple[str, bool]:
        """``op``'s name with ``budget`` unwraps left, and whether a cycle was hit.

        A result that depends on a cycle depends on where the walk entered it,
        so it is not cached.
        """
        error_type = _operation_error_type(op)
        own_name = _simple_type_name(error_type)
        if budget == 0 or error_type not in _WRAPPER_OPERATION_TYPES:
            return own_name, False
        cache_key = (op.operation_id, budget)
        cached = self._names.get(cache_key)
        if cached is not None:
            return cached, False
        visiting.add(op.operation_id)
        names: set[str] = set()
        cyclic = False
        for link in self._links(op):
            if link.operation_id in visiting:
                # A repeat: report the last error visited before it (``op``).
                names.add(own_name)
                cyclic = True
                continue
            name, link_cyclic = self._resolve(link, budget - 1, visiting)
            names.add(name)
            cyclic = cyclic or link_cyclic
        visiting.discard(op.operation_id)
        # No link: the chain cannot be followed (the error reached is reported).
        # Several distinct names: ambiguous, do not guess (rule 9).
        result = next(iter(names)) if len(names) == 1 else own_name
        if not cyclic:
            self._names[cache_key] = result
        return result, cyclic

    def operation_name(self, op: OperationInfo) -> str:
        """``error.name`` of a failed operation."""
        return self._resolve(op, _MAX_UNWRAPS, set())[0]

    def _has_candidate_ancestor(self, op: OperationInfo, candidates: set[str]) -> bool:
        seen = {op.operation_id}
        parent_id = op.parent_id
        while parent_id is not None and parent_id not in seen:
            if parent_id in candidates:
                return True
            seen.add(parent_id)
            parent = self._operations.get(parent_id)
            parent_id = parent.parent_id if parent is not None else None
        return False

    def execution_name(self, error: ErrorObject) -> str:
        """``error.name`` of the execution error.

        A wrapper is the SDK re-raising the failure of the operation it
        propagated from: a failed operation of the wrapper's operation type
        whose error carries the same fields. Only the outermost such operations
        count (one whose ancestors include another is part of that one's
        chain), and the execution takes their name (rule 8). This does not add
        an unwrap, so the rule still holds at the depth bound. A failure handled
        elsewhere in the execution has no such operation and is never used.
        """
        error_type = error.type
        if error_type not in _WRAPPER_OPERATION_TYPES:
            return _simple_type_name(error_type)
        allowed = _WRAPPER_OPERATION_TYPES[error_type]
        keys = [_error_key(error)]
        message, data, stack = keys[0]
        if (
            error_type == _CALLBACK_SUBMITTER_TYPE
            and message == _CALLBACK_SUBMITTER_MESSAGE
        ):
            # The submitter step failed with no message; the core re-raised it
            # with its default message, and the context keeps the empty one.
            keys += [("", data, stack), (None, data, stack)]
        candidates = [
            op
            for key in keys
            for op in self._by_key.get(key, ())
            if allowed is None or op.operation_type in allowed
        ]
        ids = {op.operation_id for op in candidates}
        outermost = [
            op for op in candidates if not self._has_candidate_ancestor(op, ids)
        ]
        # Every candidate has a candidate ancestor only in a parent-id cycle.
        names = {self.operation_name(op) for op in outermost or candidates}
        if len(names) == 1:
            return names.pop()
        return _simple_type_name(error_type)


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
        self,
        operations: dict[str, OperationInfo],
        error_names: _ErrorNameIndex | None = None,
    ) -> list[dict[str, Any]]:
        # One index per record build keeps error.name resolution linear in the
        # number of operations (see _ErrorNameIndex).
        if error_names is None:
            error_names = _ErrorNameIndex(operations)
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
                    "name": error_names.operation_name(op),
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
        error_names = _ErrorNameIndex(operations)
        if error is not None:
            record["error"] = {
                "name": error_names.execution_name(error),
                "message": error.message,
            }
        record["operations"] = self._build_operations(operations, error_names)

        self._scheduler.schedule(record)


def durable_execution_telemetry(
    config: DurableExecutionTelemetryConfig,
) -> DurableExecutionTelemetryPlugin:
    """Create a Durable Execution Telemetry plugin.

    Mirrors the JS ``durableExecutionTelemetry()`` factory.
    """
    return DurableExecutionTelemetryPlugin(config)

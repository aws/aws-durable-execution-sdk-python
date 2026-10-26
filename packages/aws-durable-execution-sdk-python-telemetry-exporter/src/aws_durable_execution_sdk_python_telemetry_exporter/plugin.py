# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Durable Execution Telemetry instrumentation plugin for the Durable Execution Python SDK.

Port of the JS ``durableExecutionTelemetry()`` (``aws-durable-execution-sdk-js-telemetry-exporter/src/
index.ts``). It listens to the SDK's instrumentation hooks and emits one curated
``DurableExecutionTelemetry`` record per execution to the configured exporters. The wire
record keeps the JS camelCase field names so records read identically across
SDKs.

Two lifetimes:
  ``durable_execution_telemetry(config)`` returns a FACTORY, which is what
  ``@durable_execution(plugins=[...])`` takes. The factory lives as long as the
  handler and owns everything that is not per-execution: the resolved immutable
  config, the exporters, and the ``_ExportScheduler`` that serializes export
  across executions. The SDK calls its ``create_plugin`` once per invocation and
  drops the instance it returns when the invocation scope exits, so a
  :class:`DurableExecutionTelemetryPlugin` instance serves exactly one invocation of one
  execution. Everything this environment holds for that execution is therefore
  ordinary instance state: no ARN-keyed registry, and no hook can reach an
  instance other than its own.

Operation-map sourcing:
  The Python SDK invocation hooks carry the full operation map directly:
  ``InvocationStartInfo.operations`` (a point-in-time snapshot at invocation
  start), ``InvocationEndInfo.operations`` (a fresh snapshot at invocation end),
  and ``OperationChangeInfo.operations`` (the full map at the change). Alongside
  them the invocation hooks carry ``execution_arn``, ``execution_start_time``,
  ``execution_input`` and ``execution_result``. This plugin reads those
  snapshots as the authoritative operation state -- it does NOT reconstruct the
  map by accumulating per-operation ``on_operation_end`` events. Because every
  invocation start re-seeds the map from the snapshot, a cold resume in a fresh
  Lambda environment (a brand-new instance, as every invocation now gets) still
  reports the prior terminal operations.

  The Python SDK has no ``pluginsConfig.childOperationsDepth`` equivalent, so
  ``full-tree`` records rely on the child operations being present in the
  emitting invocation's snapshot (true for single-invocation and warm-resume
  cases).
"""

from __future__ import annotations

import base64
import contextlib
import datetime
import decimal
import json
import math
import re
import threading
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from aws_durable_execution_sdk_python.concurrency.models import BatchResult
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
    _ExportState,
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

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import ErrorObject


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


def _format_record_timestamp(ts: datetime.datetime) -> str:
    """Format a record timestamp as ``YYYY-MM-DDTHH:MM:SS.sssZ`` in UTC.

    Stores such as DynamoDB compare these values as text. Text order equals
    time order only when every value has the same width. Plain ``isoformat()``
    writes 6 fraction digits, and no fraction when microseconds are 0. So this
    always writes exactly 3 fraction digits and a literal ``Z``. JavaScript's
    ``Date.prototype.toISOString()`` writes the same format.
    ``timespec="milliseconds"`` truncates the microseconds; it does not round.
    """
    return (
        ts.astimezone(datetime.UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
_ONE_MS = datetime.timedelta(milliseconds=1)


def _utc_now_ms() -> int:
    """Return the current UTC time in whole epoch milliseconds, truncated."""
    return (datetime.datetime.now(datetime.UTC) - _EPOCH) // _ONE_MS


def _iso(ts: Any) -> str | None:
    if isinstance(ts, datetime.datetime):
        return _format_record_timestamp(ts)
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
    primitive lists as plain JSON, and everything else as a ``{"t": ..., "v":
    ...}`` envelope. An envelope is decoded with that default serializer and
    converted to plain JSON values (see ``_to_json_compatible``). A payload that
    is not an envelope is parsed JSON, or the raw string when it is not JSON;
    its tree is only walked when it holds NaN, an infinite number, or a run of
    16 or more digits (a possible integer beyond ``2**53 - 1``). An
    envelope that fails to decode falls back to the parsed payload. Never
    raises.

    The hooks do not say which serializer wrote a result, so the envelope is
    recognised by shape alone: a result written by ``JsonSerDes`` or a custom
    ``SerDes`` that has this exact shape is decoded too. Only the core's known
    tags are decoded, and the core may coerce ``v`` while decoding (for
    example ``{"t": "b", "v": "yes"}`` becomes ``True``). An unknown tag fails
    to decode and passes through unchanged.
    """
    non_finite = False

    def parse_constant(name: str) -> float:
        nonlocal non_finite
        non_finite = True
        return float(name)

    def parse_float(text: str) -> float:
        nonlocal non_finite
        number = float(text)
        if not math.isfinite(number):  # for example 1e999
            non_finite = True
        return number

    try:
        parsed = json.loads(
            raw_result, parse_constant=parse_constant, parse_float=parse_float
        )
    except (ValueError, TypeError, RecursionError):
        return raw_result
    try:
        if _is_extended_type_envelope(parsed):
            try:
                decoded = EXTENDED_TYPES_SERDES.deserialize(raw_result, SerDesContext())
                return _to_json_compatible(decoded)
            except Exception:  # noqa: BLE001 - fall back to the parsed payload
                pass
        # Plain JSON is already JSON-compatible, except NaN, Infinity and
        # integers beyond 2**53 - 1. Such an integer has at least 16 digits;
        # the C-speed scan may also match digits in a string or a float, which
        # only costs a walk that leaves them unchanged.
        if non_finite or _LONG_DIGIT_RUN.search(raw_result):
            return _to_json_compatible(parsed)
        return parsed
    except Exception:  # noqa: BLE001 - one bad result never breaks the record
        return raw_result


# Largest integer that every JSON reader (including JavaScript) parses exactly.
_MAX_SAFE_INTEGER = 2**53 - 1
# 2**53 - 1 has 16 digits, so its leading digit's exponent is 15.
_MAX_SAFE_INTEGER_ADJUSTED = 15
_LONG_DIGIT_RUN = re.compile(r"\d{16,}")
# DynamoDB's number range, the narrowest of the record destinations: a
# magnitude from 1E-130 to 9.99...E+125. Its 38-significant-digit limit needs
# no check: a float written exactly has at most 17 significant digits.
_MAX_NUMBER_ADJUSTED = 125
_MIN_NUMBER_ADJUSTED = -130


def _to_json_compatible(value: Any) -> Any:
    """Convert a decoded result into plain JSON values, without recursion.

    ``int``: unchanged when ``abs(n) <= 2**53 - 1``, else ``str(n)``.
    ``Decimal``: an int when integral and ``abs(n) <= 2**53 - 1``, else a float
    when it round-trips exactly and is within DynamoDB's number range
    (magnitude 1E-130 to 9.99...E+125), else ``str(value)``; NaN and Infinity
    become ``None``. Every zero, of any sign or exponent, becomes ``0``. ``datetime`` / ``date`` -> ``isoformat()``, ``UUID`` -> string,
    ``bytes`` -> base64, ``tuple`` -> list, ``BatchResult`` -> ``to_dict()``.
    Non-finite floats become ``None``.
    """
    root: list[Any] = [None]
    # (value, target container, key in target)
    pending: list[tuple[Any, Any, Any]] = [(value, root, 0)]
    while pending:
        item, target, key = pending.pop()
        if isinstance(item, BatchResult):
            item = item.to_dict()
        if isinstance(item, (list, tuple)):
            converted_list: list[Any] = [None] * len(item)
            target[key] = converted_list
            pending.extend(
                (child, converted_list, index) for index, child in enumerate(item)
            )
        elif isinstance(item, dict):
            converted_dict: dict[str, Any] = {}
            target[key] = converted_dict
            for name, child in item.items():
                converted_dict[str(name)] = None
                pending.append((child, converted_dict, str(name)))
        else:
            target[key] = _scalar_to_json_compatible(item)
    return root[0]


def _scalar_to_json_compatible(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        # Beyond 2**53 - 1 a JavaScript reader would round it.
        return value if abs(value) <= _MAX_SAFE_INTEGER else str(value)
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            return None
        # A zero of any sign or exponent ("-0E+20", "0E+16") is integer 0.
        # Its adjusted() can be large, so the integer check below would miss it.
        if value.is_zero():
            return 0
        # adjusted() is checked first so Decimal("1E+1000000") never builds a
        # million-digit int.
        if (
            value.adjusted() <= _MAX_SAFE_INTEGER_ADJUSTED
            and value == value.to_integral_value()
        ):
            as_int = int(value)
            if abs(as_int) <= _MAX_SAFE_INTEGER:
                return as_int
        if not (_MIN_NUMBER_ADJUSTED <= value.adjusted() <= _MAX_NUMBER_ADJUSTED):
            return str(value)
        as_float = float(value)
        written = decimal.Decimal(repr(as_float))
        if math.isfinite(as_float) and written == value:
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


# A longer error name or message keeps its first 8,192 code points followed
# by "…". A Python str is a sequence of code points, so len() and slicing
# count code points and never split a surrogate pair.
_MAX_ERROR_TEXT_CHARS = 8192


def _cap(text: Any) -> Any:
    if isinstance(text, str) and len(text) > _MAX_ERROR_TEXT_CHARS:
        return text[:_MAX_ERROR_TEXT_CHARS] + "\u2026"
    return text


def _error_shape(error: ErrorObject) -> dict[str, Any]:
    """Return an error as ``{name, message}`` in that key order.

    The plugin hooks carry an ``ErrorObject`` (type, message, data, stack
    trace) and no cause, so records written by this SDK never have ``cause``.
    The name is the SDK's ErrorType, otherwise unchanged. A name or message
    longer than 8,192 code points is cut to its first 8,192 code points
    followed by ``…``.
    """
    return {"name": _cap(error.type), "message": _cap(error.message)}


@dataclass(frozen=True)
class _EmissionSnapshot:
    """One admitted build's stable operation map and ordering token.

    The plugin owns the map: adoption copies the SDK snapshot and only rebinds
    ``_operations`` afterwards, so an in-flight build keeps its original map.
    Values retain their existing shallow-snapshot semantics; customer data is
    never deep-copied while holding the invocation lock.
    """

    operations: Mapping[str, OperationInfo]
    revision: int
    closing: bool
    emitted_at: str


class _HookFrames(threading.local):
    """Nested plugin hook frames on one thread, and the drains they owe.

    Per thread, and shared by every plugin instance on that thread. Customer
    callbacks can reach hooks on any live instance. Nested hooks defer drains
    until the outermost callback has finished building or discarding its record,
    and until references displaced by locked hand-offs can be released safely.

    ``threading.local`` runs ``__init__`` once per thread, so each thread gets its
    own counter and its own list.
    """

    def __init__(self) -> None:
        self.depth = 0
        self.pending: list[DurableExecutionTelemetryPlugin] = []
        # Records displaced from the scheduler's pending slots, held until every
        # plugin lock on this thread is released. Releasing one can run a customer
        # finalizer, and a finalizer that reaches another execution's plugin must
        # not do so while this thread holds a plugin lock.
        self.releases: list[Any] = []


_hook_frames = _HookFrames()


class DurableExecutionTelemetryPlugin(DurableInstrumentationPlugin, _ExportState):
    """Everything this environment holds for one invocation of one execution.

    Built by the factory ``durable_execution_telemetry()`` returns, once per invocation,
    from that invocation's ``InvocationStartInfo`` -- the same object its
    ``on_invocation_start`` then receives. Identity comes from that info and is
    never revised afterwards: the execution ARN, the sampling decision, the
    execution start time and the cached input.

    Per-execution state is plain instance state, and the object doubles as its
    own export queue entry (via :class:`_ExportState`). Where three ARN-keyed
    structures had to agree about one execution -- this plugin's registry, the
    scheduler's pending record and its per-execution lane -- there is now one
    object and no ARN to resolve. In Java the same three-view shape produced a
    defect where two of the views disagreed.

    Two locks, disjoint field sets, and neither is ever taken to reach the
    other's fields:

    * ``_lock`` guards ``_closed``, ``_build_revision``, ``_last_emitted_ms``,
      the ``_operations`` rebind and the snapshot-admission/scheduler-handoff
      boundaries. Record building and customer transforms run outside this
      lock.
    * ``_ExportScheduler._condition``'s lock guards the export bookkeeping this
      instance carries for the scheduler; nothing outside the scheduler reads or
      writes those fields, and the scheduler never touches the fields above.

    Shared, handler-lifetime state -- resolved config, exporters, scheduler --
    lives on ``_shared`` and is read-only from here.
    """

    def __init__(
        self, shared: DurableExecutionTelemetryPluginFactory, info: InvocationStartInfo
    ) -> None:
        _ExportState.__init__(self)
        self._shared = shared
        execution_arn = info.execution_arn or ""
        self._execution_arn = execution_arn
        # Deterministic per-ARN, so the decision could be recomputed on every
        # hook; taken once here because the instance now has a place to keep it,
        # and because an unsampled instance then never parses the ARN.
        self._sampled_in = bool(execution_arn) and _should_sample(
            execution_arn, shared._sampling_rate
        )
        self._parsed_arn: dict[str, str] = (
            _parse_execution_arn(execution_arn) if self._sampled_in else {}
        )
        # Always the service-provided execution start time when present,
        # including on a cold resume in a fresh environment -- never the resume
        # time, which would corrupt duration and the date partition. `now` is the
        # fallback for an info that carries no start time at all.
        self._start_time: Any = (
            info.execution_start_time
            if info.execution_start_time is not None
            else datetime.datetime.now(datetime.UTC)
        )
        self._cached_input: Any = info.execution_input
        # operation_id -> OperationInfo, adopted verbatim from the SDK's
        # authoritative snapshot (invocation start/end and operation-change).
        self._operations: dict[str, OperationInfo] = {}
        # Set once this invocation has ended. A hook that arrives afterwards
        # must emit nothing (mirrors the Java ExecutionState.closed flag). The
        # SDK delivers every pending operation-change before the end hook; the
        # guard covers a caller that does not.
        self._closed = False
        # A duplicate End must wait for the admitted closing build before its
        # drain captures scheduler state. Nested callbacks defer that wait to
        # their outermost hook frame, after their own builds have unwound.
        self._closing_complete = threading.Event()
        # Every non-closing build reserves its revision together with its
        # operation snapshot. Customer callbacks can start a newer build on this
        # or another thread, so the hand-off must reject an overtaken snapshot.
        self._build_revision = 0
        # The epoch milliseconds of the last emittedAt this instance assigned.
        # None until the first admitted build.
        self._last_emitted_ms: int | None = None
        # Only snapshot admission and the checked scheduler hand-off are locked.
        # Holding this lock across a customer callback deadlocks two invocations
        # whose transforms synchronously call each other's plugin hooks.
        # Reentrancy is still needed for schedule()'s synchronous worker-start
        # failure diagnostic: a logging handler can re-enter a hook while the
        # checked hand-off is on this thread's stack.
        self._lock = threading.RLock()

    # -- state ----------------------------------------------------------------

    def _adopt_operations_locked(self, operations: dict[str, OperationInfo]) -> None:
        # Adopt the authoritative point-in-time snapshot. Copy so plugin state
        # never aliases the SDK-owned map, and rebind the attribute so a
        # concurrent reader holding the prior reference iterates a stable dict.
        # Callers hold self._lock.
        # Keep the replaced map alive until no plugin lock is held: an operation
        # can retain customer objects whose finalizers re-enter another hook.
        _hook_frames.releases.append(self._operations)
        self._operations = dict(operations)

    def _snapshot_emission_locked(self, *, closing: bool = False) -> _EmissionSnapshot:
        """Reserve the build while holding the same lock used to adopt its map."""
        revision = 0
        if not closing:
            self._build_revision += 1
            revision = self._build_revision
        return _EmissionSnapshot(
            self._operations, revision, closing, self._next_emitted_at_locked()
        )

    def _next_emitted_at_locked(self) -> str:
        """Return the emittedAt for the build being admitted.

        1. ``emittedAt`` has millisecond precision.
        2. In ``on-change`` mode, one invocation can admit two builds in the
           same millisecond. An example is RUNNING, then the terminal record.
        3. The SQS FIFO deduplication id is ``executionArn:emittedAt``. The
           default DynamoDB sort key is ``emittedAt``, under the partition key
           ``executionArn``.
        4. So two records with an equal ``emittedAt`` make SQS drop the second
           message, and make DynamoDB overwrite the first item.
        5. So ``emittedAt`` strictly increases within this instance. This
           instance serves one invocation.

        The value is the current time when it is later than the last value.
        Otherwise it is the last value plus 1 millisecond. Builds on different
        threads can be admitted concurrently. So callers hold ``self._lock``,
        which serializes the admissions that read and write the last value.
        """
        now_ms = _utc_now_ms()
        last = self._last_emitted_ms
        ms = now_ms if last is None or now_ms > last else last + 1
        self._last_emitted_ms = ms
        return _format_record_timestamp(_EPOCH + ms * _ONE_MS)

    # -- hooks ----------------------------------------------------------------

    @contextlib.contextmanager
    def _hook_frame(self) -> Iterator[None]:
        """Defer nested End drains and customer-reference release to hook exit.

        A transform can re-enter End before its outer build has finished. That
        End's drain belongs at the outermost frame, after the closing record is
        scheduled and any stale outer record is discarded. Displaced records and
        operation maps are retained until then so their customer finalizers never
        run while a plugin or scheduler lock is held.

        Frames are shared by every plugin on this thread: a callback may re-enter
        a different invocation, whose drain and retained references follow the
        same ordering rule.
        """
        state = _hook_frames
        state.depth += 1
        try:
            yield
        finally:
            state.depth -= 1
            if state.depth == 0:
                # Released before the drains, and with no lock held: a finalizer
                # that schedules a record is then covered by the drain that
                # follows it.
                if state.releases:
                    state.releases.clear()
                if state.pending:
                    owed, state.pending = state.pending, []
                    for plugin in owed:
                        plugin._drain()

    def _request_drain(self) -> None:
        """Ask for a drain once the outermost hook frame on this thread unwinds."""
        pending = _hook_frames.pending
        if not any(plugin is self for plugin in pending):
            pending.append(self)

    def _drain(self) -> None:
        scheduler = self._shared._scheduler
        with self._lock:
            closing = self._closed
        # The export worker cannot wait on customer code that may itself be
        # waiting for an export. Let the scheduler retain its existing refusal
        # and deferred-flush behavior for exporter-driven hook reentry.
        if closing and not scheduler._is_export_worker():
            self._closing_complete.wait()
        scheduler.drain(self)

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        if not self._sampled_in:
            return
        snapshot: _EmissionSnapshot | None = None
        with self._hook_frame():
            with self._lock:
                if self._closed:
                    return
                # A cold resume adopts the service's complete operation map.
                self._adopt_operations_locked(info.operations)
                if self._shared._emit_mode == EmitMode.ON_CHANGE:
                    snapshot = self._snapshot_emission_locked()
            if snapshot is not None:
                self._emit(
                    snapshot,
                    status="RUNNING",
                    end_time=None,
                    output_raw=None,
                    error=None,
                )

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        if not self._sampled_in:
            return
        snapshot: _EmissionSnapshot | None = None
        with self._hook_frame():
            with self._lock:
                if self._closed:
                    return
                self._adopt_operations_locked(info.operations)
                if self._shared._emit_mode == EmitMode.ON_CHANGE:
                    snapshot = self._snapshot_emission_locked()
            if snapshot is not None:
                self._emit(
                    snapshot,
                    status="RUNNING",
                    end_time=None,
                    output_raw=None,
                    error=None,
                )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        if not self._sampled_in:
            # Sampled-out executions process nothing: they neither export nor
            # flush, so instrumenting a fraction of executions costs the rest
            # nothing.
            return
        emit_mode = self._shared._emit_mode
        with self._hook_frame():
            # The drain is registered before anything can fail, not after the
            # record is built. `_emit` runs customer code -- the content and
            # result transforms, and `__del__` on an object a displaced record
            # carried -- and a failure there leaves this hook by way of the SDK's
            # containment. Registering afterwards meant such a failure skipped
            # the drain, so records this execution had already scheduled stayed
            # in a buffering exporter when the environment froze. Registering
            # here costs nothing when the hook succeeds: the frame runs the drain
            # once, on the way out, either way.
            self._request_drain()
            owns_closing = False
            try:
                snapshot: _EmissionSnapshot | None = None
                end_time: datetime.datetime | None = None
                with self._lock:
                    if not self._closed:
                        owns_closing = True
                        # Close admission before running any customer code. The
                        # closing snapshot alone may pass the now-closed gate.
                        self._closed = True
                        self._adopt_operations_locked(info.operations)
                        status = _STATUS_MAP.get(info.status, "RUNNING")
                        is_terminal = status in ("SUCCEEDED", "FAILED")
                        is_failure = status == "FAILED"

                        if emit_mode == EmitMode.ON_CHANGE:
                            should_emit = True
                        elif emit_mode == EmitMode.ON_FAILURE:
                            should_emit = is_failure
                        else:
                            should_emit = is_terminal

                        if should_emit:
                            snapshot = self._snapshot_emission_locked(closing=True)
                            if is_terminal:
                                end_time = datetime.datetime.now(datetime.UTC)
                if snapshot is not None:
                    self._emit(
                        snapshot,
                        status=status,
                        end_time=end_time,
                        output_raw=info.execution_result if is_terminal else None,
                        error=info.error if is_terminal else None,
                    )

            finally:
                if owns_closing:
                    # Also release duplicate Ends when a customer build fails:
                    # earlier records still get their normal end-of-hook drain.
                    self._closing_complete.set()

            # Nothing has to be cleared after an invocation end, including a
            # PENDING/RETRY one: this instance IS the state, and the SDK drops it
            # when the invocation scope exits. A suspended execution that resumes
            # here later gets a fresh instance, seeded from
            # InvocationStartInfo.operations.
            #
            # Drain on EVERY sampled-in invocation end, emitted record or not: JS and
            # Java flush once per sampled-in invocation end regardless, and a
            # buffering exporter has to see the same rhythm in all three languages
            # (an on-failure/on-complete mode that emits nothing for this invocation
            # may still be holding records another execution handed it). A sampled-out
            # execution returns above, so it neither exports nor flushes.
            #
            # The drain captures a finite snapshot of work already scheduled,
            # including this invocation's record. A newer snapshot coalesced into
            # an existing pending slot fulfills that slot's older obligation;
            # later work on a slot the worker already took cannot extend it.
            #
            # Defer the drain until the outermost callback unwinds, its stale
            # builds are discarded and retained customer references are released.
            # Registering it before the build ensures callback failures cannot
            # skip the invocation's flush.

    # -- emission -------------------------------------------------------------

    def _build_operations(
        self, operations: Mapping[str, OperationInfo]
    ) -> list[dict[str, Any]]:
        shared = self._shared
        records: list[dict[str, Any]] = []
        for op in operations.values():
            if op.operation_type == OperationType.EXECUTION:
                continue
            if not op.name:
                continue
            if shared._top_level_only and op.parent_id:
                continue
            override = shared._overrides_by_name.get(op.name)
            if override is not None and override.exclude:
                continue

            entry: dict[str, Any] = {"id": op.operation_id, "name": op.name}
            entry["type"] = op.operation_type.value
            if op.sub_type is not None:
                entry["subType"] = op.sub_type
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
            if shared._include_errors and op.error is not None:
                entry["error"] = _error_shape(op.error)
            if override is not None and override.result is not None:
                value = _apply_result_override(override.result, op.result)
                if value is not None:
                    entry["result"] = value
            records.append(entry)
        return records

    def _emit(
        self,
        snapshot: _EmissionSnapshot,
        *,
        status: str,
        end_time: Any,
        output_raw: str | None,
        error: Any,
    ) -> None:
        arn = self._parsed_arn
        start_time = self._start_time
        duration = _duration_ms(start_time, end_time)
        # The immutable invocation identity and this admitted map remain stable
        # while customer callbacks run without holding either internal lock.
        operations = snapshot.operations

        content = self._shared._content
        record: dict[str, Any] = {
            "recordType": "DurableExecutionTelemetry",
            "schemaVersion": "1.0",
            "emittedAt": snapshot.emitted_at,
            "executionArn": self._execution_arn,
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
            self._cached_input, content.input if content else None
        )
        output_value = _apply_data_content(
            parsed_output, content.output if content else None
        )
        if input_value is not None:
            record["input"] = input_value
        if output_value is not None:
            record["output"] = output_value
        if error is not None:
            record["error"] = _error_shape(error)
        record["operations"] = self._build_operations(operations)

        # Admission and hand-off use one lock, but building does not. A newer
        # build or End may have overtaken these customer callbacks. Check and
        # schedule atomically so a stale RUNNING record cannot replace a newer
        # snapshot or follow the closing record.
        with self._lock:
            if not snapshot.closing and (
                self._closed or snapshot.revision != self._build_revision
            ):
                _hook_frames.releases.append(record)
                return
            # The closing build is admitted exactly once after setting _closed.
            # It must reach the scheduler even though that gate is now closed.
            # schedule() returns displaced references rather than releasing them
            # under either lock; the outer hook frame releases them before drain.
            _hook_frames.releases.extend(self._shared._scheduler.schedule(self, record))


class DurableExecutionTelemetryPluginFactory:
    """The handler-lifetime half of the plugin: what is NOT per-execution.

    Built by :func:`durable_execution_telemetry`, which is the supported way to obtain one.
    The class is public because it is the declared return type of that function,
    and a ``py.typed`` consumer must be able to name the type it holds.

    Satisfies the SDK's ``DurableInstrumentationPluginFactory`` -- its
    ``create_plugin`` is called with an ``InvocationStartInfo`` and returns the
    plugin instance for that invocation. Everything it holds is either immutable
    after construction (the resolved config) or deliberately shared across
    executions:

    * the exporters, which are customer objects registered once, and
    * the ``_ExportScheduler``, because export serialization is cross-execution:
      one worker, one ``export()`` at a time, whatever the instance that
      scheduled the record.

    A class rather than a closure so the resolved config stays inspectable
    (``factory._emit_mode``, ``factory._exporters``) instead of being buried in
    cell variables.
    """

    def __init__(self, config: DurableExecutionTelemetryConfig) -> None:
        self._sampling_rate = _resolve_sampling_rate(config.sampling_rate)
        # config.emit_mode / operation_detail are already normalized to enum
        # members (or None) by DurableExecutionTelemetryConfig.__post_init__; re-wrap to
        # satisfy the static type of the union-typed config fields.
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

    def create_plugin(
        self, info: InvocationStartInfo
    ) -> DurableExecutionTelemetryPlugin:
        return DurableExecutionTelemetryPlugin(self, info)


def durable_execution_telemetry(
    config: DurableExecutionTelemetryConfig,
) -> DurableExecutionTelemetryPluginFactory:
    """Create a Durable Execution Telemetry plugin factory. Mirrors the JS ``durableExecutionTelemetry()``.

    Pass the result straight to ``@durable_execution(plugins=[...])``: the SDK
    calls its ``create_plugin`` once per invocation to build that invocation's
    plugin instance.
    """
    return DurableExecutionTelemetryPluginFactory(config)

# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Workflow Insight instrumentation plugin for the Durable Execution Python SDK.

Port of the JS ``workflowInsight()`` (``aws-durable-execution-sdk-js-insight/src/
index.ts``). It listens to the SDK's instrumentation hooks and emits one curated
``WorkflowInsight`` record per execution to the configured exporters. The wire
record keeps the JS camelCase field names so records read identically across
SDKs.

Two lifetimes:
  ``workflow_insight(config)`` returns a FACTORY, which is what
  ``@durable_execution(plugins=[...])`` takes. The factory lives as long as the
  handler and owns everything that is not per-execution: the resolved immutable
  config, the exporters, and the ``_ExportScheduler`` that serializes export
  across executions. The SDK calls its ``create_plugin`` once per invocation and
  drops the instance it returns when the invocation scope exits, so a
  :class:`WorkflowInsightPlugin` instance serves exactly one invocation of one
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

import contextlib
import datetime
import json
import math
import threading
from collections.abc import Iterator
from typing import Any, Callable

from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationInfo,
    OperationType,
)

from aws_durable_execution_sdk_python_insight._export_scheduler import (
    _ExportScheduler,
    _ExportState,
)
from aws_durable_execution_sdk_python_insight.exporters.lambda_log_exporter import (
    LambdaLogExporter,
)
from aws_durable_execution_sdk_python_insight.types import (
    ContentConfig,
    EmitMode,
    InsightExporter,
    OperationDetail,
    OperationOverride,
    WorkflowInsightConfig,
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
    try:
        parsed = json.loads(raw_result)
    except (json.JSONDecodeError, TypeError):
        parsed = raw_result
    try:
        return transform(parsed)
    except Exception:  # noqa: BLE001 - untrusted transform must never break emission
        return None


class _HookFrames(threading.local):
    """Nested plugin hook frames on one thread, and the drains they owe.

    Per thread, and shared by every plugin instance on that thread. A hook runs
    customer code while holding a plugin's ``_lock``, and that code can call a
    hook on *any* live instance, so the frame that must run a deferred drain is
    the outermost one on the thread whatever instance it belongs to.

    ``threading.local`` runs ``__init__`` once per thread, so each thread gets its
    own counter and its own list.
    """

    def __init__(self) -> None:
        self.depth = 0
        self.pending: list[WorkflowInsightPlugin] = []
        # Records displaced from the scheduler's pending slots, held until every
        # plugin lock on this thread is released. Releasing one can run a customer
        # finalizer, and a finalizer that reaches another execution's plugin must
        # not do so while this thread holds a plugin lock.
        self.releases: list[Any] = []


_hook_frames = _HookFrames()


class WorkflowInsightPlugin(DurableInstrumentationPlugin, _ExportState):
    """Everything this environment holds for one invocation of one execution.

    Built by the factory ``workflow_insight()`` returns, once per invocation,
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

    * ``_lock`` guards ``_closed``, ``_build_revision``, the ``_operations``
      rebind and record emission.
    * ``_ExportScheduler._condition``'s lock guards the export bookkeeping this
      instance carries for the scheduler; nothing outside the scheduler reads or
      writes those fields, and the scheduler never touches the fields above.

    Shared, handler-lifetime state -- resolved config, exporters, scheduler --
    lives on ``_shared`` and is read-only from here.
    """

    def __init__(
        self, shared: WorkflowInsightPluginFactory, info: InvocationStartInfo
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
        # Set once this invocation has ended. A hook that arrives afterwards (an
        # operation-change for a checkpoint that completed just before the end)
        # must emit nothing (mirrors the Java ExecutionState.closed flag).
        self._closed = False
        # Counts the record builds this instance has started. Never decremented.
        #
        # A record is a complete snapshot of one execution, so the scheduler's
        # per-execution slot takes whichever record is handed over last and never
        # compares ages. The build that hands its record over last is not the
        # build that started last: `_emit` runs customer code (the input/output
        # transforms and the operation result overrides) between the snapshot it
        # takes and the hand-off, and that code can re-enter a hook on this thread
        # and complete a newer build first. Without a comparison of build ages the
        # outer frame's older snapshot then replaces the newer one in the slot, or
        # is exported after it. Every non-terminal build takes the next value here
        # before it starts, and `_emit` hands the record over only while that value
        # is still the newest (mirrors the JS `buildRevision` and the Java
        # `AtomicLong buildRevision`).
        #
        # A plain int, not an atomic: every read and every increment happens under
        # `_lock`, so the read-modify-write cannot interleave, and the two builds
        # this counter distinguishes are nested frames on one thread rather than
        # two threads. Java needs an AtomicLong because its two builds really can
        # run at once.
        self._build_revision = 0
        # Guards `_closed`, `_build_revision`, the operations rebind and record
        # emission, so a late hook can never slip a RUNNING record in after the
        # terminal one.
        #
        # What it protects against is reentrancy, not two threads. The SDK
        # dispatches every hook synchronously on the thread that produced the
        # event, and it joins the checkpoint thread and the branch pools before
        # the invocation-end hook is dispatched, so a checkpoint-path
        # operation-change cannot overlap `on_invocation_end` -- an earlier
        # version of this comment claimed it could. The lock still earns its place
        # for the reason below, and it stays because a guard whose correctness
        # rests on the SDK's join ordering is one refactor away from being wrong.
        #
        # Reentrant on purpose: `_emit` runs the scheduler's `schedule()` inside
        # this hold, and `schedule()` releases the record it displaces, which can
        # run a customer finalizer that re-enters a hook for this same execution
        # on this same thread. A plain lock self-deadlocks the invocation thread
        # there. (Java holds no such lock: its ExecutionState carries no
        # operations map, and `cachedInput` is a bare volatile field.)
        self._lock = threading.RLock()

    # -- state ----------------------------------------------------------------

    def _adopt_operations_locked(self, operations: dict[str, OperationInfo]) -> None:
        # Adopt the authoritative point-in-time snapshot. Copy so plugin state
        # never aliases the SDK-owned map, and rebind the attribute so a
        # concurrent reader holding the prior reference iterates a stable dict.
        # Callers hold self._lock.
        self._operations = dict(operations)

    # -- hooks ----------------------------------------------------------------

    @contextlib.contextmanager
    def _hook_frame(self) -> Iterator[None]:
        """Mark a hook frame on this thread and run the drains it owes on exit.

        A hook runs customer code -- the input/output transforms, a result
        override, ``__del__`` on an object a displaced record carried -- while
        holding ``_lock``, which is reentrant so that such code re-entering a hook
        on this thread does not self-deadlock. A re-entrant
        ``on_invocation_end`` therefore used to run its drain while the outer
        frame still held ``_lock``. A drain waits for the export worker, and an
        exporter that re-enters a hook blocks that worker on the very ``_lock``
        the waiting thread holds, so neither side can proceed and the invocation
        hangs until Lambda times it out.

        A drain a nested frame asks for is therefore deferred to the outermost
        frame, which runs it after every ``_lock`` hold on this thread has been
        released. The unnested case is unchanged: the frame is the outermost one,
        so its drain runs at the same point it always did.

        The frame state is per thread and shared by every instance, because
        customer code inside one execution's build can call a hook on another
        execution's instance, and a drain deferred to the outer frame of a
        *different* instance is still a drain outside every lock.
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
        self._shared._scheduler.drain(self)

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        if not self._sampled_in:
            return
        with self._hook_frame(), self._lock:
            if self._closed:
                return
            # Seed the operation map from the full snapshot. On a cold resume
            # this rebuilds prior (terminal) operations that a fresh instance
            # never saw via per-operation hooks.
            self._adopt_operations_locked(info.operations)
            if self._shared._emit_mode == EmitMode.ON_CHANGE:
                self._emit(status="RUNNING", end_time=None, output_raw=None, error=None)

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        # No ARN to resolve: this instance belongs to the invocation the change
        # was raised in, so the hook's `execution_arn` is this execution's by
        # construction. A change hook can no longer fabricate state for an
        # execution whose invocation has ended -- there is no registry to
        # fabricate it in, and the instance it reaches is its own.
        if not self._sampled_in:
            return
        with self._hook_frame(), self._lock:
            if self._closed:
                return
            # Replace state with the full operations snapshot carried by the hook.
            self._adopt_operations_locked(info.operations)
            # on-change mode exports an updated RUNNING record on each change so
            # mid-invocation progress is observable, not only at start/end.
            if self._shared._emit_mode == EmitMode.ON_CHANGE:
                self._emit(status="RUNNING", end_time=None, output_raw=None, error=None)

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
            with self._lock:
                if not self._closed:
                    # Close the gate before emitting so a concurrent late hook for
                    # this execution cannot append a RUNNING record after the
                    # terminal one.
                    self._closed = True
                    # Refresh from the fresh end-of-invocation snapshot before
                    # emitting so the terminal record reflects the final operation
                    # map.
                    self._adopt_operations_locked(info.operations)
                    status = _STATUS_MAP.get(info.status, "RUNNING")
                    is_terminal = status in ("SUCCEEDED", "FAILED")
                    is_failure = status == "FAILED"

                    if emit_mode == EmitMode.ON_CHANGE:
                        should_emit = True
                    elif emit_mode == EmitMode.ON_FAILURE:
                        should_emit = is_failure
                    else:  # on-complete
                        should_emit = is_terminal

                    if should_emit:
                        # Only terminal (SUCCEEDED/FAILED) records carry an end time;
                        # a PENDING/RETRY invocation end maps to RUNNING (still in
                        # flight) and must omit endTime/durationMs. Passing
                        # end_time=None makes _emit drop both fields. Output and
                        # error likewise belong only to a terminal record.
                        self._emit(
                            status=status,
                            end_time=datetime.datetime.now(datetime.UTC)
                            if is_terminal
                            else None,
                            output_raw=info.execution_result if is_terminal else None,
                            error=info.error if is_terminal else None,
                            # This is the emit that closed the gate, so it always runs
                            # with `_closed` already set and must never drop itself.
                            closing=True,
                        )

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
            # The drain covers this execution only -- it names this instance, which
            # carries its own export bookkeeping: it returns once this execution's
            # own record, if any, reached the exporters and a flush that completed
            # after this call is done, without waiting on records scheduled after the
            # call by other executions.
            #
            # Asked for rather than performed, so it runs when the outermost hook
            # frame on this thread unwinds and every `_lock` hold is released. See
            # `_hook_frame`: an invocation end that customer code re-entered from
            # inside another hook's build would otherwise wait for the export
            # worker while holding the lock that worker may need. The request
            # itself is made at the top of this hook, so a failure in the build
            # below cannot skip it.

    # -- emission -------------------------------------------------------------

    def _build_operations(
        self, operations: dict[str, OperationInfo]
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
            if shared._include_errors and op.error is not None:
                entry["error"] = {"name": op.error.type, "message": op.error.message}
            if override is not None and override.result is not None:
                value = _apply_result_override(override.result, op.result)
                if value is not None:
                    entry["result"] = value
            records.append(entry)
        return records

    def _emit(
        self,
        *,
        status: str,
        end_time: Any,
        output_raw: str | None,
        error: Any,
        closing: bool = False,
    ) -> None:
        arn = self._parsed_arn
        start_time = self._start_time
        duration = _duration_ms(start_time, end_time)
        # Snapshot the operations reference once so a concurrent adopt() rebind
        # cannot change the map mid-build.
        operations = self._operations

        # The revision is taken here, before the build, never after. Customer code
        # runs inside the build below and can re-enter a hook on this thread,
        # which starts and finishes a newer build. A value read after the build
        # would already be that newer build's, so this older record would pass the
        # check and replace the newer one. Callers hold self._lock, so the
        # increment cannot interleave with another build's.
        #
        # The closing emit takes no revision; see the hand-off below.
        revision = 0
        if not closing:
            self._build_revision += 1
            revision = self._build_revision

        content = self._shared._content
        record: dict[str, Any] = {
            "recordType": "WorkflowInsight",
            "schemaVersion": "1.0",
            "emittedAt": datetime.datetime.now(datetime.UTC)
            .isoformat()
            .replace("+00:00", "Z"),
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
            record["error"] = {"name": error.type, "message": error.message}
        record["operations"] = self._build_operations(operations)

        # INVARIANT: no record for an execution reaches the scheduler after that
        # execution's closing record -- the exporters never see a RUNNING record
        # follow the terminal one for the same execution.
        #
        # Re-check the gate here, because each hook's own `if self._closed` is a
        # check-then-act and this is the act. One instance per invocation removed
        # one of the two windows this used to cover -- state can no longer be
        # discarded and recreated underneath a hook, because there is no registry
        # to recreate it in -- but not the other: everything between the two runs
        # customer code while holding self._lock (the input/output transforms
        # above, a result override in _build_operations, and __del__ on any object
        # the record carries), and self._lock is a reentrant RLock on purpose (so
        # that customer code re-entering a hook on this thread does not
        # self-deadlock). Such a re-entrant call can therefore run
        # on_invocation_end all the way through -- set `_closed`, emit the
        # terminal record and drain -- and then return here. Without this
        # re-check the outer frame hands its already-built RUNNING record to the
        # scheduler afterwards, and one execution exports
        # ['SUCCEEDED', 'RUNNING'] from a single hook call, no concurrency
        # required.
        #
        # `closing` marks the emit that set the gate (on_invocation_end's own),
        # which by construction always runs with `_closed` set and must not drop
        # itself. It is not the same test as "the record is terminal": in
        # on-change mode a PENDING/RETRY invocation end legitimately emits a
        # RUNNING record, and that record is the closing one.
        #
        # INVARIANT: the record handed to the scheduler below is the newest build
        # this instance has started -- an exporter never stores an older snapshot
        # over a newer one for the same execution.
        #
        # A build that customer code started from inside this one has already
        # handed its own, newer record over by the time control returns here, so
        # this record is superseded. Dropping it loses nothing: a record is a
        # complete snapshot of one execution, so the newer record carries
        # everything this one carries. That is the same property that makes the
        # scheduler's per-execution coalescing sound.
        #
        # The check and the hand-off are one critical section. self._lock is held
        # across both -- schedule() is called inside this hold -- and every build
        # takes that same lock, so no build can start, finish and queue its record
        # between this check and the schedule() below. A record that passes
        # therefore cannot be queued after the record that supersedes it. (Java
        # revalidates inside the scheduler's monitor instead, because there the
        # scheduler's monitor is what guards `closed` and the record slot; here
        # both facts already belong to self._lock.)
        #
        # The closing record is exempt from the revision check. Customer code
        # inside its build can start a newer non-terminal build, which would make a
        # revision taken here stale, and a checked hand-off would then drop the
        # closing record and leave a RUNNING snapshot as this execution's last
        # exported state. The exemption cannot let a stale record win, because
        # `_closed` was set in the same self._lock hold that queues this record and
        # every later non-terminal record is rejected above.
        if not closing and (self._closed or revision != self._build_revision):
            return
        # The records this hand-off displaces are released by the hook frame, not
        # here: this runs inside `_lock`, and a displaced record can carry a
        # customer object whose `__del__` re-enters a hook. Re-entering *this*
        # execution's hook is safe because `_lock` is reentrant; re-entering
        # another execution's is not, and two threads doing it to each other at
        # once would hang both invocations. See `_hook_frame`.
        _hook_frames.releases.extend(self._shared._scheduler.schedule(self, record))


class WorkflowInsightPluginFactory:
    """The handler-lifetime half of the plugin: what is NOT per-execution.

    Built by :func:`workflow_insight`, which is the supported way to obtain one.
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

    def __init__(self, config: WorkflowInsightConfig) -> None:
        self._sampling_rate = _resolve_sampling_rate(config.sampling_rate)
        # config.emit_mode / operation_detail are already normalized to enum
        # members (or None) by WorkflowInsightConfig.__post_init__; re-wrap to
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
        self._exporters: list[InsightExporter] = (
            list(config.exporters) if config.exporters else [LambdaLogExporter()]
        )
        self._scheduler = _ExportScheduler(self._exporters)

    def create_plugin(self, info: InvocationStartInfo) -> WorkflowInsightPlugin:
        return WorkflowInsightPlugin(self, info)


def workflow_insight(config: WorkflowInsightConfig) -> WorkflowInsightPluginFactory:
    """Create a Workflow Insight plugin factory. Mirrors the JS ``workflowInsight()``.

    Pass the result straight to ``@durable_execution(plugins=[...])``: the SDK
    calls its ``create_plugin`` once per invocation to build that invocation's
    plugin instance.
    """
    return WorkflowInsightPluginFactory(config)

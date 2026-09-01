# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Workflow Insight instrumentation plugin for the Durable Execution Python SDK.

Port of the JS ``workflowInsight()`` (``aws-durable-execution-sdk-js-insight/src/
index.ts``). It listens to the SDK's instrumentation hooks and emits one curated
``WorkflowInsight`` record per execution to the configured exporters. The wire
record keeps the JS camelCase field names so records read identically across
SDKs.

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

import datetime
import json
import math
import sys
import threading
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

from aws_durable_execution_sdk_python_insight.exporters.lambda_log_exporter import (
    LambdaLogExporter,
)
from aws_durable_execution_sdk_python_insight.truncation import truncate_record
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
        print(
            "[workflow-insight] sampling_rate is NaN; falling back to 1.0 "
            "(full sampling)",
            file=sys.stderr,
        )  # noqa: T201
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


class _ExecutionState:
    __slots__ = ("start_time", "parsed_arn", "cached_input", "operations")

    def __init__(self, start_time: Any, parsed_arn: dict[str, str]) -> None:
        self.start_time = start_time
        self.parsed_arn = parsed_arn
        self.cached_input: Any = None
        # operation_id -> OperationInfo, adopted verbatim from the SDK's
        # authoritative snapshot (invocation start/end and operation-change).
        self.operations: dict[str, OperationInfo] = {}


class WorkflowInsightPlugin(DurableInstrumentationPlugin):
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
            # Only terminal (SUCCEEDED/FAILED) records carry an end time; a
            # PENDING/RETRY invocation end maps to RUNNING (still in flight) and
            # must omit endTime/durationMs. Passing end_time=None makes _emit
            # drop both fields. Output and error likewise belong only to a
            # terminal record.
            self._emit(
                arn,
                state,
                status=status,
                end_time=datetime.datetime.now(datetime.UTC) if is_terminal else None,
                output_raw=info.execution_result if is_terminal else None,
                error=info.error if is_terminal else None,
            )

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
                entry["error"] = {"name": op.error.type, "message": op.error.message}
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
            "recordType": "WorkflowInsight",
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
            record["error"] = {"name": error.type, "message": error.message}
        record["operations"] = self._build_operations(operations)

        for exporter in self._exporters:
            try:
                shaped = truncate_record(
                    record, exporter.max_record_size_bytes, exporter.render
                )
                exporter.export(shaped)
            except Exception as exc:  # noqa: BLE001 - one exporter must not break others / the execution
                # NOTE (parity gap, same as JS Promise.allSettled): exporter
                # failures are swallowed so instrumentation never breaks the
                # execution. A silently broken exporter is indistinguishable
                # from success; we at least log to stderr.
                print(
                    f"[workflow-insight] exporter {type(exporter).__name__} failed: {exc}",
                    file=sys.stderr,
                )  # noqa: T201


def workflow_insight(config: WorkflowInsightConfig) -> WorkflowInsightPlugin:
    """Create a Workflow Insight plugin. Mirrors the JS ``workflowInsight()`` factory."""
    return WorkflowInsightPlugin(config)

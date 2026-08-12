# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Workflow Insight instrumentation plugin for the Durable Execution Python SDK.

Port of the JS ``workflowInsight()`` (``aws-durable-execution-sdk-js-insight/src/
index.ts``). It listens to the SDK's instrumentation hooks and emits one curated
``WorkflowInsight`` record per execution to the configured exporters. The wire
record keeps the JS camelCase field names so records read identically across
SDKs.

Capability notes vs. the JS plugin (recorded, not hidden):
  * The JS hooks carry ``executionInput`` / ``executionResult`` and the full
    ``operations`` map on the invocation hooks. The Python SDK's
    ``InvocationStartInfo`` / ``InvocationEndInfo`` did not, so this package
    ships a minimal SDK extension surfacing ``execution_input`` /
    ``execution_result``; the operations map is reconstructed by accumulating
    the per-operation ``on_operation_end`` / ``on_operation_change`` hooks into
    per-execution state (see ``ExecutionState``).
  * The Python SDK has no ``pluginsConfig.childOperationsDepth`` equivalent, so
    ``full-tree`` records rely on the child operations being live in the
    emitting invocation (true for single-invocation and warm-resume cases).
"""

from __future__ import annotations

import datetime
import json
import sys
import threading
from typing import Any, Callable

from aws_durable_execution_sdk_python.lambda_service import (
    InvocationStatus,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
)

from aws_durable_execution_sdk_python_insight.truncation import truncate_record
from aws_durable_execution_sdk_python_insight.types import (
    ContentConfig,
    InsightExporter,
    OperationOverride,
    WorkflowInsightConfig,
)


_TERMINAL_OP_STATUSES = frozenset(
    {
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.TIMED_OUT,
        OperationStatus.CANCELLED,
        OperationStatus.STOPPED,
    }
)

# Maps the SDK invocation status onto the record status. A durable execution
# suspends (PENDING) while waiting; from the execution's point of view it is
# still in flight, so surface it as RUNNING (mirrors the JS STATUS_MAP).
_STATUS_MAP = {
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
    __slots__ = ("start_time", "parsed_arn", "sampled_in", "cached_input", "operations")

    def __init__(
        self, start_time: Any, parsed_arn: dict[str, str], sampled_in: bool
    ) -> None:
        self.start_time = start_time
        self.parsed_arn = parsed_arn
        self.sampled_in = sampled_in
        self.cached_input: Any = None
        # Insertion-ordered map operation_id -> OperationInfo (creation order,
        # since on_operation_start/end fire in order).
        self.operations: dict[str, OperationInfo] = {}


class WorkflowInsightPlugin(DurableInstrumentationPlugin):
    def __init__(self, config: WorkflowInsightConfig) -> None:
        self._sampling_rate = _resolve_sampling_rate(config.sampling_rate)
        self._emit_mode = config.emit_mode or "on-complete"
        self._top_level_only = (config.operation_detail or "top-level") != "full-tree"
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
        self._exporters: list[InsightExporter] = list(config.exporters)
        self._state: dict[str, _ExecutionState] = {}
        self._lock = threading.Lock()

    # -- state ----------------------------------------------------------------

    def _get_state(self, execution_arn: str) -> _ExecutionState:
        with self._lock:
            state = self._state.get(execution_arn)
            if state is None:
                state = _ExecutionState(
                    start_time=datetime.datetime.now(datetime.UTC),
                    parsed_arn=_parse_execution_arn(execution_arn),
                    sampled_in=_should_sample(execution_arn, self._sampling_rate),
                )
                self._state[execution_arn] = state
            return state

    def _accumulate(self, execution_arn: str | None, op: OperationInfo) -> None:
        if not execution_arn:
            return
        state = self._get_state(execution_arn)
        with self._lock:
            existing = state.operations.get(op.operation_id)
            # Never downgrade a terminal operation with a later non-terminal event.
            if (
                existing is not None
                and existing.status in _TERMINAL_OP_STATUSES
                and op.status not in _TERMINAL_OP_STATUSES
            ):
                return
            state.operations[op.operation_id] = op

    # -- hooks ----------------------------------------------------------------

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        if not info.execution_arn:
            return
        state = self._get_state(info.execution_arn)
        if not state.sampled_in:
            return
        if info.is_first_invocation and info.execution_start_time is not None:
            state.start_time = info.execution_start_time
        elif info.execution_start_time is not None and state.start_time is None:
            state.start_time = info.execution_start_time
        state.cached_input = info.execution_input
        if self._emit_mode == "on-change":
            self._emit(
                info.execution_arn,
                status="RUNNING",
                end_time=None,
                output_raw=None,
                error=None,
            )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        # OperationEndInfo has no execution_arn field; accumulate under every
        # tracked execution's state is wrong. It is safe to key by the single
        # in-flight execution: the SDK runs one execution per invocation, so the
        # most-recently started execution owns this operation.
        arn = self._current_execution_arn()
        self._accumulate(arn, info)

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        for op in info.operations.values():
            self._accumulate(info.execution_arn, op)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        if not info.execution_arn:
            return
        state = self._get_state(info.execution_arn)
        status = _STATUS_MAP.get(info.status, "RUNNING")
        is_terminal = status in ("SUCCEEDED", "FAILED")
        is_failure = status == "FAILED"

        if self._emit_mode == "on-change":
            should_emit = True
        elif self._emit_mode == "on-failure":
            should_emit = is_failure
        else:  # on-complete
            should_emit = is_terminal

        if state.sampled_in and should_emit:
            self._emit(
                info.execution_arn,
                status=status,
                end_time=datetime.datetime.now(datetime.UTC),
                output_raw=info.execution_result,
                error=info.error,
            )

        if is_terminal:
            with self._lock:
                self._state.pop(info.execution_arn, None)

    # -- emission -------------------------------------------------------------

    def _current_execution_arn(self) -> str | None:
        with self._lock:
            # The most-recently created state is the in-flight execution.
            if not self._state:
                return None
            return next(reversed(self._state))

    def _build_operations(self, state: _ExecutionState) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for op in state.operations.values():
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
        *,
        status: str,
        end_time: Any,
        output_raw: str | None,
        error: Any,
    ) -> None:
        state = self._get_state(execution_arn)
        arn = state.parsed_arn
        start_time = state.start_time
        duration = _duration_ms(start_time, end_time)

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
        record["operations"] = self._build_operations(state)

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

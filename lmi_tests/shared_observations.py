# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Invocation-owned evidence from public SDK hooks, without conformance imports."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
import threading
from typing import Any, Protocol

from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)


class ObservationSink(Protocol):
    """Record SDK observations without importing fixture.Trace."""

    def observe(self, kind: str, **fields: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class GateRelease:
    gate_id: str
    observed_at_ms: float
    mode: str
    held_at_ms: float | None = None


@dataclass(frozen=True)
class OperationObservation:
    operation_id: str
    parent_id: str | None
    name: str | None
    operation_type: str
    sub_type: str | None
    status: str
    replayed: bool
    attempt: int | None
    result: str | None
    error_type: str | None
    error_message: str | None
    started_at_ms: float | None
    ended_at_ms: float | None

    @classmethod
    def from_info(cls, info: OperationInfo) -> OperationObservation:
        return cls(
            operation_id=info.operation_id,
            parent_id=info.parent_id,
            name=info.name,
            operation_type=info.operation_type.value,
            sub_type=info.sub_type,
            status=info.status.value,
            replayed=info.is_replayed,
            attempt=info.attempt,
            result=info.result,
            error_type=info.error.type if info.error else None,
            error_message=info.error.message if info.error else None,
            started_at_ms=info.start_time.timestamp() * 1000
            if info.start_time
            else None,
            ended_at_ms=info.end_time.timestamp() * 1000 if info.end_time else None,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "operationId": self.operation_id,
            "parentId": self.parent_id,
            "name": self.name,
            "operationType": self.operation_type,
            "subType": self.sub_type,
            "status": self.status,
            "isReplayed": self.replayed,
            "attempt": self.attempt,
            "serializedResult": self.result,
            "error": {"type": self.error_type, "message": self.error_message}
            if self.error_type is not None or self.error_message is not None
            else None,
            "startedAtMs": self.started_at_ms,
            "endedAtMs": self.ended_at_ms,
        }


@dataclass(frozen=True)
class UserTask:
    operation: OperationObservation
    task_id: str


class _ThreadTasks(threading.local):
    def __init__(self) -> None:
        self.stack: list[UserTask] = []


class LifecycleObservations(DurableInstrumentationPlugin):
    """One native invocation's actual operations and active public user scopes."""

    def __init__(self, sink: ObservationSink) -> None:
        self._sink = sink
        self._lock = threading.Lock()
        self._operations: dict[str, OperationObservation] = {}
        self._closed: set[str] = set()
        self._configurations: dict[tuple[str, str | None], int] = {}
        self._threads = _ThreadTasks()
        self._task_sequence = 0

    def error(self, message: str) -> None:
        self._sink.observe("observation_error", message=message)

    def _remember(self, info: OperationInfo) -> OperationObservation:
        operation = OperationObservation.from_info(info)
        with self._lock:
            self._operations[operation.operation_id] = operation
        return operation

    def current(self) -> OperationObservation | None:
        return self._threads.stack[-1].operation if self._threads.stack else None

    def parent_id(self) -> str | None:
        current = self.current()
        if current is None:
            self.error("Public user-function scope is unavailable")
            return None
        return (
            None
            if current.operation_type == OperationType.EXECUTION.value
            else current.operation_id
        )

    @contextmanager
    def root_scope(self) -> Iterator[None]:
        """Observe the actual public handler body; root user hooks do not fire."""
        with self._lock:
            roots = [
                item
                for item in self._operations.values()
                if item.operation_type == OperationType.EXECUTION.value
            ]
        if len(roots) != 1:
            self.error("Handler entry has no unique public invocation root operation")
            yield
            return
        task = UserTask(roots[0], "root")
        self._threads.stack.append(task)
        self._sink.observe(
            "task_start",
            taskId="root",
            source="public-handler",
            **task.operation.to_record(),
        )
        try:
            yield
        finally:
            self._threads.stack.pop()
            self._sink.observe(
                "task_exit",
                taskId="root",
                operationId=task.operation.operation_id,
                source="public-handler",
            )

    def body(self, name: str, value: Any, attempt: int) -> None:
        current = self.current()
        if (
            current is None
            or current.operation_type != OperationType.STEP.value
            or current.name != name
        ):
            self.error(f"Body {name!r} has no matching public STEP hook")
            return
        if current.attempt != attempt:
            self.error(f"Body {name!r} attempt differs from its public STEP hook")
            return
        self._sink.observe(
            "body",
            operation=name,
            operationId=current.operation_id,
            parentId=current.parent_id,
            attempt=attempt,
            value=value,
        )

    def closed_ancestor(self) -> str | None:
        current = self.current()
        if current is None:
            self.error("Late operation has no active public user-function scope")
            return None
        with self._lock:
            operation: OperationObservation | None = current
            while operation is not None:
                if operation.operation_id in self._closed:
                    return operation.operation_id
                operation = self._operations.get(operation.parent_id or "")
        self.error("Late operation has no observed completed ancestor")
        return None

    def completed_context(self, name: str) -> str | None:
        parent_id = self.parent_id()
        with self._lock:
            matches = [
                operation.operation_id
                for operation in self._operations.values()
                if operation.name == name
                and operation.parent_id == parent_id
                and operation.operation_type == OperationType.CONTEXT.value
                and operation.operation_id in self._closed
            ]
        if len(matches) != 1:
            self.error(
                f"Returned context {name!r} has no unique public completion hook"
            )
            return None
        return matches[0]

    @contextmanager
    def configuration(self, name: str, max_concurrency: int) -> Iterator[None]:
        """Bind an actual public call's config to its operation-start hook."""
        key = (name, self.parent_id())
        with self._lock:
            self._configurations[key] = max_concurrency
        try:
            yield
        finally:
            with self._lock:
                missing = self._configurations.pop(key, None)
            if missing is not None:
                self.error(
                    f"Configured call {name!r} had no matching public operation hook"
                )

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        operations = [
            self._remember(operation) for operation in info.operations.values()
        ]
        with self._lock:
            self._closed.update(
                operation.operation_id
                for operation in operations
                if operation.operation_type == OperationType.CONTEXT.value
                and operation.status
                in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED"}
            )
        self._sink.observe(
            "invocation_start",
            requestId=info.request_id,
            executionArn=info.execution_arn,
            isFirstInvocation=info.is_first_invocation,
            operations=[operation.to_record() for operation in operations],
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        operations = [
            self._remember(operation) for operation in info.operations.values()
        ]
        self._sink.observe(
            "invocation_end",
            outcome=info.status.value,
            operations=[operation.to_record() for operation in operations],
            serializedResult=info.execution_result,
            error={"type": info.error.type, "message": info.error.message}
            if info.error
            else None,
        )

    def on_operation_start(self, info: OperationStartInfo) -> None:
        operation = self._remember(info)
        self._sink.observe("operation_start", **operation.to_record())
        with self._lock:
            configured = self._configurations.pop(
                (operation.name or "", operation.parent_id), None
            )
        if configured is not None:
            self._sink.observe(
                "operation_config",
                operationId=operation.operation_id,
                maxConcurrency=configured,
                source="public-fixture",
            )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        operation = self._remember(info)
        self._sink.observe("operation_end", **operation.to_record())
        if info.operation_type is OperationType.CONTEXT:
            with self._lock:
                self._closed.add(operation.operation_id)
            self._sink.observe(
                "scope_closed",
                scopeId=operation.operation_id,
                parentId=operation.parent_id,
            )

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        changed = [
            self._remember(operation) for operation in info.updated_operations.values()
        ]
        self._sink.observe(
            "operation_change",
            operations=[operation.to_record() for operation in changed],
        )

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        operation = self._remember(info)
        with self._lock:
            self._task_sequence += 1
            ordinal = self._task_sequence
        task_id = (
            "root"
            if info.operation_type is OperationType.EXECUTION
            else f"user-task-{ordinal}"
        )
        self._threads.stack.append(UserTask(operation, task_id))
        self._sink.observe("task_start", taskId=task_id, **operation.to_record())

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        stack = self._threads.stack
        if not stack or stack[-1].operation.operation_id != info.operation_id:
            self.error("Public user-function end did not match its active start")
            return
        task = stack.pop()
        self._sink.observe(
            "task_exit",
            taskId=task.task_id,
            operationId=info.operation_id,
            parentId=info.parent_id,
            outcome=info.outcome.value,
            attempt=task.operation.attempt,
        )


@dataclass(frozen=True)
class LifecycleObservationFactory:
    """One factory/observer is constructed for each native fixture invocation."""

    observer: LifecycleObservations

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        return self.observer

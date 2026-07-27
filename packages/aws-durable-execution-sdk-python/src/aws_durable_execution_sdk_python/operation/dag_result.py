"""DagResultImpl + serialization for the DAG operation.

.. warning::
   **Experimental.** Internal implementation of :class:`~...dag.DagResult`.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, TypeVar, overload

from aws_durable_execution_sdk_python.concurrency.models import (
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagResult,
    SkipReason,
    TaskExecution,
    TaskHandle,
    TaskStatus,
)
from aws_durable_execution_sdk_python.exceptions import DagExecutionError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.serdes import SerDes

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aws_durable_execution_sdk_python.serdes import SerDesContext

T = TypeVar("T")

# The single converged envelope discriminator (contract: ``type: "DagResult"``).
_ENVELOPE_TYPE = "DagResult"

# result_kind discriminators
_KIND_PLAIN = "plain"
_KIND_BATCH = "batch"
_KIND_DAG = "dag"

# TaskDef.kind values whose result is a BatchResult / DagResult
_BATCH_KINDS = frozenset({"map", "parallel"})
_DAG_KINDS = frozenset({"dag"})


def _iso_millis(dt: datetime.datetime | None) -> str | None:
    """Format a datetime as the contract timestamp: UTC, millisecond precision,
    ``Z`` suffix (e.g. ``2026-07-26T03:19:01.884Z``). ``None`` stays ``None``
    (the value is genuinely unknown)."""
    if dt is None:
        return None
    utc = dt.astimezone(datetime.UTC)
    return f"{utc.strftime('%Y-%m-%dT%H:%M:%S')}.{utc.microsecond // 1000:03d}Z"


def _parse_iso(value: str | None) -> datetime.datetime | None:
    """Parse a contract timestamp back to an aware UTC datetime. Tolerant of a
    trailing ``Z`` (which older Pythons' ``fromisoformat`` rejected). ``None``
    and unparseable values yield ``None`` rather than raising -- a timestamp is
    informational and must never break deserialization (contract rule 4)."""
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):  # pragma: no cover - defensive
        return None


def _error_to_dict(err: ErrorObject | None) -> dict[str, Any] | None:
    """Serialize an error to the canonical PascalCase object with explicit nulls.

    The three canonical keys (``ErrorType``, ``ErrorMessage``, ``StackTrace``)
    are always present (``null`` when absent) so the payload is diffable across
    languages; any extra platform field (e.g. ``ErrorData``) is preserved."""
    if err is None:
        return None
    d = dict(err.to_dict())
    d.setdefault("ErrorType", None)
    d.setdefault("ErrorMessage", None)
    d.setdefault("StackTrace", None)
    return d


def dag_reason_from_core(core: CompletionReason) -> DagCompletionReason:
    """Bridge a batch ``CompletionReason`` into the DAG's superset enum."""
    return DagCompletionReason(core.value)


def _result_kind(task_kind: str | None) -> str:
    if task_kind in _BATCH_KINDS:
        return _KIND_BATCH
    if task_kind in _DAG_KINDS:
        return _KIND_DAG
    return _KIND_PLAIN



def _name_of(task: str | TaskHandle[Any]) -> str:
    return task.name if isinstance(task, TaskHandle) else task


class DagResultImpl(DagResult):
    """Concrete DAG result. Mirrors the ``BatchResult`` accessor surface.

    .. warning::
       **Experimental.**
    """

    def __init__(
        self,
        results: dict[str, TaskExecution],
        completion_reason: DagCompletionReason,
        task_kinds: dict[str, str] | None = None,
        total_count: int | None = None,
    ) -> None:
        self._results = results
        self._completion_reason = completion_reason
        self._task_kinds = task_kinds or {}
        # total_count is the number of REGISTERED tasks in the DAG (spec §2.8),
        # a fixed value independent of early completion / never-started tasks.
        # Defaults to len(results) when omitted (fully-recorded DAGs).
        self._total_count = total_count if total_count is not None else len(results)

    # region accessors
    @overload
    def get_result(self, task: TaskHandle[T]) -> T: ...
    @overload
    def get_result(self, task: str) -> Any: ...
    def get_result(self, task: str | TaskHandle[Any]) -> Any:
        """Return a task's result (or ``None`` if absent / not succeeded).

        Passing the originating :class:`TaskHandle` preserves the task's result
        type for static typing (``get_result(handle) -> T``); passing a name
        string returns ``Any``. Both resolve by task name at runtime.
        """
        te = self._results.get(_name_of(task))
        return te.result if te else None

    def get_status(self, task: str | TaskHandle[Any]) -> TaskStatus | None:
        """Return a task's status, or ``None`` if the task never ran."""
        te = self._results.get(_name_of(task))
        return te.status if te else None

    def succeeded(self) -> list[TaskExecution]:
        """Tasks that SUCCEEDED."""
        return [t for t in self._results.values() if t.status is TaskStatus.SUCCEEDED]

    def failed(self) -> list[TaskExecution]:
        """Tasks that FAILED."""
        return [t for t in self._results.values() if t.status is TaskStatus.FAILED]

    def skipped(self) -> list[TaskExecution]:
        """Tasks that were SKIPPED."""
        return [t for t in self._results.values() if t.status is TaskStatus.SKIPPED]

    @property
    def results(self) -> Mapping[str, TaskExecution]:
        """All recorded task executions, keyed by name."""
        return self._results

    @property
    def success_count(self) -> int:
        return len(self.succeeded())

    @property
    def failure_count(self) -> int:
        return len(self.failed())

    @property
    def skipped_count(self) -> int:
        return len(self.skipped())

    @property
    def total_count(self) -> int:
        return self._total_count

    @property
    def completion_reason(self) -> DagCompletionReason:
        return self._completion_reason

    def throw_if_error(self) -> None:
        """Raise :class:`DagExecutionError` if any task FAILED."""
        failures = self.failed()
        if failures:
            first = failures[0]
            detail = first.error.message if first.error else "unknown error"
            msg = (
                f"DAG completed with {len(failures)} failed task(s); "
                f"first failure '{first.name}': {detail}"
            )
            raise DagExecutionError(msg)

    # endregion accessors

    # region serialization
    def to_dict(self) -> dict[str, Any]:
        """Serialize to the converged cross-language DAG envelope.

        Single shape for both the inline and offloaded cases (the offloaded case
        drops only ``tasks``; see the degradation ladder in ``operation/dag.py``).
        Every canonical field is always present; absent values are ``null``,
        never omitted. Aggregate fields are always present even though they are
        derivable from ``tasks`` -- that redundancy is what lets the offloaded
        payload keep the same shape after ``tasks`` is dropped. Field order
        follows the contract listing for console readability (structural
        comparison ignores order).
        """
        return {
            "type": _ENVELOPE_TYPE,
            "totalCount": self._total_count,
            "successCount": self.success_count,
            "failureCount": self.failure_count,
            "skippedCount": self.skipped_count,
            "completionReason": self._completion_reason.value,
            "startedTaskNames": [
                te.name
                for te in self._results.values()
                if te.status is TaskStatus.STARTED
            ],
            "failedTaskNames": [
                te.name
                for te in self._results.values()
                if te.status is TaskStatus.FAILED
            ],
            "tasks": [self._task_to_dict(te) for te in self._results.values()],
        }

    def _task_to_dict(self, te: TaskExecution) -> dict[str, Any]:
        kind = _result_kind(self._task_kinds.get(te.name))
        result_value: Any = None
        if te.result is not None:
            if (
                kind == _KIND_BATCH
                and isinstance(te.result, BatchResult)
                or kind == _KIND_DAG
                and isinstance(te.result, DagResultImpl)
            ):
                result_value = te.result.to_dict()
            else:
                result_value = te.result
        # resultKind describes how to interpret ``result``, so it is null when there
        # is no result to interpret: a FAILED or SKIPPED task carries null for both.
        # All four SDKs agree on this (envelope contract rule 1, explicit nulls).
        serialized_kind = kind if te.status is TaskStatus.SUCCEEDED else None
        return {
            "name": te.name,
            "status": te.status.value,
            "skipReason": te.skip_reason.value if te.skip_reason else None,
            "resultKind": serialized_kind,
            "result": result_value,
            "error": _error_to_dict(te.error),
            "startedAt": _iso_millis(te.started_at),
            "completedAt": _iso_millis(te.completed_at),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DagResultImpl:
        """Deserialize the converged envelope.

        Reads the ``tasks`` array; unknown fields are ignored and a missing
        field is treated as absent rather than an error (contract rule 4,
        additive-only evolution). An envelope with no ``tasks`` (the offloaded
        case) yields an empty results map -- the offloaded path reconstructs the
        per-task detail from the child checkpoints instead (see
        ``operation/dag.py``); it does not call this method to rebuild tasks.
        """
        results: dict[str, TaskExecution] = {}
        task_kinds: dict[str, str] = {}
        for td in data.get("tasks") or []:
            name = td["name"]
            kind = td.get("resultKind", _KIND_PLAIN)
            result_value = td.get("result")
            if result_value is not None:
                if kind == _KIND_BATCH:
                    result_value = BatchResult.from_dict(result_value)
                elif kind == _KIND_DAG:
                    result_value = cls.from_dict(result_value)
            error_raw = td.get("error")
            results[name] = TaskExecution(
                name=name,
                status=TaskStatus(td["status"]),
                skip_reason=(
                    SkipReason(td["skipReason"]) if td.get("skipReason") else None
                ),
                result=result_value,
                error=ErrorObject.from_dict(error_raw) if error_raw else None,
                started_at=_parse_iso(td.get("startedAt")),
                completed_at=_parse_iso(td.get("completedAt")),
            )
            task_kinds[name] = (
                "dag"
                if kind == _KIND_DAG
                else ("map" if kind == _KIND_BATCH else "step")
            )
        return cls(
            results=results,
            completion_reason=DagCompletionReason(data["completionReason"]),
            task_kinds=task_kinds,
            total_count=data.get("totalCount"),
        )

    # endregion serialization


class DagResultSerDes(SerDes):
    """SerDes for the inline DagResult container payload.

    Serializes/deserializes the full converged envelope (with ``tasks``). The
    offloaded degradation ladder and the reconstruct path live in
    ``operation/dag.py`` because they need to manipulate the envelope structure
    and read the retained child checkpoints, which a plain SerDes cannot do.
    """

    def serialize(self, value: DagResultImpl, serdes_context: SerDesContext) -> str:
        import json

        return json.dumps(value.to_dict())

    def deserialize(self, data: str, serdes_context: SerDesContext) -> DagResultImpl:
        import json

        return DagResultImpl.from_dict(json.loads(data))


def create_dag_result_serdes() -> SerDes:
    """Return a SerDes that round-trips a :class:`DagResultImpl`."""
    return DagResultSerDes()

"""DagResultImpl + serialization for the DAG operation.

.. warning::
   **Experimental.** Internal implementation of :class:`~...dag.DagResult`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.concurrency import (
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

# result_kind discriminators
_KIND_PLAIN = "plain"
_KIND_BATCH = "batch"
_KIND_DAG = "dag"

# TaskDef.kind values whose result is a BatchResult / DagResult
_BATCH_KINDS = frozenset({"map", "parallel"})
_DAG_KINDS = frozenset({"dag"})


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
    ) -> None:
        self._results = results
        self._completion_reason = completion_reason
        self._task_kinds = task_kinds or {}

    # region accessors
    def get_result(self, task: str | TaskHandle[Any]) -> Any:
        """Return a task's result (or ``None`` if absent / not succeeded)."""
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
        return len(self._results)

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
        return {
            "results": {
                name: self._task_to_dict(te) for name, te in self._results.items()
            },
            "completionReason": self._completion_reason.value,
        }

    def _task_to_dict(self, te: TaskExecution) -> dict[str, Any]:
        kind = _result_kind(self._task_kinds.get(te.name))
        result_value: Any = None
        if te.result is not None:
            if kind == _KIND_BATCH and isinstance(te.result, BatchResult):
                result_value = te.result.to_dict()
            elif kind == _KIND_DAG and isinstance(te.result, DagResultImpl):
                result_value = te.result.to_dict()
            else:
                result_value = te.result
        return {
            "name": te.name,
            "status": te.status.value,
            "resultKind": kind,
            "skipReason": te.skip_reason.value if te.skip_reason else None,
            "result": result_value,
            "error": te.error.to_dict() if te.error else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DagResultImpl:
        results: dict[str, TaskExecution] = {}
        task_kinds: dict[str, str] = {}
        for name, td in data["results"].items():
            kind = td.get("resultKind", _KIND_PLAIN)
            result_value = td.get("result")
            if result_value is not None:
                if kind == _KIND_BATCH:
                    result_value = BatchResult.from_dict(result_value)
                elif kind == _KIND_DAG:
                    result_value = cls.from_dict(result_value)
            error_raw = td.get("error")
            results[name] = TaskExecution(
                name=td["name"],
                status=TaskStatus(td["status"]),
                skip_reason=(
                    SkipReason(td["skipReason"]) if td.get("skipReason") else None
                ),
                result=result_value,
                error=ErrorObject.from_dict(error_raw) if error_raw else None,
            )
            task_kinds[name] = "dag" if kind == _KIND_DAG else (
                "map" if kind == _KIND_BATCH else "step"
            )
        return cls(
            results=results,
            completion_reason=DagCompletionReason(data["completionReason"]),
            task_kinds=task_kinds,
        )

    # endregion serialization


class DagResultSerDes(SerDes):
    """SerDes for the DagResult container payload."""

    def serialize(self, value: DagResultImpl, serdes_context: SerDesContext) -> str:
        import json

        return json.dumps(value.to_dict())

    def deserialize(self, data: str, serdes_context: SerDesContext) -> DagResultImpl:
        import json

        return DagResultImpl.from_dict(json.loads(data))


def create_dag_result_serdes() -> SerDes:
    """Return a SerDes that round-trips a :class:`DagResultImpl`."""
    return DagResultSerDes()

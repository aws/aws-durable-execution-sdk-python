"""DagResultImpl: aggregate result of a DAG run.

Serialization (``to_dict`` / ``from_dict`` / serdes) is added in T6.

.. warning::
   **Experimental.** Internal implementation of :class:`~...dag.DagResult`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    TaskExecution,
    TaskHandle,
    TaskStatus,
)
from aws_durable_execution_sdk_python.exceptions import DagExecutionError

if TYPE_CHECKING:
    from collections.abc import Mapping


def _name_of(task: str | TaskHandle[Any]) -> str:
    return task.name if isinstance(task, TaskHandle) else task


class DagResultImpl:
    """Concrete DAG result. Mirrors the ``BatchResult`` accessor surface.

    .. warning::
       **Experimental.**
    """

    def __init__(
        self,
        results: dict[str, TaskExecution],
        completion_reason: DagCompletionReason,
    ) -> None:
        self._results = results
        self._completion_reason = completion_reason

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

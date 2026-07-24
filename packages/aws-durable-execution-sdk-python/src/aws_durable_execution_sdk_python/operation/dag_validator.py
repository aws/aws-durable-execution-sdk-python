"""DAG validation: names, duplicates, foreign deps, and cycle detection.

.. warning::
   **Experimental.** Internal validation for the DAG operation.
"""

from __future__ import annotations

import re
from collections import deque
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.exceptions import (
    DagCyclicDependencyError,
    DagDuplicateTaskError,
    DagInvalidDependencyError,
    DagInvalidTaskNameError,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.operation.dag_context import (
        DagContextImpl,
        TaskDef,
    )

_MAX_NAME_LEN = 100
_NAME_RE = re.compile(r"[a-zA-Z0-9_]+")
_RESERVED_TOKEN = "DAG_NODE_T_"


def _validate_name(name: str) -> None:
    if not name:
        msg = "Task name must be non-empty."
        raise DagInvalidTaskNameError(msg)
    if len(name) > _MAX_NAME_LEN:
        msg = f"Task name '{name}' exceeds {_MAX_NAME_LEN} characters."
        raise DagInvalidTaskNameError(msg)
    if not _NAME_RE.fullmatch(name):
        msg = (
            f"Task name '{name}' is invalid: must match ^[a-zA-Z0-9_]+$ "
            "(no '-' or other special characters)."
        )
        raise DagInvalidTaskNameError(msg)
    if _RESERVED_TOKEN in name:
        msg = f"Task name '{name}' must not contain the reserved token '{_RESERVED_TOKEN}'."
        raise DagInvalidTaskNameError(msg)


def validate_dag(dag_ctx: DagContextImpl) -> None:
    """Validate a registered DAG. Deterministic; identical result on replay.

    Raises:
        DagInvalidTaskNameError: invalid or reserved task name.
        DagDuplicateTaskError: two tasks share a name.
        DagInvalidDependencyError: a dep is not registered in this scope.
        DagCyclicDependencyError: the dependency graph contains a cycle.
    """
    registration_order: list[TaskDef] = dag_ctx.get_registration_order()
    tasks: dict[str, TaskDef] = dag_ctx.get_tasks()

    # 1. names + 2. duplicates (in registration order for determinism)
    seen: set[str] = set()
    for task in registration_order:
        _validate_name(task.name)
        if task.name in seen:
            msg = f"Duplicate task name '{task.name}' in DAG scope."
            raise DagDuplicateTaskError(msg)
        seen.add(task.name)

    # 3. deps must be registered in this scope
    for task in tasks.values():
        for dep in task.all_deps:
            if dep.name not in tasks:
                msg = (
                    f"Task '{task.name}' depends on '{dep.name}', which is not "
                    "registered in this DAG scope."
                )
                raise DagInvalidDependencyError(msg)

    # 4. cycle detection via Kahn's algorithm over all_deps
    _detect_cycle(tasks)


def _detect_cycle(tasks: dict[str, TaskDef]) -> None:
    indegree: dict[str, int] = {name: 0 for name in tasks}
    dependents: dict[str, list[str]] = {name: [] for name in tasks}
    for name, task in tasks.items():
        dep_names = {dep.name for dep in task.all_deps}
        indegree[name] = len(dep_names)
        for dep_name in dep_names:
            dependents[dep_name].append(name)

    queue: deque[str] = deque(n for n, d in indegree.items() if d == 0)
    processed = 0
    while queue:
        current = queue.popleft()
        processed += 1
        for child in dependents[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if processed != len(tasks):
        cyclic = sorted(n for n, d in indegree.items() if d > 0)
        msg = f"DAG contains a dependency cycle among tasks: {cyclic}"
        raise DagCyclicDependencyError(msg)

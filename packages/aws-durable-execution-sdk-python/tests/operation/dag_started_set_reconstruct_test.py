"""The STARTED-set fix: offloaded reconstruct must not restart an in-flight task.

Python previously documented losing the STARTED set under large-payload early
completion -- an in-flight task could restart on replay (a duplicated customer
side effect). The converged envelope carries ``startedTaskNames`` precisely so
the offloaded reconstruct path can seed those tasks as STARTED and never
reschedule them.

Because Python's drain-based scheduler bubbles the whole DAG on any suspend, a
*returned* DagResult never itself contains a STARTED task, so a natural offloaded
envelope has an empty started set. To exercise the fix deterministically this
test crafts the offloaded container checkpoint directly: a two-root DAG is run
once (so ``winner`` has a real, name-based child checkpoint), then the container
is rewritten to the offloaded shape (``ReplayChildren``, ``tasks`` dropped) and
``laggard``'s child checkpoint is removed so that -- absent the started set --
reconstruct WOULD reschedule and re-run its body. The two cases are contrasted:

* with ``startedTaskNames == ["laggard"]`` the body never runs and the task is
  reproduced STARTED (the fix);
* with ``startedTaskNames == []`` the body runs again (the pre-fix restart),
  proving the started set is what prevents it.
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

from aws_durable_execution_sdk_python.dag import DagCompletionReason, TaskStatus
from aws_durable_execution_sdk_python.lambda_service import (
    ContextDetails,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from tests.dag_support import InMemoryServiceClient, make_context, make_state
from tests.operation.dag_concurrency_coverage_test import _seeded_state

_DAG_NAME = "bigdag"


def _register_factory(calls: dict[str, int]):
    def register(d: Any) -> None:
        d.step(lambda deps, sc: "W", name="winner")

        def laggard_body(_deps: Any, _sc: Any) -> str:
            calls["laggard"] += 1
            return "L"

        d.step(laggard_body, name="laggard")

    return register


def _craft_offloaded_container(
    client: InMemoryServiceClient, started_task_names: list[str]
) -> None:
    """Rewrite the DAG container to the offloaded shape and drop laggard's
    child checkpoint so reconstruct must rely on the started set."""
    # Rewrite the container: SUCCEEDED + ReplayChildren + envelope w/o tasks.
    for op_id, op in list(client.operations.items()):
        if (
            op.operation_type is OperationType.CONTEXT
            and op.sub_type is OperationSubType.DAG
        ):
            envelope = {
                "type": "DagResult",
                "totalCount": 2,
                "successCount": 1,
                "failureCount": 0,
                "skippedCount": 0,
                "completionReason": "MIN_SUCCESSFUL_REACHED",
                "startedTaskNames": started_task_names,
                "failedTaskNames": [],
            }
            client.operations[op_id] = dataclasses.replace(
                op,
                status=OperationStatus.SUCCEEDED,
                context_details=ContextDetails(
                    replay_children=True, result=json.dumps(envelope), error=None
                ),
            )
    # Remove laggard's own child checkpoint: without the started set, reconstruct
    # would find it un-checkpointed and re-run the body.
    for op_id, op in list(client.operations.items()):
        if op.name == "laggard" and op.sub_type is not OperationSubType.DAG:
            del client.operations[op_id]


def _reconstruct_once(started_task_names: list[str]) -> tuple[Any, dict[str, int]]:
    calls = {"laggard": 0}
    register = _register_factory(calls)

    # First run: both roots complete and checkpoint (small, inline).
    state, client = make_state()
    make_context(state).dag(register, name=_DAG_NAME)
    assert calls["laggard"] == 1  # body ran once on the first invocation

    # Craft the offloaded container and reset the counter to observe reconstruct.
    _craft_offloaded_container(client, started_task_names)
    calls["laggard"] = 0

    # Re-invoke: the container is SUCCEEDED + ReplayChildren -> reconstruct.
    recon_state = _seeded_state(client, dict(client.operations))
    result = make_context(recon_state).dag(register, name=_DAG_NAME)
    return result, calls


def test_started_task_is_not_restarted_on_reconstruct() -> None:
    """With ``laggard`` in the started set, reconstruct seeds it STARTED and its
    body never runs; ``winner`` fast-paths from its checkpoint."""
    result, calls = _reconstruct_once(started_task_names=["laggard"])

    assert calls["laggard"] == 0, "in-flight task was restarted on reconstruct"
    assert result.get_status("laggard") is TaskStatus.STARTED
    assert result.get_status("winner") is TaskStatus.SUCCEEDED
    assert result.get_result("winner") == "W"
    # Aggregates come from the envelope, not re-derivation.
    assert result.completion_reason is DagCompletionReason.MIN_SUCCESSFUL_REACHED
    assert result.total_count == 2
    assert result.success_count == 1


def test_without_started_set_the_task_restarts() -> None:
    """Control: with an empty started set the same reconstruct re-runs the
    laggard body -- the exact restart the started set exists to prevent."""
    result, calls = _reconstruct_once(started_task_names=[])

    assert calls["laggard"] == 1, "expected the pre-fix restart without the set"
    # Re-run, so it lands SUCCEEDED rather than STARTED.
    assert result.get_status("laggard") is TaskStatus.SUCCEEDED

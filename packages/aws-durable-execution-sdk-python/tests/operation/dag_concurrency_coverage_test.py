"""Integration coverage for the concurrency + abort gaps (conformance 10-12..10-14).

These run the *exact* graphs of the three new conformance scenarios in-process so
the regressions the cloud suite cannot catch are caught here without a deploy:

* **10-13 concurrent overlap** — asserts the result AND that peak observed
  concurrency was >= 2 AND that every task's recorded operation id is name-based
  (derived from the ``DAG_NODE_T_<name>`` pre-image). The id assertion is the one
  the cloud suite deliberately cannot make: at unset ``max_concurrency`` with
  out-of-order completion a counter-based regression would hand out different ids
  on replay and terminate the execution, but only a per-SDK test can prove the
  ids are *positively* name-based rather than merely internally consistent. Runs
  on the lightweight in-memory harness (``tests/dag_support``).
* **10-14 inverted readiness** — drives the graph through a multi-invocation
  replay loop on the in-memory harness: the two waits suspend on the first
  invocation, then a driver marks each wait's checkpoint SUCCEEDED in resume
  order (fast, then slow) and re-invokes on a state seeded from the accumulated
  operations, simulating the platform resuming each timer. It asserts the result
  AND that the downstream pair ran in the reverse of registration order across
  the suspend (afterFast before afterSlow) with no replay-consistency error.
* **10-12 abort** — asserts the typed ``DagPredicateError`` surfaced to the caller
  and that the ``ALL_FAILED`` compensation body was never invoked (external
  counter). Runs on the in-memory harness.

Python bounds every backend operation id through ``blake2b(...)[:64]``, so the
``DAG_NODE_T_<name>`` token lives in the id's *pre-image*, not the digest. The id
helper recomputes the digest from ``(parent_id, name)`` and asserts the recorded
id equals it — proving, per task, that the id is the name-based one and could
never be produced by a counter scheme. This mirrors the ``_structural_checks``
technique in ``tests/conformance/dag_conformance_test.py``.
"""

from __future__ import annotations

import dataclasses
import hashlib
import signal
import threading
import time
from contextlib import contextmanager
from typing import Any

import pytest

from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagConfig,
    TriggerRule,
)
from aws_durable_execution_sdk_python.exceptions import (
    DagPredicateError,
    SuspendExecution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.state import ExecutionState
from tests.dag_support import InMemoryServiceClient, make_context, make_state


@contextmanager
def _fail_on_hang(seconds: int = 30):
    """Turn a scheduler hang into an assertion failure rather than blocking the
    whole test session. SIGALRM fires on the main thread (where pytest runs)."""

    def _handler(_signum, _frame):
        raise AssertionError("context.dag() hung (concurrency/abort regression)")

    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def _name_based_task_ops(client: InMemoryServiceClient) -> dict[str, Any]:
    """Return recorded ops whose id is exactly the name-based DAG-task digest.

    A DAG task's backend id is ``blake2b(f"{parent}-DAG_NODE_T_{name}")[:64]``.
    Any op whose id equals that recomputation from its own ``(parent_id, name)``
    is a name-based task op; counter ops (e.g. the top-level DAG container) never
    match, so this positively isolates the task ops.
    """
    by_name: dict[str, Any] = {}
    for op in client.operations.values():
        if op.name is None:
            continue
        preimage = (
            f"{op.parent_id}-DAG_NODE_T_{op.name}"
            if op.parent_id
            else f"DAG_NODE_T_{op.name}"
        )
        expected = hashlib.blake2b(preimage.encode()).hexdigest()[:64]
        if op.operation_id == expected:
            by_name[op.name] = op
    return by_name


def _assert_ids_name_based(client: InMemoryServiceClient, names: set[str]) -> None:
    task_ops = _name_based_task_ops(client)
    for name in names:
        assert name in task_ops, f"no name-based op recorded for task {name!r}"
        op = task_ops[name]
        preimage = (
            f"{op.parent_id}-DAG_NODE_T_{op.name}"
            if op.parent_id
            else f"DAG_NODE_T_{op.name}"
        )
        # The DAG_NODE_T_<name> segment lives in the id's PRE-IMAGE; asserting
        # the recorded digest equals the hash of that pre-image proves the id is
        # name-based for THIS task and could not come from a counter scheme.
        assert f"DAG_NODE_T_{name}" in preimage
        assert op.operation_id == hashlib.blake2b(preimage.encode()).hexdigest()[:64]


_OVERLAP_TASKS = {"root", "slow", "fast", "afterSlow", "afterFast", "merge"}


def test_10_13_concurrent_overlap() -> None:
    """The 10-13 graph: slow + fast overlap inside one invocation, fast finishes
    first (inverting registration order), and every task id is name-based."""
    tracker = {"current": 0, "peak": 0}
    lock = threading.Lock()
    # Both slow and fast must be simultaneously in-flight to clear this barrier;
    # if the scheduler serialized them it would time out -> BrokenBarrierError,
    # failing the task and this test. This makes the overlap *deterministic*
    # rather than relying on sleep timing.
    barrier = threading.Barrier(2, timeout=10)

    def _enter() -> None:
        with lock:
            tracker["current"] += 1
            tracker["peak"] = max(tracker["peak"], tracker["current"])

    def _leave() -> None:
        with lock:
            tracker["current"] -= 1

    def slow(_deps: Any, _sc: Any) -> str:
        _enter()
        try:
            barrier.wait()
            time.sleep(0.2)  # ensure fast completes first -> out-of-order
            return "S"
        finally:
            _leave()

    def fast(_deps: Any, _sc: Any) -> str:
        _enter()
        try:
            barrier.wait()
            return "F"
        finally:
            _leave()

    def register(d: Any) -> None:
        root = d.step(lambda deps, sc: 1, name="root")
        slow_h = d.step(slow, deps=[root], name="slow")  # registered FIRST
        fast_h = d.step(fast, deps=[root], name="fast")
        after_slow = d.step(  # registered FIRST
            lambda deps, sc: deps[slow_h] + "s", deps=[slow_h], name="afterSlow"
        )
        after_fast = d.step(
            lambda deps, sc: deps[fast_h] + "f", deps=[fast_h], name="afterFast"
        )
        d.step(
            lambda deps, sc: deps[after_slow] + deps[after_fast],
            deps=[after_slow, after_fast],
            name="merge",
        )

    state, client = make_state()
    with _fail_on_hang():
        result = make_context(state).dag(register, name="overlapdag")

    assert result.get_result("merge") == "SsFf"
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert (
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ) == (6, 0, 0, 6)
    # Genuine overlap: both bodies were simultaneously in-flight.
    assert tracker["peak"] >= 2
    _assert_ids_name_based(client, _OVERLAP_TASKS)


def _seeded_state(
    client: InMemoryServiceClient, operations: dict[str, Any]
) -> ExecutionState:
    """A fresh ExecutionState seeded with prior operations, wired to ``client``.

    Mirrors ``dag_support.make_state`` but seeds the operations a real
    re-invocation would have fetched from the backend, and starts the background
    checkpoint thread so synchronous checkpoints don't deadlock.
    """
    state = ExecutionState(
        durable_execution_arn="test-arn",
        initial_checkpoint_token="token",  # noqa: S106
        operations=operations,
        service_client=client,
        plugin_executor=PluginExecutor(plugins=None),
    )
    thread = threading.Thread(
        target=state.checkpoint_batches_forever,
        name="dag-suspend-checkpointer",
        daemon=True,
    )
    thread.start()
    return state


def _complete_wait(client: InMemoryServiceClient, name: str) -> bool:
    """Mark the wait op with ``name`` SUCCEEDED, simulating its timer firing."""
    for op_id, op in list(client.operations.items()):
        if op.name == name and op.operation_type is OperationType.WAIT:
            client.operations[op_id] = dataclasses.replace(
                op, status=OperationStatus.SUCCEEDED
            )
            return True
    return False


def test_10_14_inverted_readiness_across_suspend() -> None:
    """The 10-14 graph across a real suspend: two waits are in flight when the
    invocation suspends; resuming fast (2s) before slow (8s) makes afterFast ready
    an invocation before afterSlow. The downstream pair therefore starts in the
    reverse of registration order across invocations, and the run completes with
    merge == "SF" and no replay-consistency error."""
    run_order: list[str] = []
    order_lock = threading.Lock()

    def _record(name: str) -> str:
        with order_lock:
            run_order.append(name)
        return "S" if name == "afterSlow" else "F"

    def register(d: Any) -> None:
        root = d.step(lambda deps, sc: 1, name="root")
        slow = d.wait(8, deps=[root], name="slow")  # registered FIRST
        fast = d.wait(2, deps=[root], name="fast")
        after_slow = d.step(lambda deps, sc: _record("afterSlow"), name="afterSlow")
        after_slow.after(slow)  # registered FIRST
        after_fast = d.step(lambda deps, sc: _record("afterFast"), name="afterFast")
        after_fast.after(fast)
        d.step(
            lambda deps, sc: deps[after_slow] + deps[after_fast],
            deps=[after_slow, after_fast],
            name="merge",
        )

    client = InMemoryServiceClient()
    # fast's timer (2s) fires before slow's (8s), so the platform resumes fast
    # first; the driver mirrors that resume order.
    resume_order = ["fast", "slow"]
    result = None
    with _fail_on_hang():
        for _ in range(len(resume_order) + 1):
            state = _seeded_state(client, dict(client.operations))
            ctx = DurableContext(
                state=state,
                execution_context=ExecutionContext(
                    durable_execution_arn=state.durable_execution_arn
                ),
                parent_id=None,
            )
            try:
                result = ctx.dag(register, name="suspenddag")
                break
            except SuspendExecution:
                assert resume_order, "suspended more times than there are waits"
                assert _complete_wait(client, resume_order.pop(0))

    assert result is not None, "DAG never completed across resumes"
    assert result.get_result("merge") == "SF"
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert (
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ) == (6, 0, 0, 6)
    # Inverted readiness across the suspend: afterFast ran an invocation before
    # afterSlow, i.e. the reverse of their registration order. A counter-based id
    # scheme could not survive this out-of-order resume without a replay error.
    assert run_order == ["afterFast", "afterSlow"]


def test_10_12_run_if_abort() -> None:
    """The 10-12 graph: a throwing run_if aborts the DAG with a typed error and
    the ALL_FAILED compensation body is never invoked.

    Through the ``context.dag()`` child-context boundary the caller observes a
    ``DagPredicateError`` whose *message* names the offending task and embeds the
    original error; ``task_name`` / ``__cause__`` are intentionally not
    reconstructed across that boundary (so the first run matches replay — the
    executor-level richness is covered by ``dag_executor_test``). We assert
    exactly what the caller observes.
    """
    calls = {"guarded_body": 0, "refund_body": 0}
    calls_lock = threading.Lock()

    def _bump(key: str) -> None:
        with calls_lock:
            calls[key] += 1

    def guarded_body(_deps: Any, _sc: Any) -> str:
        _bump("guarded_body")  # MUST NOT happen
        return "ran"

    def refund_body(_deps: Any, _sc: Any) -> str:
        _bump("refund_body")  # MUST NOT happen
        return "refunded"

    def boom(_deps: Any) -> bool:
        raise RuntimeError("predicate boom")

    def register(d: Any) -> None:
        gate = d.step(lambda deps, sc: 1, name="gate")
        guarded = d.step(guarded_body, deps=[gate], name="guarded", run_if=boom)
        d.step(refund_body, name="refund").after(guarded).trigger_rule(
            TriggerRule.ALL_FAILED
        )

    state, _ = make_state()
    with _fail_on_hang(), pytest.raises(DagPredicateError) as ei:
        make_context(state).dag(
            register, name="abortdag", config=DagConfig(max_concurrency=1)
        )

    # The typed error names the offending task and embeds the original cause.
    assert "guarded" in str(ei.value)
    assert "predicate boom" in str(ei.value)
    # Neither the guarded body nor the ALL_FAILED compensation ever ran.
    assert calls["guarded_body"] == 0
    assert calls["refund_body"] == 0

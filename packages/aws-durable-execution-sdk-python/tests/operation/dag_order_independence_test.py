"""DAG order-independence guards (Python analogue of JS ``DAG-19``, Go
``TestDagE2E_OrderIndependenceReplay`` and Java ``diamondWithWaitReplays…`` /
``wideFanOutTasksAlwaysObserveUpstreamValue``).

The whole DAG design rests on one property: a task's backend entity/operation id
is a **pure function of ``(scope-prefix, name)``** and carries *no* completion-
or registration-order input (``context.py::_create_task_id`` ->
``blake2b(f"{parent}-DAG_NODE_T_{name}")[:64]``, deliberately NOT touching the
per-context step counter). If that ever regressed to a counter-based id the DAG
would trip ``NonDeterministicExecutionError`` (or, worse, alias two tasks onto
one checkpoint) the first time tasks completed in a different order across a
replay boundary — the exact failure name-based ids exist to prevent.

Before this file Python had concurrency/throttle tests but *no* order-
independence guard, unlike the other three SDKs (see
``dag-review/GAP_concurrent_completion.md`` §2). These tests close that hole from
three complementary angles:

(a) ``test_registration_order_independence_identical_record`` — the DAG-19
    equivalent, the strongest signal: build the same diamond twice with the task
    *registration order permuted* and assert the derived per-task ids AND the
    normalized checkpoint record are identical. Asserts on the actual derived
    ids (not just results), so a counter-based-id regression cannot hide.
(b) ``test_completion_order_independence_under_concurrency`` — with
    ``max_concurrency=2`` and an event seam that forces a later-registered branch
    to complete FIRST (deterministic, not racy), assert the per-task ids and the
    DagResult equal a serial run.
(c) ``test_wide_fan_out_readers_observe_correct_upstream_value`` — the Java B1
    analogue: a barrier makes many readers complete simultaneously (concurrent
    writes to the shared results map) and a collector then verifies every task
    observed the correct upstream value, across many iterations.
"""

from __future__ import annotations

import hashlib
import threading
from typing import Any

from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DagConfig,
    TaskStatus,
)
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from tests.dag_support import InMemoryServiceClient, make_context, make_state

# ─────────────────────────────────────────────────────────────────────────
# Helpers: extract the derived ids / normalized record from the checkpoint
# stream that ``InMemoryServiceClient`` persisted during a ``.dag()`` run.
# ─────────────────────────────────────────────────────────────────────────


def _ids_by_name(client: InMemoryServiceClient) -> dict[str | None, str]:
    """Map every persisted operation's ``name`` -> its backend ``operation_id``.

    Includes the DAG container itself. Each DAG task mints exactly one operation
    id (its Start/Succeed updates reuse it), so this is 1:1.
    """
    return {op.name: op.operation_id for op in client.operations.values()}


def _task_ops(client: InMemoryServiceClient) -> list[Any]:
    """Every non-container (i.e. real task) operation."""
    return [
        op
        for op in client.operations.values()
        if op.sub_type is not OperationSubType.DAG and op.name is not None
    ]


def _assert_name_based(client: InMemoryServiceClient) -> None:
    """Assert every task id is *exactly* the name-based blake2b derivation.

    This is the direct structural guard: it recomputes
    ``blake2b(f"{parent}-DAG_NODE_T_{name}")[:64]`` from each op's own
    ``(parent_id, name)`` and asserts equality. A counter-based (or otherwise
    order-dependent) id would not match this recomputation.
    """
    task_ops = _task_ops(client)
    assert task_ops, "expected at least one task operation in the checkpoint stream"
    for op in task_ops:
        preimage = (
            f"{op.parent_id}-DAG_NODE_T_{op.name}"
            if op.parent_id
            else f"DAG_NODE_T_{op.name}"
        )
        expected = hashlib.blake2b(preimage.encode()).hexdigest()[:64]
        assert op.operation_id == expected, (
            f"task {op.name!r} id is not the name-based derivation: "
            f"{op.operation_id} != {expected}"
        )


def _dag_result_view(result: Any, names: list[str]) -> dict[str, Any]:
    """A name-keyed, order-independent semantic view of a ``DagResult``."""
    return {
        "results": {n: result.get_result(n) for n in names},
        "statuses": {n: result.get_status(n).name for n in names},
        "counts": (
            result.success_count,
            result.failure_count,
            result.skipped_count,
            result.total_count,
        ),
        "reason": result.completion_reason.name,
    }


def _normalized_record(
    client: InMemoryServiceClient, result: Any, names: list[str]
) -> dict[str, Any]:
    """A normalized (sorted, order-independent) checkpoint record.

    Mirrors JS ``DAG-19``'s ``sortDeep`` record: the per-operation projection is
    sorted by ``(name, operation_id)`` so that a mere change in
    completion/registration *ordering* does not perturb it — but a change in any
    ``operation_id`` (the counter-based-id regression) does.
    """
    ops = sorted(
        (
            {
                "name": op.name,
                "operation_id": op.operation_id,
                "parent_id": op.parent_id,
                "sub_type": op.sub_type.name if op.sub_type else None,
                "status": op.status.name,
                # Step results are simple JSON-able values here; the container
                # op carries no step_details (its serialized DagResult payload is
                # completion-order sensitive and is asserted semantically via
                # ``_dag_result_view`` instead).
                "result": op.step_details.result if op.step_details else None,
            }
            for op in client.operations.values()
        ),
        key=lambda r: (r["name"] or "", r["operation_id"]),
    )
    return {"ops": ops, "dag_result": _dag_result_view(result, names)}


# ─────────────────────────────────────────────────────────────────────────
# Diamond builder used by (a) and (b): root -> {b, c} -> merge.
# ─────────────────────────────────────────────────────────────────────────

_DIAMOND_NAMES = ["root", "b", "c", "merge"]


def _diamond_register(order: str) -> Any:
    """Return a ``register`` callback building root -> {b, c} -> merge, with the
    two middle branches declared in ``order`` ('bc' or 'cb'). The *logical* graph
    is identical for both; only the registration order changes.
    """

    def register(d: Any) -> None:
        root = d.step(lambda deps, sc: 100, name="root")
        if order == "bc":
            b = d.step(lambda deps, sc: deps["root"] + 1, deps=[root], name="b")
            c = d.step(lambda deps, sc: deps["root"] + 2, deps=[root], name="c")
        else:
            c = d.step(lambda deps, sc: deps["root"] + 2, deps=[root], name="c")
            b = d.step(lambda deps, sc: deps["root"] + 1, deps=[root], name="b")
        d.step(lambda deps, sc: deps["b"] + deps["c"], deps=[b, c], name="merge")

    return register


def _assert_diamond_results(result: Any) -> None:
    assert result.get_result("root") == 100
    assert result.get_result("b") == 101
    assert result.get_result("c") == 102
    assert result.get_result("merge") == 203
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert (
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ) == (4, 0, 0, 4)


# ─────────────────────────────────────────────────────────────────────────
# (a) Registration-order independence — the DAG-19 equivalent.
# ─────────────────────────────────────────────────────────────────────────


def test_registration_order_independence_identical_record():
    """Build the same diamond twice with b/c registered in swapped order and
    assert the derived per-task ids and the whole normalized checkpoint record
    are IDENTICAL.

    This directly proves ids are a pure function of ``(scope, name)`` and not of
    order. ``max_concurrency=1`` makes the scheduler fully deterministic so that,
    were ids counter-based, the permuted registration would deterministically
    assign b/c *different* ids between the two runs — i.e. this test has teeth
    (verified by the sabotage check in the implementation note).
    """
    cfg = DagConfig(max_concurrency=1)

    state1, client1 = make_state()
    r1 = make_context(state1).dag(_diamond_register("bc"), name="dag_oi", config=cfg)
    state2, client2 = make_state()
    r2 = make_context(state2).dag(_diamond_register("cb"), name="dag_oi", config=cfg)

    _assert_diamond_results(r1)
    _assert_diamond_results(r2)

    # Every task id is exactly the name-based blake2b derivation ...
    _assert_name_based(client1)
    _assert_name_based(client2)
    # ... and is byte-for-byte identical across the two registration orders.
    assert _ids_by_name(client1) == _ids_by_name(client2)
    # The full normalized checkpoint record is identical (the DAG-19 property).
    assert _normalized_record(client1, r1, _DIAMOND_NAMES) == _normalized_record(
        client2, r2, _DIAMOND_NAMES
    )


# ─────────────────────────────────────────────────────────────────────────
# (b) Completion-order independence under real concurrency.
# ─────────────────────────────────────────────────────────────────────────


def test_completion_order_independence_under_concurrency():
    """With ``max_concurrency=2`` force a later-registered branch to complete
    FIRST (deterministically, via an event seam — NOT sleeps/races) and assert
    the derived per-task ids and the DagResult equal a serial baseline.

    Determinism: ``b`` is registered first but blocks on an event that ``c``
    (registered second) sets right before it returns, so the observed completion
    order is always ``[c, b]`` — the reverse of registration. Because both must
    be in flight at once for ``c`` to unblock ``b``, this also proves the two
    branches genuinely ran concurrently (it would deadlock at concurrency 1).
    The core assertion (ids/result equal the serial run) is itself timing-
    independent since ids are name-based, so the test cannot flake.
    """
    # Serial baseline.
    state_s, client_s = make_state()
    r_s = make_context(state_s).dag(
        _diamond_register("bc"), name="dag_co", config=DagConfig(max_concurrency=1)
    )

    completion: list[str] = []
    completion_lock = threading.Lock()
    c_done = threading.Event()

    def register(d: Any) -> None:
        root = d.step(lambda deps, sc: 100, name="root")

        def b_body(deps: Any, sc: Any) -> int:
            # Registered FIRST, but wait until c has completed -> finishes SECOND.
            assert c_done.wait(timeout=5), "concurrency seam broke: c never completed"
            with completion_lock:
                completion.append("b")
            return deps["root"] + 1

        b = d.step(b_body, deps=[root], name="b")

        def c_body(deps: Any, sc: Any) -> int:
            # Registered SECOND, returns immediately -> finishes FIRST, then
            # unblocks b. Record completion before signalling so the order is
            # deterministic.
            with completion_lock:
                completion.append("c")
            value = deps["root"] + 2
            c_done.set()
            return value

        c = d.step(c_body, deps=[root], name="c")
        d.step(lambda deps, sc: deps["b"] + deps["c"], deps=[b, c], name="merge")

    state_c, client_c = make_state()
    r_c = make_context(state_c).dag(
        register, name="dag_co", config=DagConfig(max_concurrency=2)
    )

    _assert_diamond_results(r_s)
    _assert_diamond_results(r_c)

    # Completion order was the REVERSE of registration, deterministically.
    assert completion == ["c", "b"]
    # Ids are name-based -> identical to the serial run despite inverted timing.
    _assert_name_based(client_c)
    assert _ids_by_name(client_c) == _ids_by_name(client_s)
    # And the DagResult is identical.
    assert _dag_result_view(r_c, _DIAMOND_NAMES) == _dag_result_view(
        r_s, _DIAMOND_NAMES
    )


# ─────────────────────────────────────────────────────────────────────────
# (c) Wide fan-out — Java B1 analogue (shared results-map regression guard).
# ─────────────────────────────────────────────────────────────────────────


def test_wide_fan_out_readers_observe_correct_upstream_value():
    """Many readers of a common upstream, completing simultaneously under high
    concurrency, must each observe the CORRECT upstream value; a downstream
    collector must then observe every reader's correct value.

    A ``Barrier`` sized to the reader count forces all readers to be in flight
    and to complete at the same instant, maximizing concurrent writes to the
    shared results map (the exact condition of the Java B1 race,
    ``wideFanOutTasksAlwaysObserveUpstreamValue``). Python's results map is
    lock-guarded, so this is a regression guard that must pass reliably; it is
    repeated over many iterations to be meaningful while staying fast.
    """
    fan_out = 16
    iterations = 20
    sentinel = {"marker": "root-value", "n": 987654321}
    mismatches: list[Any] = []
    mismatch_lock = threading.Lock()

    def make_reader(idx: int) -> Any:
        def reader(deps: Any, sc: Any) -> int:
            if deps["root"] != sentinel:
                with mismatch_lock:
                    mismatches.append(("reader", idx, deps["root"]))
            # Complete simultaneously with the other readers -> concurrent writes
            # into the shared results map.
            try:
                barrier.wait()
            except threading.BrokenBarrierError:  # pragma: no cover - only on hang
                with mismatch_lock:
                    mismatches.append(("barrier-broken", idx))
            return idx

        return reader

    def collector(deps: Any, sc: Any) -> str:
        for i in range(fan_out):
            if deps[f"r{i}"] != i:
                with mismatch_lock:
                    mismatches.append(("collector", i, deps[f"r{i}"]))
        return "ok"

    for _ in range(iterations):
        barrier = threading.Barrier(fan_out, timeout=10)

        def register(d: Any) -> None:
            root = d.step(lambda deps, sc: sentinel, name="root")
            readers = [
                d.step(make_reader(i), deps=[root], name=f"r{i}")
                for i in range(fan_out)
            ]
            d.step(collector, deps=readers, name="collector")

        state, _ = make_state()
        result = make_context(state).dag(
            register, name="dag_fanout", config=DagConfig(max_concurrency=fan_out)
        )

        assert result.get_status("collector") is TaskStatus.SUCCEEDED
        assert result.get_result("collector") == "ok"
        assert result.success_count == fan_out + 2  # root + readers + collector
        assert result.completion_reason is DagCompletionReason.ALL_COMPLETED

    assert mismatches == [], f"tasks observed wrong dep values under concurrency: {mismatches}"

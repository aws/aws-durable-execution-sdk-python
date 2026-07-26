"""Integration coverage for the large-payload gap (conformance 10-15).

Runs the *exact* ``10-15`` graph in-process across a real container replay, so
the regressions the cloud suite cannot catch are caught here without a deploy.

The DAG ``bigdag`` has eight root step tasks ``p1``..``p8``; task ``pN`` returns
its letter repeated 51200 times, so the aggregate is ~410KB (8 * 51200 = 409600
chars) -- comfortably over the 256KB checkpoint limit -- while every individual
task result stays far under it. When the container result is checkpointed the
aggregate is OFFLOADED: Python marks the container ``ReplayChildren=true`` and
writes no envelope (unlike JS, which writes an SDK-owned ``DagSummary``).

The reconstruct-vs-re-execute divergence only fires when a SUCCEEDED container is
REPLAYED, so the driver mirrors the handler: it runs ``dag()``, a checkpointed
``digestBefore`` step, then a ``wait`` that SUSPENDS the invocation; the next
invocation replays the completed container. Three things are asserted:

* **Aggregate fidelity** across the replay -- every task result is individually
  retrievable and byte-identical afterwards, including one full 51200-char value,
  and the language-neutral digest ``"8:409600:abcdefgh"`` matches before and
  after the suspend.
* **Task bodies are not re-invoked** -- external per-task counters prove each body
  ran exactly once across the offload and the container replay. Under
  re-execution the DAG child body re-runs, but each task's step operation must
  fast-path from its own (small, normally-checkpointed) result; a body running
  twice is a duplicated customer side effect, the bug this test exists to catch.
* **The re-execution path was actually taken** -- Python uses ``ReplayChildren``,
  not an envelope, so the container operation is asserted to carry
  ``replay_children is True`` with an empty checkpointed payload (no envelope).
  This is the observable hook that distinguishes Python's strategy from JS's.
"""

from __future__ import annotations

import threading
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.dag import DagCompletionReason, DagConfig
from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from aws_durable_execution_sdk_python.lambda_service import (
    OperationSubType,
    OperationType,
)
from tests.dag_support import InMemoryServiceClient
from tests.operation.dag_concurrency_coverage_test import (  # reuse the proven harness
    _complete_wait,
    _fail_on_hang,
    _seeded_state,
)

_TASK_COUNT = 8
_REPEAT = 51200
_TASK_NAMES = [f"p{i}" for i in range(1, _TASK_COUNT + 1)]
_EXPECTED_DIGEST = "8:409600:abcdefgh"


def _digest(result: Any) -> str:
    """``"<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>"`` over p1..p8."""
    total_length = 0
    first_chars = []
    for name in _TASK_NAMES:
        value = result.get_result(name)
        total_length += len(value)
        first_chars.append(value[0])
    return f"{len(_TASK_NAMES)}:{total_length}:{''.join(first_chars)}"


def _dag_container_op(client: InMemoryServiceClient) -> Any:
    """The single DAG-container operation (CONTEXT op with SubType=Dag)."""
    containers = [
        op
        for op in client.operations.values()
        if op.operation_type is OperationType.CONTEXT
        and op.sub_type is OperationSubType.DAG
    ]
    assert len(containers) == 1, f"expected one DAG container, found {len(containers)}"
    return containers[0]


def test_10_15_large_payload_survives_container_replay() -> None:
    """The 10-15 graph across a real suspend: the ~410KB aggregate is offloaded,
    the completed container is replayed on the next invocation, and the aggregate
    comes back byte-identical via child-body re-execution.

    A single driver run exercises all three assertions (fidelity, single
    invocation per body, and the re-execution path) so the counters and the
    stored container operation observe the same replay.
    """
    # External per-task counters: a body that runs twice increments twice.
    calls = {name: 0 for name in _TASK_NAMES}
    calls_lock = threading.Lock()

    def _bump(name: str) -> None:
        with calls_lock:
            calls[name] += 1

    def _make_body(name: str, letter: str):
        def _body(_deps: Any, _sc: Any) -> str:
            _bump(name)
            return letter * _REPEAT

        return _body

    # register runs once per DAG-body execution. It runs a second time on the
    # replay iff the container is re-executed (ReplayChildren) rather than
    # reconstructed from an envelope -- direct evidence of the re-execution path.
    register_calls = {"n": 0}

    def register(d: Any) -> None:
        register_calls["n"] += 1
        for i, name in enumerate(_TASK_NAMES):
            d.step(_make_body(name, chr(ord("a") + i)), name=name)

    invocations = {"n": 0}

    def run(ctx: DurableContext):
        invocations["n"] += 1
        result = ctx.dag(
            register, name="bigdag", config=DagConfig(max_concurrency=1)
        )
        # Checkpointed step: computed once from the live DagResult, fast-pathed
        # from its own checkpoint after the suspend, so it carries the
        # pre-suspend digest across the boundary.
        digest_before: str = ctx.step(
            lambda _sc: _digest(result), name="digestBefore"
        )
        # Ends the invocation; the next one replays the completed container.
        ctx.wait(Duration.from_seconds(2), name="pauseForReplay")
        # Recomputed from the REPLAYED DagResult after resume.
        digest_after = _digest(result)
        return result, digest_before, digest_after

    client = InMemoryServiceClient()
    final: tuple[Any, str, str] | None = None
    with _fail_on_hang():
        # First invocation resolves the DAG then suspends on the wait; the
        # second replays the completed container and finishes.
        for _ in range(2):
            state = _seeded_state(client, dict(client.operations))
            ctx = DurableContext(
                state=state,
                execution_context=ExecutionContext(
                    durable_execution_arn=state.durable_execution_arn
                ),
                parent_id=None,
            )
            try:
                final = run(ctx)
                break
            except SuspendExecution:
                assert _complete_wait(client, "pauseForReplay")

    assert final is not None, "handler never completed across the suspend"
    result, digest_before, digest_after = final

    # --- Aggregate fidelity across the replay --------------------------------
    assert result.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert (
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ) == (8, 0, 0, 8)
    # Every task result is individually retrievable and byte-identical, and at
    # least one full 51200-char value is checked in its entirety.
    for i, name in enumerate(_TASK_NAMES):
        expected = chr(ord("a") + i) * _REPEAT
        assert result.get_result(name) == expected
    assert result.get_result("p1") == "a" * _REPEAT  # full-value check
    assert len(result.get_result("p1")) == _REPEAT
    # The language-neutral assertion: the digest survived the offload and came
    # back identical through the replay strategy.
    assert digest_before == _EXPECTED_DIGEST
    assert digest_after == _EXPECTED_DIGEST
    assert digest_before == digest_after

    # --- Task bodies were not re-invoked -------------------------------------
    # The DAG child body re-runs under ReplayChildren, but each task step must
    # fast-path from its own checkpoint. Exactly one invocation per body.
    for name in _TASK_NAMES:
        assert calls[name] == 1, f"task {name} body ran {calls[name]} times, expected 1"

    # --- The re-execution path was actually taken ----------------------------
    # Python offloads via ReplayChildren with NO envelope. The container op
    # carries replay_children=True (proving the aggregate exceeded the 256KB
    # limit and was offloaded) and an empty checkpointed payload (proving no
    # DagSummary envelope was written -- the JS-only strategy).
    container = _dag_container_op(client)
    assert container.context_details is not None
    assert container.context_details.replay_children is True
    assert (container.context_details.result or "") == ""

    # Corroborating evidence that the interesting path was genuinely exercised:
    # the invocation actually suspended and resumed (two invocations), and the
    # DAG body was re-executed on the replay (register ran twice) rather than
    # reconstructed from an envelope.
    assert invocations["n"] == 2, "container was not replayed across a suspend"
    assert register_calls["n"] == 2, "DAG body was not re-executed on replay"

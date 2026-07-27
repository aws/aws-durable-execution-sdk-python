"""Integration coverage for the nested + large-payload intersection (10-17).

Runs the *exact* ``10-17`` graph in-process across a real container replay, so
the regression the cloud suite cannot catch is caught here without a deploy.

The outer DAG ``outernested`` (``max_concurrency=1``) has a single task
``inner`` that is itself a nested DAG (``max_concurrency=1``) with six root step
tasks ``p1``..``p6``; task ``pN`` returns its letter repeated 51200 times, so the
inner aggregate is ~307KB (6 * 51200 = 307200 chars) -- comfortably over the
256KB checkpoint limit. The inner container therefore OFFLOADS (drops ``tasks``,
sets ``ReplayChildren``), and because the outer embeds the inner result in full,
the outer aggregate is over the limit too, so the OUTER container offloads as
well. This is the untested intersection: an offloaded outer whose one task is an
offloaded inner.

The bug this guards against (confirmed in TypeScript): on the outer's
reconstruct path the inner DagResult is rebuilt from the inner container's
tasks-less envelope alone, coming back EMPTY while still claiming
``ALL_COMPLETED`` -- so the per-task detail is silently lost. The fix is that the
outer's reconstruct re-runs its register graph, and the ``inner`` nested-dag task
re-runs through the container executor, which detects the offloaded inner and
RECONSTRUCTS it from the inner's own retained child checkpoints, recursively
(contract rule 2). Three things are asserted:

* **Recursive fidelity** -- after the outer container is replayed, the inner
  DagResult reports ``ALL_COMPLETED`` with counts ``[6,0,0,6]`` AND every inner
  per-task result is individually retrievable and byte-identical, so the
  language-neutral digest ``"6:307200:abcdef"`` matches before and after the
  suspend. The digest, not the reason, is the decisive check: under the bug the
  reason still reads ``ALL_COMPLETED`` from the honest aggregate while the digest
  after replay would differ (empty inner).
* **Task bodies are not re-invoked** -- external per-task counters prove each
  inner body ran exactly once across the offload of BOTH containers and the
  double replay. Nesting doubles the number of containers that replay, so a body
  running twice is the duplicated-side-effect bug this test exists to catch.
* **Both containers offloaded + reconstructed** -- the outer AND the inner DAG
  container operations each carry ``replay_children is True`` with ``tasks``
  dropped but the aggregate summary present, and both register graphs re-ran on
  the reconstruct.
"""

from __future__ import annotations

import json
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

_INNER_COUNT = 6
_REPEAT = 51200
_INNER_NAMES = [f"p{i}" for i in range(1, _INNER_COUNT + 1)]
_EXPECTED_DIGEST = "6:307200:abcdef"


def _digest(inner: Any) -> str:
    """``"<taskCount>:<totalLength>:<firstCharOfEachInnerTaskInOrder>"`` over
    p1..p6, so the first-char run is deterministic regardless of completion
    order. For the 10-17 inner graph this is exactly ``"6:307200:abcdef"``."""
    total_length = 0
    first_chars = []
    for name in _INNER_NAMES:
        value = inner.get_result(name)
        total_length += len(value)
        first_chars.append(value[0])
    return f"{len(_INNER_NAMES)}:{total_length}:{''.join(first_chars)}"


def _dag_container_ops(client: InMemoryServiceClient) -> list[Any]:
    """Every DAG-container operation (CONTEXT op with SubType=Dag)."""
    return [
        op
        for op in client.operations.values()
        if op.operation_type is OperationType.CONTEXT
        and op.sub_type is OperationSubType.DAG
    ]


def test_10_17_nested_large_payload_survives_container_replay() -> None:
    """The 10-17 graph across a real suspend: the ~307KB inner aggregate offloads
    both the inner AND the outer container; the completed outer is replayed on
    the next invocation and the inner per-task detail comes back byte-identical
    via recursive reconstruction from the inner's own child checkpoints."""
    # External per-inner-task counters: a body that runs twice increments twice.
    calls = {name: 0 for name in _INNER_NAMES}
    calls_lock = threading.Lock()

    def _bump(name: str) -> None:
        with calls_lock:
            calls[name] += 1

    def _make_body(name: str, letter: str):
        def _body(_deps: Any, _sc: Any) -> str:
            _bump(name)
            return letter * _REPEAT

        return _body

    # Each register runs once per DAG-body execution; it runs a second time on
    # the replay iff its container is reconstructed (ReplayChildren) -- direct
    # evidence that both the outer and inner reconstruct paths were taken.
    outer_register_calls = {"n": 0}
    inner_register_calls = {"n": 0}

    def register(d: Any) -> None:
        outer_register_calls["n"] += 1

        def inner_register(sd: Any) -> None:
            inner_register_calls["n"] += 1
            for i, name in enumerate(_INNER_NAMES):
                sd.step(_make_body(name, chr(ord("a") + i)), name=name)

        d.dag(inner_register, name="inner", config=DagConfig(max_concurrency=1))

    invocations = {"n": 0}

    def run(ctx: DurableContext):
        invocations["n"] += 1
        outer = ctx.dag(
            register, name="outernested", config=DagConfig(max_concurrency=1)
        )
        # Checkpointed step: the inner digest computed once from the live inner
        # DagResult, fast-pathed from its own checkpoint after the suspend, so it
        # carries the pre-suspend digest across the boundary.
        digest_before: str = ctx.step(
            lambda _sc: _digest(outer.get_result("inner")), name="digestBefore"
        )
        # Ends the invocation; the next one replays the completed outer container.
        ctx.wait(Duration.from_seconds(2), name="pauseForReplay")
        # Recomputed from the REPLAYED (recursively reconstructed) inner result.
        digest_after = _digest(outer.get_result("inner"))
        return outer, digest_before, digest_after

    client = InMemoryServiceClient()
    final: tuple[Any, str, str] | None = None
    with _fail_on_hang():
        # First invocation resolves the nested DAG then suspends on the wait; the
        # second replays the completed outer container and finishes.
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
    outer, digest_before, digest_after = final

    # --- Recursive fidelity: the inner survived the offload of BOTH containers -
    inner = outer.get_result("inner")
    assert inner is not None, "inner DagResult was lost across the replay"
    assert inner.completion_reason is DagCompletionReason.ALL_COMPLETED
    assert (
        inner.success_count,
        inner.failure_count,
        inner.skipped_count,
        inner.total_count,
    ) == (6, 0, 0, 6)
    # Every inner task result is individually retrievable and byte-identical,
    # and at least one full 51200-char value is checked in its entirety. This is
    # the rule-2 assertion: the per-task detail is PRESENT, not just the honest
    # aggregate.
    for i, name in enumerate(_INNER_NAMES):
        expected = chr(ord("a") + i) * _REPEAT
        assert inner.get_result(name) == expected
    assert inner.get_result("p1") == "a" * _REPEAT  # full-value check
    assert len(inner.get_result("p1")) == _REPEAT
    # The decisive language-neutral assertion.
    assert digest_before == _EXPECTED_DIGEST
    assert digest_after == _EXPECTED_DIGEST
    assert digest_before == digest_after

    # --- Inner task bodies were not re-invoked --------------------------------
    for name in _INNER_NAMES:
        assert calls[name] == 1, f"inner body {name} ran {calls[name]} times, expected 1"

    # --- Both containers offloaded + reconstructed ----------------------------
    containers = _dag_container_ops(client)
    assert len(containers) == 2, (
        f"expected outer + inner DAG containers, found {len(containers)}"
    )
    for container in containers:
        assert container.context_details is not None
        assert container.context_details.replay_children is True, (
            "a DAG container did not offload (ReplayChildren) as expected"
        )
        envelope = json.loads(container.context_details.result)
        assert envelope["type"] == "DagResult"
        assert "tasks" not in envelope  # per-task detail offloaded to children
        assert envelope["completionReason"] == "ALL_COMPLETED"

    # The inner container's aggregate summary is canonical and correct.
    inner_container = next(c for c in containers if c.name == "inner")
    inner_env = json.loads(inner_container.context_details.result)
    assert inner_env["totalCount"] == _INNER_COUNT
    assert inner_env["successCount"] == _INNER_COUNT
    assert inner_env["failureCount"] == 0
    assert inner_env["skippedCount"] == 0
    assert inner_env["startedTaskNames"] == []

    # Corroborating evidence the interesting path was genuinely exercised: the
    # invocation suspended and resumed (two invocations), and BOTH register
    # graphs re-ran on the reconstruct so each inner task could fast-path from
    # its own retained child checkpoint.
    assert invocations["n"] == 2, "outer container was not replayed across a suspend"
    assert outer_register_calls["n"] == 2, "outer DAG graph was not re-run on reconstruct"
    assert inner_register_calls["n"] == 2, "inner DAG graph was not re-run on reconstruct"

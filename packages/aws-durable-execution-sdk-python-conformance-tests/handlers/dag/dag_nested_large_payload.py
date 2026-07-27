"""DAG conformance 10-17: nested DAG whose inner aggregate offloads, across a
container replay -- the untested intersection of nesting and large payloads.

Modeled on ``10-15`` (flat large payload) but with the large aggregate produced
by a NESTED DAG. ``max_concurrency=1`` everywhere for determinism.

Outer DAG ``outernested`` has a single task ``inner`` that is itself a nested DAG
with six root step tasks ``p1``..``p6``; task ``pN`` returns its letter repeated
51200 times (``p1`` -> "a" x 51200, ... ``p6`` -> "f" x 51200). The inner
aggregate is ~307KB (6 * 51200 = 307200 chars), comfortably over the 256KB
checkpoint threshold, so the INNER container is OFFLOADED. Because the outer
embeds the inner result in full, the OUTER aggregate is over the limit too, so
the outer container is offloaded as well. Every individual inner task result
stays far under the limit, so only the two aggregates offload.

The reconstruct-vs-inline divergence only fires when a SUCCEEDED CONTAINER IS
REPLAYED, so -- exactly as in 10-15 -- the handler resolves the DAG, records a
checkpointed digest, then SUSPENDS on an outer 2s wait so the next invocation
replays BOTH completed containers:

1. ``dag(...)`` resolves ``outernested``; its one task ``inner`` resolves the
   ~307KB inner aggregate (inner container offloaded, outer container offloaded).
2. An outer (handler-level) step reads the inner ``DagResult`` and computes a
   compact digest ``"<innerTaskCount>:<totalLen>:<firstCharOfEachInnerTask>"``
   -> exactly ``"6:307200:abcdef"``. Being a checkpointed step it survives the
   suspend as ``digestBefore``.
3. An outer ``wait`` of 2 seconds ends the invocation; the next one replays both
   completed containers.
4. After the resume the identical digest is recomputed from the REPLAYED inner
   result -> ``digestAfter``.

On replay the outer container reconstructs from its retained child checkpoints;
its ``inner`` task re-runs through the DAG container executor, which detects the
offloaded inner and reconstructs it RECURSIVELY from the inner's own child
checkpoints, restoring full per-task detail. The decisive, language-neutral
assertion is ``digestBefore == digestAfter == "6:307200:abcdef"`` with
``match: true``: it proves the inner per-task detail survived the offload of BOTH
containers. Under the bug the inner comes back empty, so ``digestAfter`` differs
while ``innerReason`` would still read ``ALL_COMPLETED`` from a fabricated
result -- which is exactly why the digest, not the reason, is the decisive check.

Returns the canonical summary defined by test-requirements/dag/10-17.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution

_INNER_COUNT = 6
_REPEAT = 51200
_INNER_NAMES = [f"p{i}" for i in range(1, _INNER_COUNT + 1)]


def _inner_counts(inner: DagResult) -> list[int]:
    """``[total, failed, skipped, succeeded]`` for the inner DagResult."""
    return [
        inner.total_count,
        inner.failure_count,
        inner.skipped_count,
        inner.success_count,
    ]


def _digest(inner: DagResult) -> str:
    """``"<innerTaskCount>:<totalLength>:<firstCharOfEachInnerTaskInOrder>"``.

    Computed over p1..p6 in order, so the first-char run is deterministic
    regardless of completion order. For the 10-17 inner graph this is exactly
    ``"6:307200:abcdef"``.
    """
    total_length = 0
    first_chars = []
    for name in _INNER_NAMES:
        value = inner.get_result(name)
        total_length += len(value)
        first_chars.append(value[0])
    return f"{len(_INNER_NAMES)}:{total_length}:{''.join(first_chars)}"


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        def inner_register(sd: DagContext) -> None:
            for i, name in enumerate(_INNER_NAMES):
                letter = chr(ord("a") + i)
                # Default-arg binding captures this task's letter (avoids the
                # late-binding closure trap over the loop variable).
                sd.step(
                    lambda deps, sc, _letter=letter: _letter * _REPEAT, name=name
                )

        d.dag(inner_register, name="inner", config=DagConfig(max_concurrency=1))

    result = context.dag(
        register, name="outernested", config=DagConfig(max_concurrency=1)
    )

    # Checkpointed step: computed once from the live inner DagResult, then
    # fast-pathed from its own checkpoint on the post-suspend replay, so it
    # carries the pre-suspend digest across the boundary.
    digest_before: str = context.step(
        lambda _sc: _digest(result.get_result("inner")), name="digestBefore"
    )

    # Forces the invocation to end; the next one replays both completed containers.
    context.wait(Duration.from_seconds(2), name="pauseForReplay")

    # Recomputed from the REPLAYED (recursively reconstructed) inner result.
    inner = result.get_result("inner")
    digest_after = _digest(inner)

    return {
        "reason": result.completion_reason.value,
        "innerReason": inner.completion_reason.value,
        "innerCounts": _inner_counts(inner),
        "digestBefore": digest_before,
        "digestAfter": digest_after,
        "match": digest_before == digest_after,
    }

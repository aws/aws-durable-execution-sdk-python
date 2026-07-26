"""DAG conformance 10-15: large-payload aggregate offload survives a replay.

max_concurrency=1 for determinism. DAG name ``bigdag``. Eight root step tasks
``p1``..``p8`` with no deps; task ``pN`` returns its own letter repeated 51200
times (``p1`` -> "a" x 51200, ``p2`` -> "b" x 51200, ... ``p8`` -> "h" x 51200).
The aggregate is ~410KB (8 * 51200 = 409600 chars), comfortably over the 256KB
checkpoint threshold, so the container result is OFFLOADED; every individual
task result stays far under it, so only the aggregate is offloaded.

The reconstruct-vs-re-execute divergence (JS writes a DagSummary envelope and
reconstructs from it; Python/Java/Go re-execute the DAG child body via
ReplayChildren with no envelope) only fires when a SUCCEEDED CONTAINER IS
REPLAYED. A DAG that completes and returns in one invocation never exercises it.
So this handler deliberately SUSPENDS after the DAG resolves, via a 2s wait, so
the next invocation replays the completed container:

1. ``dag(...)`` resolves the ~410KB aggregate.
2. A step (outside the DAG) computes a digest from the DagResult:
   ``"<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>"`` -> exactly
   ``"8:409600:abcdefgh"``. Because it is a step it is checkpointed and survives
   the suspend as ``digestBefore``.
3. A ``wait`` of 2 seconds ends the invocation.
4. After the resume, the same digest is recomputed from the REPLAYED DagResult
   -> ``digestAfter``.

The language-neutral assertion is ``digestBefore == digestAfter ==
"8:409600:abcdefgh"``: the aggregate survived the offload AND came back
identical through whichever replay strategy the SDK uses (child-body
re-execution here). Assert outcome only -- the container's succeeded payload
legitimately differs across SDKs, so 10-15.yaml pins no ExpectedExecutionHistory.

This scenario deliberately uses NO completionConfig: Python has a documented
exception where a faithful STARTED-set is not reproduced under large-payload
early completion (inherited from map/parallel). All eight tasks complete, so it
does not walk into that. The returned summary is kept small -- the digest, never
the payload.

Returns the canonical summary defined by test-requirements/dag/10-15.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution

_TASK_COUNT = 8
_REPEAT = 51200
_TASK_NAMES = [f"p{i}" for i in range(1, _TASK_COUNT + 1)]


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


def _digest(result: DagResult) -> str:
    """``"<taskCount>:<totalLength>:<firstCharOfEachTaskInOrder>"``.

    Computed over p1..p8 in order, so the first-char run is deterministic
    regardless of task completion order. For the 10-15 graph this is exactly
    ``"8:409600:abcdefgh"``.
    """
    total_length = 0
    first_chars = []
    for name in _TASK_NAMES:
        value = result.get_result(name)
        total_length += len(value)
        first_chars.append(value[0])
    return f"{len(_TASK_NAMES)}:{total_length}:{''.join(first_chars)}"


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        for i, name in enumerate(_TASK_NAMES):
            letter = chr(ord("a") + i)
            # Default-arg binding captures this task's letter (avoids the
            # late-binding closure trap over the loop variable).
            d.step(lambda deps, sc, _letter=letter: _letter * _REPEAT, name=name)

    result = context.dag(register, name="bigdag", config=DagConfig(max_concurrency=1))

    # Checkpointed step: computed once from the live DagResult, then fast-pathed
    # from its own checkpoint on the post-suspend replay, so it carries the
    # pre-suspend digest across the boundary.
    digest_before: str = context.step(
        lambda _sc: _digest(result), name="digestBefore"
    )

    # Forces the invocation to end; the next one replays the completed container.
    context.wait(Duration.from_seconds(2), name="pauseForReplay")

    # Recomputed from the REPLAYED DagResult after resume.
    digest_after = _digest(result)

    return {
        "reason": result.completion_reason.value,
        "counts": _counts(result),
        "digestBefore": digest_before,
        "digestAfter": digest_after,
        "match": digest_before == digest_after,
    }

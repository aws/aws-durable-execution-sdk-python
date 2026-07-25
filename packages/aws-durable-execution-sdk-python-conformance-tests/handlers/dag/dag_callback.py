"""DAG conformance 10-11: task that is a wait-for-callback (flat).

pre(step->"ready") -> cb(callback[dep pre]) -> post(step[dep cb] ->
cb + "_done"). The callback's native WaitForCallback op is checkpointed directly
under the Dag container (flat, name-based). The submitter receives the generated
callback id and does nothing durable (same as the 7-1 wait_for_callback
handler); the conformance runner completes the callback externally with a
success payload, which ``cb`` resolves to (a string). max_concurrency=1 for a
deterministic topological order.

This scenario suspends until the external callback arrives.

Returns the canonical summary defined by test-requirements/dag/10-11.yaml.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    WaitForCallbackContext,
)
from aws_durable_execution_sdk_python.dag import DagConfig, DagContext, DagResult
from aws_durable_execution_sdk_python.execution import durable_execution


def _counts(result: DagResult) -> list[int]:
    return [
        result.success_count,
        result.failure_count,
        result.skipped_count,
        result.total_count,
    ]


def _normalize(value: str) -> str:
    """Strip a single pair of surrounding double-quote characters if present.

    The default callback deserializer returns the raw payload text, which in
    some SDKs includes the surrounding quote characters. The runner's payload is
    alphanumeric, so stripping one surrounding pair is unambiguous.
    """
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


def _submitter(_deps, _callback_id: str, _ctx: WaitForCallbackContext) -> None:
    """Receives the generated callback id; does nothing durable."""


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    def register(d: DagContext) -> None:
        pre = d.step(lambda deps, sc: "ready", name="pre")
        cb = d.wait_for_callback(_submitter, deps=[pre], name="cb")
        d.step(lambda deps, sc: _normalize(deps[cb]) + "_done", deps=[cb], name="post")

    result = context.dag(
        register, name="callbackdag", config=DagConfig(max_concurrency=1)
    )
    return {
        "reason": result.completion_reason.value,
        "statuses": {name: te.status.value for name, te in result.results.items()},
        "counts": _counts(result),
        "cb": _normalize(result.results["cb"].result),
        "post": result.results["post"].result,
    }

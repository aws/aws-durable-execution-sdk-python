"""Custom (non-identity) SerDes round-trip across every operation.

Demonstrates the first-run / replay result-equality guarantee: with a
non-identity ``SerDes``, each durable operation returns the *round-tripped*
value - the value produced by ``serialize`` then ``deserialize`` - on the first
run, which is exactly the value a replay reconstructs from the checkpoint.
Returning the raw in-memory result on the first run would make the first run and
replay disagree whenever the serdes is not a perfect round-trip.

A single deployed function exercises the guarantee across every operation that
checkpoints a serialized result, so there is no need to deploy a separate
example per operation:

* ``step``
* ``wait_for_condition``
* ``run_in_child_context`` - normal, virtual, and large-payload (ReplayChildren)

The shared ``MarkerSerDes`` strips a ``round_tripped`` marker on ``serialize``
and re-adds it on ``deserialize``, so ``deserialize(serialize(x)) != x``. Every
operation's result therefore carries ``round_tripped=True`` only if the SDK
handed back the canonical round-tripped value rather than the raw one.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import ChildConfig, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)

# Results larger than this are not checkpointed in full; the child context
# switches to ReplayChildren mode (a compact summary is checkpointed and the
# child re-executes on replay). Kept in sync with the SDK constant.
CHECKPOINT_SIZE_LIMIT_BYTES = 256 * 1024


class MarkerSerDes(SerDes[dict[str, Any]]):
    """Non-identity serdes: ``deserialize`` re-adds a marker ``serialize`` strips.

    ``deserialize(serialize(value)) == {**value, "round_tripped": True}``, which
    differs from ``value`` whenever ``value`` lacks the marker. That makes the
    round-trip observable in the value each operation returns.
    """

    def serialize(self, value: dict[str, Any], _: SerDesContext) -> str:
        payload = {k: v for k, v in dict(value).items() if k != "round_tripped"}
        return json.dumps(payload)

    def deserialize(self, data: str, _: SerDesContext) -> dict[str, Any]:
        return {**json.loads(data), "round_tripped": True}


def _large_child_summary(_result: dict[str, Any]) -> str:
    """Compact summary checkpointed in place of the large child result."""
    return json.dumps({"type": "large-child-result"})


def _stop_immediately(
    _state: dict[str, Any], _attempt: int
) -> WaitForConditionDecision:
    """Meet the wait_for_condition on the first check."""
    return WaitForConditionDecision.stop_polling()


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, bool]:
    """Run every operation with a non-identity serdes and report the round-trip.

    Each returned value carries ``round_tripped=True`` only because the SDK
    returned ``deserialize(serialize(result))`` on the first run - the same
    value the corresponding replay path produces.
    """
    serdes = MarkerSerDes()

    step_result = context.step(
        lambda _step_ctx: {"op": "step"},
        name="step",
        config=StepConfig(serdes=serdes),
    )

    child_result = context.run_in_child_context(
        lambda _child_ctx: {"op": "child"},
        name="child",
        config=ChildConfig(serdes=serdes),
    )

    virtual_result = context.run_in_child_context(
        lambda _child_ctx: {"op": "virtual-child"},
        name="virtual-child",
        config=ChildConfig(serdes=serdes, is_virtual=True),
    )

    # A result larger than the checkpoint limit forces ReplayChildren mode: only
    # the summary is checkpointed, yet the returned value is still the full
    # round-tripped result.
    large_blob = "x" * (CHECKPOINT_SIZE_LIMIT_BYTES + 1)
    large_result = context.run_in_child_context(
        lambda _child_ctx: {"op": "large-child", "blob": large_blob},
        name="large-child",
        config=ChildConfig(serdes=serdes, summary_generator=_large_child_summary),
    )

    wait_for_condition_result = context.wait_for_condition(
        check=lambda _state, _ctx: {"op": "wait-for-condition"},
        config=WaitForConditionConfig(
            initial_state={},
            wait_strategy=_stop_immediately,
            serdes=serdes,
        ),
        name="wait-for-condition",
    )

    # Only the marker booleans are returned (never the large blob) so the
    # handler result stays small and easy to assert on.
    return {
        "step_round_tripped": step_result.get("round_tripped", False),
        "child_round_tripped": child_result.get("round_tripped", False),
        "virtual_child_round_tripped": virtual_result.get("round_tripped", False),
        "large_child_round_tripped": large_result.get("round_tripped", False),
        "wait_for_condition_round_tripped": wait_for_condition_result.get(
            "round_tripped", False
        ),
    }

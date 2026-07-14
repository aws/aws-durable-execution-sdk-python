"""Step with a custom (non-identity) SerDes.

Demonstrates that a step returns the *canonical, round-tripped* value — the
value produced by ``serialize`` then ``deserialize`` — rather than the raw
in-memory object the step function returned. Because a step's result is
deserialized from the checkpoint on replay, returning the round-tripped value
on the first run keeps the result identical across the first run and every
replay.

Here the custom SerDes normalizes the order on the way to the checkpoint: it
persists only the canonical fields, sorts the tags, and drops a transient,
non-persisted field. The handler uses the step result immediately, so the value
observed on the first run is already the canonical form (sorted tags, transient
field dropped) — the same value a replay would deserialize from the checkpoint.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


class OrderSerDes(SerDes[dict[str, Any]]):
    """Persist a canonical order: keep stable fields, sort tags, drop transients.

    ``deserialize(serialize(order))`` is intentionally not the identity — the
    transient field is dropped and the tags are sorted — so the difference
    between the raw result and the round-tripped result is observable.
    """

    def serialize(self, value: dict[str, Any], _: SerDesContext) -> str:
        canonical: dict[str, Any] = {
            "order_id": value["order_id"],
            "tags": sorted(value["tags"]),
        }
        return json.dumps(canonical)

    def deserialize(self, data: str, _: SerDesContext) -> dict[str, Any]:
        return json.loads(data)


@durable_execution
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    """Build an order in a step and use its round-tripped result immediately."""
    order = context.step(
        lambda _: {
            "order_id": "ORD-42",
            "tags": ["priority", "gift", "fragile"],  # unsorted on purpose
            "transient_score": 0.99,  # not persisted by OrderSerDes
        },
        name="build_order",
        config=StepConfig(serdes=OrderSerDes()),
    )

    # `order` is the canonical, round-tripped value even on the first run:
    # tags are sorted and the transient field is gone. A replay would produce
    # the exact same value by deserializing the checkpoint.
    return {
        "order_id": order["order_id"],
        "tags": order["tags"],  # canonical: sorted
        "has_transient": "transient_score" in order,  # False after round-trip
    }

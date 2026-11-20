# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Python expected outcomes and worker identity for shared evidence normalization."""

from collections.abc import Mapping, Sequence
from inspect import signature

from aws_durable_execution_conformance_tests.lmi.source_evidence import (
    build_evidence as shared_build_evidence,
)


# One observed execution per durable operation ID, except for the retry fixture.
# Each map item owns a distinct operation ID despite sharing the name "item".
_EXPECTED_REPLAY_EFFECTS = {
    "success": 1,
    "failure": 1,
    "retry": 2,
    "item": 1,
    "left": 1,
    "right": 1,
}


def expected_outcome(item, scenario):
    fixture = item.get("payload", {}).get("scenario")
    if fixture == "failure":
        return {
            "status": "FAILED",
            "error": {
                "type": "ValueError",
                "message": "expected:" + item["marker"],
            },
        }
    elif fixture == "deadline":
        # This fixture asserts native deadline/retry behavior. Its logical
        # execution may still be active or may finish after normal release.
        return {
            "allowedStatuses": [
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "TIMED_OUT",
                "STOPPED",
            ]
        }
    else:
        return {"status": "SUCCEEDED", "result": item["marker"]}


def replay_effects(
    bodies: Sequence[Mapping],
) -> tuple[dict[str, int], list[str]]:
    owners: dict[tuple[str, int | None], str] = {}
    allowances: dict[str, int] = {}
    expected = {
        (name, item): count
        for name, count in _EXPECTED_REPLAY_EFFECTS.items()
        for item in ((1, 2, 3) if name == "item" else (None,))
    }
    for body in bodies:
        name, identity = body["operation"], body["operationId"]
        item = body.get("value") if name == "item" else None
        key = (name, item)
        if (name == "item" and type(item) is not int) or key not in expected:
            allowances.setdefault(identity, 0)
            continue
        owner = owners.setdefault(key, identity)
        allowances.setdefault(identity, expected[key] if identity == owner else 0)
    missing = [
        f"item:{item}" if name == "item" else name
        for name, item in expected
        if (name, item) not in owners
    ]
    return allowances, missing


def build_evidence(context, cloud, *, phase="observations"):
    parameters = signature(shared_build_evidence).parameters
    options: dict = {}
    if "expected_replay_effects" in parameters:
        options["expected_replay_effects"] = _EXPECTED_REPLAY_EFFECTS
    if "replay_effect_policy" in parameters:
        options["replay_effect_policy"] = replay_effects
    return shared_build_evidence(
        context,
        cloud,
        runtime="python",
        expected_outcome=expected_outcome,
        worker_identity=lambda raw: raw["process"],
        phase=phase,
        **options,
    )

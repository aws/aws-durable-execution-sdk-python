# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Assertions on service and external evidence, including negative controls."""


class ProvisioningError(RuntimeError):
    """Unsupported configuration, unavailable capacity, or failed deployment."""


class PlacementError(RuntimeError):
    """The required runtime placement was not established; never a test pass."""


class CollectionError(RuntimeError):
    """Evidence is missing/unavailable; distinct from an SDK regression assertion."""


def select(events, phase, marker=None):
    return [
        e
        for e in events
        if e["phase"] == phase and (marker is None or e["marker"] == marker)
    ]


def check_controls(events):
    errors = select(events, "CONTROL_ERROR")
    if errors:
        error = errors[0]
        raise CollectionError(f"Control {error['gate']} failed: {error['error']}")


def overlap(events, markers, count, environment=None):
    blocked = [e for e in select(events, "BLOCKED") if e["marker"] in markers]
    for start in blocked:
        env = start["environment"]
        if environment is not None and env != environment:
            continue
        active = []
        for candidate in blocked:
            if candidate["environment"] != env or candidate["time"] > start["time"]:
                continue
            alive = [
                e
                for e in events
                if e["phase"] in {"ALIVE", "EFFECT"}
                and all(
                    e.get(k) == candidate.get(k)
                    for k in ("request", "environment", "process", "gate")
                )
                and e["time"] >= candidate["time"]
            ]
            # Missing exit records (e.g. a retired process) do not prove liveness.
            if not alive or max(e["time"] for e in alive) < start["time"]:
                continue
            exits = [
                e
                for e in select(events, "IO_EXIT")
                if e["request"] == candidate["request"]
                and e.get("gate") == candidate.get("gate")
                and e["time"] >= candidate["time"]
            ]
            if not exits or min(e["time"] for e in exits) > start["time"]:
                active.append(candidate)
        if (
            len({e["request"] for e in active}) >= count
            and len({e["process"] for e in active}) >= count
            and len({e["pid"] for e in active}) >= count
            and len({e["execution"] for e in active}) >= count
        ):
            return env
    raise PlacementError(f"No {count}-process overlap in one execution environment")

    # A runtime-supported worker retirement can have no final diagnostic. Recovery
    # and full-capacity placement must therefore also be asserted by the driver.

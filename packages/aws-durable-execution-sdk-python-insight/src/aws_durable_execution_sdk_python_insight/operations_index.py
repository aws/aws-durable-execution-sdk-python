# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Name-keyed operation summary index.

Direct port of the JS ``operations-index.ts`` (``buildOperationsByName`` /
``withOperationsByName``). Point-access exporters (CloudWatch Logs) carry the
name-keyed ``operationsByName`` map instead of the lossless ``operations``
array. Operations without a name are skipped; a name that occurs more than once
aggregates metrics and DROPS ``result``/``error`` (no single representative
value). Scalar fields (``type``/``subType``/``status``) reflect the most-recently
seen occurrence (the runtime appends newer operations to the end of the array).
"""

from __future__ import annotations

from typing import Any


def build_operations_by_name(
    operations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}

    for op in operations:
        name = op.get("name")
        if not name:
            continue

        duration = op.get("durationMs")
        duration = duration if isinstance(duration, (int, float)) else None
        attempt = op.get("attempt")
        attempt = attempt if isinstance(attempt, int) else None
        failed = 1 if op.get("status") == "FAILED" else 0

        existing = groups.get(name)
        if existing is None:
            summary: dict[str, Any] = {
                "type": op.get("type"),
                "count": 1,
                "failedCount": failed,
                "status": op.get("status"),
            }
            if op.get("subType") is not None:
                summary["subType"] = op.get("subType")
            if duration is not None:
                summary["minDurationMs"] = duration
                summary["maxDurationMs"] = duration
                summary["totalDurationMs"] = duration
            if attempt is not None:
                summary["maxAttempt"] = attempt
            if op.get("result") is not None:
                summary["result"] = op.get("result")
            if op.get("error") is not None:
                summary["error"] = op.get("error")
            groups[name] = summary
            continue

        # Repeated name: aggregate and drop the per-occurrence result/error.
        existing["count"] += 1
        existing["failedCount"] += failed
        existing["type"] = op.get("type")
        existing["status"] = op.get("status")
        if op.get("subType") is not None:
            existing["subType"] = op.get("subType")
        else:
            existing.pop("subType", None)
        if duration is not None:
            existing["minDurationMs"] = (
                duration
                if existing.get("minDurationMs") is None
                else min(existing["minDurationMs"], duration)
            )
            existing["maxDurationMs"] = (
                duration
                if existing.get("maxDurationMs") is None
                else max(existing["maxDurationMs"], duration)
            )
            existing["totalDurationMs"] = (
                existing.get("totalDurationMs") or 0
            ) + duration
        if attempt is not None:
            existing["maxAttempt"] = (
                attempt
                if existing.get("maxAttempt") is None
                else max(existing["maxAttempt"], attempt)
            )
        existing.pop("result", None)
        existing.pop("error", None)

    return groups


def with_operations_by_name(record: dict[str, Any]) -> dict[str, Any]:
    """Return the record with ``operations`` replaced by ``operationsByName``."""
    out = {key: value for key, value in record.items() if key != "operations"}
    out["operationsByName"] = build_operations_by_name(record.get("operations", []))
    return out

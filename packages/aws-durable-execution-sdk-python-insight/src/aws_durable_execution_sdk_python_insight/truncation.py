# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Best-effort record size limiter.

Direct port of the JS ``truncation.ts``. Drop order:
  1. operation ``result`` fields, oldest operation first (each dropped op marked
     ``truncated: true``);
  2. whole operations, oldest first (``droppedOperations`` count);
  3. last resort — execution ``input`` then ``output`` (``droppedInput`` /
     ``droppedOutput``).

Identity/timeline fields are never dropped. The input record is never mutated.
``render`` maps the record to the exact value the exporter serializes, so the
size check measures what is actually emitted. Byte size is measured with
JS-compatible compact JSON (no whitespace, non-ASCII preserved) to match
``JSON.stringify`` byte counts.
"""

from __future__ import annotations

import json
from typing import Any, Callable


def json_byte_size(value: Any) -> int | None:
    try:
        return len(
            json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        )
    except (TypeError, ValueError):
        return None


def truncate_record(
    record: dict[str, Any],
    max_bytes: int | None,
    render: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    render = render or (lambda r: r)
    if max_bytes is None or max_bytes <= 0:
        return record

    initial = json_byte_size(render(record))
    if initial is None or initial <= max_bytes:
        return record

    ops: list[dict[str, Any]] = [dict(op) for op in record.get("operations", [])]
    kept = [True] * len(ops)
    # Oldest-first by ISO startTime string (UTC 'Z' ISO timestamps sort
    # chronologically as strings); operations without a startTime sort last.
    order = sorted(
        range(len(ops)), key=lambda i: (ops[i].get("startTime") or "\uffff", i)
    )

    any_result = False
    dropped_ops = 0
    dropped_input = False
    dropped_output = False

    def candidate() -> dict[str, Any]:
        out = dict(record)
        out["operations"] = [op for i, op in enumerate(ops) if kept[i]]
        out["truncated"] = True
        if dropped_ops > 0:
            out["droppedOperations"] = dropped_ops
        if dropped_input:
            out.pop("input", None)
            out["droppedInput"] = True
        if dropped_output:
            out.pop("output", None)
            out["droppedOutput"] = True
        return out

    def fits() -> bool:
        size = json_byte_size(render(candidate()))
        return size is not None and size <= max_bytes

    # Phase 1: drop operation results oldest-first.
    for idx in order:
        if fits():
            break
        if kept[idx] and ops[idx].get("result") is not None:
            trimmed = dict(ops[idx])
            trimmed.pop("result", None)
            trimmed["truncated"] = True
            ops[idx] = trimmed
            any_result = True

    # Phase 2: drop whole operations oldest-first.
    for idx in order:
        if fits():
            break
        if kept[idx]:
            kept[idx] = False
            dropped_ops += 1

    # Phase 3 (last resort): drop input then output.
    if not fits() and record.get("input") is not None:
        dropped_input = True
    if not fits() and record.get("output") is not None:
        dropped_output = True

    if not any_result and dropped_ops == 0 and not dropped_input and not dropped_output:
        return record

    return candidate()

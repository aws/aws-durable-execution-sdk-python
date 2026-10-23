# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Best-effort record size limiter.

Direct port of the JS ``truncation.ts``. Drop order:
  0. every error ``cause`` (``droppedCauses: true``). Records written by this
     SDK never carry a cause, so this step only changes a record whose errors
     were given a ``cause`` by other code;
  1. operation ``result`` fields, oldest operation first (each dropped op marked
     ``truncated: true``);
  2. whole operations, oldest first (``droppedOperations`` count);
  3. last resort — execution ``input`` then ``output`` (``droppedInput`` /
     ``droppedOutput``).

Identity/timeline fields are never dropped. The input record is never mutated.
``render`` maps the record to the exact value the exporter serializes, so the
size check measures what is actually emitted. By default the byte size is that
of JS-compatible compact JSON (no whitespace, non-ASCII preserved), matching
``JSON.stringify`` byte counts. An exporter that writes the rendered value
differently passes its own ``measure`` (``LambdaLogExporter`` does, because it
escapes some characters in the printed line). A value that cannot be measured
(for example one holding a lone surrogate, which has no UTF-8 encoding) is
returned unchanged, never raised.
"""

from __future__ import annotations

import json
from typing import Any, Callable


def json_byte_size(value: Any) -> int | None:
    """Return the UTF-8 size of ``value`` as compact JSON, or ``None``.

    ``None`` when the value is not JSON-serializable or has no UTF-8 encoding
    (a lone surrogate raises ``UnicodeEncodeError``, a ``ValueError``).
    """
    try:
        text = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        return len(text.encode("utf-8"))
    except (TypeError, ValueError):
        return None


def _has_cause(holder: dict[str, Any]) -> bool:
    error = holder.get("error")
    return isinstance(error, dict) and "cause" in error


def _drop_cause(holder: dict[str, Any]) -> None:
    # ``holder`` is already a copy; the error is copied too, never mutated.
    if _has_cause(holder):
        holder["error"] = {
            key: value for key, value in holder["error"].items() if key != "cause"
        }


def truncate_record(
    record: dict[str, Any],
    max_bytes: int | None,
    render: Callable[[dict[str, Any]], Any] | None = None,
    measure: Callable[[Any], int | None] | None = None,
) -> dict[str, Any]:
    render = render or (lambda r: r)
    measure = measure or json_byte_size
    if max_bytes is None or max_bytes <= 0:
        return record

    initial = measure(render(record))
    if initial is None or initial <= max_bytes:
        return record

    ops: list[dict[str, Any]] = [dict(op) for op in record.get("operations", [])]
    kept = [True] * len(ops)
    # Oldest-first by the startTime string. Every record timestamp has the same
    # fixed-width UTC format, so text order equals time order. Operations
    # without a startTime sort last.
    order = sorted(
        range(len(ops)), key=lambda i: (ops[i].get("startTime") or "\uffff", i)
    )

    # Phase 0: drop every cause. The cause chain hangs off each error only, so
    # removing the top-level ``cause`` removes the whole chain.
    dropped_causes = _has_cause(record) or any(_has_cause(op) for op in ops)
    for op in ops:
        _drop_cause(op)

    any_result = False
    dropped_ops = 0
    dropped_input = False
    dropped_output = False

    def candidate() -> dict[str, Any]:
        out = dict(record)
        out["operations"] = [op for i, op in enumerate(ops) if kept[i]]
        out["truncated"] = True
        if dropped_causes:
            _drop_cause(out)
            out["droppedCauses"] = True
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
        size = measure(render(candidate()))
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

    if (
        not dropped_causes
        and not any_result
        and dropped_ops == 0
        and not dropped_input
        and not dropped_output
    ):
        return record

    return candidate()

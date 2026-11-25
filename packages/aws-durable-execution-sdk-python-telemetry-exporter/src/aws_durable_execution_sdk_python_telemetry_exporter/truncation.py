# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Best-effort record size limiter.

Direct port of the JS ``truncation.ts``. Drop order:
  0. every error ``cause`` (``droppedCauses: true``). Records written by this
     SDK never carry a cause, so this step only changes a record whose errors
     were given a ``cause`` by other code;
  1. operation ``result`` fields, oldest operation first (each dropped op marked
     ``truncated: true``). A result whose JSON is 7 bytes or shorter stays,
     because clearing it would make the record bigger;
  2. whole operations, oldest first (``droppedOperations`` count);
  3. last resort — execution ``input`` then ``output`` (``droppedInput`` /
     ``droppedOutput``).

Steps 1 and 2 find their drop count with a binary search, so a record is
rendered about ``2 * log2(n)`` times for ``n`` operations.

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


# Size of ``"truncated":true`` minus the size of ``"result":`` (7 bytes).
_MAX_KEPT_RESULT_SIZE = len('"truncated":true') - len('"result":')


def _shrinks_when_cleared(result: Any) -> bool:
    # A result that cannot be measured is cleared, as before.
    size = json_byte_size(result)
    return size is None or size > _MAX_KEPT_RESULT_SIZE


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
    # rank[i] is the position of operation i in ``order``. A candidate removes
    # the operations with rank below ``dropped_ops``.
    rank = [0] * len(ops)
    for position, idx in enumerate(order):
        rank[idx] = position
    # Clearing replaces ``"result":<json>`` with ``"truncated":true`` (16
    # bytes). When the result JSON is 7 bytes or shorter (for example ``1``,
    # ``true`` or ``"ok"``), clearing makes the record bigger, so it cannot help
    # the record fit. So only longer results are cleared. result_rank[i] is
    # the position of operation i among these results, oldest first. A
    # candidate clears the results with result_rank below ``cleared_results``.
    result_ops = [
        i
        for i in order
        if ops[i].get("result") is not None and _shrinks_when_cleared(ops[i]["result"])
    ]
    result_rank = [len(ops)] * len(ops)
    for position, idx in enumerate(result_ops):
        result_rank[idx] = position
    cleared = [
        {**{k: v for k, v in op.items() if k != "result"}, "truncated": True}
        if result_rank[i] < len(ops)
        else op
        for i, op in enumerate(ops)
    ]

    dropped_input = False
    dropped_output = False

    def candidate(cleared_results: int, dropped_ops: int) -> dict[str, Any]:
        out = dict(record)
        out["operations"] = [
            cleared[i] if result_rank[i] < cleared_results else op
            for i, op in enumerate(ops)
            if rank[i] >= dropped_ops
        ]
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

    def fits(cleared_results: int, dropped_ops: int) -> bool:
        size = measure(render(candidate(cleared_results, dropped_ops)))
        return size is not None and size <= max_bytes

    def first_fit(low: int, high: int, fits_at: Callable[[int], bool]) -> int | None:
        # Binary search for the smallest count in ``low..high`` that fits. The
        # search assumes that a larger count never makes the record larger.
        # In the array form this holds, because only results that shrink the
        # record are cleared. So the array form drops the fewest. The by-name
        # map can break it: when one of two operations with the same name is
        # dropped, the result and error of the other one appear. Then the
        # search can return a larger count than the smallest one that fits.
        # So extra drops can happen only in the by-name form (and the by-name
        # part of "both"). The returned count was always measured. So the
        # output never goes over the limit.
        found = None
        while low <= high:
            middle = (low + high) // 2
            if fits_at(middle):
                found, high = middle, middle - 1
            else:
                low = middle + 1
        return found

    # Phase 1: drop operation results oldest-first. The record after phase 0
    # is measured first, so a record that fits without clearing a result keeps
    # every result.
    count = len(ops)
    results = len(result_ops)
    found = 0 if fits(0, 0) else first_fit(1, results, lambda j: fits(j, 0))
    cleared_results = results if found is None else found
    dropped_ops = 0
    any_result = cleared_results > 0

    # Phase 2: drop whole operations oldest-first.
    if found is None:
        found = first_fit(1, count, lambda k: fits(results, k))
        dropped_ops = count if found is None else found

    # Phase 3 (last resort): drop input then output.
    if not fits(cleared_results, dropped_ops) and record.get("input") is not None:
        dropped_input = True
    if not fits(cleared_results, dropped_ops) and record.get("output") is not None:
        dropped_output = True

    if (
        not dropped_causes
        and not any_result
        and dropped_ops == 0
        and not dropped_input
        and not dropped_output
    ):
        return record

    return candidate(cleared_results, dropped_ops)

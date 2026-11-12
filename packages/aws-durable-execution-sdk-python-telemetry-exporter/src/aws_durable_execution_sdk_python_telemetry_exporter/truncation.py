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

Cost. The candidate rendering used to be measured once per operation, so both
the result phase and the operation phase grew with the square of the operation
count (the issue's ``LambdaLogExporter`` repro took ~58 s for 4,000 operations).
Two changes remove the quadratic work without changing the returned record:

* the result phase measures the candidate once after the causes are dropped and
  then only after an operation actually loses its ``result`` — an operation
  without a result can never shrink the record, so the repeated measurements
  only ever repeated the same answer;
* when dropping whole operations is guaranteed to only shrink the rendering, the
  operation phase binary-searches the smallest fitting drop count instead of
  measuring after every drop. That guarantee holds for the lossless
  ``operations`` array and the identity rendering always, and for the name-keyed
  summary while every operation name is unique; a repeated name can make the
  summary grow when the older occurrence is dropped, so that case keeps the
  exact per-drop check, and so does a custom ``render``/``measure``.
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


# Rendering tag the built-in exporters stamp on their ``render`` callable (or
# bound method), so ``truncate_record`` can tell whether dropping whole
# operations is monotonic without inspecting the rendered value. An exporter
# that renders the lossless ``operations`` array uses ``"array"``; one that
# renders the name-keyed ``operationsByName`` summary (or both) uses ``"by-name"``
# / ``"both"``. An untagged render is a custom rendering whose behavior is
# unknown, so it keeps the exact per-drop check.
_OPS_RENDERING_ATTR = "ops_rendering"


def _names_are_distinct(ops: list[dict[str, Any]]) -> bool:
    """True when no non-empty operation name occurs more than once.

    When every name is unique, dropping whole operations can only shrink a
    name-keyed summary: the dropped operation's name had a single entry, and no
    surviving name's representative can change, so the summary never grows. With
    a repeated name, dropping the older occurrence can make the newer one the
    representative; a newer failed occurrence then adds its ``error`` and the
    record can grow (the JS/Go SDK note the same hazard).
    """
    seen: set[str] = set()
    for op in ops:
        name = op.get("name")
        if not name:
            continue
        if name in seen:
            return False
        seen.add(name)
    return True


def _ops_drop_is_monotonic(
    rendered: Any, record: dict[str, Any], render: Callable[[dict[str, Any]], Any]
) -> bool:
    """Whether dropping whole operations is guaranteed to only shrink ``render``.

    The identity rendering and the lossless ``operations`` array only lose the
    dropped operation's own bytes, so they are always monotonic. A by-name
    rendering is monotonic only while every name is unique. Anything else keeps
    the exact check.
    """
    if rendered is record:
        return True  # identity rendering: no derived content can grow
    rendering = getattr(render, _OPS_RENDERING_ATTR, None)
    if rendering == "array":
        return True
    if rendering in ("by-name", "both"):
        return _names_are_distinct(record.get("operations", []))
    return False


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

    initial_rendered = render(record)
    initial = measure(initial_rendered)
    if initial is None or initial <= max_bytes:
        return record

    ops_monotonic = _ops_drop_is_monotonic(initial_rendered, record, render)

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

    # ``candidate()`` is the value the exporter serializes, so the cached size is
    # the size of what is actually emitted. It is recomputed only after the
    # candidate changes; every phase below relies on the invariant that
    # ``fits()`` was evaluated since the last change.
    cached_size: list[int | None] = [None]

    def measured() -> int | None:
        if cached_size[0] is None:
            cached_size[0] = measure(render(candidate()))
        return cached_size[0]

    def invalidate() -> None:
        cached_size[0] = None

    def fits() -> bool:
        size = measured()
        return size is not None and size <= max_bytes

    # Phase 1: drop operation results oldest-first. Measuring the candidate once
    # after the causes are removed (the only change before this phase) is enough:
    # an operation without a result cannot shrink the record, so the per-drop
    # measurement that used to run for every operation only ever repeats the
    # same answer. Measure again solely after a result is actually dropped.
    if not fits():
        for idx in order:
            if not kept[idx] or ops[idx].get("result") is None:
                continue
            trimmed = dict(ops[idx])
            trimmed.pop("result", None)
            trimmed["truncated"] = True
            ops[idx] = trimmed
            any_result = True
            invalidate()
            if fits():
                break

    # Phase 2: drop whole operations oldest-first.
    if not fits():
        remaining = [idx for idx in order if kept[idx]]
        if ops_monotonic:
            # The rendered size is monotonic in the number of dropped
            # operations, so binary-search the smallest count that fits.
            # ``remaining`` is oldest-first, matching the drop order.
            def drop_first(count: int) -> bool:
                nonlocal dropped_ops
                for position, idx in enumerate(remaining):
                    kept[idx] = position >= count
                dropped_ops = count
                invalidate()
                return fits()

            if drop_first(len(remaining)):
                low, high = 1, len(remaining)
                while low < high:
                    mid = (low + high) // 2
                    if drop_first(mid):
                        high = mid
                    else:
                        low = mid + 1
                drop_first(low)
        else:
            for idx in order:
                if fits():
                    break
                if kept[idx]:
                    kept[idx] = False
                    dropped_ops += 1
                    invalidate()

    # Phase 3 (last resort): drop input then output.
    if not fits() and record.get("input") is not None:
        dropped_input = True
        invalidate()
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

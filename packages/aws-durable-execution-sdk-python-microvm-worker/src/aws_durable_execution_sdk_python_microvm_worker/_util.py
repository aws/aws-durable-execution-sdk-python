# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Small helpers that several modules of the package share."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


def safe_get(read: Callable[[], T]) -> T | None:
    """Return what ``read`` returns, or ``None`` when it raises."""
    try:
        return read()
    except Exception:  # noqa: BLE001
        return None


def safe_text(read: Callable[[], object], fallback: str) -> str:
    """Return the text that ``read`` gives, or ``fallback``.

    ``fallback`` is used when ``read`` raises or returns something other than
    a string. A ``__str__`` that raises is the usual cause.
    """
    value = safe_get(read)
    return value if isinstance(value, str) else fallback


def is_finite_number(value: object) -> bool:
    """Whether a value is a JSON number that is finite.

    1. A ``bool`` is an ``int`` in Python, and JSON ``true`` is not a number.
       So a ``bool`` is not a number here.
    2. An ``int`` is always finite. ``math.isfinite`` would raise
       ``OverflowError`` for an ``int`` too large for a float. So only a
       ``float`` goes through it.
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)

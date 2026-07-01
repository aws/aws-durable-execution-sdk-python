"""Helpers for scaling local durable timer delays in tests."""

from __future__ import annotations

import logging
import os


logger = logging.getLogger(__name__)

_TIME_SCALE_ENV = "DURABLE_EXECUTION_TIME_SCALE"


def get_time_scale() -> float:
    """Return the configured local durable timer scale."""
    raw_scale = os.getenv(_TIME_SCALE_ENV, "1.0")
    try:
        scale = float(raw_scale)
    except ValueError:
        logger.warning("Ignoring invalid %s value: %s", _TIME_SCALE_ENV, raw_scale)
        return 1.0

    if scale < 0:
        logger.warning("Ignoring negative %s value: %s", _TIME_SCALE_ENV, raw_scale)
        return 1.0

    return scale


def scale_delay(delay: float | int, *, minimum: float = 0) -> float:
    """Scale a durable timer delay for local testing."""
    scaled_delay = float(delay) * get_time_scale()
    if minimum <= 0:
        return scaled_delay

    return max(scaled_delay, min(float(delay), minimum))

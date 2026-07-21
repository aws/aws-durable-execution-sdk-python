"""Target function - plain Lambda handler (no @durable_execution)."""

from typing import Any


def handler(event: Any, _context: Any) -> str:
    return "non-durable result"

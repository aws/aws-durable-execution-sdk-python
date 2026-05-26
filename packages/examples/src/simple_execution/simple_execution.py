"""Demonstrates handler execution without any durable operations."""

import json
import time
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, _context: DurableContext) -> dict[str, Any]:
    """Handler that executes without any durable operations."""
    return {
        "received": json.dumps(event),
        "timestamp": int(time.time() * 1000),  # milliseconds since epoch
        "message": "Handler completed successfully",
    }

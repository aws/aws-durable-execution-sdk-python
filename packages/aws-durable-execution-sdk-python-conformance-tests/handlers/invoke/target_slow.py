"""Target function that waits longer than timeout."""

import time
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, _context: DurableContext) -> str:
    time.sleep(60)
    return "should not reach here"

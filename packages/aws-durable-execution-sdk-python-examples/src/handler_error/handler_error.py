"""Demonstrates how handler-level errors are captured and structured in results."""

from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(_event: Any, _context: DurableContext) -> None:
    """Handler demonstrating handler-level error capture."""
    # Simulate a handler-level error that might occur in real applications
    raise Exception("Intentional handler failure")

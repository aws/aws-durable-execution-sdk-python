"""Test helpers for generating expected step IDs."""

from unittest.mock import Mock

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import ExecutionState


def operation_id_sequence(parent_id: str | None = None):
    """Generator that yields step IDs in sequence using DurableContext."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test-arn"

    context = DurableContext(state=mock_state, parent_id=parent_id)

    while True:
        yield context._create_step_id()  # noqa: SLF001

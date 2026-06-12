"""Unit tests for InMemoryServiceClient."""

import datetime
from unittest.mock import Mock

from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    OperationAction,
    OperationType,
    OperationUpdate,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient


def test_checkpoint():
    """Test checkpoint method delegates to processor."""
    processor = Mock()
    expected_output = CheckpointOutput(
        checkpoint_token="new-token",  # noqa: S106
        new_execution_state=Mock(),
    )
    processor.process_checkpoint.return_value = expected_output

    client = InMemoryServiceClient(processor)

    updates = [
        OperationUpdate(
            operation_id="test-id",
            operation_type=OperationType.STEP,
            action=OperationAction.START,
        )
    ]

    result = client.checkpoint(
        "arn:aws:lambda:us-east-1:123456789012:function:test",
        "token",
        updates,
        "client-token",
    )

    assert result == expected_output
    processor.process_checkpoint.assert_called_once_with(
        "token", updates, "client-token"
    )


def test_get_execution_state():
    """Test get_execution_state method delegates to processor."""
    processor = Mock()
    expected_output = StateOutput(operations=[], next_marker="marker")
    processor.get_execution_state.return_value = expected_output

    client = InMemoryServiceClient(processor)

    result = client.get_execution_state(
        "arn:aws:lambda:us-east-1:123456789012:function:test", "token", "marker", 500
    )

    assert result == expected_output
    processor.get_execution_state.assert_called_once_with("token", "marker", 500)


def test_get_execution_state_default_max_items():
    """Test get_execution_state with default max_items."""
    processor = Mock()
    expected_output = StateOutput(operations=[], next_marker="marker")
    processor.get_execution_state.return_value = expected_output

    client = InMemoryServiceClient(processor)

    result = client.get_execution_state(
        "arn:aws:lambda:us-east-1:123456789012:function:test", "token", "marker"
    )

    assert result == expected_output
    processor.get_execution_state.assert_called_once_with("token", "marker", 1000)


def test_stop():
    """Test stop method returns current datetime."""
    processor = Mock()
    client = InMemoryServiceClient(processor)

    before = datetime.datetime.now(tz=datetime.UTC)
    result = client.stop(
        "arn:aws:states:us-east-1:123456789012:execution:test", b"payload"
    )
    after = datetime.datetime.now(tz=datetime.UTC)

    assert isinstance(result, datetime.datetime)
    assert before <= result <= after


def test_stop_with_none_payload():
    """Test stop method with None payload."""
    processor = Mock()
    client = InMemoryServiceClient(processor)

    result = client.stop("arn:aws:states:us-east-1:123456789012:execution:test", None)

    assert isinstance(result, datetime.datetime)

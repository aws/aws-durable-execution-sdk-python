"""Integration tests for filesystem serdes end-to-end execution.

Tests the full execution flow with filesystem serdes:
- First invocation: step executes, writes to filesystem, checkpoints envelope
- Replay: step deserializes from checkpointed envelope, reads from filesystem
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.filesystem_serdes import (
    FileSystemPathEncoding,
    FileSystemSerdesConfig,
    FileSystemSerdesMode,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    build_preview,
    create_filesystem_serdes,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    StepDetails,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import StepContext


def _create_lambda_context():
    """Create a mock Lambda context."""
    ctx = Mock()
    ctx.aws_request_id = "test-request-id"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    ctx.invoked_function_arn = "test-arn"
    ctx.tenant_id = None
    return ctx


def _create_initial_event(input_payload: str = "{}"):
    """Create a fresh execution event (first invocation)."""
    return {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-func:1/durable-execution/exec-001/inv-001",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": input_payload},
                }
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _create_replay_event(operations: list[dict], input_payload: str = "{}"):
    """Create a replay event with pre-existing operations."""
    base_ops = [
        {
            "Id": "execution-1",
            "Type": "EXECUTION",
            "Status": "STARTED",
            "ExecutionDetails": {"InputPayload": input_payload},
        }
    ]
    return {
        "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-func:1/durable-execution/exec-001/inv-001",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": base_ops + operations,
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def test_filesystem_serdes_first_invocation(tmp_path):
    """Test first invocation: step executes and writes result to filesystem.

    Verifies that:
    - The handler completes successfully
    - The checkpoint payload contains a file pointer envelope
    - The file was actually written to the filesystem
    """
    mount_path = str(tmp_path)

    @durable_execution
    def my_handler(event, context: DurableContext) -> dict[str, Any]:
        fs_serdes = create_filesystem_serdes(mount_path)
        result = context.step(
            lambda _: {"order_id": "ORD-123", "total": 99.99},
            name="process_order",
            config=StepConfig(serdes=fs_serdes),
        )
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)
            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        event = _create_initial_event()
        result = my_handler(event, _create_lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    # Verify the result is correct
    result_data = json.loads(result["Result"])
    assert result_data == {"order_id": "ORD-123", "total": 99.99}

    # Verify checkpoint payload contains file pointer envelope
    all_operations = [op for batch in checkpoint_calls for op in batch]
    succeed_ops = [op for op in all_operations if op.action == OperationAction.SUCCEED]
    assert len(succeed_ops) >= 1

    # The step's succeed payload should be a file envelope
    step_payload = succeed_ops[0].payload
    envelope = json.loads(step_payload)
    assert "file" in envelope
    assert os.path.exists(envelope["file"])

    # Verify file contains the actual data
    with open(envelope["file"]) as f:
        stored_data = json.load(f)
    assert stored_data == {"order_id": "ORD-123", "total": 99.99}


def test_filesystem_serdes_replay_from_checkpoint(tmp_path):
    """Test replay: step result is deserialized from filesystem via envelope.

    Simulates a replay scenario where:
    1. A file already exists on the filesystem (from a previous invocation)
    2. The checkpoint contains a file pointer envelope
    3. On replay, the serdes reads the file to get the result
    """
    mount_path = str(tmp_path)

    # Generate the deterministic step ID that the SDK will produce
    from tests.test_helpers import operation_id_sequence

    step_id = next(operation_id_sequence())

    # Pre-create the file that would have been written in the first invocation
    arn = "arn:aws:lambda:us-east-1:123456789012:function:test-func:1/durable-execution/exec-001/inv-001"
    dir_path = os.path.join(mount_path, "test-func", "exec-001", "inv-001")
    os.makedirs(dir_path, exist_ok=True)

    from urllib.parse import quote

    file_name = f"{quote(step_id, safe='')}.json"
    file_path = os.path.join(dir_path, file_name)
    with open(file_path, "w") as f:
        json.dump({"order_id": "ORD-123", "total": 99.99}, f)

    # The envelope that would have been checkpointed
    envelope = json.dumps({"file": file_path})

    @durable_execution
    def my_handler(event, context: DurableContext) -> dict[str, Any]:
        fs_serdes = create_filesystem_serdes(mount_path)
        result = context.step(
            lambda _: (_ for _ in ()).throw(
                RuntimeError("Should not execute on replay")
            ),
            name="process_order",
            config=StepConfig(serdes=fs_serdes),
        )
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)
            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create replay event with the step already SUCCEEDED using the correct ID
        event = _create_replay_event(
            [
                {
                    "Id": step_id,
                    "Type": "STEP",
                    "Status": "SUCCEEDED",
                    "ParentId": "execution-1",
                    "StepDetails": {"Result": envelope},
                }
            ]
        )

        result = my_handler(event, _create_lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    # Verify the deserialized result matches the file content
    result_data = json.loads(result["Result"])
    assert result_data == {"order_id": "ORD-123", "total": 99.99}


def test_filesystem_serdes_overflow_mode_small_inline(tmp_path):
    """Test OVERFLOW mode: small values stay inline in the checkpoint."""
    mount_path = str(tmp_path)

    @durable_execution
    def my_handler(event, context: DurableContext) -> dict[str, Any]:
        fs_serdes = create_filesystem_serdes(
            mount_path,
            FileSystemSerdesConfig(storage_mode=FileSystemSerdesMode.OVERFLOW),
        )
        result = context.step(
            lambda _: {"status": "ok", "count": 5},
            name="small_step",
            config=StepConfig(serdes=fs_serdes),
        )
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)
            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        event = _create_initial_event()
        result = my_handler(event, _create_lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    result_data = json.loads(result["Result"])
    assert result_data == {"status": "ok", "count": 5}

    # Verify checkpoint payload is inline (data envelope, no file)
    all_operations = [op for batch in checkpoint_calls for op in batch]
    succeed_ops = [op for op in all_operations if op.action == OperationAction.SUCCEED]
    assert len(succeed_ops) >= 1

    step_payload = succeed_ops[0].payload
    envelope = json.loads(step_payload)
    assert "data" in envelope
    assert "file" not in envelope

    # No files should have been written
    json_files = list(tmp_path.rglob("*.json"))
    assert len(json_files) == 0


def test_filesystem_serdes_with_preview(tmp_path):
    """Test that preview is stored in the checkpoint envelope alongside file pointer."""
    mount_path = str(tmp_path)

    @durable_execution
    def my_handler(event, context: DurableContext) -> dict[str, Any]:
        fs_serdes = create_filesystem_serdes(
            mount_path,
            FileSystemSerdesConfig(
                generate_preview=lambda value: build_preview(
                    value,
                    PreviewConfig(
                        mode=PreviewMode.EXCLUDE_ALL,
                        include=[PreviewField(name="order_id")],
                        mask=[PreviewField(name="email")],
                    ),
                ),
            ),
        )
        result = context.step(
            lambda _: {
                "order_id": "ORD-456",
                "email": "secret@example.com",
                "items": [{"sku": "A", "qty": 2}],
            },
            name="order_step",
            config=StepConfig(serdes=fs_serdes),
        )
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)
            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        event = _create_initial_event()
        result = my_handler(event, _create_lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    # Verify the checkpoint envelope includes preview
    all_operations = [op for batch in checkpoint_calls for op in batch]
    succeed_ops = [op for op in all_operations if op.action == OperationAction.SUCCEED]

    step_payload = succeed_ops[0].payload
    envelope = json.loads(step_payload)

    assert "file" in envelope
    assert "preview" in envelope
    assert envelope["preview"]["order_id"] == "ORD-456"
    assert envelope["preview"]["email"] == "***"
    assert "items" not in envelope["preview"]


def test_filesystem_serdes_multiple_steps(tmp_path):
    """Test multiple steps using filesystem serdes in a single execution."""
    mount_path = str(tmp_path)

    @durable_execution
    def my_handler(event, context: DurableContext) -> dict[str, Any]:
        fs_serdes = create_filesystem_serdes(mount_path)

        step1 = context.step(
            lambda _: {"step": 1, "data": "first"},
            name="step_one",
            config=StepConfig(serdes=fs_serdes),
        )
        step2 = context.step(
            lambda _: {"step": 2, "data": "second", "prev": step1["data"]},
            name="step_two",
            config=StepConfig(serdes=fs_serdes),
        )
        return {"results": [step1, step2]}

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        event = _create_initial_event()
        result = my_handler(event, _create_lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    result_data = json.loads(result["Result"])
    assert result_data["results"][0] == {"step": 1, "data": "first"}
    assert result_data["results"][1] == {
        "step": 2,
        "data": "second",
        "prev": "first",
    }

    # Two separate files should exist
    json_files = list(tmp_path.rglob("*.json"))
    assert len(json_files) == 2


def test_filesystem_serdes_hash_encoding(tmp_path):
    """Test that HASH path encoding produces fixed-length file names."""
    mount_path = str(tmp_path)

    @durable_execution
    def my_handler(event, context: DurableContext) -> dict[str, Any]:
        fs_serdes = create_filesystem_serdes(
            mount_path,
            FileSystemSerdesConfig(path_encoding=FileSystemPathEncoding.HASH),
        )
        result = context.step(
            lambda _: {"encoded": True},
            name="hash_step",
            config=StepConfig(serdes=fs_serdes),
        )
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        event = _create_initial_event()
        result = my_handler(event, _create_lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    # Verify HASH encoding: directory and file should be hex digests
    json_files = list(tmp_path.rglob("*.json"))
    assert len(json_files) == 1

    file_name = json_files[0].name
    # Hash (64 chars) + ".json" (5 chars) = 69 chars
    assert len(file_name) == 69

    # Directory should be a hash too
    dir_name = json_files[0].parent.name
    assert len(dir_name) == 64

"""Integration tests for the first-run/replay result equality guarantee.

With a non-identity custom ``SerDes`` (serialize/deserialize is not a round-trip
identity), an operation must return the *round-tripped* value on the first run
so that it matches the value returned on replay, where the result is
deserialized from the checkpoint. Returning the raw in-memory result on the
first run would make the first-run and replay results diverge.

This is exercised below for ``step``, ``wait_for_condition``, and
``run_in_child_context`` in all three modes: normal, virtual, and large-payload
(ReplayChildren).

The serdes used here strips a marker key on ``serialize`` and re-adds it on
``deserialize`` so that ``deserialize(serialize(x)) != x``. That makes the bug
observable end-to-end through a full ``durable_execution`` invocation.
"""

import json
from typing import Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.config import ChildConfig, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationAction,
)
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)


class MarkerSerDes(SerDes[Any]):
    """Non-identity serdes: deserialize() re-adds a marker serialize() strips.

    ``deserialize(serialize(value)) == {**value, "round_tripped": True}`` which
    differs from ``value`` whenever ``value`` lacks the marker. This makes any
    first-run vs replay divergence observable.
    """

    def serialize(self, value: Any, _: SerDesContext) -> str:
        payload = {k: v for k, v in dict(value).items() if k != "round_tripped"}
        return json.dumps(payload)

    def deserialize(self, data: str, _: SerDesContext) -> dict[str, Any]:
        parsed = json.loads(data)
        return {**parsed, "round_tripped": True}


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


def _patched_client():
    """Patch LambdaClient and record checkpoint batches."""
    checkpoint_calls: list = []

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

    return checkpoint_calls, mock_checkpoint


def test_step_first_run_matches_replay_with_non_identity_serdes():
    """First-run result equals replay result for a non-identity serdes."""

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        return context.step(
            lambda _: {"order_id": "ORD-123"},
            name="process_order",
            config=StepConfig(serdes=MarkerSerDes()),
        )

    # --- First invocation ---
    checkpoint_calls, mock_checkpoint = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint

        first_result = handler(_create_initial_event(), _create_lambda_context())

    assert first_result["Status"] == InvocationStatus.SUCCEEDED.value
    first_data = json.loads(first_result["Result"])

    # The step's SUCCEED payload is the *serialized* value (marker stripped).
    all_operations = [op for batch in checkpoint_calls for op in batch]
    step_succeed_ops = [
        op
        for op in all_operations
        if op.action == OperationAction.SUCCEED and op.operation_id != "execution-1"
    ]
    assert len(step_succeed_ops) == 1
    step_payload = step_succeed_ops[0].payload
    assert json.loads(step_payload) == {"order_id": "ORD-123"}  # no marker

    # --- Replay: rebuild the execution with the step already SUCCEEDED ---
    from tests.test_helpers import operation_id_sequence

    step_id = next(operation_id_sequence())

    checkpoint_calls_replay, mock_checkpoint_replay = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint_replay

        replay_event = _create_replay_event(
            [
                {
                    "Id": step_id,
                    "Type": "STEP",
                    "Status": "SUCCEEDED",
                    "ParentId": "execution-1",
                    "StepDetails": {"Result": step_payload},
                }
            ]
        )
        replay_result = handler(replay_event, _create_lambda_context())

    assert replay_result["Status"] == InvocationStatus.SUCCEEDED.value
    replay_data = json.loads(replay_result["Result"])

    # First run equals replay, and both carry the round-trip marker.
    assert first_data == replay_data
    assert first_data == {"order_id": "ORD-123", "round_tripped": True}


def test_child_first_run_matches_replay_with_non_identity_serdes():
    """First-run result equals replay result for run_in_child_context."""

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        return context.run_in_child_context(
            lambda _child_ctx: {"order_id": "ORD-123"},
            name="process_order",
            config=ChildConfig(serdes=MarkerSerDes()),
        )

    # --- First invocation ---
    checkpoint_calls, mock_checkpoint = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint

        first_result = handler(_create_initial_event(), _create_lambda_context())

    assert first_result["Status"] == InvocationStatus.SUCCEEDED.value
    first_data = json.loads(first_result["Result"])

    # The child's SUCCEED payload is the *serialized* value (marker stripped).
    all_operations = [op for batch in checkpoint_calls for op in batch]
    child_succeed_ops = [
        op
        for op in all_operations
        if op.action == OperationAction.SUCCEED and op.operation_id != "execution-1"
    ]
    assert len(child_succeed_ops) == 1
    child_payload = child_succeed_ops[0].payload
    assert json.loads(child_payload) == {"order_id": "ORD-123"}  # no marker

    # --- Replay: rebuild the execution with the child already SUCCEEDED ---
    from tests.test_helpers import operation_id_sequence

    child_id = next(operation_id_sequence())

    checkpoint_calls_replay, mock_checkpoint_replay = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint_replay

        replay_event = _create_replay_event(
            [
                {
                    "Id": child_id,
                    "Type": "CONTEXT",
                    "Status": "SUCCEEDED",
                    "ParentId": "execution-1",
                    "ContextDetails": {"Result": child_payload},
                }
            ]
        )
        replay_result = handler(replay_event, _create_lambda_context())

    assert replay_result["Status"] == InvocationStatus.SUCCEEDED.value
    replay_data = json.loads(replay_result["Result"])

    # First run equals replay, and both carry the round-trip marker.
    assert first_data == replay_data
    assert first_data == {"order_id": "ORD-123", "round_tripped": True}


def test_wait_for_condition_first_run_matches_replay_with_non_identity_serdes():
    """First-run result equals replay result for wait_for_condition."""

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        return context.wait_for_condition(
            check=lambda _state, _ctx: {"order_id": "ORD-123"},
            config=WaitForConditionConfig(
                initial_state={},
                wait_strategy=lambda _s, _a: WaitForConditionDecision.stop_polling(),
                serdes=MarkerSerDes(),
            ),
            name="process_order",
        )

    # --- First invocation ---
    checkpoint_calls, mock_checkpoint = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint

        first_result = handler(_create_initial_event(), _create_lambda_context())

    assert first_result["Status"] == InvocationStatus.SUCCEEDED.value
    first_data = json.loads(first_result["Result"])

    # The SUCCEED payload is the *serialized* state (marker stripped). A
    # wait_for_condition operation is checkpointed as a STEP-typed operation.
    all_operations = [op for batch in checkpoint_calls for op in batch]
    succeed_ops = [
        op
        for op in all_operations
        if op.action == OperationAction.SUCCEED and op.operation_id != "execution-1"
    ]
    assert len(succeed_ops) == 1
    wfc_payload = succeed_ops[0].payload
    assert json.loads(wfc_payload) == {"order_id": "ORD-123"}  # no marker

    # --- Replay: rebuild the execution with the operation already SUCCEEDED ---
    from tests.test_helpers import operation_id_sequence

    wfc_id = next(operation_id_sequence())

    checkpoint_calls_replay, mock_checkpoint_replay = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint_replay

        replay_event = _create_replay_event(
            [
                {
                    "Id": wfc_id,
                    "Type": "STEP",
                    "Status": "SUCCEEDED",
                    "ParentId": "execution-1",
                    "StepDetails": {"Result": wfc_payload},
                }
            ]
        )
        replay_result = handler(replay_event, _create_lambda_context())

    assert replay_result["Status"] == InvocationStatus.SUCCEEDED.value
    replay_data = json.loads(replay_result["Result"])

    # First run equals replay, and both carry the round-trip marker.
    assert first_data == replay_data
    assert first_data == {"order_id": "ORD-123", "round_tripped": True}


def test_virtual_child_first_run_returns_round_tripped_result():
    """A virtual child returns the round-tripped value and writes no checkpoint."""

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        return context.run_in_child_context(
            lambda _child_ctx: {"order_id": "ORD-123"},
            name="process_order",
            config=ChildConfig(serdes=MarkerSerDes(), is_virtual=True),
        )

    checkpoint_calls, mock_checkpoint = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint

        first_result = handler(_create_initial_event(), _create_lambda_context())

    assert first_result["Status"] == InvocationStatus.SUCCEEDED.value
    first_data = json.loads(first_result["Result"])

    # Virtual contexts re-execute on replay and never checkpoint, so the
    # round-tripped first-run result is what every replay reproduces.
    assert first_data == {"order_id": "ORD-123", "round_tripped": True}
    all_operations = [op for batch in checkpoint_calls for op in batch]
    assert not [op for op in all_operations if op.operation_id != "execution-1"]


def test_large_child_first_run_matches_replay_with_non_identity_serdes():
    """First-run result equals replay result for a large-payload child."""
    blob = "x" * (256 * 1024 + 100)

    @durable_execution
    def handler(event, context: DurableContext) -> dict[str, Any]:
        return context.run_in_child_context(
            lambda _child_ctx: {"order_id": "ORD-123", "blob": blob},
            name="process_order",
            config=ChildConfig(serdes=MarkerSerDes()),
        )

    # --- First invocation: large result -> ReplayChildren + summary checkpoint ---
    checkpoint_calls, mock_checkpoint = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint

        first_result = handler(_create_initial_event(), _create_lambda_context())

    assert first_result["Status"] == InvocationStatus.SUCCEEDED.value
    first_data = json.loads(first_result["Result"])

    all_operations = [op for batch in checkpoint_calls for op in batch]
    child_succeed_ops = [
        op
        for op in all_operations
        if op.action == OperationAction.SUCCEED and op.operation_id != "execution-1"
    ]
    assert len(child_succeed_ops) == 1
    # The full result is not checkpointed; only a summary with ReplayChildren.
    assert child_succeed_ops[0].context_options.replay_children is True

    # --- Replay: child re-executes (ReplayChildren) and round-trips ---
    from tests.test_helpers import operation_id_sequence

    child_id = next(operation_id_sequence())

    checkpoint_calls_replay, mock_checkpoint_replay = _patched_client()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client
        mock_client.checkpoint = mock_checkpoint_replay

        replay_event = _create_replay_event(
            [
                {
                    "Id": child_id,
                    "Type": "CONTEXT",
                    "Status": "SUCCEEDED",
                    "ParentId": "execution-1",
                    "ContextDetails": {"Result": "", "ReplayChildren": True},
                }
            ]
        )
        replay_result = handler(replay_event, _create_lambda_context())

    assert replay_result["Status"] == InvocationStatus.SUCCEEDED.value
    replay_data = json.loads(replay_result["Result"])

    assert first_data == replay_data
    assert first_data == {"order_id": "ORD-123", "blob": blob, "round_tripped": True}

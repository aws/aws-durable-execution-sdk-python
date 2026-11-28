"""ReplayChildren lifecycle and wire representation of the local service."""

import json
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    InitialExecutionState,
)
from aws_durable_execution_sdk_python.lambda_service import (
    ContextOptions,
    Operation,
    OperationAction,
    OperationType,
    OperationUpdate,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.context import (
    ContextProcessor,
)
from aws_durable_execution_sdk_python_testing.invoker import (
    _invocation_input_payload,
)
from aws_durable_execution_sdk_python_testing.model import (
    CheckpointUpdatedExecutionState,
    GetDurableExecutionStateResponse,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _process(action, options):
    update = OperationUpdate(
        operation_id="ctx",
        operation_type=OperationType.CONTEXT,
        action=action,
        payload="p",
        context_options=options,
    )
    return ContextProcessor().process(update, None, ExecutionNotifier(), "arn", NOW)


def _context_details(wire):
    return wire["Operations"][0]["ContextDetails"]


@pytest.mark.parametrize("action", [OperationAction.SUCCEED, OperationAction.FAIL])
@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (None, False),
        (ContextOptions(), False),
        (ContextOptions(replay_children=False), False),
        (ContextOptions(replay_children=True), True),
    ],
)
def test_replay_children_is_recorded_only_by_closing_update(action, options, expected):
    operation = _process(action, options)

    assert operation.context_details.replay_children is expected


def test_start_does_not_retain_replay_children():
    operation = _process(OperationAction.START, ContextOptions(replay_children=True))

    assert operation.context_details.replay_children is False


@pytest.mark.parametrize("retained", [True, False])
def test_state_and_checkpoint_responses_send_explicit_boolean_when_completed(retained):
    operation = _process(
        OperationAction.SUCCEED, ContextOptions(replay_children=retained)
    )

    for response in (
        GetDurableExecutionStateResponse(operations=[operation]),
        CheckpointUpdatedExecutionState(operations=[operation]),
    ):
        assert _context_details(response.to_dict())["ReplayChildren"] is retained


def test_started_context_omits_replay_children_on_every_path():
    started = _process(OperationAction.START, ContextOptions(replay_children=True))

    state = GetDurableExecutionStateResponse(operations=[started]).to_dict()
    checkpoint = CheckpointUpdatedExecutionState(operations=[started]).to_dict()
    invocation = _invocation_input_payload(
        DurableExecutionInvocationInput(
            durable_execution_arn="arn",
            checkpoint_token="t",
            initial_execution_state=InitialExecutionState(
                operations=[started], next_marker=""
            ),
        )
    )

    assert "ReplayChildren" not in _context_details(state)
    assert "ReplayChildren" not in _context_details(checkpoint)
    assert (
        "ReplayChildren"
        not in invocation["InitialExecutionState"]["Operations"][0]["ContextDetails"]
    )


def test_invocation_payload_has_false_for_completed_context_and_keeps_others():
    closed = _process(OperationAction.SUCCEED, None)
    step = Operation.from_dict(
        {"Id": "s", "Type": "STEP", "Status": "STARTED", "StepDetails": {"Attempt": 1}}
    )
    original = closed.to_dict()

    payload = _invocation_input_payload(
        DurableExecutionInvocationInput(
            durable_execution_arn="arn",
            checkpoint_token="t",
            initial_execution_state=InitialExecutionState(
                operations=[closed, step], next_marker=""
            ),
        )
    )
    operations = json.loads(json.dumps(payload))["InitialExecutionState"]["Operations"]

    assert operations[0]["ContextDetails"]["ReplayChildren"] is False
    assert operations[1] == json.loads(json.dumps(step.to_json_dict()))
    # The supplied operation is not mutated by the projection.
    assert closed.to_dict() == original
    assert "ReplayChildren" not in closed.to_dict()["ContextDetails"]

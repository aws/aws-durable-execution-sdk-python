"""The service's retained-operation tree applies only to invocation/state reads."""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ContextDetails,
    ContextOptions,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python_testing.execution import (
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.invoker import (
    InProcessInvoker,
    LambdaInvoker,
)
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
    GetDurableExecutionStateResponse,
    CheckpointUpdatedExecutionState,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.context import (
    ContextProcessor,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)


def step(identifier, parent=None, status=OperationStatus.SUCCEEDED):
    return Operation(
        operation_id=identifier,
        parent_id=parent,
        operation_type=OperationType.STEP,
        status=status,
    )


def ctx(identifier, parent=None, status=OperationStatus.SUCCEEDED, replay=None):
    return Operation(
        operation_id=identifier,
        parent_id=parent,
        operation_type=OperationType.CONTEXT,
        status=status,
        context_details=ContextDetails.from_dict(
            {} if replay is None else {"ReplayChildren": replay}
        ),
    )


TREE = [
    step("1-step"),
    ctx("2-context", status=OperationStatus.STARTED),
    step("2.1-step", "2-context", OperationStatus.PENDING),
    step("3-step", "missing"),
    ctx("4-context", status=OperationStatus.FAILED),
    ctx("4.1-context", "4-context", OperationStatus.FAILED, True),
    step("4.1.1-step", "4.1-context", OperationStatus.FAILED),
    step("4.1.2-step", "4.1-context", OperationStatus.FAILED),
    ctx("5-context", replay=True),
    ctx("5.1-context", "5-context", replay=False),
    step("5.1.1-step", "5.1-context"),
    step("5.1-step", "5-context"),
    ctx("map"),
    ctx("running-branch", "map", OperationStatus.STARTED),
    step("pending-branch-step", "running-branch", OperationStatus.PENDING),
    ctx("retained-inside-pruned", "running-branch", replay=True),
    step("nested-step", "retained-inside-pruned"),
]
KEPT = [
    "1-step",
    "2-context",
    "2.1-step",
    "3-step",
    "4-context",
    "5-context",
    "5.1-context",
    "5.1-step",
    "map",
]


def execution_with(operations):
    start = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="parity",
        function_qualifier="$LATEST",
        execution_name="parity",
        execution_timeout_seconds=60,
        execution_retention_period_days=1,
        invocation_id="invocation",
    )
    execution = Execution(
        "arn:aws:lambda:us-west-2:123456789012:function:parity:$LATEST/durable-execution/parity/id",
        start,
        list(operations),
    )
    for op in operations:
        execution.touch_operation(op.operation_id)
    execution.updated_operation_ids = ["4.1.1-step", "pending-branch-step"]
    return execution


@pytest.mark.parametrize("filesystem", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_service_tree_paging_inputs_and_get_state_keep_raw_inspection(
    filesystem, reverse, tmp_path
):
    operations = list(reversed(TREE)) if reverse else list(TREE)
    expected = list(reversed(KEPT)) if reverse else KEPT
    execution = execution_with(operations)
    store = (
        FileSystemExecutionStore.create(tmp_path / "state")
        if filesystem
        else InMemoryExecutionStore()
    )
    store.save(execution)
    restored = store.load(execution.durable_execution_arn)
    paginator = OperationPaginatorState.pin(restored)
    paged = []
    marker = None
    while True:
        page, marker = paginator.page(marker, 100000, max_items=2)
        paged.extend(op.operation_id for op in page)
        if marker is None:
            break
    assert paged == expected
    for invoker in [InProcessInvoker(Mock(), Mock()), LambdaInvoker(Mock())]:
        invocation = invoker.create_invocation_input(restored)
        assert [
            o.operation_id for o in invocation.initial_execution_state.operations
        ] == expected
        assert invocation.updated_operation_ids == ["4.1.1-step", "pending-branch-step"]
    state = CheckpointProcessor(store, Mock()).get_execution_state(
        restored.get_new_checkpoint_token(), ""
    )
    assert [o.operation_id for o in state.operations] == expected
    # Checkpoint deltas, raw store, inspection, and hidden updated IDs remain full.
    assert paginator.unseen_operations() == operations
    assert (
        restored.operations == operations
        and restored.get_assertable_operations() == operations
    )
    assert restored.updated_operation_ids == ["4.1.1-step", "pending-branch-step"]


@pytest.mark.parametrize("action", [OperationAction.SUCCEED, OperationAction.FAIL])
@pytest.mark.parametrize(
    "close_options",
    [
        None,
        ContextOptions(replay_children=True),
        ContextOptions(replay_children=False),
        ContextOptions(),
    ],
)
def test_replay_children_is_recorded_only_by_closing_update(action, close_options):
    processor = ContextProcessor()
    now = datetime.now(UTC)
    notifier = Mock()
    start = OperationUpdate(
        operation_id="ctx",
        operation_type=OperationType.CONTEXT,
        action=OperationAction.START,
        context_options=ContextOptions(replay_children=True),
    )
    opened = processor.process(start, None, notifier, "execution", now)
    assert (
        opened.context_details is not None
        and opened.context_details.replay_children is False
    )
    assert "ReplayChildren" not in opened.to_json_dict()["ContextDetails"]
    close = OperationUpdate(
        operation_id="ctx",
        operation_type=OperationType.CONTEXT,
        action=action,
        context_options=close_options,
        payload="saved",
    )
    completed = processor.process(close, opened, notifier, "execution", now)
    expected = close_options is not None and close_options.replay_children is True
    assert completed.context_details.replay_children is expected
    assert (
        GetDurableExecutionStateResponse([completed]).to_dict()["Operations"][0][
            "ContextDetails"
        ]["ReplayChildren"]
        is expected
    )
    assert (
        CheckpointUpdatedExecutionState([completed]).to_dict()["Operations"][0][
            "ContextDetails"
        ]["ReplayChildren"]
        is expected
    )
    assert completed.context_details.result == "saved"


@pytest.mark.parametrize(
    "operations", [[step("a", "a")], [step("a", "b"), step("b", "a")]]
)
def test_invalid_cycles_terminate_without_dropping_unrelated_operations(operations):
    execution = execution_with(operations)
    assert OperationPaginatorState.pin(execution).page(None, 100000)[0] == operations


def test_lambda_invocation_projects_close_false_and_start_absence():
    from io import BytesIO
    import json

    execution = execution_with(TREE)
    client = Mock()
    client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": BytesIO(b'{"Status":"SUCCEEDED","Result":"null"}'),
    }
    invoker = LambdaInvoker(client)
    invocation = invoker.create_invocation_input(execution)
    invoker.invoke("target", invocation)
    payload = json.loads(client.invoke.call_args.kwargs["Payload"])
    operations = {op["Id"]: op for op in payload["InitialExecutionState"]["Operations"]}
    assert list(operations) == KEPT
    assert "ReplayChildren" not in operations["2-context"]["ContextDetails"]
    assert operations["4-context"]["ContextDetails"]["ReplayChildren"] is False
    assert operations["5-context"]["ContextDetails"]["ReplayChildren"] is True
    assert operations["5.1-context"]["ContextDetails"]["ReplayChildren"] is False
    assert payload["UpdatedOperationIds"] == ["4.1.1-step", "pending-branch-step"]

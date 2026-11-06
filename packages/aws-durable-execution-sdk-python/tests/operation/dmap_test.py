"""Unit tests for map run handler."""

from __future__ import annotations

import json
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.dmap.models import (
    DistributedMapResult,
    DistributedMapSummary,
)
from aws_durable_execution_sdk_python.config import (
    Duration,
    DistributedMapItemStatus,
    DistributedMapStatus,
    DistributedMapCompletionConfig,
    DistributedMapConfig,
    DistributedMapResultConfig,
    DistributedMapCsvDelimiter,
    DistributedMapDestination,
    DistributedMapDestinationConfig,
    DistributedMapSource,
    DistributedMapProcessor,
    DistributedMapProcessorRetryConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
    DistributedMapError,
    NonDeterministicExecutionError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    DistributedMapDetails,
    DistributedMapFunctionResponseType,
    DistributedMapOptions,
    DistributedMapResultCollectionMode,
    DistributedMapResultItem as DistributedMapResultItemApi,
    DistributedMapSourceType,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.operation.dmap import (
    DistributedMapOperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import SerDes
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState


# Test helper - wraps DistributedMapOperationExecutor with a simple handler signature.
def distributed_map_handler(
    source, processor, max_concurrency, state, operation_identifier, config=None
):
    """Test helper that wraps DistributedMapOperationExecutor and runs it.

    ``processor`` may be a function-name string (wrapped as a batch-outcome
    processor) or an already-built DistributedMapProcessor.
    """
    if not config:
        config = DistributedMapConfig()
    if isinstance(processor, str):
        processor = DistributedMapProcessor.report_batch_outcome(processor)
    executor = DistributedMapOperationExecutor(
        source=source,
        processor=processor,
        max_concurrency=max_concurrency,
        state=state,
        operation_identifier=operation_identifier,
        config=config,
    )
    return executor.process()


def _identifier(
    operation_id: str, name: str | None = "test_map_run"
) -> OperationIdentifier:
    return OperationIdentifier(
        operation_id,
        OperationSubType.DISTRIBUTED_MAP,
        None,
        name,
        operation_type=OperationType.DISTRIBUTED_MAP,
    )


def test_map_run_handler_already_succeeded():
    """Test distributed_map_handler returns a summary when the operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr1",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=5,
            failure_count=0,
            unprocessed_count=0,
            total_count=5,
            distributed_map_run_arn="arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1/distributed-map-run/abc",
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a", "b"],
        processor="test_processor",
        max_concurrency=10,
        state=mock_state,
        operation_identifier=_identifier("mr1"),
    )

    assert result.status is DistributedMapStatus.SUCCEEDED
    assert result.source_exhausted is True
    assert result.error is None
    assert result.success_count == 5
    assert result.failure_count == 0
    assert result.total_count == 5
    assert result.distributed_map_run_arn.endswith("/distributed-map-run/abc")
    mock_state.create_checkpoint.assert_not_called()


def test_map_run_handler_resolves_non_success_without_raising():
    """Test a non-SUCCEEDED run resolves with a summary rather than raising.

    The run failed, so distributed_map must still return the summary carrying
    the counts and let the caller opt into raising.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr2",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.FAILED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=False,
            error=ErrorObject(
                message="failure tolerance exceeded",
                type="DistributedMapFailure",
                data=None,
                stack_trace=None,
            ),
            success_count=3,
            failure_count=2,
            unprocessed_count=1,
            distributed_map_run_arn="arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1/distributed-map-run/abc",
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a"],
        processor="test_processor",
        max_concurrency=10,
        state=mock_state,
        operation_identifier=_identifier("mr2"),
    )

    assert result.status is DistributedMapStatus.FAILED
    assert result.source_exhausted is False
    assert result.error is not None
    assert result.error.message == "failure tolerance exceeded"
    assert result.failure_count == 2
    assert result.has_failure is True


def test_map_run_handler_succeeded_no_details_raises():
    """Test a succeeded operation carrying no DistributedMapDetails raises ExecutionError."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr3",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=None,
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    with pytest.raises(ExecutionError):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr3"),
        )


def test_map_run_handler_already_started():
    """Test distributed_map_handler suspends when the operation is already started."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr5",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    with pytest.raises(
        SuspendExecution, match="Map run mr5 started, suspending for completion"
    ):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr5"),
        )


def test_map_run_handler_new_operation():
    """Test distributed_map_handler creates a START checkpoint for a new operation."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr6",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        distributed_map_handler(
            source=["a", "b", "c"],
            processor="test_processor",
            max_concurrency=42,
            state=mock_state,
            operation_identifier=_identifier("mr6"),
        )

    mock_state.create_checkpoint.assert_called_once()
    operation_update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    assert operation_update.operation_id == "mr6"
    assert operation_update.operation_type == OperationType.DISTRIBUTED_MAP
    assert operation_update.action == OperationAction.START
    assert operation_update.name == "test_map_run"

    distributed_map_options = operation_update.to_dict()["DistributedMapOptions"]
    assert distributed_map_options["MaxConcurrency"] == 42
    assert distributed_map_options["Processor"]["FunctionName"] == "test_processor"
    assert distributed_map_options["Source"]["InlineSourceConfig"]["Items"] == [
        '"a"',
        '"b"',
        '"c"',
    ]


def test_map_run_handler_no_config():
    """Test distributed_map_handler uses a default config when none is provided."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr7",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr7"),
            config=None,
        )

    mock_state.create_checkpoint.assert_called_once()


# Immediate Response Handling Tests
# ============================================================================


def test_map_run_immediate_response_get_checkpoint_result_called_twice():
    """Test get_checkpoint_result is called twice when a checkpoint is created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr8",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr8"),
        )

    assert mock_state.get_checkpoint_result.call_count == 2


def test_map_run_immediate_response_create_checkpoint_is_sync_true():
    """Test create_checkpoint is called with is_sync=True."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr9",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr9"),
        )

    mock_state.create_checkpoint.assert_called_once()
    assert mock_state.create_checkpoint.call_args[1]["is_sync"] is True


def test_map_run_immediate_response_immediate_success():
    """Test immediate success: second check returns SUCCEEDED, summary returned."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    succeeded_op = Operation(
        operation_id="mr10",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
            total_count=1,
        ),
    )
    succeeded = CheckpointedResult.create_from_operation(succeeded_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, succeeded]

    result = distributed_map_handler(
        source=["a"],
        processor="test_processor",
        max_concurrency=10,
        state=mock_state,
        operation_identifier=_identifier("mr10"),
    )

    assert result.status is DistributedMapStatus.SUCCEEDED
    assert result.success_count == 1
    mock_state.create_checkpoint.assert_called_once()
    assert mock_state.get_checkpoint_result.call_count == 2


def test_map_run_immediate_response_no_immediate_response():
    """Test no immediate response: second check returns STARTED, suspends."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr12",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    with pytest.raises(SuspendExecution):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr12"),
        )

    mock_state.create_checkpoint.assert_called_once()
    assert mock_state.get_checkpoint_result.call_count == 2


def test_map_run_immediate_response_already_completed():
    """Test already completed: first check is SUCCEEDED, no checkpoint created."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    succeeded_op = Operation(
        operation_id="mr13",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
            total_count=1,
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(succeeded_op)
    )

    result = distributed_map_handler(
        source=["a"],
        processor="test_processor",
        max_concurrency=10,
        state=mock_state,
        operation_identifier=_identifier("mr13"),
    )

    assert result.status is DistributedMapStatus.SUCCEEDED
    mock_state.create_checkpoint.assert_not_called()
    assert mock_state.get_checkpoint_result.call_count == 1


@patch(
    "aws_durable_execution_sdk_python.operation.dmap.suspend_with_optional_resume_delay"
)
def test_map_run_handler_suspend_does_not_raise(mock_suspend):
    """Test distributed_map_handler raises ExecutionError if suspend does not raise."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr14",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STARTED,
    )
    started = CheckpointedResult.create_from_operation(started_op)
    mock_state.get_checkpoint_result.side_effect = [not_found, started]

    mock_suspend.return_value = None

    with pytest.raises(
        ExecutionError,
        match="suspend_with_optional_resume_delay should have raised an exception, but did not.",
    ):
        distributed_map_handler(
            source=["a"],
            processor="test_processor",
            max_concurrency=10,
            state=mock_state,
            operation_identifier=_identifier("mr14"),
        )

    mock_suspend.assert_called_once()


# Serialization and result-collection tests
# ============================================================================


def _start_options(state_calls, source, processor, max_concurrency, config):
    """Run the executor through the new-operation path and return the sent options dict."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.get_checkpoint_result.side_effect = state_calls

    executor = DistributedMapOperationExecutor(
        source=source,
        processor=processor,
        max_concurrency=max_concurrency,
        state=mock_state,
        operation_identifier=_identifier("mrw"),
        config=config,
    )
    with pytest.raises(SuspendExecution):
        executor.process()
    update = mock_state.create_checkpoint.call_args[1]["operation_update"]
    return update.to_dict()["DistributedMapOptions"]


def _new_op_state_calls():
    not_found = CheckpointedResult.create_not_found()
    started = CheckpointedResult.create_from_operation(
        Operation(
            operation_id="mrw",
            operation_type=OperationType.DISTRIBUTED_MAP,
            sub_type=OperationSubType.DISTRIBUTED_MAP,
            name="test_map_run",
            status=OperationStatus.STARTED,
        )
    )
    return [not_found, started]


def test_processor_item_outcomes_sets_response_types():
    """report_item_outcomes serializes FunctionResponseTypes=REPORT_BATCH_ITEM_OUTCOMES."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes("proc", batch_size=25),
        max_concurrency=4,
        config=DistributedMapConfig(),
    )
    processor = options["Processor"]
    assert processor["FunctionName"] == "proc"
    assert processor["FunctionResponseTypes"] == ["REPORT_BATCH_ITEM_OUTCOMES"]
    assert processor["BatchSize"] == 25


def test_processor_batch_outcome_sets_no_response_types():
    """report_batch_outcome omits FunctionResponseTypes, selecting default batch mode."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=4,
        config=DistributedMapConfig(),
    )
    assert "FunctionResponseTypes" not in options["Processor"]


def test_processor_unlimited_retries_maps_to_negative_one():
    """DistributedMapProcessorRetryConfig.UNLIMITED serializes to MaxRetryAttempts=-1."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes(
            "proc",
            retry=DistributedMapProcessorRetryConfig(
                max_retry_attempts=DistributedMapProcessorRetryConfig.UNLIMITED,
                max_retry_duration=Duration.from_hours(1),
            ),
        ),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    processor = options["Processor"]
    assert processor["FunctionResponseTypes"] == ["REPORT_BATCH_ITEM_OUTCOMES"]
    assert processor["MaxRetryAttempts"] == -1
    assert processor["MaxRetryDurationSeconds"] == 3600


def test_processor_explicit_retry_attempts_pass_through():
    """A plain int retry count passes through unchanged (no sentinel mapping)."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome(
            "proc", retry=DistributedMapProcessorRetryConfig(max_retry_attempts=0)
        ),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    assert options["Processor"]["MaxRetryAttempts"] == 0
    # batch mode reports no per-item response types
    assert "FunctionResponseTypes" not in options["Processor"]


def test_s3_source_serializes_config():
    """An S3 json_lines source serializes to an S3SourceConfig block."""
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_json_lines(
            "s3://bucket/data.jsonl", max_items=500
        ),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=2,
        config=DistributedMapConfig(),
    )
    source = options["Source"]
    assert source["Type"] == "S3"
    assert source["MaxItemsToRead"] == 500
    s3 = source["S3SourceConfig"]
    assert s3["Type"] == "OBJECT"
    assert s3["Bucket"] == "bucket"
    assert s3["ObjectConfig"]["Key"] == "data.jsonl"
    assert s3["ObjectConfig"]["Format"] == "JSON_LINES"
    assert "ListedObjectMetadataConfig" not in s3
    assert "ListedObjectsConfig" not in s3


def test_full_config_serializes_all_blocks():
    """Completion, destination, timeout, and result-collection blocks all serialize."""
    config = DistributedMapResultConfig(
        completion_config=DistributedMapCompletionConfig.failure_percentage(
            5, minimum_sample_size=200
        ),
        destination=DistributedMapDestinationConfig(
            on_success=DistributedMapDestination.s3_successes("s3://out/ok"),
            on_failure=DistributedMapDestination.s3_failures("s3://out/bad"),
        ),
        timeout=Duration.from_minutes(30),
    )
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=8,
        config=config,
    )
    assert options["CompletionConfig"] == {
        "ToleratedFailurePercentage": 5,
        "MinimumSampleSize": 200,
    }
    on_success = options["Destination"]["OnSuccess"]
    assert on_success["Type"] == "S3"
    assert on_success["S3DestinationConfig"]["Bucket"] == "out"
    assert on_success["S3DestinationConfig"]["KeyPrefix"] == "ok"
    on_failure = options["Destination"]["OnFailure"]
    assert on_failure["Include"] == ["INPUT", "ERROR"]
    assert on_failure["S3DestinationConfig"]["Bucket"] == "out"
    assert options["TimeoutSeconds"] == 1800
    assert options["ResultCollection"] == {"Mode": "INLINE"}


def test_result_config_returns_map_run_result_with_items():
    """With a DistributedMapResultConfig, a DistributedMapResult with per-item results is built."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    error = ErrorObject(message="boom", type="ItemError", data=None, stack_trace=None)
    operation = Operation(
        operation_id="mrr",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=1,
            unprocessed_count=0,
            total_count=2,
            results=(
                DistributedMapResultItemApi(
                    item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output="42"
                ),
                DistributedMapResultItemApi(
                    item_id="1", status=DistributedMapItemStatus.FAILED, error=error
                ),
            ),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a", "b"],
        processor=DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=2,
        state=mock_state,
        operation_identifier=_identifier("mrr"),
        config=DistributedMapResultConfig(),
    )

    assert isinstance(result, DistributedMapResult)
    assert len(result.all) == 2
    assert result.get_results() == [42]
    errors = result.get_errors()
    assert len(errors) == 1
    assert errors[0].type == "ItemError"
    assert errors[0].message == "boom"


def test_item_error_keeps_every_error_member():
    """An item's error reaches the caller as the full ErrorObject, data and trace included."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mre",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=0,
            failure_count=1,
            unprocessed_count=0,
            total_count=1,
            results=(
                DistributedMapResultItemApi(
                    item_id="0",
                    status=DistributedMapItemStatus.FAILED,
                    error=ErrorObject(
                        message="boom",
                        type="ItemError",
                        data="payload",
                        stack_trace=["frame one", "frame two"],
                    ),
                ),
            ),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("mre"),
        config=DistributedMapResultConfig(),
    )

    error = result.get_errors()[0]
    assert error is not None
    assert error.data == "payload"
    assert error.stack_trace == ["frame one", "frame two"]

    # The raised item failure carries the same members.
    with pytest.raises(DistributedMapError) as exc:
        result.throw_if_error()
    assert exc.value.error_type == "ItemError"
    assert exc.value.data == "payload"
    assert exc.value.stack_trace == ["frame one", "frame two"]


def test_item_error_members_absent_from_the_backend_stay_none():
    """An omitted error member stays None rather than becoming an empty string."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mren",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=0,
            failure_count=1,
            unprocessed_count=0,
            total_count=1,
            results=(
                DistributedMapResultItemApi(
                    item_id="0",
                    status=DistributedMapItemStatus.FAILED,
                    error=ErrorObject(
                        message=None, type=None, data=None, stack_trace=None
                    ),
                ),
            ),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("mren"),
        config=DistributedMapResultConfig(),
    )

    error = result.get_errors()[0]
    assert error is not None
    assert error.type is None
    assert error.message is None


def test_plain_config_returns_plain_summary():
    """With a plain DistributedMapConfig, a DistributedMapSummary is returned."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrs",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=2,
            failure_count=0,
            unprocessed_count=0,
            total_count=2,
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a", "b"],
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=2,
        state=mock_state,
        operation_identifier=_identifier("mrs"),
    )

    assert isinstance(result, DistributedMapSummary)
    assert not isinstance(result, DistributedMapResult)
    assert result.success_count == 2


def test_csv_source_header_location():
    """Explicit headers map to GIVEN, omitting them maps to FIRST_ROW."""
    # headers -> HeaderLocation GIVEN, headers sent
    given = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_csv("s3://b/data.csv", headers=["a", "b"]),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    csv_opts = given["Source"]["S3SourceConfig"]["ObjectConfig"]["CsvFormatOptions"]
    assert csv_opts["HeaderLocation"] == "GIVEN"
    assert csv_opts["Headers"] == ["a", "b"]
    assert csv_opts["Delimiter"] == "COMMA"

    # no headers -> HeaderLocation FIRST_ROW
    first_row = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_csv("s3://b/data.csv"),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    object_cfg = first_row["Source"]["S3SourceConfig"]["ObjectConfig"]
    assert object_cfg["CsvFormatOptions"]["HeaderLocation"] == "FIRST_ROW"
    assert "Headers" not in object_cfg["CsvFormatOptions"]


# Call-site validation and serdes tests
# ============================================================================


def test_csv_delimiter_accepts_enum():
    opts = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_csv(
            "s3://b/data.csv", delimiter=DistributedMapCsvDelimiter.PIPE
        ),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    assert (
        opts["Source"]["S3SourceConfig"]["ObjectConfig"]["CsvFormatOptions"][
            "Delimiter"
        ]
        == "PIPE"
    )


def test_inline_source_over_1mb_rejected():
    big = ["x" * 100_000] * 12  # ~1.2 MB serialized
    with pytest.raises(ValidationError, match="1 MB limit"):
        _start_options(
            _new_op_state_calls(),
            source=big,
            processor=DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
            config=DistributedMapConfig(),
        )


class _RawTextSerDes(SerDes):
    """A serdes that writes non-ASCII characters raw rather than escaped."""

    def serialize(self, value, _serdes_context):  # noqa: ANN001, ANN201
        return json.dumps(value, ensure_ascii=False)

    def deserialize(self, data, _serdes_context):  # noqa: ANN001, ANN201
        return json.loads(data)


def test_inline_source_of_raw_non_ascii_is_measured_as_utf8():
    """Raw non-ASCII under the limit in UTF-8 is accepted.

    200,000 of U+00E9 is 400,008 bytes as the compact UTF-8 JSON array the
    service measures, and 1,200,008 bytes once each character is escaped as
    \\uXXXX. Measuring the escaped form would reject a source the service
    accepts.
    """
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.inline(
            ["\u00e9" * 200_000], serdes=_RawTextSerDes()
        ),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    [item] = options["Source"]["InlineSourceConfig"]["Items"]
    assert item == json.dumps("\u00e9" * 200_000, ensure_ascii=False)


def test_inline_source_of_raw_non_ascii_over_the_limit_is_rejected():
    """Raw non-ASCII over the limit in UTF-8 is still rejected.

    U+00E9 is two UTF-8 bytes, so 600,000 of them is 1,200,002 bytes before the
    array's own punctuation, over the 1 MB limit.
    """
    with pytest.raises(ValidationError, match="1 MB limit"):
        _start_options(
            _new_op_state_calls(),
            source=DistributedMapSource.inline(
                ["\u00e9" * 600_000], serdes=_RawTextSerDes()
            ),
            processor=DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
            config=DistributedMapConfig(),
        )


def test_inline_source_measurement_is_unchanged_for_the_default_serdes():
    """The default serdes escapes non-ASCII itself, so its measurement is unaffected.

    It writes U+00E9 as the six ASCII characters \\u00e9, so the serialized item
    holds no non-ASCII and both measurements give the same byte count. The same
    content is rejected before and after the change.
    """
    with pytest.raises(ValidationError, match="1 MB limit"):
        _start_options(
            _new_op_state_calls(),
            source=["\u00e9" * 200_000],
            processor=DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
            config=DistributedMapConfig(),
        )


def test_reader_state_serialized_and_capped():
    # typed initial_state is serialized into the opaque state string
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.reader_function(
            "reader", initial_state={"page": 0}
        ),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    reader_cfg = options["Source"]["ReaderFunctionSourceConfig"]
    assert reader_cfg["FunctionName"] == "reader"
    assert reader_cfg["InitialState"] == '{"page": 0}'

    # oversize state is rejected
    with pytest.raises(ValidationError, match="32 KB limit"):
        _start_options(
            _new_op_state_calls(),
            source=DistributedMapSource.reader_function(
                "reader", initial_state="x" * 40_000
            ),
            processor=DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
            config=DistributedMapConfig(),
        )


def test_inline_custom_serdes_applied_to_items():
    """A custom inline serdes transforms each item's serialized value."""
    from aws_durable_execution_sdk_python.serdes import SerDes

    class _UpperSerDes(SerDes):
        def serialize(self, value, _serdes_context):  # noqa: ANN001, ANN201
            return json.dumps(value.upper())

        def deserialize(self, data, _serdes_context):  # noqa: ANN001, ANN201
            return json.loads(data).lower()

    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.inline(["a", "b"], serdes=_UpperSerDes()),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    assert options["Source"]["InlineSourceConfig"]["Items"] == ['"A"', '"B"']


# Result helpers, from_dict, destinations, and source variants
# ============================================================================


def test_map_run_options_from_dict_round_trip():
    sent = _start_options(
        _new_op_state_calls(),
        source=["a", "b"],
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=7,
        config=DistributedMapConfig(),
    )
    parsed = DistributedMapOptions.from_dict(sent)
    assert parsed.max_concurrency == 7
    assert parsed.source.source_type is DistributedMapSourceType.INLINE
    assert parsed.processor.function_name == "proc"


def test_destination_only_success():
    opts = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.s3_successes("s3://out/ok")
            )
        ),
    )
    dest = opts["Destination"]
    assert "OnSuccess" in dest
    assert "OnFailure" not in dest


def test_destination_only_failure():
    opts = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_failure=DistributedMapDestination.s3_failures("s3://out/bad")
            )
        ),
    )
    dest = opts["Destination"]
    assert "OnFailure" in dest
    assert "OnSuccess" not in dest


def test_s3_objects_source_config():
    opts = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_objects("s3://b/prefix/"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = opts["Source"]["S3SourceConfig"]
    assert s3["Type"] == "LISTED_OBJECT_METADATA"
    assert s3["ListedObjectMetadataConfig"]["KeyPrefix"] == "prefix/"
    assert "ObjectConfig" not in s3
    assert "ListedObjectsConfig" not in s3


def test_s3_flattened_json_lines_source_config():
    opts = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_flattened_json_lines("s3://b/prefix/"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = opts["Source"]["S3SourceConfig"]
    assert s3["Type"] == "LISTED_OBJECTS"
    assert s3["ListedObjectsConfig"]["KeyPrefix"] == "prefix/"
    assert s3["ListedObjectsConfig"]["Format"] == "JSON_LINES"


# Validations, config branches, and round-trips
# ============================================================================


def _start_executor(source, processor, config, max_concurrency=1):
    """Build an executor on the new-operation path (for error-path tests)."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    mock_state.get_checkpoint_result.side_effect = _new_op_state_calls()
    return DistributedMapOperationExecutor(
        source=source,
        processor=processor,
        max_concurrency=max_concurrency,
        state=mock_state,
        operation_identifier=_identifier("mrw"),
        config=config,
    )


# --- Completion config translation ---


def test_completion_config_percentage_and_sample_size_sent():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            completion_config=DistributedMapCompletionConfig.failure_percentage(
                25, minimum_sample_size=50
            )
        ),
    )
    assert options["CompletionConfig"] == {
        "ToleratedFailurePercentage": 25,
        "MinimumSampleSize": 50,
    }


# --- Source translation variants ---


def test_objects_whole_bucket_prefix_config():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_objects("s3://bucket"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = options["Source"]["S3SourceConfig"]
    assert s3["Type"] == "LISTED_OBJECT_METADATA"
    assert s3["ListedObjectMetadataConfig"]["KeyPrefix"] == ""
    assert "ObjectConfig" not in s3


def test_flattened_csv_source_config():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_flattened_csv(
            "s3://b/prefix", headers=["a", "b"]
        ),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    listed = options["Source"]["S3SourceConfig"]["ListedObjectsConfig"]
    assert listed["Format"] == "CSV"
    assert listed["CsvFormatOptions"]["HeaderLocation"] == "GIVEN"
    assert listed["CsvFormatOptions"]["Headers"] == ["a", "b"]


def test_reader_source_without_initial_state_omits_state():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.reader_function("reader"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    reader = options["Source"]["ReaderFunctionSourceConfig"]
    assert reader["FunctionName"] == "reader"
    assert "InitialState" not in reader


def test_inline_non_json_serdes_supported():
    """Items are opaque strings, so a serdes need not produce JSON."""
    from aws_durable_execution_sdk_python.serdes import SerDes

    class _PlainTextSerDes(SerDes):
        def serialize(self, value, _serdes_context):  # noqa: ANN001, ANN201
            return f"item-{value}"

        def deserialize(self, data, _serdes_context):  # noqa: ANN001, ANN201, ARG002
            return data

    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.inline([1, 2], serdes=_PlainTextSerDes()),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    assert options["Source"]["InlineSourceConfig"]["Items"] == ["item-1", "item-2"]


# --- Destination translation permutations ---


def test_success_destination_include_input_and_owner():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.s3_successes(
                    "s3://out/ok",
                    include_input=True,
                    include_output=True,
                    expected_bucket_owner="123456789012",
                )
            )
        ),
    )
    on_success = options["Destination"]["OnSuccess"]
    assert on_success["Include"] == ["INPUT", "OUTPUT"]
    assert on_success["S3DestinationConfig"]["ExpectedBucketOwner"] == "123456789012"
    assert "OnFailure" not in options["Destination"]


def test_failure_destination_error_only_and_owner():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_failure=DistributedMapDestination.s3_failures(
                    "s3://out/bad",
                    include_input=False,
                    include_error=True,
                    expected_bucket_owner="123456789012",
                )
            )
        ),
    )
    on_failure = options["Destination"]["OnFailure"]
    assert on_failure["Include"] == ["ERROR"]
    assert on_failure["S3DestinationConfig"]["ExpectedBucketOwner"] == "123456789012"


# --- Operation round-trips through the checkpoint ---


def test_operation_to_dict_round_trip_preserves_results():
    op = Operation(
        operation_id="opx",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=1,
            unprocessed_count=0,
            total_count=2,
            distributed_map_run_arn="arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1/distributed-map-run/z",
            error=ErrorObject(
                message="tolerated",
                type="DistributedMapFailure",
                data=None,
                stack_trace=None,
            ),
            results=(
                DistributedMapResultItemApi(
                    item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output=5
                ),
                DistributedMapResultItemApi(
                    item_id="1",
                    status=DistributedMapItemStatus.FAILED,
                    error=ErrorObject(
                        message="boom", type="E", data=None, stack_trace=None
                    ),
                ),
            ),
        ),
    )
    block = op.to_dict()["DistributedMapDetails"]
    assert block["Results"][0] == {"ItemId": "0", "Status": "SUCCEEDED", "Output": 5}
    assert block["DistributedMapRunArn"].endswith("/distributed-map-run/z")
    assert block["SourceExhausted"] is True
    assert block["Error"]["ErrorType"] == "DistributedMapFailure"
    assert block["TotalCount"] == 2

    parsed = Operation.from_dict(op.to_dict())
    assert parsed.distributed_map_details is not None
    assert parsed.distributed_map_details.source_exhausted is True
    assert parsed.distributed_map_details.error.message == "tolerated"
    assert parsed.distributed_map_details.results[0].output == 5
    assert parsed.distributed_map_details.results[1].error.type == "E"


def test_options_full_round_trip():
    options_dict = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_csv("s3://b/f.csv", headers=["a"]),
        processor=DistributedMapProcessor.report_item_outcomes(
            "proc",
            batch_size=5,
            retry=DistributedMapProcessorRetryConfig(
                max_retry_attempts=DistributedMapProcessorRetryConfig.UNLIMITED,
                max_retry_duration=Duration.from_minutes(10),
            ),
            durable_execution_name_prefix="pfx",
        ),
        max_concurrency=3,
        config=DistributedMapResultConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.s3_successes("s3://o/ok"),
                on_failure=DistributedMapDestination.s3_failures("s3://o/bad"),
            ),
            completion_config=DistributedMapCompletionConfig.failure_count(2),
            timeout=Duration.from_minutes(5),
        ),
    )
    parsed = DistributedMapOptions.from_dict(options_dict)
    assert parsed.max_concurrency == 3
    assert parsed.source.source_type is DistributedMapSourceType.S3
    assert parsed.processor.function_response_types == [
        DistributedMapFunctionResponseType.REPORT_BATCH_ITEM_OUTCOMES
    ]
    assert parsed.processor.max_retry_attempts == -1
    assert parsed.processor.durable_execution_name_prefix == "pfx"
    assert parsed.destination is not None
    assert parsed.completion_config.tolerated_failure_count == 2
    assert parsed.result_collection.mode is DistributedMapResultCollectionMode.INLINE
    assert parsed.timeout_seconds == 300
    assert parsed.to_dict()["MaxConcurrency"] == 3


# Remaining validation and config branches
# ============================================================================


def test_csv_first_row_no_headers():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_csv("s3://b/f.csv"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    csv_opts = options["Source"]["S3SourceConfig"]["ObjectConfig"]["CsvFormatOptions"]
    assert csv_opts["HeaderLocation"] == "FIRST_ROW"
    assert "Headers" not in csv_opts
    assert csv_opts["Delimiter"] == "COMMA"


def test_s3_source_expected_bucket_owner():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.s3_json_lines(
            "s3://b/k.jsonl", expected_bucket_owner="123456789012"
        ),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    assert options["Source"]["S3SourceConfig"]["ExpectedBucketOwner"] == "123456789012"


def test_empty_destination_config_omitted():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(destination=DistributedMapDestinationConfig()),
    )
    assert "Destination" not in options


def test_success_destination_input_only():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.s3_successes(
                    "s3://out/ok", include_input=True, include_output=False
                )
            )
        ),
    )
    assert options["Destination"]["OnSuccess"]["Include"] == ["INPUT"]


def test_failure_destination_input_only():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_failure=DistributedMapDestination.s3_failures(
                    "s3://out/bad", include_input=True, include_error=False
                )
            )
        ),
    )
    assert options["Destination"]["OnFailure"]["Include"] == ["INPUT"]


def test_reader_source_options_round_trip():
    options_dict = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.reader_function(
            "reader", initial_state={"page": 0}
        ),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    parsed = DistributedMapOptions.from_dict(options_dict)
    assert parsed.source.source_type is DistributedMapSourceType.READER_FUNCTION
    assert parsed.source.reader_config is not None
    assert parsed.source.reader_config.function_name == "reader"


def test_operation_to_dict_minimal_details_omits_optionals():
    op = Operation(
        operation_id="opm",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
        ),
    )
    block = op.to_dict()["DistributedMapDetails"]
    assert "DistributedMapRunArn" not in block
    assert "CompletionDetails" not in block
    assert "TotalCount" not in block
    assert "Results" not in block


@pytest.mark.parametrize(
    "status",
    [OperationStatus.FAILED, OperationStatus.STOPPED, OperationStatus.TIMED_OUT],
)
def test_operation_level_terminal_failure_without_details_raises(status):
    """A terminal failure carrying no details raises, since counts cannot be built."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrf",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=status,
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    with pytest.raises(ExecutionError):
        distributed_map_handler(
            source=["a"],
            processor="p",
            max_concurrency=1,
            state=mock_state,
            operation_identifier=_identifier("mrf"),
        )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (OperationStatus.FAILED, DistributedMapStatus.FAILED),
        (OperationStatus.TIMED_OUT, DistributedMapStatus.TIMED_OUT),
        (OperationStatus.STOPPED, DistributedMapStatus.STOPPED),
    ],
)
def test_terminal_failure_with_details_resolves_with_summary(status, expected):
    """A terminal non-success run resolves with its summary rather than raising."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrf",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=status,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=False,
            success_count=1,
            failure_count=2,
            unprocessed_count=0,
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    summary = distributed_map_handler(
        source=["a"],
        processor="p",
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("mrf"),
    )
    assert summary.status is expected
    assert summary.failure_count == 2
    # The caller opts into raising rather than having it forced on them.
    with pytest.raises(DistributedMapError):
        summary.throw_if_error()


def test_terminal_without_source_exhausted_raises():
    """A terminal operation whose run exists but omits SourceExhausted is surfaced, not papered over."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrf",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.STOPPED,
        distributed_map_details=DistributedMapDetails(
            distributed_map_run_arn="arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1/distributed-map-run/abc",
            success_count=0,
            failure_count=0,
            unprocessed_count=3,
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    with pytest.raises(ExecutionError, match="carried no SourceExhausted"):
        distributed_map_handler(
            source=["a"],
            processor="p",
            max_concurrency=1,
            state=mock_state,
            operation_identifier=_identifier("mrf"),
        )


# Serdes decode, falsy output, and duration-only retry
# ============================================================================


def test_custom_result_serdes_applied_on_decode():
    """A custom result_serdes transforms each per-item output on decode."""
    from aws_durable_execution_sdk_python.serdes import SerDes

    class _UpperSerDes(SerDes):
        def serialize(self, value, _serdes_context):  # noqa: ANN001, ANN201
            return json.dumps(value)

        def deserialize(self, data, _serdes_context):  # noqa: ANN001, ANN201
            return json.loads(data).upper()

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="cs",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
            total_count=1,
            results=(
                DistributedMapResultItemApi(
                    item_id="0",
                    status=DistributedMapItemStatus.SUCCEEDED,
                    output='"abc"',
                ),
            ),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    result = distributed_map_handler(
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("cs"),
        config=DistributedMapResultConfig(result_serdes=_UpperSerDes()),
    )
    assert result.get_results() == ["ABC"]


@pytest.mark.parametrize("value", [0, False, "", [], {}])
def test_falsy_output_round_trips(value):
    """A falsy-but-present output survives the round-trip and decode (not dropped)."""
    entry = DistributedMapResultItemApi(
        item_id="0",
        status=DistributedMapItemStatus.SUCCEEDED,
        output=json.dumps(value),
    )
    assert entry.to_dict()["Output"] == json.dumps(value)

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="fo",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
            total_count=1,
            results=(entry,),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    result = distributed_map_handler(
        source=["a"],
        processor=DistributedMapProcessor.report_item_outcomes("proc"),
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("fo"),
        config=DistributedMapResultConfig(),
    )
    assert result.get_results() == [value]


def test_processor_retry_duration_only():
    """A retry config with only a duration sends the duration and omits attempts."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome(
            "proc",
            retry=DistributedMapProcessorRetryConfig(
                max_retry_duration=Duration.from_minutes(5),
            ),
        ),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    processor = options["Processor"]
    assert processor["MaxRetryDurationSeconds"] == 300
    assert "MaxRetryAttempts" not in processor


def test_resolved_summary_uses_operation_status_not_details():
    """_resolve_summary carries the derived status when details omit it."""
    operation = Operation(
        operation_id="dmap-id",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="test_map_run",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=2,
            failure_count=0,
            unprocessed_count=0,
            total_count=2,
        ),
    )
    executor = DistributedMapOperationExecutor(
        source=["a", "b"],
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        state=Mock(spec=ExecutionState),
        operation_identifier=_identifier("dmap-id"),
        config=DistributedMapConfig(),
    )

    summary = executor._resolve_summary(operation)

    assert summary.status is DistributedMapStatus.SUCCEEDED
    assert summary.source_exhausted is True
    assert summary.success_count == 2


def test_checkpoint_from_a_different_map_is_rejected():
    """A checkpoint whose identity differs from the current call is not consumed."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="dmap-id",
        operation_type=OperationType.DISTRIBUTED_MAP,
        sub_type=OperationSubType.DISTRIBUTED_MAP,
        name="a_different_map",
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            source_exhausted=True,
            success_count=999,
            failure_count=0,
            unprocessed_count=0,
            total_count=999,
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    with pytest.raises(NonDeterministicExecutionError, match="name checkpoint="):
        distributed_map_handler(
            source=["a", "b"],
            processor=DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
            state=mock_state,
            operation_identifier=_identifier("dmap-id"),
        )

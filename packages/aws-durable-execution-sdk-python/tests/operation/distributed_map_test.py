"""Unit tests for map run handler."""

from __future__ import annotations

import json
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.concurrency.models import (
    DistributedMapCompletionReason,
    DistributedMapItemError,
    DistributedMapResult,
    DistributedMapResultItem,
    DistributedMapStatus,
    DistributedMapSummary,
)
from aws_durable_execution_sdk_python.config import (
    Duration,
    DistributedMapCompletionConfig,
    DistributedMapConfig,
    DistributedMapDestination,
    DistributedMapDestinationConfig,
    DistributedMapProcessor,
    DistributedMapSource,
    ProcessorRetryConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
    DistributedMapError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    DistributedMapDetails,
    DistributedMapOptions,
    DistributedMapResultItemWire,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.operation.distributed_map import (
    DistributedMapOperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import DEFAULT_JSON_SERDES
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
        operation_id, OperationSubType.DISTRIBUTED_MAP, None, name
    )


def test_map_run_handler_already_succeeded():
    """Test distributed_map_handler returns a summary when the operation already succeeded."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr1",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=5,
            failure_count=0,
            unprocessed_count=0,
            total_count=5,
            distributed_map_run_arn="arn:aws:lambda:us-east-1:123456789012:map-run:abc",
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
    assert result.completion_reason is DistributedMapCompletionReason.ALL_COMPLETED
    assert result.success_count == 5
    assert result.failure_count == 0
    assert result.total_count == 5
    assert result.distributed_map_id == "abc"
    mock_state.create_checkpoint.assert_not_called()


def test_map_run_handler_resolves_non_success_without_raising():
    """Test a non-SUCCEEDED run resolves with a summary rather than raising.

    The durable operation succeeded (it delivered a result), but the run's own
    status is FAILED. distributed_map must return the summary, not raise.
    """
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr2",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="FAILED",
            completion_reason="FAILURE_TOLERANCE_EXCEEDED",
            success_count=3,
            failure_count=2,
            unprocessed_count=1,
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
    assert (
        result.completion_reason
        is DistributedMapCompletionReason.FAILURE_TOLERANCE_EXCEEDED
    )
    assert result.failure_count == 2
    assert result.has_failure is True


def test_map_run_handler_succeeded_no_details_raises():
    """Test a succeeded operation carrying no DistributedMapDetails raises ExecutionError."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    operation = Operation(
        operation_id="mr3",
        operation_type=OperationType.DISTRIBUTED_MAP,
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
        "a",
        "b",
        "c",
    ]


def test_map_run_handler_no_config():
    """Test distributed_map_handler uses a default config when none is provided."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr7",
        operation_type=OperationType.DISTRIBUTED_MAP,
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


# ============================================================================
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
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=1,
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
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED", completion_reason="ALL_COMPLETED"
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
    "aws_durable_execution_sdk_python.operation.distributed_map.suspend_with_optional_resume_delay"
)
def test_map_run_handler_suspend_does_not_raise(mock_suspend):
    """Test distributed_map_handler raises ExecutionError if suspend does not raise."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"

    not_found = CheckpointedResult.create_not_found()
    started_op = Operation(
        operation_id="mr14",
        operation_type=OperationType.DISTRIBUTED_MAP,
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


# ============================================================================
# Wire serialization and result-collection tests (slices 3-4)
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
            status=OperationStatus.STARTED,
        )
    )
    return [not_found, started]


def test_processor_report_failed_items_sets_response_types():
    """report_failed_items serializes FunctionResponseTypes=ReportBatchItemFailures."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_failed_items("proc", batch_size=25),
        max_concurrency=4,
        config=DistributedMapConfig(),
    )
    processor = options["Processor"]
    assert processor["FunctionName"] == "proc"
    assert processor["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert processor["BatchSize"] == 25


def test_processor_unlimited_retries_maps_to_negative_one():
    """ProcessorRetryConfig.UNLIMITED serializes to MaxRetryAttempts=-1."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_item_results(
            "proc",
            retry=ProcessorRetryConfig(
                max_retry_attempts=ProcessorRetryConfig.UNLIMITED,
                max_retry_duration=Duration.from_hours(1),
            ),
        ),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    processor = options["Processor"]
    assert processor["FunctionResponseTypes"] == ["ReportBatchItemResults"]
    assert processor["MaxRetryAttempts"] == -1
    assert processor["MaxRetryDurationSeconds"] == 3600


def test_processor_explicit_retry_attempts_pass_through():
    """A plain int retry count passes through unchanged (no sentinel mapping)."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome(
            "proc", retry=ProcessorRetryConfig(max_retry_attempts=0)
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
        source=DistributedMapSource.S3.json_lines(
            "s3://bucket/data.jsonl", max_items=500
        ),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=2,
        config=DistributedMapConfig(),
    )
    source = options["Source"]
    assert source["Type"] == "S3"
    assert source["MaxItemsToRead"] == 500
    assert source["S3SourceConfig"]["Bucket"] == "bucket"
    assert source["S3SourceConfig"]["Key"] == "data.jsonl"
    assert source["S3SourceConfig"]["Format"] == "JSON_LINES"


def test_full_config_serializes_all_blocks():
    """Completion, destination, timeout, and result-collection blocks all serialize."""
    config = DistributedMapConfig(
        completion_config=DistributedMapCompletionConfig.failure_percentage(
            5, minimum_sample_size=200
        ),
        destination=DistributedMapDestinationConfig(
            on_success=DistributedMapDestination.S3.successes("s3://out/ok"),
            on_failure=DistributedMapDestination.S3.failures("s3://out/bad"),
        ),
        timeout=Duration.from_minutes(30),
        collect_results=True,
    )
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_item_results("proc"),
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
    assert on_success["S3DestinationConfig"]["Prefix"] == "ok"
    on_failure = options["Destination"]["OnFailure"]
    assert on_failure["Include"] == ["INPUT", "ERROR"]
    assert on_failure["S3DestinationConfig"]["Bucket"] == "out"
    assert options["TimeoutSeconds"] == 1800
    assert options["ResultCollection"] == {"Mode": "INLINE"}


def test_collect_results_returns_map_run_result_with_items():
    """When collect_results is set, a DistributedMapResult with per-item results is built."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    error = ErrorObject(message="boom", type="ItemError", data=None, stack_trace=None)
    operation = Operation(
        operation_id="mrr",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=1,
            failure_count=1,
            unprocessed_count=0,
            total_count=2,
            results=(
                DistributedMapResultItemWire(
                    item_id="0", status="SUCCEEDED", output=42
                ),
                DistributedMapResultItemWire(item_id="1", status="FAILED", error=error),
            ),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )

    result = distributed_map_handler(
        source=["a", "b"],
        processor=DistributedMapProcessor.report_item_results("proc"),
        max_concurrency=2,
        state=mock_state,
        operation_identifier=_identifier("mrr"),
        config=DistributedMapConfig(collect_results=True),
    )

    assert isinstance(result, DistributedMapResult)
    assert len(result.all) == 2
    assert result.get_results() == [42]
    errors = result.get_errors()
    assert len(errors) == 1
    assert errors[0].error_type == "ItemError"
    assert errors[0].error_message == "boom"


def test_collect_results_disabled_returns_plain_summary():
    """Without collect_results, a plain DistributedMapSummary is returned (not DistributedMapResult)."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrs",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=2,
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


def test_csv_source_header_location_wire():
    """CSV headers map to GIVEN; expected_columns stays client-side (FIRST_ROW, not sent)."""
    # headers -> HeaderLocation GIVEN, headers sent
    given = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.csv("s3://b/data.csv", headers=["a", "b"]),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    csv_opts = given["Source"]["S3SourceConfig"]["CsvFormatOptions"]
    assert csv_opts["HeaderLocation"] == "GIVEN"
    assert csv_opts["Headers"] == ["a", "b"]
    assert csv_opts["Delimiter"] == "COMMA"

    # no headers -> HeaderLocation FIRST_ROW
    first_row = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.csv("s3://b/data.csv"),
        processor=DistributedMapProcessor.report_batch_outcome("proc"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3_cfg = first_row["Source"]["S3SourceConfig"]
    assert s3_cfg["CsvFormatOptions"]["HeaderLocation"] == "FIRST_ROW"
    assert "Headers" not in s3_cfg["CsvFormatOptions"]


# ============================================================================
# Call-site validation and wire-serdes tests
# ============================================================================


def test_retry_duration_out_of_range_rejected():
    with pytest.raises(ValidationError, match="between 1 minute and 6 hours"):
        ProcessorRetryConfig(max_retry_duration=Duration.from_seconds(30))
    with pytest.raises(ValidationError, match="between 1 minute and 6 hours"):
        ProcessorRetryConfig(max_retry_duration=Duration.from_hours(7))


def test_expected_bucket_owner_must_be_12_digits():
    with pytest.raises(ValidationError, match="12-digit"):
        DistributedMapSource.S3.json_lines(
            "s3://b/k.jsonl", expected_bucket_owner="123"
        )


def test_csv_headers_duplicates_rejected():
    with pytest.raises(ValidationError, match="duplicates"):
        DistributedMapSource.S3.csv("s3://b/k.csv", headers=["a", "a"])


def test_json_lines_requires_key():
    with pytest.raises(ValidationError, match="object key"):
        DistributedMapSource.S3.json_lines("s3://bucket-only")


def test_timeout_out_of_range_rejected():
    with pytest.raises(ValidationError, match="at most 90 days"):
        DistributedMapConfig(timeout=Duration.from_days(91))


def test_item_serdes_requires_collect_results():
    with pytest.raises(ValidationError, match="requires collect_results"):
        DistributedMapConfig(item_serdes=DEFAULT_JSON_SERDES)


def test_empty_function_name_rejected():
    with pytest.raises(ValidationError, match="non-empty"):
        DistributedMapProcessor.report_batch_outcome("")


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


def test_reader_state_serialized_into_wire_and_capped():
    # typed initial_state is serialized into the opaque wire string
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.Reader.from_function(
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
            source=DistributedMapSource.Reader.from_function(
                "reader", initial_state="x" * 40_000
            ),
            processor=DistributedMapProcessor.report_batch_outcome("proc"),
            max_concurrency=1,
            config=DistributedMapConfig(),
        )


def test_invalid_function_reference_rejected():
    with pytest.raises(ValidationError, match="Lambda function reference"):
        DistributedMapProcessor.report_batch_outcome("bad name!")


def test_valid_function_references_accepted():
    for ref in (
        "my-func",
        "my-func:PROD",
        "123456789012:function:my-func",
        "arn:aws:lambda:us-east-1:123456789012:function:my-func",
        "arn:aws:lambda:us-east-1:123456789012:function:my-func:1",
    ):
        # Should not raise.
        DistributedMapProcessor.report_batch_outcome(ref)


def test_inline_custom_serdes_applied_to_wire():
    """A custom inline serdes transforms each item's wire value."""
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
    assert options["Source"]["InlineSourceConfig"]["Items"] == ["A", "B"]


# ============================================================================
# Coverage-gap tests: result helpers, from_dict, destinations, source variants
# ============================================================================


def test_summary_throw_if_error():
    ok = DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        completion_reason=DistributedMapCompletionReason.ALL_COMPLETED,
        success_count=2,
        failure_count=0,
        unprocessed_count=0,
    )
    ok.throw_if_error()  # no raise

    failed = DistributedMapSummary(
        status=DistributedMapStatus.FAILED,
        completion_reason=DistributedMapCompletionReason.FAILURE_TOLERANCE_EXCEEDED,
        success_count=0,
        failure_count=1,
        unprocessed_count=0,
    )
    with pytest.raises(DistributedMapError):
        failed.throw_if_error()

    succeeded_with_failures = DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        completion_reason=DistributedMapCompletionReason.ALL_COMPLETED,
        success_count=1,
        failure_count=1,
        unprocessed_count=0,
    )
    with pytest.raises(DistributedMapError):
        succeeded_with_failures.throw_if_error()


def test_map_run_result_succeeded_failed_filters():
    items = [
        DistributedMapResultItem(item_id="0", status="SUCCEEDED", output=1),
        DistributedMapResultItem(
            item_id="1",
            status="FAILED",
            error=DistributedMapItemError(error_type="E", error_message="boom"),
        ),
    ]
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
        completion_reason=DistributedMapCompletionReason.ALL_COMPLETED,
        success_count=1,
        failure_count=1,
        unprocessed_count=0,
        all=items,
    )
    assert [i.item_id for i in result.succeeded()] == ["0"]
    assert [i.item_id for i in result.failed()] == ["1"]
    assert result.get_results() == [1]
    assert result.get_errors()[0].error_message == "boom"


def test_map_run_details_from_dict_parses_results():
    data = {
        "Status": "SUCCEEDED",
        "CompletionReason": "ALL_COMPLETED",
        "SuccessCount": 1,
        "FailureCount": 1,
        "UnprocessedCount": 0,
        "TotalCount": 2,
        "DistributedMapRunArn": "arn:aws:lambda:us-east-1:123456789012:map-run:x",
        "Results": [
            {"ItemId": "0", "Status": "SUCCEEDED", "Output": 5},
            {
                "ItemId": "1",
                "Status": "FAILED",
                "Error": {"ErrorType": "E", "ErrorMessage": "boom"},
            },
        ],
    }
    details = DistributedMapDetails.from_dict(data)
    assert details.status == "SUCCEEDED"
    assert details.total_count == 2
    assert details.results is not None
    assert details.results[0].item_id == "0"
    assert details.results[1].error is not None
    assert details.results[1].error.type == "E"


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
    assert parsed.source.source_type == "INLINE"
    assert parsed.processor.function_name == "proc"


def test_destination_only_success():
    opts = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.S3.successes("s3://out/ok")
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
                on_failure=DistributedMapDestination.S3.failures("s3://out/bad")
            )
        ),
    )
    dest = opts["Destination"]
    assert "OnFailure" in dest
    assert "OnSuccess" not in dest


def test_s3_objects_source_wire():
    opts = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.objects("s3://b/prefix/"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = opts["Source"]["S3SourceConfig"]
    assert s3["Transform"] == "NONE"
    assert s3["Prefix"] == "prefix/"
    assert "Format" not in s3


def test_s3_flattened_json_lines_source_wire():
    opts = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.flattened_json_lines("s3://b/prefix/"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = opts["Source"]["S3SourceConfig"]
    assert s3["Transform"] == "LOAD_AND_FLATTEN"
    assert s3["Format"] == "JSON_LINES"


# ============================================================================
# Coverage-gap tests (batch 2): validations, wire branches, round-trips
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


# --- Completion config validations ---


def test_completion_count_and_percentage_mutually_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        DistributedMapCompletionConfig(
            tolerated_failure_count=1, tolerated_failure_percentage=5
        )


def test_completion_sample_size_requires_percentage():
    with pytest.raises(ValidationError, match="minimum_sample_size"):
        DistributedMapCompletionConfig(minimum_sample_size=10)


def test_completion_negative_count_rejected():
    with pytest.raises(ValidationError, match="non-negative"):
        DistributedMapCompletionConfig(tolerated_failure_count=-1)


def test_completion_percentage_out_of_range_rejected():
    with pytest.raises(ValidationError, match="between 0 and 100"):
        DistributedMapCompletionConfig(tolerated_failure_percentage=150)


def test_completion_sample_size_below_one_rejected():
    with pytest.raises(ValidationError, match="at least 1"):
        DistributedMapCompletionConfig(
            tolerated_failure_percentage=5, minimum_sample_size=0
        )


def test_completion_failure_count_factory():
    assert DistributedMapCompletionConfig.failure_count(3).tolerated_failure_count == 3


def test_empty_completion_config_omitted_from_wire():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(completion_config=DistributedMapCompletionConfig()),
    )
    assert "CompletionConfig" not in options


# --- Retry duration lower bound ---


def test_retry_duration_below_minimum_rejected():
    with pytest.raises(ValidationError, match="1 minute and 6 hours"):
        ProcessorRetryConfig(max_retry_duration=Duration.from_seconds(30))


# --- Source validations / variants ---


def test_max_items_below_one_rejected():
    with pytest.raises(ValidationError, match="at least 1"):
        DistributedMapSource.S3.json_lines("s3://b/k.jsonl", max_items=0)


def test_csv_requires_object_key():
    with pytest.raises(ValidationError, match="csv requires an S3 object key"):
        DistributedMapSource.S3.csv("s3://bucket")


def test_objects_whole_bucket_prefix_wire():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.objects("s3://bucket"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = options["Source"]["S3SourceConfig"]
    assert s3["Prefix"] == ""
    assert s3["Transform"] == "NONE"
    assert "Key" not in s3


def test_flattened_csv_source_wire():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.flattened_csv(
            "s3://b/prefix", headers=["a", "b"]
        ),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    s3 = options["Source"]["S3SourceConfig"]
    assert s3["Transform"] == "LOAD_AND_FLATTEN"
    assert s3["Format"] == "CSV"
    assert s3["CsvFormatOptions"]["HeaderLocation"] == "GIVEN"
    assert s3["CsvFormatOptions"]["Headers"] == ["a", "b"]


def test_reader_source_without_initial_state_omits_state():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.Reader.from_function("reader"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    reader = options["Source"]["ReaderFunctionSourceConfig"]
    assert reader["FunctionName"] == "reader"
    assert "InitialState" not in reader


def test_inline_serdes_non_json_rejected():
    from aws_durable_execution_sdk_python.serdes import SerDes

    class _BadSerDes(SerDes):
        def serialize(self, value, _serdes_context):  # noqa: ANN001, ANN201, ARG002
            return "{not json"

        def deserialize(self, data, _serdes_context):  # noqa: ANN001, ANN201, ARG002
            return data

    executor = _start_executor(
        DistributedMapSource.inline([1], serdes=_BadSerDes()),
        DistributedMapProcessor.report_batch_outcome("p"),
        DistributedMapConfig(),
    )
    with pytest.raises(ValidationError, match="must produce a JSON value"):
        executor.process()


def test_unsupported_source_type_raises():
    executor = _start_executor(
        DistributedMapSource(source_type="BOGUS"),
        DistributedMapProcessor.report_batch_outcome("p"),
        DistributedMapConfig(),
    )
    with pytest.raises(ExecutionError, match="Unsupported map run source type"):
        executor.process()


# --- Destination include permutations + validation ---


def test_success_destination_include_input_and_owner():
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.S3.successes(
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
                on_failure=DistributedMapDestination.S3.failures(
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


def test_success_destination_all_false_rejected():
    with pytest.raises(ValidationError, match="success destination must include"):
        DistributedMapDestination.S3.successes(
            "s3://out/ok", include_input=False, include_output=False
        )


def test_failure_destination_all_false_rejected():
    with pytest.raises(ValidationError, match="failure destination must include"):
        DistributedMapDestination.S3.failures(
            "s3://out/bad", include_input=False, include_error=False
        )


# --- Unknown backend enum on resume ---


def test_unknown_status_raises_execution_error():
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrx",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="BOGUS",
            completion_reason="ALL_COMPLETED",
            success_count=0,
            failure_count=0,
            unprocessed_count=0,
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    with pytest.raises(ExecutionError, match="Unknown map run status"):
        distributed_map_handler(
            source=["a"],
            processor="p",
            max_concurrency=1,
            state=mock_state,
            operation_identifier=_identifier("mrx"),
        )


# --- Summary / result helpers ---


def test_summary_distributed_map_id_none_without_arn():
    summary = DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        completion_reason=DistributedMapCompletionReason.ALL_COMPLETED,
        success_count=0,
        failure_count=0,
        unprocessed_count=0,
    )
    assert summary.distributed_map_id is None
    assert summary.has_failure is False


def _result(status, completion_reason, failure_count, items):
    return DistributedMapResult(
        status=status,
        completion_reason=completion_reason,
        success_count=len(items) - failure_count,
        failure_count=failure_count,
        unprocessed_count=0,
        all=items,
    )


def test_result_throw_if_error_raises_first_item_error():
    result = _result(
        DistributedMapStatus.SUCCEEDED,
        DistributedMapCompletionReason.ALL_COMPLETED,
        1,
        [
            DistributedMapResultItem(item_id="0", status="SUCCEEDED", output=1),
            DistributedMapResultItem(
                item_id="1",
                status="FAILED",
                error=DistributedMapItemError(error_type="E", error_message="boom"),
            ),
        ],
    )
    with pytest.raises(DistributedMapError, match="E: boom"):
        result.throw_if_error()


def test_result_throw_if_error_names_item_without_detail():
    result = _result(
        DistributedMapStatus.SUCCEEDED,
        DistributedMapCompletionReason.ALL_COMPLETED,
        1,
        [DistributedMapResultItem(item_id="7", status="FAILED", error=None)],
    )
    with pytest.raises(DistributedMapError, match="item 7 failed"):
        result.throw_if_error()


def test_result_throw_if_error_falls_back_to_summary_when_no_items():
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
        completion_reason=DistributedMapCompletionReason.ALL_COMPLETED,
        success_count=0,
        failure_count=2,
        unprocessed_count=0,
        all=[],
    )
    with pytest.raises(DistributedMapError, match="2 item"):
        result.throw_if_error()


def test_result_throw_if_error_run_level_failure():
    result = DistributedMapResult(
        status=DistributedMapStatus.FAILED,
        completion_reason=DistributedMapCompletionReason.FAILURE_TOLERANCE_EXCEEDED,
        success_count=0,
        failure_count=1,
        unprocessed_count=0,
        all=[],
    )
    with pytest.raises(DistributedMapError, match="Map run ended FAILED"):
        result.throw_if_error()


def test_result_throw_if_error_clean_success_does_not_raise():
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
        completion_reason=DistributedMapCompletionReason.ALL_COMPLETED,
        success_count=1,
        failure_count=0,
        unprocessed_count=0,
        all=[DistributedMapResultItem(item_id="0", status="SUCCEEDED", output=1)],
    )
    result.throw_if_error()


# --- Wire round-trips (lambda_service) ---


def test_result_item_wire_round_trip():
    item = DistributedMapResultItemWire.from_dict(
        {"ItemId": "0", "Status": "SUCCEEDED", "Output": {"x": 1}}
    )
    assert item.output == {"x": 1}
    assert item.to_dict() == {"ItemId": "0", "Status": "SUCCEEDED", "Output": {"x": 1}}

    failed = DistributedMapResultItemWire.from_dict(
        {
            "ItemId": "1",
            "Status": "FAILED",
            "Error": {"ErrorType": "E", "ErrorMessage": "boom"},
        }
    )
    dumped = failed.to_dict()
    assert dumped["Error"]["ErrorType"] == "E"
    assert "Output" not in dumped


def test_operation_to_dict_round_trip_preserves_results():
    op = Operation(
        operation_id="opx",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=1,
            failure_count=1,
            unprocessed_count=0,
            total_count=2,
            distributed_map_run_arn="arn:aws:lambda:us-east-1:123456789012:map-run:z",
            completion_details="done",
            results=(
                DistributedMapResultItemWire(item_id="0", status="SUCCEEDED", output=5),
                DistributedMapResultItemWire(
                    item_id="1",
                    status="FAILED",
                    error=ErrorObject(
                        message="boom", type="E", data=None, stack_trace=None
                    ),
                ),
            ),
        ),
    )
    block = op.to_dict()["DistributedMapDetails"]
    assert block["Results"][0] == {"ItemId": "0", "Status": "SUCCEEDED", "Output": 5}
    assert block["DistributedMapRunArn"].endswith("map-run:z")
    assert block["CompletionDetails"] == "done"
    assert block["TotalCount"] == 2

    parsed = Operation.from_dict(op.to_dict())
    assert parsed.distributed_map_details is not None
    assert parsed.distributed_map_details.results[0].output == 5
    assert parsed.distributed_map_details.results[1].error.type == "E"


def test_options_full_round_trip():
    options_dict = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.csv("s3://b/f.csv", headers=["a"]),
        processor=DistributedMapProcessor.report_item_results(
            "proc",
            batch_size=5,
            retry=ProcessorRetryConfig(
                max_retry_attempts=ProcessorRetryConfig.UNLIMITED,
                max_retry_duration=Duration.from_minutes(10),
            ),
            durable_execution_name_prefix="pfx",
        ),
        max_concurrency=3,
        config=DistributedMapConfig(
            destination=DistributedMapDestinationConfig(
                on_success=DistributedMapDestination.S3.successes("s3://o/ok"),
                on_failure=DistributedMapDestination.S3.failures("s3://o/bad"),
            ),
            completion_config=DistributedMapCompletionConfig.failure_count(2),
            timeout=Duration.from_minutes(5),
            collect_results=True,
        ),
    )
    parsed = DistributedMapOptions.from_dict(options_dict)
    assert parsed.max_concurrency == 3
    assert parsed.source.source_type == "S3"
    assert parsed.processor.function_response_types == ("ReportBatchItemResults",)
    assert parsed.processor.max_retry_attempts == -1
    assert parsed.processor.durable_execution_name_prefix == "pfx"
    assert parsed.destination is not None
    assert parsed.completion_config.tolerated_failure_count == 2
    assert parsed.result_collection.mode == "INLINE"
    assert parsed.timeout_seconds == 300
    assert parsed.to_dict()["MaxConcurrency"] == 3


# ============================================================================
# Coverage-gap tests (batch 3): remaining validation and wire branches
# ============================================================================


def test_invalid_s3_uri_rejected():
    with pytest.raises(ValidationError, match="Invalid S3 URI"):
        DistributedMapSource.S3.json_lines("s3:///key.jsonl")


def test_csv_empty_headers_rejected():
    with pytest.raises(ValidationError, match="must be non-empty"):
        DistributedMapSource.S3.csv("s3://b/f.csv", headers=[])


def test_negative_retry_attempts_rejected():
    with pytest.raises(ValidationError, match="non-negative"):
        ProcessorRetryConfig(max_retry_attempts=-5)


def test_batch_size_out_of_range_rejected():
    with pytest.raises(ValidationError, match="between 1 and 10000"):
        DistributedMapProcessor.report_batch_outcome("p", batch_size=0)


def test_csv_first_row_no_headers_wire():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.csv("s3://b/f.csv"),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    csv_opts = options["Source"]["S3SourceConfig"]["CsvFormatOptions"]
    assert csv_opts["HeaderLocation"] == "FIRST_ROW"
    assert "Headers" not in csv_opts
    assert csv_opts["Delimiter"] == "COMMA"


def test_s3_source_expected_bucket_owner_wire():
    options = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.S3.json_lines(
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
                on_success=DistributedMapDestination.S3.successes(
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
                on_failure=DistributedMapDestination.S3.failures(
                    "s3://out/bad", include_input=True, include_error=False
                )
            )
        ),
    )
    assert options["Destination"]["OnFailure"]["Include"] == ["INPUT"]


def test_reader_source_options_round_trip():
    options_dict = _start_options(
        _new_op_state_calls(),
        source=DistributedMapSource.Reader.from_function(
            "reader", initial_state={"page": 0}
        ),
        processor=DistributedMapProcessor.report_batch_outcome("p"),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    parsed = DistributedMapOptions.from_dict(options_dict)
    assert parsed.source.source_type == "READER_FUNCTION"
    assert parsed.source.reader_config["FunctionName"] == "reader"


def test_operation_to_dict_minimal_details_omits_optionals():
    op = Operation(
        operation_id="opm",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
        ),
    )
    block = op.to_dict()["DistributedMapDetails"]
    assert block["Status"] == "SUCCEEDED"
    assert "DistributedMapRunArn" not in block
    assert "CompletionDetails" not in block
    assert "TotalCount" not in block
    assert "Results" not in block


@pytest.mark.parametrize(
    "status",
    [OperationStatus.FAILED, OperationStatus.STOPPED, OperationStatus.TIMED_OUT],
)
def test_operation_level_terminal_failure_raises(status):
    """An operation-level terminal failure raises rather than hanging."""
    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="mrf",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=status,
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    with pytest.raises(DistributedMapError):
        distributed_map_handler(
            source=["a"],
            processor="p",
            max_concurrency=1,
            state=mock_state,
            operation_identifier=_identifier("mrf"),
        )


# ============================================================================
# Coverage-gap tests (batch 4): serdes decode, falsy output, duration-only retry
# ============================================================================


def test_custom_item_serdes_applied_on_decode():
    """A custom item_serdes transforms each per-item output on decode."""
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
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
            results=(
                DistributedMapResultItemWire(
                    item_id="0", status="SUCCEEDED", output="abc"
                ),
            ),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    result = distributed_map_handler(
        source=["a"],
        processor=DistributedMapProcessor.report_item_results("proc"),
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("cs"),
        config=DistributedMapConfig(collect_results=True, item_serdes=_UpperSerDes()),
    )
    assert result.get_results() == ["ABC"]


@pytest.mark.parametrize("value", [0, False, "", [], {}])
def test_falsy_output_round_trips(value):
    """A falsy-but-present output survives wire round-trip and decode (not dropped)."""
    wire = DistributedMapResultItemWire(item_id="0", status="SUCCEEDED", output=value)
    assert wire.to_dict()["Output"] == value

    mock_state = Mock(spec=ExecutionState)
    mock_state.durable_execution_arn = "test_arn"
    operation = Operation(
        operation_id="fo",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=OperationStatus.SUCCEEDED,
        distributed_map_details=DistributedMapDetails(
            status="SUCCEEDED",
            completion_reason="ALL_COMPLETED",
            success_count=1,
            failure_count=0,
            unprocessed_count=0,
            results=(wire,),
        ),
    )
    mock_state.get_checkpoint_result.return_value = (
        CheckpointedResult.create_from_operation(operation)
    )
    result = distributed_map_handler(
        source=["a"],
        processor=DistributedMapProcessor.report_item_results("proc"),
        max_concurrency=1,
        state=mock_state,
        operation_identifier=_identifier("fo"),
        config=DistributedMapConfig(collect_results=True),
    )
    assert result.get_results() == [value]


def test_processor_retry_duration_only():
    """A retry config with only a duration sends the duration and omits attempts."""
    options = _start_options(
        _new_op_state_calls(),
        source=["a"],
        processor=DistributedMapProcessor.report_batch_outcome(
            "proc",
            retry=ProcessorRetryConfig(max_retry_duration=Duration.from_minutes(5)),
        ),
        max_concurrency=1,
        config=DistributedMapConfig(),
    )
    processor = options["Processor"]
    assert processor["MaxRetryDurationSeconds"] == 300
    assert "MaxRetryAttempts" not in processor

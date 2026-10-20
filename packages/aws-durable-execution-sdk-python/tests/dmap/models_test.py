"""Unit tests for the distributed map result types."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python.config import (
    DistributedMapItemStatus,
    DistributedMapStatus,
)
from aws_durable_execution_sdk_python.dmap.models import (
    DistributedMapResult,
    DistributedMapResultItem,
    DistributedMapRunArn,
    DistributedMapSummary,
    _to_distributed_map_status,
)
from aws_durable_execution_sdk_python.exceptions import (
    DistributedMapError,
    DurableOperationError,
    ExecutionError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    DistributedMapDetails,
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
)


def _terminal_operation(
    status: OperationStatus, details: DistributedMapDetails
) -> Operation:
    return Operation(
        operation_id="dmap-id",
        operation_type=OperationType.DISTRIBUTED_MAP,
        status=status,
        distributed_map_details=details,
    )


def _result(status, failure_count, items):
    return DistributedMapResult(
        status=status,
        success_count=len(items) - failure_count,
        failure_count=failure_count,
        unprocessed_count=0,
        all=items,
    )


def test_summary_throw_if_error():
    ok = DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=2,
        failure_count=0,
        unprocessed_count=0,
    )
    ok.throw_if_error()  # no raise

    failed = DistributedMapSummary(
        status=DistributedMapStatus.FAILED,
        success_count=0,
        failure_count=1,
        unprocessed_count=0,
    )
    with pytest.raises(DistributedMapError):
        failed.throw_if_error()

    succeeded_with_failures = DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=1,
        failure_count=1,
        unprocessed_count=0,
    )
    with pytest.raises(DistributedMapError):
        succeeded_with_failures.throw_if_error()


def test_map_run_result_succeeded_failed_filters():
    items = [
        DistributedMapResultItem(
            item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output=1
        ),
        DistributedMapResultItem(
            item_id="1",
            status=DistributedMapItemStatus.FAILED,
            error=ErrorObject(message="boom", type="E", data=None, stack_trace=None),
        ),
    ]
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=1,
        failure_count=1,
        unprocessed_count=0,
        all=items,
    )
    assert [i.item_id for i in result.succeeded()] == ["0"]
    assert [i.item_id for i in result.failed()] == ["1"]
    assert result.get_results() == [1]
    assert result.get_errors()[0].message == "boom"


def test_map_run_result_accessors_keep_items_with_nothing_recorded():
    items = [
        DistributedMapResultItem(
            item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output=None
        ),
        DistributedMapResultItem(
            item_id="1", status=DistributedMapItemStatus.SUCCEEDED, output=1
        ),
        DistributedMapResultItem(
            item_id="2", status=DistributedMapItemStatus.FAILED, error=None
        ),
        DistributedMapResultItem(
            item_id="3",
            status=DistributedMapItemStatus.FAILED,
            error=ErrorObject(message="boom", type="E", data=None, stack_trace=None),
        ),
    ]
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=2,
        failure_count=2,
        unprocessed_count=0,
        all=items,
    )
    # One entry per item of that status, so both lists stay aligned with
    # succeeded() and failed().
    assert result.get_results() == [None, 1]
    assert len(result.get_results()) == len(result.succeeded())
    assert [e.message if e else None for e in result.get_errors()] == [
        None,
        "boom",
    ]
    assert len(result.get_errors()) == len(result.failed())


def test_map_run_result_accessors_omit_an_unknown_item_status():
    """An item of an unrecognized status stays in all and is in no accessor."""
    items = [
        DistributedMapResultItem(
            item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output=1
        ),
        DistributedMapResultItem(
            item_id="1",
            status=DistributedMapItemStatus.UNKNOWN_TO_SDK_VERSION,
            output=2,
        ),
        DistributedMapResultItem(
            item_id="2",
            status=DistributedMapItemStatus.FAILED,
            error=ErrorObject(message="boom", type="E", data=None, stack_trace=None),
        ),
    ]
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=1,
        failure_count=1,
        unprocessed_count=0,
        all=items,
    )
    assert [i.item_id for i in result.all] == ["0", "1", "2"]
    assert [i.item_id for i in result.succeeded()] == ["0"]
    assert [i.item_id for i in result.failed()] == ["2"]
    assert result.get_results() == [1]
    assert [e.message if e else None for e in result.get_errors()] == ["boom"]


def test_summary_has_failure_false_without_failures():
    summary = DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=0,
        failure_count=0,
        unprocessed_count=0,
    )
    assert summary.has_failure is False


_RUN_ARN = (
    "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
    "/durable-execution/exec1/invoke1/distributed-map-run/abc"
)


def _summary_with_arn(arn):
    return DistributedMapSummary(
        status=DistributedMapStatus.SUCCEEDED,
        success_count=0,
        failure_count=0,
        unprocessed_count=0,
        distributed_map_run_arn=arn,
        source_exhausted=True,
    )


def test_run_arn_from_arn_splits_parent_and_run_id():
    parsed = DistributedMapRunArn.from_arn(_RUN_ARN)
    assert parsed is not None
    assert parsed.run_id == "abc"
    assert parsed.durable_execution_arn == (
        "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
        "/durable-execution/exec1/invoke1"
    )


@pytest.mark.parametrize(
    "parent",
    [
        "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
        "/durable-execution/distributed-map-run/invoke1",
        "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
        "/durable-execution/exec1/distributed-map-run",
    ],
)
def test_run_arn_from_arn_splits_on_the_last_separator(parent):
    """A parent whose execution name or invocation id is the separator text still parses."""
    parsed = DistributedMapRunArn.from_arn(parent + "/distributed-map-run/abc")
    assert parsed is not None
    assert parsed.run_id == "abc"
    assert parsed.durable_execution_arn == parent


@pytest.mark.parametrize(
    "arn",
    [
        "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST/durable-execution/exec1/invoke1",
        "arn:aws:states:us-east-1:123456789012:mapRun:sm/abc",
        _RUN_ARN + "/extra",
        "not-an-arn/distributed-map-run/abc",
        "abc",
        "",
    ],
)
def test_run_arn_from_arn_returns_none_for_other_shapes(arn):
    assert DistributedMapRunArn.from_arn(arn) is None


def test_summary_distributed_map_id_parses_run_arn():
    assert _summary_with_arn(_RUN_ARN).distributed_map_id == "abc"


def test_summary_distributed_map_id_none_without_arn():
    assert _summary_with_arn(None).distributed_map_id is None


def test_summary_distributed_map_id_none_for_unparseable_arn():
    assert _summary_with_arn("not-an-arn").distributed_map_id is None


def test_summary_carries_source_exhausted_and_run_error():
    """The terminal details' SourceExhausted and Error reach the summary."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        source_exhausted=False,
        error=ErrorObject(
            message="tolerance exceeded",
            type="DistributedMapFailure",
            data="d",
            stack_trace=["line"],
        ),
        success_count=1,
        failure_count=2,
        unprocessed_count=3,
    )
    summary = DistributedMapSummary.from_operation(
        _terminal_operation(OperationStatus.FAILED, details)
    )
    assert summary.source_exhausted is False
    assert summary.error is not None
    assert summary.error.message == "tolerance exceeded"
    assert summary.error.data == "d"
    assert summary.error.stack_trace == ["line"]


def test_summary_throw_if_error_carries_the_run_error():
    """A thrown run-level error carries every member of the summary's error object."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        source_exhausted=True,
        error=ErrorObject(
            message="boom", type="SourceFailure", data="d", stack_trace=["line"]
        ),
        success_count=0,
        failure_count=1,
        unprocessed_count=0,
    )
    summary = DistributedMapSummary.from_operation(
        _terminal_operation(OperationStatus.FAILED, details)
    )
    with pytest.raises(DistributedMapError, match="Map run ended FAILED: boom") as exc:
        summary.throw_if_error()
    assert exc.value.error_type == "SourceFailure"
    assert exc.value.data == "d"
    assert exc.value.stack_trace == ["line"]


def test_summary_missing_details_names_the_operation_status():
    """The missing-details error reports the status the operation actually ended in."""
    op = _terminal_operation(OperationStatus.FAILED, None)
    with pytest.raises(
        ExecutionError, match="ended FAILED but carried no DistributedMapDetails"
    ):
        DistributedMapSummary.from_operation(op)


def test_summary_missing_source_exhausted_raises_once_a_run_exists():
    """SourceExhausted is required at terminal once a run was created."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        success_count=1,
        failure_count=0,
        unprocessed_count=0,
    )
    op = _terminal_operation(OperationStatus.SUCCEEDED, details)
    with pytest.raises(ExecutionError, match="carried no SourceExhausted"):
        DistributedMapSummary.from_operation(op)


@pytest.mark.parametrize(
    ("omitted", "field_name"),
    [
        ("success_count", "SuccessCount"),
        ("failure_count", "FailureCount"),
        ("unprocessed_count", "UnprocessedCount"),
    ],
)
def test_summary_requires_every_count_at_terminal(omitted, field_name):
    """An omitted count raises and names the count, rather than reading as zero."""
    counts = {"success_count": 2, "failure_count": 1, "unprocessed_count": 0}
    del counts[omitted]
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN, source_exhausted=True, **counts
    )
    op = _terminal_operation(OperationStatus.FAILED, details)
    with pytest.raises(
        ExecutionError, match=f"missing distributed map {field_name} from backend"
    ):
        DistributedMapSummary.from_operation(op)


def test_summary_requires_total_count_when_the_run_succeeded():
    """A SUCCEEDED run counted its whole source, so TotalCount is required."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        source_exhausted=True,
        success_count=2,
        failure_count=0,
        unprocessed_count=0,
    )
    op = _terminal_operation(OperationStatus.SUCCEEDED, details)
    with pytest.raises(
        ExecutionError, match="missing distributed map TotalCount from backend"
    ):
        DistributedMapSummary.from_operation(op)


def test_summary_allows_missing_total_count_when_the_run_did_not_succeed():
    """A run that was stopped may never have counted its source."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        source_exhausted=False,
        success_count=1,
        failure_count=0,
        unprocessed_count=1,
    )
    summary = DistributedMapSummary.from_operation(
        _terminal_operation(OperationStatus.STOPPED, details)
    )
    assert summary.total_count is None
    assert summary.success_count == 1
    assert summary.unprocessed_count == 1


def test_summary_accepts_a_count_of_zero():
    """Zero is a value the service sends, so it must not be read as absent."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        source_exhausted=True,
        success_count=0,
        failure_count=0,
        unprocessed_count=0,
        total_count=0,
    )
    summary = DistributedMapSummary.from_operation(
        _terminal_operation(OperationStatus.SUCCEEDED, details)
    )
    assert summary.success_count == 0
    assert summary.total_count == 0


def test_summary_allows_missing_source_exhausted_before_run_creation():
    """An operation that failed before run creation carries neither ARN nor SourceExhausted."""
    details = DistributedMapDetails(
        error=ErrorObject(
            message="admission denied", type="E", data=None, stack_trace=None
        ),
        success_count=0,
        failure_count=0,
        unprocessed_count=0,
    )
    summary = DistributedMapSummary.from_operation(
        _terminal_operation(OperationStatus.FAILED, details)
    )
    assert summary.distributed_map_run_arn is None
    assert summary.source_exhausted is None
    assert summary.success_count == 0
    assert summary.failure_count == 0
    assert summary.unprocessed_count == 0


def test_result_throw_if_error_raises_first_item_error():
    result = _result(
        DistributedMapStatus.SUCCEEDED,
        1,
        [
            DistributedMapResultItem(
                item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output=1
            ),
            DistributedMapResultItem(
                item_id="1",
                status=DistributedMapItemStatus.FAILED,
                error=ErrorObject(
                    message="boom", type="E", data=None, stack_trace=None
                ),
            ),
        ],
    )
    with pytest.raises(DistributedMapError, match="boom") as exc_info:
        result.throw_if_error()
    assert exc_info.value.error_type == "E"
    assert isinstance(exc_info.value.__cause__, DurableOperationError)


def test_result_throw_if_error_names_item_without_detail():
    result = _result(
        DistributedMapStatus.SUCCEEDED,
        1,
        [
            DistributedMapResultItem(
                item_id="7", status=DistributedMapItemStatus.FAILED, error=None
            )
        ],
    )
    with pytest.raises(DistributedMapError, match="item 7 failed"):
        result.throw_if_error()


def test_result_throw_if_error_falls_back_to_summary_when_no_items():
    result = DistributedMapResult(
        status=DistributedMapStatus.SUCCEEDED,
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
        success_count=1,
        failure_count=0,
        unprocessed_count=0,
        all=[
            DistributedMapResultItem(
                item_id="0", status=DistributedMapItemStatus.SUCCEEDED, output=1
            )
        ],
    )
    result.throw_if_error()


def test_result_from_the_inherited_factory_has_no_items():
    """The inherited summary factory builds a result whose all is empty."""
    details = DistributedMapDetails(
        distributed_map_run_arn=_RUN_ARN,
        source_exhausted=True,
        success_count=0,
        failure_count=0,
        unprocessed_count=0,
        total_count=0,
    )
    result = DistributedMapResult.from_operation(
        _terminal_operation(OperationStatus.SUCCEEDED, details)
    )
    assert isinstance(result, DistributedMapResult)
    assert result.all == []
    assert result.status is DistributedMapStatus.SUCCEEDED


def test_status_derived_from_operation_when_details_omit_status():
    """Status comes from the operation, not from details."""
    details = DistributedMapDetails(
        success_count=3,
        total_count=3,
    )
    op = _terminal_operation(OperationStatus.SUCCEEDED, details)
    assert _to_distributed_map_status(op) is DistributedMapStatus.SUCCEEDED


def test_to_distributed_map_status_rejects_non_terminal():
    """A non-terminal operation status cannot map to a DistributedMapStatus."""
    op = _terminal_operation(
        OperationStatus.STARTED,
        DistributedMapDetails(success_count=0),
    )
    with pytest.raises(ExecutionError, match="Cannot derive distributed map status"):
        _to_distributed_map_status(op)

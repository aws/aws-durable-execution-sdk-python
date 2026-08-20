"""Implement the Durable map run operation."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.concurrency.models import (
    DistributedMapCompletionReason,
    DistributedMapItemError,
    DistributedMapResult,
    DistributedMapResultItem,
    DistributedMapStatus,
    DistributedMapSummary,
)
from aws_durable_execution_sdk_python.config import (
    DistributedMapProcessor,
    DistributedMapSource,
    ProcessorRetryConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    DistributedMapError,
    ExecutionError,
    ValidationError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    DistributedMapCompletionConfigWire,
    DistributedMapDestinationWire,
    DistributedMapOptions,
    DistributedMapProcessorWire,
    DistributedMapResultCollectionWire,
    DistributedMapSourceWire,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.operation.base import (
    CheckResult,
    OperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import (
    DEFAULT_JSON_SERDES,
    deserialize,
    serialize,
)
from aws_durable_execution_sdk_python.suspend import suspend_with_optional_resume_delay

if TYPE_CHECKING:
    from collections.abc import Sequence

    from aws_durable_execution_sdk_python.config import (
        DistributedMapConfig,
        DistributedMapDestinationConfig,
        S3SourceConfig,
    )
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.lambda_service import DistributedMapDetails
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

# The backend uses -1 for unlimited retries, set when a customer passes ProcessorRetryConfig.UNLIMITED.
_UNLIMITED_RETRY_WIRE = -1

# Size limits for the inline item list (1 MB) and the reader's saved state (32 KB).
_INLINE_SIZE_LIMIT = 1024 * 1024
_READER_STATE_LIMIT = 32 * 1024


def _s3_config_to_wire(s3: S3SourceConfig) -> dict[str, Any]:
    """Translate a resolved S3 source config into its wire dict."""
    result: dict[str, Any] = {"Bucket": s3.bucket}
    if s3.key is not None:
        result["Key"] = s3.key
    if s3.prefix is not None:
        result["Prefix"] = s3.prefix
    if s3.transform is not None:
        result["Transform"] = s3.transform
    if s3.expected_bucket_owner is not None:
        result["ExpectedBucketOwner"] = s3.expected_bucket_owner
    if s3.fmt is not None:
        result["Format"] = s3.fmt
    if s3.fmt == "CSV":
        csv_options: dict[str, Any] = {
            "HeaderLocation": "GIVEN" if s3.headers is not None else "FIRST_ROW",
        }
        if s3.headers is not None:
            csv_options["Headers"] = list(s3.headers)
        if s3.delimiter is not None:
            csv_options["Delimiter"] = s3.delimiter
        result["CsvFormatOptions"] = csv_options
    return result


def _inline_items_to_wire(
    items: tuple[Any, ...],
    serdes: Any,
    operation_id: str,
    durable_execution_arn: str,
) -> tuple[Any, ...]:
    """Serialize each inline item to its wire JSON value, enforcing the 1 MB cap."""
    wire_items: list[Any] = []
    for item in items:
        serialized = serialize(
            serdes=serdes,
            value=item,
            operation_id=operation_id,
            durable_execution_arn=durable_execution_arn,
        )
        try:
            wire_items.append(json.loads(serialized))
        except json.JSONDecodeError as e:
            msg = "inline source serdes must produce a JSON value for each item"
            raise ValidationError(msg) from e
    total = len(json.dumps(wire_items, separators=(",", ":")).encode("utf-8"))
    if total > _INLINE_SIZE_LIMIT:
        msg = (
            f"inline source exceeds the {_INLINE_SIZE_LIMIT // 1024 // 1024} MB limit "
            f"(serialized size: {total} bytes)"
        )
        raise ValidationError(msg)
    return tuple(wire_items)


def _source_to_wire(
    source: DistributedMapSource | Sequence[Any],
    operation_id: str,
    durable_execution_arn: str,
) -> DistributedMapSourceWire:
    """Translate a source (typed or plain-list shorthand) into its wire form."""
    if not isinstance(source, DistributedMapSource):
        # A plain list is treated as an inline source with the default serializer.
        wire_items = _inline_items_to_wire(
            tuple(source), DEFAULT_JSON_SERDES, operation_id, durable_execution_arn
        )
        return DistributedMapSourceWire(source_type="INLINE", inline_items=wire_items)

    if source.source_type == "INLINE":
        wire_items = _inline_items_to_wire(
            source.inline_items or (),
            source.inline_serdes or DEFAULT_JSON_SERDES,
            operation_id,
            durable_execution_arn,
        )
        return DistributedMapSourceWire(
            source_type="INLINE",
            inline_items=wire_items,
            max_items=source.max_items,
        )
    if source.source_type == "S3" and source.s3 is not None:
        return DistributedMapSourceWire(
            source_type="S3",
            max_items=source.max_items,
            s3_config=_s3_config_to_wire(source.s3),
        )
    if source.source_type == "READER_FUNCTION" and source.reader is not None:
        reader_config: dict[str, Any] = {"FunctionName": source.reader.function_name}
        if source.reader.initial_state is not None:
            state = serialize(
                serdes=source.reader.state_serdes or DEFAULT_JSON_SERDES,
                value=source.reader.initial_state,
                operation_id=operation_id,
                durable_execution_arn=durable_execution_arn,
            )
            if len(state.encode("utf-8")) > _READER_STATE_LIMIT:
                msg = (
                    f"reader initial_state exceeds the "
                    f"{_READER_STATE_LIMIT // 1024} KB limit"
                )
                raise ValidationError(msg)
            reader_config["InitialState"] = state
        return DistributedMapSourceWire(
            source_type="READER_FUNCTION",
            max_items=source.max_items,
            reader_config=reader_config,
        )
    msg = f"Unsupported map run source type: {source.source_type}"
    raise ExecutionError(msg)


def _processor_to_wire(
    processor: DistributedMapProcessor,
) -> DistributedMapProcessorWire:
    """Translate a processor config into its wire form, mapping unlimited to -1."""
    response_types = (
        (processor.response_mode,) if processor.response_mode is not None else None
    )

    max_retry_attempts: int | None = None
    max_retry_duration_seconds: int | None = None
    if processor.retry is not None:
        attempts = processor.retry.max_retry_attempts
        if attempts == ProcessorRetryConfig.UNLIMITED:
            max_retry_attempts = _UNLIMITED_RETRY_WIRE
        elif isinstance(attempts, int):
            max_retry_attempts = attempts
        if processor.retry.max_retry_duration is not None:
            max_retry_duration_seconds = processor.retry.max_retry_duration.to_seconds()

    return DistributedMapProcessorWire(
        function_name=processor.function_name,
        function_response_types=response_types,
        batch_size=processor.batch_size,
        max_retry_attempts=max_retry_attempts,
        max_retry_duration_seconds=max_retry_duration_seconds,
        durable_execution_name_prefix=processor.durable_execution_name_prefix,
    )


def _completion_to_wire(
    config: DistributedMapConfig,
) -> DistributedMapCompletionConfigWire | None:
    completion = config.completion_config
    if completion is None or (
        completion.tolerated_failure_count is None
        and completion.tolerated_failure_percentage is None
        and completion.minimum_sample_size is None
    ):
        return None
    return DistributedMapCompletionConfigWire(
        tolerated_failure_count=completion.tolerated_failure_count,
        tolerated_failure_percentage=completion.tolerated_failure_percentage,
        minimum_sample_size=completion.minimum_sample_size,
    )


def _destination_to_wire(
    destination: DistributedMapDestinationConfig | None,
) -> DistributedMapDestinationWire | None:
    if destination is None:
        return None
    on_success: dict[str, Any] | None = None
    on_failure: dict[str, Any] | None = None
    if destination.on_success is not None:
        s = destination.on_success
        include: list[str] = []
        if s.include_input:
            include.append("INPUT")
        if s.include_output:
            include.append("OUTPUT")
        s3_config: dict[str, Any] = {"Bucket": s.bucket, "Prefix": s.prefix}
        if s.expected_bucket_owner is not None:
            s3_config["ExpectedBucketOwner"] = s.expected_bucket_owner
        on_success = {
            "Type": "S3",
            "Include": include,
            "S3DestinationConfig": s3_config,
        }
    if destination.on_failure is not None:
        f = destination.on_failure
        f_include: list[str] = []
        if f.include_input:
            f_include.append("INPUT")
        if f.include_error:
            f_include.append("ERROR")
        f_s3_config: dict[str, Any] = {"Bucket": f.bucket, "Prefix": f.prefix}
        if f.expected_bucket_owner is not None:
            f_s3_config["ExpectedBucketOwner"] = f.expected_bucket_owner
        on_failure = {
            "Type": "S3",
            "Include": f_include,
            "S3DestinationConfig": f_s3_config,
        }
    if on_success is None and on_failure is None:
        return None
    return DistributedMapDestinationWire(on_success=on_success, on_failure=on_failure)


def _build_distributed_map_options(
    source: DistributedMapSource | Sequence[Any],
    processor: DistributedMapProcessor,
    max_concurrency: int,
    config: DistributedMapConfig,
    operation_id: str,
    durable_execution_arn: str,
) -> DistributedMapOptions:
    """Assemble the wire options payload from the operands and config."""
    result_collection = (
        DistributedMapResultCollectionWire(mode="INLINE")
        if config.collect_results
        else None
    )
    return DistributedMapOptions(
        max_concurrency=max_concurrency,
        source=_source_to_wire(source, operation_id, durable_execution_arn),
        processor=_processor_to_wire(processor),
        destination=_destination_to_wire(config.destination),
        completion_config=_completion_to_wire(config),
        result_collection=result_collection,
        timeout_seconds=config.timeout.to_seconds()
        if config.timeout is not None
        else None,
    )


def _summary_fields(details: DistributedMapDetails) -> dict[str, Any]:
    """Shared summary fields extracted from the terminal details block."""
    try:
        status = DistributedMapStatus(details.status)
        completion_reason = DistributedMapCompletionReason(details.completion_reason)
    except ValueError as e:
        msg = (
            f"Unknown map run status or completion reason from the backend "
            f"({details.status!r}, {details.completion_reason!r})"
        )
        raise ExecutionError(msg) from e
    return {
        "status": status,
        "completion_reason": completion_reason,
        "success_count": details.success_count,
        "failure_count": details.failure_count,
        "unprocessed_count": details.unprocessed_count,
        "distributed_map_run_arn": details.distributed_map_run_arn,
        "completion_details": details.completion_details,
        "total_count": details.total_count,
    }


class DistributedMapOperationExecutor(OperationExecutor[DistributedMapSummary]):
    """Executor for map run operations.

    Creates the START checkpoint if none exists, then suspends until the
    backend completes the run and re-invokes the parent. On resume, a
    ``DistributedMapSummary`` (or ``DistributedMapResult`` when result collection is enabled)
    is built from the checkpointed ``DistributedMapDetails``.
    """

    def __init__(
        self,
        source: DistributedMapSource | Sequence[Any],
        processor: DistributedMapProcessor,
        max_concurrency: int,
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
        config: DistributedMapConfig,
    ):
        """Initialize the map run operation executor.

        Args:
            source: The items to process (typed source or plain-list shorthand)
            processor: The processor configuration
            max_concurrency: Maximum concurrent processor invocations
            state: The execution state
            operation_identifier: The operation identifier
            config: Configuration for the map run operation
        """
        self.source = source
        self.processor = processor
        self.max_concurrency = max_concurrency
        self.state = state
        self.operation_identifier = operation_identifier
        self.config = config

    def _resolve_summary(
        self, details: DistributedMapDetails | None
    ) -> DistributedMapSummary:
        """Reconstruct the resolved summary/result from the terminal details."""
        if details is None:
            msg = "DISTRIBUTED_MAP operation succeeded but carried no DistributedMapDetails"
            raise ExecutionError(msg)
        fields = _summary_fields(details)
        if not self.config.collect_results:
            return DistributedMapSummary(**fields)
        return DistributedMapResult(**fields, all=self._deserialize_items(details))

    def _deserialize_items(
        self, details: DistributedMapDetails
    ) -> list[DistributedMapResultItem]:
        """Deserialize the per-item wire results into customer result items."""
        items: list[DistributedMapResultItem] = []
        for wire in details.results or ():
            output: Any | None = None
            if wire.output is not None:
                # Output is already a JSON value; re-dump to text for the serdes.
                output = deserialize(
                    serdes=self.config.item_serdes or DEFAULT_JSON_SERDES,
                    data=json.dumps(wire.output),
                    operation_id=self.operation_identifier.operation_id,
                    durable_execution_arn=self.state.durable_execution_arn,
                )
            error = (
                DistributedMapItemError(
                    error_type=wire.error.type or "",
                    error_message=wire.error.message or "",
                )
                if wire.error is not None
                else None
            )
            items.append(
                DistributedMapResultItem(
                    item_id=wire.item_id,
                    status=wire.status,
                    output=output,
                    error=error,
                )
            )
        return items

    def check_result_status(self) -> CheckResult[DistributedMapSummary]:
        """Check operation status and create the START checkpoint if needed.

        Called twice by process() when creating synchronous checkpoints: once before
        and once after, to detect if the operation completed immediately.

        Returns:
            CheckResult indicating the next action to take

        Raises:
            SuspendExecution: For STARTED operations waiting for completion
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Terminal success - build the summary/result from DistributedMapDetails
        if checkpointed_result.is_succeeded():
            operation = checkpointed_result.operation
            summary = self._resolve_summary(
                operation.distributed_map_details if operation else None
            )
            return CheckResult.create_completed(summary)

        # Operation-level terminal failure
        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            msg = (
                f"Distributed map operation "
                f"'{self.operation_identifier.name or self.operation_identifier.operation_id}' "
                f"ended with status "
                f"{checkpointed_result.status.value if checkpointed_result.status else 'UNKNOWN'}"
            )
            checkpointed_result.raise_operation_error(DistributedMapError, msg=msg)

        # Started - ready to suspend
        if checkpointed_result.is_started():
            logger.debug(
                "⏳ Map run %s still in progress, will suspend",
                self.operation_identifier.name
                or self.operation_identifier.operation_id,
            )
            return CheckResult.create_is_ready_to_execute(checkpointed_result)

        # Create START checkpoint if not exists
        if not checkpointed_result.is_existent():
            start_operation: OperationUpdate = (
                OperationUpdate.create_distributed_map_start(
                    identifier=self.operation_identifier,
                    distributed_map_options=_build_distributed_map_options(
                        source=self.source,
                        processor=self.processor,
                        max_concurrency=self.max_concurrency,
                        config=self.config,
                        operation_id=self.operation_identifier.operation_id,
                        durable_execution_arn=self.state.durable_execution_arn,
                    ),
                )
            )
            # Checkpoint map run START with blocking (is_sync=True).
            # Must ensure the map run is recorded before suspending execution.
            self.state.create_checkpoint(operation_update=start_operation, is_sync=True)

            logger.debug(
                "🚀 Map run %s started, will check for immediate completion",
                self.operation_identifier.name
                or self.operation_identifier.operation_id,
            )

            # Signal to process() that checkpoint was created - to recheck status
            # for immediate completion before proceeding.
            return CheckResult.create_started()

        # Ready to suspend (checkpoint exists but not in a terminal or started state)
        return CheckResult.create_is_ready_to_execute(checkpointed_result)

    def execute(
        self, _checkpointed_result: CheckpointedResult
    ) -> DistributedMapSummary:
        """Execute map run operation by suspending to wait for async completion.

        The map run operation doesn't execute synchronously - it suspends and
        the backend runs the map run asynchronously.

        Args:
            checkpointed_result: The checkpoint data (unused, but required by interface)

        Returns:
            Never returns - always suspends

        Raises:
            Always suspends via suspend_with_optional_resume_delay
            ExecutionError: If suspend doesn't raise (should never happen)
        """
        msg: str = f"Map run {self.operation_identifier.operation_id} started, suspending for completion"
        suspend_with_optional_resume_delay(msg)
        # This line should never be reached since suspend_with_optional_resume_delay always raises
        error_msg: str = "suspend_with_optional_resume_delay should have raised an exception, but did not."
        raise ExecutionError(error_msg) from None

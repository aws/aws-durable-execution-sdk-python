"""Implement the Durable map run operation."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import (
    DistributedMapResultConfig,
    DistributedMapSource,
)
from aws_durable_execution_sdk_python.constants import READER_STATE_LIMIT_BYTES
from aws_durable_execution_sdk_python.dmap.models import (
    DistributedMapResult,
    DistributedMapResultItem,
    DistributedMapSummary,
)
from aws_durable_execution_sdk_python.exceptions import (
    ExecutionError,
    ValidationError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    DistributedMapCompletionConfig,
    DistributedMapDestinationConfig,
    DistributedMapOptions,
    DistributedMapProcessorConfig,
    DistributedMapResultCollectionConfig,
    DistributedMapResultCollectionMode,
    DistributedMapSourceConfig,
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
        DistributedMapProcessor,
        ReaderSource,
    )
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.lambda_service import (
        DistributedMapDetails,
        Operation,
    )
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

logger = logging.getLogger(__name__)

# Size limit for the inline item list (1 MB). The reader's state limit is shared
# with the reader wrapper, so it lives in constants.
_INLINE_SIZE_LIMIT = 1024 * 1024


class DistributedMapOperationExecutor(OperationExecutor[DistributedMapSummary]):
    """Executor for map run operations.

    Creates the START checkpoint if none exists, then suspends until the
    backend completes the run and re-invokes the parent. On resume, a
    ``DistributedMapSummary`` (or ``DistributedMapResult`` when result collection is enabled)
    is built from the checkpointed ``DistributedMapDetails``.
    """

    def __init__(
        self,
        source: DistributedMapSource | list[Any] | tuple[Any, ...],
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

    def _serialize(self, serdes: SerDes | None, value: Any) -> str:
        """Serialize a value with this operation's id and execution ARN."""
        return serialize(
            serdes=serdes or DEFAULT_JSON_SERDES,
            value=value,
            operation_id=self.operation_identifier.operation_id,
            durable_execution_arn=self.state.durable_execution_arn,
        )

    def _serialize_inline_items(
        self, items: Sequence[Any], serdes: SerDes | None
    ) -> list[str]:
        """Serialize each inline item, enforcing the 1 MB cap on the whole list."""
        serialized_items = [self._serialize(serdes, item) for item in items]
        # The service measures the items as one compact UTF-8 JSON array.
        # ensure_ascii=False matters: the default escapes each non-ASCII
        # character as \\uXXXX, counting 6 bytes where UTF-8 counts 2 to 4, so a
        # custom serdes writing raw non-ASCII would be rejected under the limit.
        items_json = json.dumps(
            serialized_items, separators=(",", ":"), ensure_ascii=False
        )
        total = len(items_json.encode("utf-8"))
        if total > _INLINE_SIZE_LIMIT:
            msg = (
                f"inline source exceeds the {_INLINE_SIZE_LIMIT // 1024 // 1024} MB limit "
                f"(serialized size: {total} bytes)"
            )
            raise ValidationError(msg)
        return serialized_items

    def _reader_initial_state(self, reader: ReaderSource) -> str | None:
        """Serialize the reader's initial state, enforcing the 32 KB cap."""
        if reader.initial_state is None:
            return None
        state = self._serialize(reader.state_serdes, reader.initial_state)
        if len(state.encode("utf-8")) > READER_STATE_LIMIT_BYTES:
            msg = (
                f"reader initial_state exceeds the "
                f"{READER_STATE_LIMIT_BYTES // 1024} KB limit"
            )
            raise ValidationError(msg)
        return state

    def _build_source_config(self) -> DistributedMapSourceConfig:
        """Translate the source (typed or plain-list shorthand) into its source config."""
        source = self.source
        if not isinstance(source, DistributedMapSource):
            # A plain list is treated as an inline source with the default serializer.
            return DistributedMapSourceConfig.create_inline(
                self._serialize_inline_items(source, None)
            )
        if source.inline_items is not None:
            return DistributedMapSourceConfig.create_inline(
                self._serialize_inline_items(source.inline_items, source.inline_serdes),
                max_items=source.max_items,
            )
        if source.s3 is not None:
            return DistributedMapSourceConfig.create_s3(
                source.s3, max_items=source.max_items
            )
        if source.reader is not None:
            return DistributedMapSourceConfig.create_reader(
                source.reader.function_name,
                self._reader_initial_state(source.reader),
                max_items=source.max_items,
            )
        msg = "Distributed map source has no configured items"
        raise ExecutionError(msg)

    def _build_options(self) -> DistributedMapOptions:
        """Assemble the DistributedMapOptions payload from the operands and config."""
        config = self.config
        return DistributedMapOptions(
            max_concurrency=self.max_concurrency,
            source=self._build_source_config(),
            processor=DistributedMapProcessorConfig.from_processor(self.processor),
            destination=(
                DistributedMapDestinationConfig.from_config(config.destination)
                if config.destination is not None
                else None
            ),
            completion_config=(
                DistributedMapCompletionConfig.from_config(config.completion_config)
                if config.completion_config is not None
                else None
            ),
            result_collection=(
                DistributedMapResultCollectionConfig(
                    mode=DistributedMapResultCollectionMode.INLINE
                )
                if isinstance(config, DistributedMapResultConfig)
                else None
            ),
            timeout_seconds=config.timeout.to_seconds()
            if config.timeout is not None
            else None,
        )

    def _resolve_summary(self, operation: Operation | None) -> DistributedMapSummary:
        """Reconstruct the resolved summary/result from the terminal operation."""
        if not isinstance(self.config, DistributedMapResultConfig):
            return DistributedMapSummary.from_operation(operation)
        details = operation.distributed_map_details if operation else None
        items = (
            self._deserialize_items(details, self.config.result_serdes)
            if details is not None
            else []
        )
        return DistributedMapResult.from_operation_and_items(operation, items)

    def _deserialize_items(
        self, details: DistributedMapDetails, result_serdes: SerDes | None
    ) -> list[DistributedMapResultItem]:
        """Deserialize the service result items into customer result items."""
        items: list[DistributedMapResultItem] = []
        for entry in details.results or ():
            output: Any | None = None
            if entry.output is not None:
                output = deserialize(
                    serdes=result_serdes or DEFAULT_JSON_SERDES,
                    data=entry.output,
                    operation_id=self.operation_identifier.operation_id,
                    durable_execution_arn=self.state.durable_execution_arn,
                )
            items.append(
                DistributedMapResultItem(
                    item_id=entry.item_id,
                    status=entry.status,
                    output=output,
                    error=entry.error,
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
        checkpointed_result: CheckpointedResult = self._get_checkpoint_result()

        # Terminal success - build the summary/result from the operation
        if checkpointed_result.is_succeeded():
            operation = checkpointed_result.operation
            summary = self._resolve_summary(operation)
            return CheckResult.create_completed(summary)

        # Operation-level terminal failure. Every terminal state resolves with the
        # summary, and throw_if_error opts into raising.
        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            operation = checkpointed_result.operation
            if operation is not None and operation.distributed_map_details is not None:
                return CheckResult.create_completed(self._resolve_summary(operation))
            status_value = (
                checkpointed_result.status.value
                if checkpointed_result.status
                else "UNKNOWN"
            )
            msg = (
                f"DISTRIBUTED_MAP operation ended {status_value} but carried no "
                f"DistributedMapDetails"
            )
            raise ExecutionError(msg)

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
                    distributed_map_options=self._build_options(),
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

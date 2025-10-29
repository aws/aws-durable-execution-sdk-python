"""Implementation for Durable Parallel operation."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.concurrency import ConcurrentExecutor, Executable
from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.lambda_service import OperationSubType

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.concurrency import BatchResult
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import ExecutionState
    from aws_durable_execution_sdk_python.types import SummaryGenerator

logger = logging.getLogger(__name__)

# Result type
R = TypeVar("R")


class ParallelExecutor(ConcurrentExecutor[Callable, R]):
    def __init__(
        self,
        executables: list[Executable[Callable]],
        max_concurrency: int | None,
        completion_config,
        top_level_sub_type: OperationSubType,
        iteration_sub_type: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        summary_generator: SummaryGenerator | None = None,
        item_serdes: SerDes | None = None,
    ):
        super().__init__(
            executables=executables,
            max_concurrency=max_concurrency,
            completion_config=completion_config,
            sub_type_top=top_level_sub_type,
            sub_type_iteration=iteration_sub_type,
            name_prefix=name_prefix,
            serdes=serdes,
            summary_generator=summary_generator,
            item_serdes=item_serdes,
        )

    @classmethod
    def from_callables(
        cls,
        callables: Sequence[Callable],
        config: ParallelConfig,
    ) -> ParallelExecutor:
        """Create ParallelExecutor from a sequence of callables."""
        executables: list[Executable[Callable]] = [
            Executable(index=i, func=func) for i, func in enumerate(callables)
        ]
        return cls(
            executables=executables,
            max_concurrency=config.max_concurrency,
            completion_config=config.completion_config,
            top_level_sub_type=OperationSubType.PARALLEL,
            iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
            name_prefix="parallel-branch-",
            serdes=config.serdes,
            summary_generator=config.summary_generator,
        )

    def execute_item(self, child_context, executable: Executable[Callable]) -> R:  # noqa: PLR6301
        logger.debug("🔀 Processing parallel branch: %s", executable.index)
        result: R = executable.func(child_context)
        logger.debug("✅ Processed parallel branch: %s", executable.index)
        return result


def parallel_handler(
    callables: Sequence[Callable],
    config: ParallelConfig | None,
    execution_state: ExecutionState,
    parallel_context: DurableContext,
    operation_identifier: OperationIdentifier,
) -> BatchResult[R]:
    """Execute multiple operations in parallel."""
    # Summary Generator Construction (matches TypeScript implementation):
    # Construct the summary generator at the handler level, just like TypeScript does in parallel-handler.ts.
    # This matches the pattern where handlers are responsible for configuring operation-specific behavior.
    #
    # See TypeScript reference: aws-durable-execution-sdk-js/src/handlers/parallel-handler/parallel-handler.ts (~line 112)

    executor = ParallelExecutor.from_callables(
        callables,
        config or ParallelConfig(summary_generator=ParallelSummaryGenerator()),
    )

    checkpoint = execution_state.get_checkpoint_result(
        operation_identifier.operation_id
    )
    if checkpoint.is_succeeded():
        return executor.replay(execution_state, parallel_context)
    return executor.execute(execution_state, executor_context=parallel_context)


class ParallelSummaryGenerator:
    def __call__(self, result: BatchResult) -> str:
        fields = {
            "totalCount": result.total_count,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "completionReason": result.completion_reason.value,
            "status": result.status.value,
            "startedCount": result.started_count,
            "type": "ParallelResult",
        }

        return json.dumps(fields)

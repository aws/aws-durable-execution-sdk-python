"""AWS Lambda Durable Executions Python SDK."""

# Package metadata
from aws_durable_execution_sdk_python.__about__ import __version__

# Main context - used in every durable function
# Helper decorators - commonly used for step functions
# Concurrency
from aws_durable_execution_sdk_python.concurrency.models import (
    BatchCompletionError,
    BatchResult,
)
from aws_durable_execution_sdk_python.config import (
    BatchItemStatus,
    CompletionDecision,
    CompletionItemStatus,
    CompletionOutcome,
    CompletionStatus,
    DistributedMapItemStatus,
    DistributedMapStatus,
    ParallelBranch,
    complete_batch,
    continue_batch,
)
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_parallel_branch,
    durable_step,
    durable_wait_for_callback,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.dmap.handlers import (
    distributed_map_batch_handler,
    distributed_map_item_handler,
    distributed_map_reader,
    durable_distributed_map_batch_handler,
    durable_distributed_map_item_handler,
)
from aws_durable_execution_sdk_python.dmap.models import (
    DistributedMapResult,
    DistributedMapResultItem,
    DistributedMapSummary,
)

# Most common exceptions - users need to handle these exceptions
from aws_durable_execution_sdk_python.exceptions import (
    CallbackError,
    CallbackExternalError,
    CallbackSubmitterError,
    CallbackTimeoutError,
    ChildContextError,
    DistributedMapError,
    DurableExecutionsError,
    DurableOperationError,
    ExecutionError,
    InvocationError,
    InvokeError,
    PluginLoadError,
    RetryableSerDesError,
    SerDesError,
    StepError,
    ValidationError,
    WaitForConditionError,
)

# Core decorator - used in every durable function
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import WithRetryConfig, with_retry

# Essential context types - passed to user functions
from aws_durable_execution_sdk_python.types import StepContext


__all__ = [
    "BatchCompletionError",
    "BatchItemStatus",
    "BatchResult",
    "CallbackError",
    "CallbackExternalError",
    "CallbackSubmitterError",
    "CallbackTimeoutError",
    "ChildContextError",
    "CompletionDecision",
    "CompletionItemStatus",
    "CompletionOutcome",
    "CompletionStatus",
    "DistributedMapError",
    "DistributedMapItemStatus",
    "DistributedMapResult",
    "DistributedMapResultItem",
    "DistributedMapStatus",
    "DistributedMapSummary",
    "DurableContext",
    "DurableExecutionsError",
    "DurableOperationError",
    "ExecutionError",
    "InvocationError",
    "InvokeError",
    "ParallelBranch",
    "PluginLoadError",
    "RetryableSerDesError",
    "SerDesError",
    "StepContext",
    "StepError",
    "ValidationError",
    "WaitForConditionError",
    "WithRetryConfig",
    "__version__",
    "complete_batch",
    "continue_batch",
    "distributed_map_batch_handler",
    "distributed_map_item_handler",
    "distributed_map_reader",
    "durable_distributed_map_batch_handler",
    "durable_distributed_map_item_handler",
    "durable_execution",
    "durable_parallel_branch",
    "durable_step",
    "durable_wait_for_callback",
    "durable_with_child_context",
    "with_retry",
]

import contextlib
import datetime
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, MutableMapping

from aws_durable_execution_sdk_python.lambda_service import (
    OperationType,
    OperationStatus,
    OperationAction,
    OperationSubType,
    ErrorObject,
    InvocationStatus,
    Operation,
    OperationUpdate,
    DurableExecutionInvocationOutput,
)
from aws_durable_execution_sdk_python.types import LambdaContext


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OperationStartInfo:
    operation_id: str
    operation_type: OperationType
    sub_type: OperationSubType | None = None
    name: str | None = None
    parent_id: str | None = None
    start_timestamp: datetime.datetime | None = None


@dataclass(frozen=True)
class OperationEndInfo(OperationStartInfo):
    status: OperationStatus = OperationStatus.SUCCEEDED
    end_timestamp: datetime.datetime | None = None
    attempt: int | None = None
    error: ErrorObject | None = None


@dataclass(frozen=True)
class AttemptStartInfo(OperationStartInfo):
    attempt: int = 1


@dataclass(frozen=True)
class AttemptEndInfo(AttemptStartInfo):
    succeeded: bool | None = None
    end_timestamp: datetime.datetime | None = None
    error: ErrorObject | None = None
    next_attempt_delay_seconds: int | None = None


@dataclass(frozen=True)
class InvocationStartInfo:
    request_id: str | None
    execution_arn: str | None
    start_timestamp: datetime.datetime | None


@dataclass(frozen=True)
class InvocationEndInfo(InvocationStartInfo):
    status: InvocationStatus = InvocationStatus.SUCCEEDED
    end_timestamp: datetime.datetime | None = None
    error: ErrorObject | None = None


@dataclass(frozen=True)
class ExecutionStartInfo(InvocationStartInfo):
    pass


@dataclass(frozen=True)
class ExecutionEndInfo(ExecutionStartInfo):
    status: InvocationStatus = InvocationStatus.SUCCEEDED
    end_timestamp: datetime.datetime | None = None
    error: ErrorObject | None = None


class DurableExecutionPlugin:
    """Base class for plugins. Override only the methods you need."""

    def on_execution_start(self, info: ExecutionStartInfo) -> None:
        pass

    def on_execution_end(self, info: ExecutionEndInfo) -> None:
        pass

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        pass

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        pass

    def on_operation_start(self, info: OperationStartInfo) -> None:
        pass

    def on_operation_end(self, info: OperationEndInfo) -> None:
        pass

    def on_operation_attempt_start(self, info: AttemptStartInfo) -> None:
        pass

    def on_operation_attempt_end(self, info: AttemptEndInfo) -> None:
        pass

    # Todo: further discussions required to finalize the following interface
    # def enrich_log_context(self, info: OperationStartInfo | None) -> Dict[str, Any] | None: pass


class PluginExecutor:
    def __init__(self, plugins: list[DurableExecutionPlugin] | None):
        self._plugins = plugins or []
        self._execution_operation: Operation | None = None
        self._durable_execution_arn: str | None = None
        self._aws_request_id: str | None = None
        self._executor: ThreadPoolExecutor | None = None

    @contextlib.contextmanager
    def run(self):
        if self._plugins:
            self._executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="plugin-executor",
            )
        try:
            yield
        finally:
            # Shut down the thread pool, waiting for pending tasks to complete.
            if self._executor:
                self._executor.shutdown(wait=True)

    @staticmethod
    def _dispatch_plugin(plugin: DurableExecutionPlugin, info) -> None:
        """Invoke the appropriate plugin callback. Runs inside the thread pool."""
        try:
            match info:
                case ExecutionEndInfo():
                    plugin.on_execution_end(info)
                case InvocationEndInfo():
                    plugin.on_invocation_end(info)
                case ExecutionStartInfo():
                    plugin.on_execution_start(info)
                case InvocationStartInfo():
                    plugin.on_invocation_start(info)
                case AttemptEndInfo():
                    plugin.on_operation_attempt_end(info)
                case OperationEndInfo():
                    plugin.on_operation_end(info)
                case AttemptStartInfo():
                    plugin.on_operation_attempt_start(info)
                case OperationStartInfo():
                    plugin.on_operation_start(info)
                case _:
                    raise ValueError(f"Unknown info type: {type(info)}")
        except Exception:
            # log and ignore the exception
            logger.exception("Plugin %s exception ignored", plugin.__class__.__name__)

    def execute_plugins(self, info):
        if not self._executor:
            return
        for plugin in self._plugins:
            self._executor.submit(self._dispatch_plugin, plugin, info)

    def on_invocation_start(
        self,
        durable_execution_arn: str,
        context: LambdaContext | None,
        execution_operation: Operation | None,
        is_replaying: bool,
    ) -> None:
        self._durable_execution_arn = durable_execution_arn
        self._execution_operation = execution_operation
        self._aws_request_id = context.aws_request_id if context else None
        start_timestamp = (
            execution_operation.start_timestamp if execution_operation else None
        )

        if not is_replaying:
            self.execute_plugins(
                ExecutionStartInfo(
                    request_id=self._aws_request_id,
                    execution_arn=durable_execution_arn,
                    start_timestamp=start_timestamp,
                )
            )

        self.execute_plugins(
            InvocationStartInfo(
                request_id=self._aws_request_id,
                execution_arn=durable_execution_arn,
                start_timestamp=start_timestamp,
            )
        )

    def on_invocation_end(
        self,
        output: "DurableExecutionInvocationOutput",
    ) -> None:
        start_timestamp = (
            self._execution_operation.start_timestamp
            if self._execution_operation
            else None
        )
        # the actual end timestamp may be unknown because it's not checkpointed yet
        end_timestamp: datetime.datetime = (
            self._execution_operation.end_timestamp
            if self._execution_operation
            else None
        ) or datetime.datetime.now(datetime.UTC)

        self.execute_plugins(
            InvocationEndInfo(
                request_id=self._aws_request_id,
                execution_arn=self._durable_execution_arn,
                start_timestamp=start_timestamp,
                status=output.status,
                end_timestamp=end_timestamp,
                error=output.error,
            )
        )

        if output.status in [InvocationStatus.SUCCEEDED, InvocationStatus.FAILED]:
            self.execute_plugins(
                ExecutionEndInfo(
                    request_id=self._aws_request_id,
                    execution_arn=self._durable_execution_arn,
                    start_timestamp=start_timestamp,
                    status=output.status,
                    end_timestamp=end_timestamp,
                    error=output.error,
                )
            )

    def on_operation_action(self, operation: Operation | None, update: OperationUpdate):
        """Execute any registered plugins for a given operation before it is updated.

        Args:
            operation: the operation after update
            update: the operation update that is checkpointed
        """
        if update.action is not OperationAction.START:
            return

        self.execute_plugins(
            OperationStartInfo(
                operation_id=update.operation_id,
                operation_type=update.operation_type,
                sub_type=update.sub_type,
                name=update.name,
                parent_id=update.parent_id,
                start_timestamp=datetime.datetime.now(datetime.UTC),
            )
        )

        if update.operation_type is OperationType.STEP:
            attempt = (
                operation.step_details.attempt
                if operation and operation.step_details
                else 1
            )
            self.execute_plugins(
                AttemptStartInfo(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    sub_type=update.sub_type,
                    name=update.name,
                    parent_id=update.parent_id,
                    start_timestamp=datetime.datetime.now(datetime.UTC),
                    attempt=attempt,
                )
            )

    def on_operation_update(self, operation):
        """Execute any registered plugins for a given operation after it is updated.

        Updates such as STARTED might be omitted because START and completion action (e.g. SUCCEED/FAIL) may be
        checkpointed in batch and the backend returns only the terminal status (e.g. SUCCEEDED/PENDING/FAILED).

        Args:
            operation: the operation is just checkpointed
        """
        params = dict(
            operation_id=operation.operation_id,
            operation_type=operation.operation_type,
            sub_type=operation.sub_type,
            name=operation.name,
            parent_id=operation.parent_id,
            start_timestamp=operation.start_timestamp,
        )
        if operation.step_details and (
            self._is_terminal_status(operation.status)
            # PENDING in addition to terminal status
            or operation.status is OperationStatus.PENDING
        ):
            self.execute_plugins(
                AttemptEndInfo(
                    **params,
                    end_timestamp=operation.end_timestamp,
                    attempt=operation.step_details.attempt,
                    succeeded=operation.status is OperationStatus.SUCCEEDED,
                    error=operation.step_details.error,
                    next_attempt_delay_seconds=operation.step_details.next_attempt_timestamp,
                )
            )

        if self._is_terminal_status(operation.status):
            attempt = operation.step_details.attempt if operation.step_details else None
            self.execute_plugins(
                OperationEndInfo(
                    **params,
                    end_timestamp=operation.end_timestamp,
                    status=operation.status,
                    error=self._extract_error(operation),
                    attempt=attempt,
                )
            )

    @staticmethod
    def _extract_error(operation: Operation):
        if operation.step_details and operation.step_details.error:
            return operation.step_details.error
        if operation.callback_details and operation.callback_details.error:
            return operation.callback_details.error
        if operation.chained_invoke_details and operation.chained_invoke_details.error:
            return operation.chained_invoke_details.error
        if operation.context_details and operation.context_details.error:
            return operation.context_details.error
        return None

    @staticmethod
    def _is_terminal_status(status):
        return status in [
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.TIMED_OUT,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
        ]


def handle_plugins(plugin_executor: PluginExecutor):
    def decorator(func: Callable[[Any, LambdaContext], MutableMapping[str, Any]]):
        @functools.wraps(func)
        def wrapper(event: Any, context: LambdaContext):
            with plugin_executor.run():
                try:
                    output = func(event, context)

                    plugin_executor.on_invocation_end(
                        output=DurableExecutionInvocationOutput.from_dict(output),
                    )
                    return output
                except Exception as e:
                    plugin_executor.on_invocation_end(
                        output=DurableExecutionInvocationOutput.create_retry(
                            ErrorObject.from_exception(e)
                        ),
                    )
                    raise

        return wrapper

    return decorator

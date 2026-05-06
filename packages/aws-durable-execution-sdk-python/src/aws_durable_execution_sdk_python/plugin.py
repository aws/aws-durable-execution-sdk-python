import contextlib
import datetime
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, MutableMapping

from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
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
class OperationInfo:
    operation_id: str
    operation_type: OperationType
    sub_type: OperationSubType | None
    name: str | None
    parent_id: str | None
    start_time: datetime.datetime | None


@dataclass(frozen=True)
class OperationStartInfo(OperationInfo):
    pass


@dataclass(frozen=True)
class OperationEndInfo(OperationInfo):
    status: OperationStatus
    end_time: datetime.datetime | None
    error: ErrorObject | None


class UserFunctionOutcome(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"

    @classmethod
    def from_error(cls, error: ErrorObject | None) -> "UserFunctionOutcome":
        if error is None:
            return cls(cls.SUCCEEDED)
        elif error.type == SuspendExecution.__name__:
            return cls(cls.PENDING)
        else:
            return cls(cls.FAILED)


@dataclass(frozen=True)
class UserFunctionStartInfo(OperationInfo):
    is_replay_children: bool = (
        False  # True if user function is called to replay children (MAP/PARALLEL)
    )
    attempt: int | None = (
        None  # None for user function called more than once in CONTEXT
    )


@dataclass(frozen=True)
class UserFunctionEndInfo(OperationInfo):
    is_replay_children: (
        bool  # True if user function is called to replay children (MAP/PARALLEL)
    )
    attempt: int | None  # None for user function called more than once in CONTEXT
    outcome: UserFunctionOutcome
    end_time: datetime.datetime | None
    error: ErrorObject | None

    @classmethod
    def from_start_info(
        cls, start_info: UserFunctionStartInfo, error: ErrorObject | None
    ) -> "UserFunctionEndInfo":
        return UserFunctionEndInfo(
            operation_id=start_info.operation_id,
            operation_type=start_info.operation_type,
            sub_type=start_info.sub_type,
            name=start_info.name,
            parent_id=start_info.parent_id,
            start_time=start_info.start_time,
            is_replay_children=start_info.is_replay_children,
            attempt=start_info.attempt,
            outcome=UserFunctionOutcome.from_error(error),
            end_time=datetime.datetime.now(datetime.UTC),
            error=error,
        )


@dataclass(frozen=True)
class InvocationInfo:
    request_id: str | None
    execution_arn: str | None
    start_time: datetime.datetime | None
    is_first_invocation: bool


@dataclass(frozen=True)
class InvocationStartInfo(InvocationInfo):
    pass


@dataclass(frozen=True)
class InvocationEndInfo(InvocationInfo):
    status: InvocationStatus
    end_time: datetime.datetime | None
    error: ErrorObject | None

    @classmethod
    def from_durable_execution_invocation_output(
        cls,
        invocation_start_info: InvocationStartInfo,
        output: "DurableExecutionInvocationOutput",
    ):
        return InvocationEndInfo(
            request_id=invocation_start_info.request_id,
            execution_arn=invocation_start_info.execution_arn,
            start_time=invocation_start_info.start_time,
            is_first_invocation=invocation_start_info.is_first_invocation,
            status=output.status,
            end_time=datetime.datetime.now(datetime.UTC),
            error=output.error,
        )


class DurableInstrumentationPlugin:
    """Base class for plugins. Override only the methods you need."""

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        """Called when an invocation starts. This is called within the thread that runs user function handler.

        Args:
            info: Information about the invocation.
        """
        pass

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        """Called when an invocation ends. This is called within the thread that runs user function handler.

        Args:
            info: Information about the invocation.
        """
        pass

    def on_operation_start(self, info: OperationStartInfo) -> None:
        """
        Called when an operation checkpoints STARTED status. This is called NOT within the thread that runs operation.

        Args:
            info: Information about the operation.

        """
        pass

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """
        Called when an operation checkpoints a terminal status. This is called NOT within the thread that runs operation.

        Args:
            info: Information about the operation.
        """
        pass

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        """Called when an operation starts to execute user provided function. This is called within the thread that runs user provided function.

        Args:
            info: Information about the operation attempt.
        """
        pass

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        """Called when an operation finishes executing user provided function. This is called within the thread that runs user provided function.

        Args:
            info: Information about the operation attempt.
        """
        pass

    # Todo: further discussions required to finalize the following interface
    # def enrich_log_context(self, info: OperationStartInfo | None) -> Dict[str, Any] | None: pass


class PluginExecutor:
    def __init__(self, plugins: list[DurableInstrumentationPlugin] | None):
        self._plugins = plugins or []
        self._executor: ThreadPoolExecutor | None = None
        self._invocation_status: InvocationStartInfo | None = None

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
            self._invocation_status = None
            # Shut down the thread pool, waiting for pending tasks to complete.
            if self._executor:
                self._executor.shutdown(wait=True)

    @staticmethod
    def _dispatch_plugin(plugin: DurableInstrumentationPlugin, info) -> None:
        """Invoke the appropriate plugin callback. Runs inside the thread pool."""
        try:
            match info:
                case InvocationStartInfo():
                    plugin.on_invocation_start(info)
                case InvocationEndInfo():
                    plugin.on_invocation_end(info)
                case OperationStartInfo():
                    plugin.on_operation_start(info)
                case OperationEndInfo():
                    plugin.on_operation_end(info)
                case UserFunctionStartInfo():
                    plugin.on_user_function_start(info)
                case UserFunctionEndInfo():
                    plugin.on_user_function_end(info)
                case _:
                    raise RuntimeError(f"Unknown info type: {type(info)}")
        except Exception:
            # log and ignore the exception
            logger.exception("Plugin %s exception ignored", plugin.__class__.__name__)

    def execute_plugins(self, info, sync):
        if not self._executor:
            return
        for plugin in self._plugins:
            if sync:
                # this is called synchronously, so plugins will be able to manipulate thread local objects
                self._dispatch_plugin(plugin, info)
            else:
                # this is called asynchronously, so plugins cannot manipulate thread local objects
                self._executor.submit(self._dispatch_plugin, plugin, info)

    def on_invocation_start(
        self,
        execution_arn: str,
        is_first_invocation: bool,
        execution_start_time: datetime.datetime | None,
        lambda_context: LambdaContext | None,
    ) -> None:
        aws_request_id = lambda_context.aws_request_id if lambda_context else None
        invocation_start_time = (
            datetime.datetime.now(datetime.UTC)
            if is_first_invocation
            else execution_start_time
        )
        self._invocation_status = InvocationStartInfo(
            execution_arn=execution_arn,
            request_id=aws_request_id,
            is_first_invocation=is_first_invocation,
            start_time=invocation_start_time,
        )
        self.execute_plugins(self._invocation_status, sync=True)

    def on_invocation_end(
        self,
        output: "DurableExecutionInvocationOutput",
    ) -> None:
        if self._invocation_status is None:
            # on_invocation_start not called, skip
            return

        invocation_end_info = (
            InvocationEndInfo.from_durable_execution_invocation_output(
                self._invocation_status, output
            )
        )
        self.execute_plugins(invocation_end_info, sync=True)

    def on_user_function_start(
        self,
        operation_identifier: OperationIdentifier,
        is_replay_children: bool = False,
        attempt: int | None = None,
    ) -> UserFunctionStartInfo:
        """Execute any registered plugins for the operation when its user function starts to execute."""
        start_info = UserFunctionStartInfo(
            operation_id=operation_identifier.operation_id,
            operation_type=operation_identifier.type,
            sub_type=operation_identifier.sub_type,
            name=operation_identifier.name,
            parent_id=operation_identifier.parent_id,
            start_time=datetime.datetime.now(datetime.UTC),
            is_replay_children=is_replay_children,
            attempt=attempt,
        )
        self.execute_plugins(start_info, sync=True)
        return start_info

    def on_user_function_end(self, start_info: UserFunctionStartInfo, error) -> None:
        """Execute any registered plugins for the operation when its user function finishes execution."""
        self.execute_plugins(
            UserFunctionEndInfo.from_start_info(start_info, error), sync=True
        )

    def on_operation_action(self, update: OperationUpdate):
        """Execute any registered plugins for a given operation when an update is checkpointed

        Args:
            update: the operation update that is checkpointed
        """
        if update.action is OperationAction.START:
            # we handle only START action here because on_operation_update may not be able to see a STARTED update
            # when START is checkpointed in batch with terminal status updates.
            self.execute_plugins(
                OperationStartInfo(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    sub_type=update.sub_type,
                    name=update.name,
                    parent_id=update.parent_id,
                    start_time=datetime.datetime.now(datetime.UTC),
                ),
                sync=False,
            )

    def on_operation_update(self, operation: Operation | None):
        """Execute any registered plugins for a given operation when it receives an update

        Updates such as STARTED might be omitted because START and completion action (e.g. SUCCEED/FAIL) may be
        checkpointed in batch and the backend returns only the terminal status (e.g. SUCCEEDED/PENDING/FAILED).

        Note: the operation may not be up-to-date if the checkpoint is called asynchronously.

        Args:
            operation: the operation is just checkpointed
        """
        if operation and self._is_terminal_status(operation.status):
            self.execute_plugins(
                OperationEndInfo(
                    operation_id=operation.operation_id,
                    operation_type=operation.operation_type,
                    sub_type=operation.sub_type,
                    name=operation.name,
                    parent_id=operation.parent_id,
                    start_time=operation.start_timestamp,
                    end_time=operation.end_timestamp,
                    status=operation.status,
                    error=self._extract_error(operation),
                ),
                sync=False,
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

    @property
    def handle_durable_output(self):
        def decorator(func: Callable[[Any, LambdaContext], MutableMapping[str, Any]]):
            @functools.wraps(func)
            def wrapper(event: Any, context: LambdaContext):
                with self.run():
                    try:
                        output = func(event, context)

                        self.on_invocation_end(
                            output=DurableExecutionInvocationOutput.from_dict(output),
                        )
                        return output
                    except Exception as e:
                        self.on_invocation_end(
                            output=DurableExecutionInvocationOutput.create_retry(
                                ErrorObject.from_exception(e)
                            ),
                        )
                        raise

            return wrapper

        return decorator

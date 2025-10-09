from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Concatenate, Generic, ParamSpec, TypeVar

from aws_durable_execution_sdk_python.config import (
    BatchedInput,
    CallbackConfig,
    ChildConfig,
    InvokeConfig,
    MapConfig,
    ParallelConfig,
    StepConfig,
    WaitForCallbackConfig,
    WaitForConditionConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    FatalError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_context import (
    LambdaContext,
    make_dict_from_obj,
)
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.logger import Logger, LogInfo
from aws_durable_execution_sdk_python.operation.callback import (
    create_callback_handler,
    wait_for_callback_handler,
)
from aws_durable_execution_sdk_python.operation.child import child_handler
from aws_durable_execution_sdk_python.operation.invoke import invoke_handler
from aws_durable_execution_sdk_python.operation.map import map_handler
from aws_durable_execution_sdk_python.operation.parallel import parallel_handler
from aws_durable_execution_sdk_python.operation.step import step_handler
from aws_durable_execution_sdk_python.operation.wait import wait_handler
from aws_durable_execution_sdk_python.operation.wait_for_condition import (
    wait_for_condition_handler,
)
from aws_durable_execution_sdk_python.serdes import SerDes, deserialize
from aws_durable_execution_sdk_python.state import ExecutionState  # noqa: TCH001
from aws_durable_execution_sdk_python.threading import OrderedCounter
from aws_durable_execution_sdk_python.types import (
    BatchResult,
    LoggerInterface,
    StepContext,
    WaitForConditionCheckContext,
)
from aws_durable_execution_sdk_python.types import Callback as CallbackProtocol
from aws_durable_execution_sdk_python.types import (
    DurableContext as DurableContextProtocol,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from aws_durable_execution_sdk_python.state import CheckpointedResult

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")
U = TypeVar("U")
Params = ParamSpec("Params")


logger = logging.getLogger(__name__)


def durable_step(
    func: Callable[Concatenate[StepContext, Params], T],
) -> Callable[Params, Callable[[StepContext], T]]:
    """Wrap your callable into a named function that a Durable step can run."""

    def wrapper(*args, **kwargs):
        def function_with_arguments(context: StepContext):
            return func(context, *args, **kwargs)

        function_with_arguments._original_name = func.__name__  # noqa: SLF001
        return function_with_arguments

    return wrapper


def durable_with_child_context(
    func: Callable[Concatenate[DurableContext, Params], T],
) -> Callable[Params, Callable[[DurableContext], T]]:
    """Wrap your callable into a Durable child context."""

    def wrapper(*args, **kwargs):
        def function_with_arguments(child_context: DurableContext):
            return func(child_context, *args, **kwargs)

        function_with_arguments._original_name = func.__name__  # noqa: SLF001
        return function_with_arguments

    return wrapper


class Callback(Generic[T], CallbackProtocol[T]):
    """A future that will block on result() until callback_id returns."""

    def __init__(
        self,
        callback_id: str,
        operation_id: str,
        state: ExecutionState,
        serdes: SerDes[T] | None = None,
    ):
        self.callback_id: str = callback_id
        self.operation_id: str = operation_id
        self.state: ExecutionState = state
        self.serdes: SerDes[T] | None = serdes

    def result(self) -> T | None:
        """Return the result of the future. Will block until result is available.

        This will suspend the current execution while waiting for the result to
        become available. Durable Functions will replay the execution once the
        result is ready, and proceed when it reaches the .result() call.

        Use the callback id with the following APIs to send back the result, error or
        heartbeats: SendDurableExecutionCallbackSuccess, SendDurableExecutionCallbackFailure
        and SendDurableExecutionCallbackHeartbeat.
        """
        checkpointed_result: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_id
        )
        if checkpointed_result.is_started():
            msg: str = "Calback result not received yet. Suspending execution while waiting for result."
            raise SuspendExecution(msg)

        if checkpointed_result.is_failed() or checkpointed_result.is_timed_out():
            checkpointed_result.raise_callable_error()

        if checkpointed_result.is_succeeded():
            if checkpointed_result.result is None:
                return None  # type: ignore

            return deserialize(
                serdes=self.serdes,
                data=checkpointed_result.result,
                operation_id=self.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

        msg = "Callback must be started before you can await the result."
        raise FatalError(msg)


# It really would be great NOT to have to inherit from the LambdaContext.
# lot of noise here that we're not actually using. Alternative is to include
# via composition rather than inheritance
class DurableContext(LambdaContext, DurableContextProtocol):
    def __init__(
        self,
        state: ExecutionState,
        parent_id: str | None = None,
        logger: Logger | None = None,
        # LambdaContext members follow
        invoke_id=None,
        client_context=None,
        cognito_identity=None,
        epoch_deadline_time_in_ms=0,
        invoked_function_arn=None,
        tenant_id=None,
    ) -> None:
        super().__init__(
            invoke_id=invoke_id,
            client_context=client_context,
            cognito_identity=cognito_identity,
            epoch_deadline_time_in_ms=epoch_deadline_time_in_ms,
            invoked_function_arn=invoked_function_arn,
            tenant_id=tenant_id,
        )
        self.state: ExecutionState = state
        self._parent_id: str | None = parent_id
        self._step_counter: OrderedCounter = OrderedCounter()

        log_info = LogInfo(
            execution_arn=state.durable_execution_arn, parent_id=parent_id
        )
        self._log_info = log_info
        self.logger: Logger = (
            logger
            if logger
            else Logger.from_log_info(
                logger=logging.getLogger(),
                info=log_info,
            )
        )

    # region factories
    @staticmethod
    def from_lambda_context(
        state: ExecutionState,
        lambda_context: LambdaContext,
    ):
        return DurableContext(
            state=state,
            parent_id=None,
            invoke_id=lambda_context.aws_request_id,
            client_context=make_dict_from_obj(lambda_context.client_context),
            cognito_identity=make_dict_from_obj(lambda_context.identity),
            # not great to have to use the private-ish accessor here, but for the moment not messing with LambdaContext signature
            epoch_deadline_time_in_ms=lambda_context._epoch_deadline_time_in_ms,  # noqa: SLF001
            invoked_function_arn=lambda_context.invoked_function_arn,
            tenant_id=lambda_context.tenant_id,
        )

    def create_child_context(self, parent_id: str) -> DurableContext:
        """Create a child context from the given parent."""
        logger.debug("Creating child context for parent %s", parent_id)
        return DurableContext(
            state=self.state,
            parent_id=parent_id,
            logger=self.logger.with_log_info(
                LogInfo(
                    execution_arn=self.state.durable_execution_arn, parent_id=parent_id
                )
            ),
            invoke_id=self.aws_request_id,
            client_context=make_dict_from_obj(self.client_context),
            cognito_identity=make_dict_from_obj(self.identity),
            epoch_deadline_time_in_ms=self._epoch_deadline_time_in_ms,
            invoked_function_arn=self.invoked_function_arn,
            tenant_id=self.tenant_id,
        )

    # endregion factories

    @staticmethod
    def _resolve_step_name(name: str | None, func: Callable) -> str | None:
        """Resolve the step name.

        Returns:
            str | None: The provided name, and if that doesn't exist the callable function's name if it has one.
        """
        # callable's name will override name if name is falsy ('' or None)
        return name if name else getattr(func, "_original_name", None)

    def set_logger(self, new_logger: LoggerInterface):
        """Set the logger for the current context."""
        self.logger = Logger.from_log_info(
            logger=new_logger,
            info=self._log_info,
        )

    def _create_step_id(self) -> str:
        """Generate a thread-safe step id, incrementing in order of invocation.

        This method is an internal implementation detail. Do not rely the exact format of
        the id generated by this method. It is subject to change without notice.
        """
        new_counter: int = self._step_counter.increment()
        return (
            f"{self._parent_id}-{new_counter}" if self._parent_id else str(new_counter)
        )

    # region Operations

    def create_callback(
        self, name: str | None = None, config: CallbackConfig | None = None
    ) -> Callback:
        """Create a callback.

        This generates a future with a callback id. External systems can signal
        your Durable Function to proceed by using this callback id with the
        SendDurableExecutionCallbackSuccess, SendDurableExecutionCallbackFailure and
        SendDurableExecutionCallbackHeartbeat APIs.

        Args:
            name (str): Optional name for the operation.
            config (CallbackConfig): Configuration for the callback.

        Return:
            Callback future. Use result() on this future to wait for the callback resuilt.
        """
        if not config:
            config = CallbackConfig()
        operation_id: str = self._create_step_id()
        callback_id: str = create_callback_handler(
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id, parent_id=self._parent_id, name=name
            ),
            config=config,
        )

        return Callback(
            callback_id=callback_id,
            operation_id=operation_id,
            state=self.state,
            serdes=config.serdes,
        )

    def invoke(
        self,
        function_name: str,
        payload: P,
        name: str | None = None,
        config: InvokeConfig[P, R] | None = None,
    ) -> R:
        """Invoke another Durable Function.

        Args:
            function_name: Name of the function to invoke
            payload: Input payload to send to the function
            name: Optional name for the operation
            config: Optional configuration for the invoke operation

        Returns:
            The result of the invoked function
        """
        return invoke_handler(
            function_name=function_name,
            payload=payload,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=self._create_step_id(),
                parent_id=self._parent_id,
                name=name,
            ),
            config=config,
        )

    def map(
        self,
        inputs: Sequence[U],
        func: Callable[[DurableContext, U | BatchedInput[Any, U], int, Sequence[U]], T],
        name: str | None = None,
        config: MapConfig | None = None,
    ) -> BatchResult[R]:
        """Execute a callable for each item in parallel."""
        map_name: str | None = self._resolve_step_name(name, func)

        def map_in_child_context(child_context):
            return map_handler(
                items=inputs,
                func=func,
                config=config,
                execution_state=self.state,
                run_in_child_context=child_context.run_in_child_context,
            )

        return self.run_in_child_context(
            func=map_in_child_context,
            name=map_name,
            config=ChildConfig(sub_type=OperationSubType.MAP),
        )

    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute multiple callables in parallel."""

        def parallel_in_child_context(child_context):
            return parallel_handler(
                callables=functions,
                config=config,
                execution_state=self.state,
                run_in_child_context=child_context.run_in_child_context,
            )

        return self.run_in_child_context(
            func=parallel_in_child_context,
            name=name,
            config=ChildConfig(sub_type=OperationSubType.PARALLEL),
        )

    def run_in_child_context(
        self,
        func: Callable[[DurableContext], T],
        name: str | None = None,
        config: ChildConfig | None = None,
    ) -> T:
        """Run the callable and pass a child context to it.

        Use this to nest and group operations.

        Args:
            callable (Callable[[DurableContext], T]): Run this callable and pass the child context as the argument to it.
            name (str | None): name for the operation.
            config (ChildConfig | None = None): c

        Returns:
            T: The result of the callable.
        """
        step_name: str | None = self._resolve_step_name(name, func)
        # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
        operation_id = self._create_step_id()

        def callable_with_child_context():
            return func(self.create_child_context(parent_id=operation_id))

        return child_handler(
            func=callable_with_child_context,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id, parent_id=self._parent_id, name=step_name
            ),
            config=config,
        )

    def step(
        self,
        func: Callable[[StepContext], T],
        name: str | None = None,
        config: StepConfig | None = None,
    ) -> T:
        step_name = self._resolve_step_name(name, func)
        logger.debug("Step name: %s", step_name)

        return step_handler(
            func=func,
            config=config,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=self._create_step_id(),
                parent_id=self._parent_id,
                name=step_name,
            ),
            context_logger=self.logger,
        )

    def wait(self, seconds: int, name: str | None = None) -> None:
        """Wait for a specified amount of time.

        Args:
            seconds: Time to wait in seconds
            name: Optional name for the wait step
        """
        if seconds < 1:
            msg = "seconds must be an integer greater than 0"
            raise ValidationError(msg)
        wait_handler(
            seconds=seconds,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=self._create_step_id(),
                parent_id=self._parent_id,
                name=name,
            ),
        )

    def wait_for_callback(
        self,
        submitter: Callable[[str], None],
        name: str | None = None,
        config: WaitForCallbackConfig | None = None,
    ) -> Any:
        step_name: str | None = self._resolve_step_name(name, submitter)
        logger.debug("wait_for_callback name: %s", step_name)

        def wait_in_child_context(context: DurableContext):
            return wait_for_callback_handler(context, submitter, step_name, config)

        return self.run_in_child_context(
            wait_in_child_context,
            step_name,
        )

    def wait_for_condition(
        self,
        check: Callable[[T, WaitForConditionCheckContext], T],
        config: WaitForConditionConfig[T],
        name: str | None = None,
    ) -> T:
        """Wait for a condition to be met by polling.

        Args:
            check (Callable[[T, WaitForConditionCheckContext], T]): Function that checks the condition and returns updated state
            config (WaitForConditionConfig[T]): Configuration including wait strategy and initial state
            name (str | None): Optional name for the operation

        Returns:
            The final state when condition is met.
        """
        if check is None:
            msg = "`check` is required for wait_for_condition"
            raise ValidationError(msg)
        if not config:
            msg = "`config` is required for wait_for_condition"
            raise ValidationError(msg)

        return wait_for_condition_handler(
            check=check,
            config=config,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=self._create_step_id(),
                parent_id=self._parent_id,
                name=name,
            ),
            context_logger=self.logger,
        )


# endregion Operations

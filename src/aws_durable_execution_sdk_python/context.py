from __future__ import annotations

import hashlib
import logging
from typing import TYPE_CHECKING, Any, Concatenate, Generic, ParamSpec, TypeVar

from aws_durable_execution_sdk_python.config import (
    BatchedInput,
    CallbackConfig,
    ChildConfig,
    Duration,
    InvokeConfig,
    MapConfig,
    ParallelConfig,
    StepConfig,
    WaitForCallbackConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallbackError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
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
from aws_durable_execution_sdk_python.serdes import (
    PassThroughSerDes,
    SerDes,
    deserialize,
)
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
    from aws_durable_execution_sdk_python.types import LambdaContext
    from aws_durable_execution_sdk_python.waits import WaitForConditionConfig

P = TypeVar("P")  # Payload type
R = TypeVar("R")  # Result type
T = TypeVar("T")
U = TypeVar("U")
Params = ParamSpec("Params")


logger = logging.getLogger(__name__)

PASS_THROUGH_SERDES: SerDes[Any] = PassThroughSerDes()


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


class Callback(Generic[T], CallbackProtocol[T]):  # noqa: PYI059
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

        if not checkpointed_result.is_existent():
            msg = "Callback operation must exist"
            raise CallbackError(msg)

        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_cancelled()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            checkpointed_result.raise_callable_error()

        if checkpointed_result.is_succeeded():
            if checkpointed_result.result is None:
                return None  # type: ignore

            return deserialize(
                serdes=self.serdes if self.serdes is not None else PASS_THROUGH_SERDES,
                data=checkpointed_result.result,
                operation_id=self.operation_id,
                durable_execution_arn=self.state.durable_execution_arn,
            )

        # operation exists; it has not terminated (successfully or otherwise)
        # therefore we should wait
        msg = "Callback result not received yet. Suspending execution while waiting for result."
        raise SuspendExecution(msg)


class DurableContext(DurableContextProtocol):
    def __init__(
        self,
        state: ExecutionState,
        lambda_context: LambdaContext | None = None,
        parent_id: str | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.state: ExecutionState = state
        self.lambda_context = lambda_context
        self._parent_id: str | None = parent_id
        self._step_counter: OrderedCounter = OrderedCounter()

        log_info = LogInfo(
            execution_arn=state.durable_execution_arn, parent_id=parent_id
        )
        self._log_info = log_info
        self.logger: Logger = logger or Logger.from_log_info(
            logger=logging.getLogger(),
            info=log_info,
        )

    @property
    def tenant_id(self) -> str | None:
        return getattr(self.lambda_context, "tenant_id", None)

    # region factories
    @staticmethod
    def from_lambda_context(
        state: ExecutionState,
        lambda_context: LambdaContext,
    ):
        return DurableContext(
            state=state,
            lambda_context=lambda_context,
            parent_id=None,
        )

    def create_child_context(self, parent_id: str) -> DurableContext:
        """Create a child context from the given parent."""
        logger.debug("Creating child context for parent %s", parent_id)
        return DurableContext(
            state=self.state,
            lambda_context=self.lambda_context,
            parent_id=parent_id,
            logger=self.logger.with_log_info(
                LogInfo(
                    execution_arn=self.state.durable_execution_arn, parent_id=parent_id
                )
            ),
        )

    # endregion factories

    @staticmethod
    def _resolve_step_name(name: str | None, func: Callable) -> str | None:
        """Resolve the step name.

        Returns:
            str | None: The provided name, and if that doesn't exist the callable function's name if it has one.
        """
        # callable's name will override name if name is falsy ('' or None)
        return name or getattr(func, "_original_name", None)

    def set_logger(self, new_logger: LoggerInterface):
        """Set the logger for the current context."""
        self.logger = Logger.from_log_info(
            logger=new_logger,
            info=self._log_info,
        )

    def _create_step_id_for_logical_step(self, step: int) -> str:
        """
        Generate a step_id based on the given logical step.
        This allows us to recover operation ids or even look
        forward without changing the internal state of this context.
        """
        step_id = f"{self._parent_id}-{step}" if self._parent_id else str(step)
        return hashlib.blake2b(step_id.encode()).hexdigest()[:64]

    def _create_step_id(self) -> str:
        """Generate a thread-safe step id, incrementing in order of invocation.

        This method is an internal implementation detail. Do not rely the exact format of
        the id generated by this method. It is subject to change without notice.
        """
        new_counter: int = self._step_counter.increment()
        return self._create_step_id_for_logical_step(new_counter)

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

        Notes:
            config: InvokeConfig contains a tenant_id.
            In the event that the tenant is not set, we will default to
            using the same tenant as the current invocation.

            In the event that the tenant is set, we will respect

        Returns:
            The result of the invoked function
        """

        if config is None:
            config = InvokeConfig()
        config = config.with_tenant_id_if_unset(self.tenant_id)

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

        operation_id = self._create_step_id()
        operation_identifier = OperationIdentifier(
            operation_id=operation_id, parent_id=self._parent_id, name=map_name
        )
        map_context = self.create_child_context(parent_id=operation_id)

        def map_in_child_context() -> BatchResult[R]:
            # map_context is a child_context of the context upon which `.map`
            # was called. We are calling it `map_context` to make it explicit
            # that any operations happening from hereon are done on the context
            # that owns the branches
            return map_handler(
                items=inputs,
                func=func,
                config=config,
                execution_state=self.state,
                map_context=map_context,
                operation_identifier=operation_identifier,
            )

        return child_handler(
            func=map_in_child_context,
            state=self.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                sub_type=OperationSubType.MAP,
                serdes=getattr(config, "serdes", None),
                # child_handler should only know the serdes of the parent serdes,
                # the item serdes will be passed when we are actually executing
                # the branch within its own child_handler.
                item_serdes=None,
            ),
        )

    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute multiple callables in parallel."""
        # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
        operation_id = self._create_step_id()
        parallel_context = self.create_child_context(parent_id=operation_id)
        operation_identifier = OperationIdentifier(
            operation_id=operation_id, parent_id=self._parent_id, name=name
        )

        def parallel_in_child_context() -> BatchResult[T]:
            # parallel_context is a child_context of the context upon which `.map`
            # was called. We are calling it `parallel_context` to make it explicit
            # that any operations happening from hereon are done on the context
            # that owns the branches
            return parallel_handler(
                callables=functions,
                config=config,
                execution_state=self.state,
                parallel_context=parallel_context,
                operation_identifier=operation_identifier,
            )

        return child_handler(
            func=parallel_in_child_context,
            state=self.state,
            operation_identifier=operation_identifier,
            config=ChildConfig(
                sub_type=OperationSubType.PARALLEL,
                serdes=getattr(config, "serdes", None),
                # child_handler should only know the serdes of the parent serdes,
                # the item serdes will be passed when we are actually executing
                # the branch within its own child_handler.
                item_serdes=None,
            ),
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

    def wait(self, duration: Duration, name: str | None = None) -> None:
        """Wait for a specified amount of time.

        Args:
            duration: Duration to wait
            name: Optional name for the wait step
        """
        seconds = duration.to_seconds()
        if seconds < 1:
            msg = "duration must be at least 1 second"
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

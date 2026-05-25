from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Concatenate, Generic, ParamSpec, TypeVar

from aws_durable_execution_sdk_python.config import (
    BatchedInput,
    CallbackConfig,
    ChildConfig,
    Duration,
    InvokeConfig,
    MapConfig,
    ParallelBranch,
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
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.logger import Logger, LogInfo
from aws_durable_execution_sdk_python.operation.callback import (
    CallbackOperationExecutor,
    wait_for_callback_handler,
)
from aws_durable_execution_sdk_python.operation.child import child_handler
from aws_durable_execution_sdk_python.operation.invoke import InvokeOperationExecutor
from aws_durable_execution_sdk_python.operation.map import map_handler
from aws_durable_execution_sdk_python.operation.parallel import parallel_handler
from aws_durable_execution_sdk_python.operation.step import StepOperationExecutor
from aws_durable_execution_sdk_python.operation.wait import WaitOperationExecutor
from aws_durable_execution_sdk_python.operation.wait_for_condition import (
    WaitForConditionOperationExecutor,
)
from aws_durable_execution_sdk_python.serdes import (
    PassThroughSerDes,
    SerDes,
    deserialize,
)
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus  # noqa: TCH001
from aws_durable_execution_sdk_python.threading import OrderedCounter
from aws_durable_execution_sdk_python.types import Callback as CallbackProtocol
from aws_durable_execution_sdk_python.types import (
    DurableContext as DurableContextProtocol,
)
from aws_durable_execution_sdk_python.types import (
    LoggerInterface,
    StepContext,
    WaitForCallbackContext,
    WaitForConditionCheckContext,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from aws_durable_execution_sdk_python.concurrency.models import BatchResult
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


@dataclass(frozen=True)
class ExecutionContext:
    """Readonly metadata about the current durable execution context.

    This class provides immutable access to execution-level metadata.

    Attributes:
        durable_execution_arn: The Amazon Resource Name (ARN) of the current
            durable execution.
    """

    durable_execution_arn: str


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


def durable_parallel_branch(
    name: str | None = None,
) -> Callable[
    [Callable[Concatenate[DurableContext, Params], T]],
    Callable[Params, ParallelBranch[T]],
]:
    """Wrap your callable into a named ParallelBranch for use with context.parallel().

    This is a decorator factory — call it with an optional name to produce
    the actual decorator.

    Args:
        name: Optional custom name for this branch. When provided, replaces
            the default "parallel-branch-{index}" naming in execution history.
            If None, the function's __name__ is used.

    Example:
        @durable_parallel_branch(name="fetch-user-data")
        def fetch_user(ctx: DurableContext, user_id: str) -> dict:
            return ctx.step(lambda _: {"id": user_id, "name": "Jane"}, name="load_user")

        @durable_parallel_branch(name="fetch-orders")
        def fetch_orders(ctx: DurableContext, user_id: str) -> list:
            return ctx.step(lambda _: ["order1", "order2"], name="load_orders")

        # Usage in a durable handler:
        results = context.parallel(
            functions=[fetch_user(user_id), fetch_orders(user_id)],
            name="load-data",
        )
    """

    def decorator(
        func: Callable[Concatenate[DurableContext, Params], T],
    ) -> Callable[Params, ParallelBranch[T]]:
        def wrapper(*args, **kwargs) -> ParallelBranch[T]:
            def function_with_arguments(ctx: DurableContext) -> T:
                return func(ctx, *args, **kwargs)

            return ParallelBranch(func=function_with_arguments, name=name)

        return wrapper

    return decorator


def durable_wait_for_callback(
    func: Callable[Concatenate[str, WaitForCallbackContext, Params], T],
) -> Callable[Params, Callable[[str, WaitForCallbackContext], T]]:
    """Wrap your callable into a wait_for_callback submitter function.

    This decorator allows you to define a submitter function with additional
    parameters that will be bound when called.

    Args:
        func: A callable that takes callback_id, context, and additional parameters

    Returns:
        A wrapper function that binds the additional parameters and returns
        a submitter function compatible with wait_for_callback

    Example:
        @durable_wait_for_callback
        def submit_to_external_system(
            callback_id: str,
            context: WaitForCallbackContext,
            task_name: str,
            priority: int
        ):
            context.logger.info(f"Submitting {task_name} with callback {callback_id}")
            external_api.submit_task(
                task_name=task_name,
                priority=priority,
                callback_id=callback_id
            )

        # Usage in durable handler:
        result = context.wait_for_callback(
            submit_to_external_system("my_task", priority=5)
        )
    """

    def wrapper(*args, **kwargs):
        def submitter_with_arguments(callback_id: str, context: WaitForCallbackContext):
            return func(callback_id, context, *args, **kwargs)

        submitter_with_arguments._original_name = func.__name__  # noqa: SLF001
        return submitter_with_arguments

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
            raise CallbackError(message=msg, callback_id=self.callback_id)

        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_cancelled()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            msg = (
                checkpointed_result.error.message
                if checkpointed_result.error and checkpointed_result.error.message
                else "Callback failed"
            )
            raise CallbackError(message=msg, callback_id=self.callback_id)

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
        execution_context: ExecutionContext,
        lambda_context: LambdaContext | None = None,
        parent_id: str | None = None,
        logger: Logger | None = None,
        step_id_prefix: str | None = None,
        replay_status: ReplayStatus = ReplayStatus.NEW,
    ) -> None:
        self.state: ExecutionState = state
        self.execution_context: ExecutionContext = execution_context
        self.lambda_context = lambda_context
        # operations inside this context use this id as their parent
        self._parent_id: str | None = parent_id
        # child operations use this to generate deterministic step ids.
        # differs from `parent_id` only for virtual contexts.
        self._step_id_prefix: str | None = (
            step_id_prefix if step_id_prefix is not None else parent_id
        )
        # cached at construction to make invariant even if parent/prefix mutates.
        self._is_virtual: bool = self._parent_id != self._step_id_prefix
        self._step_counter: OrderedCounter = OrderedCounter()
        self._replay_status: ReplayStatus = replay_status
        self._visited_operations: set[str] = set()

        log_info = LogInfo(
            context=self,
            parent_id=parent_id,
        )
        self._log_info = log_info
        self.logger: Logger = logger or Logger.from_log_info(
            logger=logging.getLogger(),
            info=log_info,
        )

    @property
    def is_virtual(self) -> bool:
        """True if this context does not checkpoint its own start and completion.

        You create a virtual context by `create_child_context(..., is_virtual=True)`.
        FLAT-mode `map`/`parallel` branches uses virtual contexts. Inner operations
        use the grandfather as parent (enclosing non-virtual ancestor, skipping the branch level
        in the hierarchy), while step ids are still prefixed with the branch's own
        operation id so replay stays deterministic.
        """
        return self._is_virtual

    def is_replaying(self) -> bool:
        """Check if this context is currently in replay mode."""
        return self._replay_status is ReplayStatus.REPLAY

    def track_replay(self, operation_id: str) -> None:
        """Track an operation visit and evaluate replay transition.

        Adds the operation_id to this context's visited set. If the context
        is in REPLAY status, checks whether all completed operations scoped
        to this context have been visited. If so, transitions to NEW.

        Args:
            operation_id: The operation ID being processed.
        """
        if self._replay_status is ReplayStatus.NEW:
            return

        self._visited_operations.add(operation_id)
        scoped_completed = self._get_scoped_completed_operations()
        if scoped_completed.issubset(self._visited_operations):
            self._replay_status = ReplayStatus.NEW

    def _get_scoped_completed_operations(self) -> set[str]:
        """Compute the set of completed operation IDs scoped to this context.

        Filters ExecutionState operations to find those that:
        1. Are not of type EXECUTION
        2. Have a terminal status (SUCCEEDED, FAILED, CANCELLED, STOPPED, TIMED_OUT)
        3. Are scoped to this context (parent_id matches this context's _parent_id)

        Returns:
            Set of operation IDs that are completed and scoped to this context.
        """
        completed_statuses = {
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
            OperationStatus.TIMED_OUT,
        }

        scoped_ops: set[str] = set()
        with self.state._operations_lock:
            for op_id, op in self.state.operations.items():
                if op.operation_type is OperationType.EXECUTION:
                    continue
                if op.status not in completed_statuses:
                    continue
                if self._is_operation_scoped_to_context(op):
                    scoped_ops.add(op_id)
        return scoped_ops

    def _is_operation_scoped_to_context(self, operation: Operation) -> bool:
        """Determine if an operation belongs to this context's scope.

        An operation is scoped to this context if its parent_id matches
        this context's _parent_id. Operations created by this context have
        parent_id == self._parent_id (set via OperationIdentifier in each
        operation method).

        Args:
            operation: The operation to check.

        Returns:
            True if the operation is scoped to this context.
        """
        return operation.parent_id == self._parent_id

    def _determine_child_replay_status(
        self, child_parent_id: str | None
    ) -> ReplayStatus:
        """Determine initial replay status for a child context.

        Returns REPLAY if any completed operations exist with the given
        parent_id, otherwise returns NEW.

        Args:
            child_parent_id: The parent_id that the child context's operations
                would have.

        Returns:
            ReplayStatus.REPLAY if completed operations exist for the child's
            scope, otherwise ReplayStatus.NEW.
        """
        completed_statuses = {
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
            OperationStatus.TIMED_OUT,
        }

        with self.state._operations_lock:
            has_scoped_completed = any(
                op.parent_id == child_parent_id
                and op.operation_type is not OperationType.EXECUTION
                and op.status in completed_statuses
                for op in self.state.operations.values()
            )
        return ReplayStatus.REPLAY if has_scoped_completed else ReplayStatus.NEW

    # region factories
    @staticmethod
    def from_lambda_context(
        state: ExecutionState,
        lambda_context: LambdaContext,
        replay_status: ReplayStatus = ReplayStatus.NEW,
    ):
        return DurableContext(
            state=state,
            execution_context=ExecutionContext(
                durable_execution_arn=state.durable_execution_arn
            ),
            lambda_context=lambda_context,
            parent_id=None,
            replay_status=replay_status,
        )

    def create_child_context(
        self, operation_id: str, *, is_virtual: bool = False
    ) -> DurableContext:
        """Create a child context for the given operation.

        Args:
            operation_id: The operation id that owns the child context. Used as
                the child's step-id prefix in all cases.
            is_virtual: When `True`, create a virtual child whose inner
                operations report to this context's own `_parent_id` (one
                level up the hierarchy). When `False` (default), produce a
                regular child whose inner operations report to
                `operation_id`.

        Returns:
            A new `DurableContext` child for the current context.
        """
        # For a virtual child, propagate the current `_parent_id` so its
        # inner operations refer to the grandparent rather than the parent.
        # For a regular non-virtual child, the child's own `operation_id` is
        # the parent id for its inner operations (standard nesting).
        child_parent_id: str | None = self._parent_id if is_virtual else operation_id
        child_replay_status = self._determine_child_replay_status(child_parent_id)
        logger.debug(
            "Creating child context for operation %s (is_virtual=%s)",
            operation_id,
            is_virtual,
        )
        child_context = DurableContext(
            state=self.state,
            execution_context=self.execution_context,
            lambda_context=self.lambda_context,
            parent_id=child_parent_id,
            step_id_prefix=operation_id,
            replay_status=child_replay_status,
            logger=None,
        )
        # Build the child's logger using the parent's underlying logger instance
        # so custom loggers (set via set_logger) are inherited by children.
        child_log_info = LogInfo(
            context=child_context,
            parent_id=child_parent_id,
        )
        child_context._log_info = child_log_info
        child_context.logger = Logger.from_log_info(
            logger=self.logger.get_logger(),
            info=child_log_info,
        )
        return child_context

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
        prefix: str | None = self._step_id_prefix
        step_id: str = f"{prefix}-{step}" if prefix else str(step)
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
        executor: CallbackOperationExecutor = CallbackOperationExecutor(
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                parent_id=self._parent_id,
                name=name,
            ),
            config=config,
        )
        callback_id: str = executor.process()
        result: Callback = Callback(
            callback_id=callback_id,
            operation_id=operation_id,
            state=self.state,
            serdes=config.serdes,
        )
        self.track_replay(operation_id=operation_id)
        return result

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
        if not config:
            config = InvokeConfig[P, R]()
        operation_id = self._create_step_id()
        executor: InvokeOperationExecutor[R] = InvokeOperationExecutor(
            function_name=function_name,
            payload=payload,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                parent_id=self._parent_id,
                name=name,
            ),
            config=config,
        )
        result: R = executor.process()
        self.track_replay(operation_id=operation_id)
        return result

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
            operation_id=operation_id,
            parent_id=self._parent_id,
            name=map_name,
        )
        map_context = self.create_child_context(operation_id=operation_id)

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

        result: BatchResult[R] = child_handler(
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
        self.track_replay(operation_id=operation_id)
        return result

    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], T] | ParallelBranch[T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute multiple callables in parallel."""
        # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
        operation_id = self._create_step_id()
        parallel_context = self.create_child_context(operation_id=operation_id)
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

        result: BatchResult[T] = child_handler(
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
        self.track_replay(operation_id=operation_id)
        return result

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

        is_virtual: bool = config.is_virtual if config else False

        def callable_with_child_context():
            return func(
                self.create_child_context(
                    operation_id=operation_id, is_virtual=is_virtual
                )
            )

        result: T = child_handler(
            func=callable_with_child_context,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                parent_id=self._parent_id,
                name=step_name,
            ),
            config=config,
        )
        self.track_replay(operation_id=operation_id)
        return result

    def step(
        self,
        func: Callable[[StepContext], T],
        name: str | None = None,
        config: StepConfig | None = None,
    ) -> T:
        step_name = self._resolve_step_name(name, func)
        logger.debug("Step name: %s", step_name)
        if not config:
            config = StepConfig()
        operation_id = self._create_step_id()
        executor: StepOperationExecutor[T] = StepOperationExecutor(
            func=func,
            config=config,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                parent_id=self._parent_id,
                name=step_name,
            ),
            context_logger=self.logger,
        )
        result: T = executor.process()
        self.track_replay(operation_id=operation_id)
        return result

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
        operation_id = self._create_step_id()
        wait_seconds = duration.seconds
        executor: WaitOperationExecutor = WaitOperationExecutor(
            seconds=wait_seconds,
            state=self.state,
            operation_identifier=OperationIdentifier(
                operation_id=operation_id,
                parent_id=self._parent_id,
                name=name,
            ),
        )
        executor.process()
        self.track_replay(operation_id=operation_id)

    def wait_for_callback(
        self,
        submitter: Callable[[str, WaitForCallbackContext], None],
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

        operation_id = self._create_step_id()
        executor: WaitForConditionOperationExecutor[T] = (
            WaitForConditionOperationExecutor(
                check=check,
                config=config,
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    parent_id=self._parent_id,
                    name=name,
                ),
                context_logger=self.logger,
            )
        )
        result: T = executor.process()
        self.track_replay(operation_id=operation_id)
        return result


# endregion Operations

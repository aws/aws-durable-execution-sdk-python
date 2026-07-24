from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    NoReturn,
    ParamSpec,
    TypeVar,
)

from aws_durable_execution_sdk_python.config import (
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
    CallbackExternalError,
    CallbackSubmitterError,
    CallbackTimeoutError,
    ChildContextError,
    StepError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
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
from aws_durable_execution_sdk_python.state import (  # noqa: TCH001
    ExecutionState,
    ReplayStatus,
)
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
    from aws_durable_execution_sdk_python.lambda_service import ErrorObject
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
            # Should never happen (create_callback already checkpointed this op).
            # Not external/timeout/submitter, so raise the base CallbackError.
            msg = "Callback operation must exist"
            raise CallbackError(message=msg)

        if (
            checkpointed_result.is_failed()
            or checkpointed_result.is_cancelled()
            or checkpointed_result.is_timed_out()
            or checkpointed_result.is_stopped()
        ):
            self._raise_terminal_failure(checkpointed_result)

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

    def _raise_terminal_failure(
        self, checkpointed_result: CheckpointedResult
    ) -> NoReturn:
        """Raise the graded error for a terminal callback failure.

        TIMED_OUT -> CallbackTimeoutError; any other non-succeeded terminal
        (external FAILED, cancelled, stopped) -> CallbackExternalError.
        Carries the message/data/stack_trace; the external error's own type
        stays on the callback operation's details.
        """
        error: ErrorObject | None = checkpointed_result.error
        is_timeout: bool = checkpointed_result.is_timed_out()
        error_cls: type[CallbackError] = (
            CallbackTimeoutError if is_timeout else CallbackExternalError
        )
        default_message: str = "Callback timed out" if is_timeout else "Callback failed"
        message: str = error.message if error and error.message else default_message
        raise error_cls(
            message=message,
            data=error.data if error else None,
            stack_trace=error.stack_trace if error else None,
        )


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

        # Replay status is tracked per-context.
        # A context starts in the status inherited from its creator and refines
        # itself to NEW via look-ahead as it reaches its own replay boundary.
        # Concurrent branches each get their own child context, so the lock
        # guards refinement when branches share a context reference.
        self._replay_status: ReplayStatus = replay_status
        self._replay_status_lock: Lock = Lock()

        log_info = LogInfo(
            execution_state=state,
            parent_id=parent_id,
        )
        self._log_info = log_info
        # The logger consults THIS context's replay status for de-duplication.
        # A child inherits the parent's underlying logger/extra but must report
        # its own status, so rebind the replay source onto self.
        if logger is not None:
            self.logger: Logger = logger.with_is_replaying(self.is_replaying)
        else:
            self.logger = Logger.from_log_info(
                logger=logging.getLogger(),
                info=log_info,
                is_replaying=self.is_replaying,
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
        logger.debug(
            "Creating child context for operation %s (is_virtual=%s)",
            operation_id,
            is_virtual,
        )
        return DurableContext(
            state=self.state,
            execution_context=self.execution_context,
            lambda_context=self.lambda_context,
            parent_id=child_parent_id,
            step_id_prefix=operation_id,
            # Inherit the creator's current replay status; the child refines
            # itself to NEW via look-ahead against its own step ids.
            replay_status=(
                ReplayStatus.REPLAY if self.is_replaying() else ReplayStatus.NEW
            ),
            logger=self.logger.with_log_info(
                LogInfo(
                    execution_state=self.state,
                    parent_id=child_parent_id,
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
            is_replaying=self.is_replaying,
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

    # region replay status

    def is_replaying(self) -> bool:
        """Return True if this context is currently replaying prior operations."""
        with self._replay_status_lock:
            return self._replay_status is ReplayStatus.REPLAY

    def _set_replay_status_new(self) -> None:
        with self._replay_status_lock:
            self._replay_status = ReplayStatus.NEW

    def _peek_next_operation_id(self) -> str:
        """Return the operation id the next operation will take, without consuming it."""
        return self._create_step_id_for_logical_step(
            self._step_counter.get_current() + 1
        )

    def _peek_next_checkpoint(self) -> CheckpointedResult:
        """Return the checkpoint for this context's next operation, without consuming it."""
        return self.state.get_checkpoint_result(self._peek_next_operation_id())

    def _next_operation_exists(self) -> bool:
        """True if a checkpoint exists for this context's next operation."""
        return self._peek_next_checkpoint().is_existent()

    @contextmanager
    def _replay_aware(self):
        """Wrap a single operation with replay-boundary detection.

        The operation kind is inferred from its own checkpoint (when one
        exists), so no per-call-site flag is needed.

        The boundary has these parts:

        - Existence flip (before the op): if we are replaying and the next
          operation has no checkpoint at all, it is brand-new code, so flip to
          NEW immediately so the operation and its logs count as new.
        - Deferred status flip (after the op): a non-terminal next operation is
          a pure resume point with no user body. We keep replay status through the
          operation and flip to NEW afterwards, so the resuming operation's own
          logs stay de-duplicated but subsequent code counts as new.
        - Post-op existence flip (after the op): if we are still replaying once
          the operation completes and the *following* operation does not exist
          yet, we have reached the replay boundary.
        """
        was_replaying: bool = self.is_replaying()
        # Only peek when replaying; avoids unnecessary checkpoint lookups (and
        # any step-id side effects) on the common non-replay path.
        next_checkpoint: CheckpointedResult | None = (
            self._peek_next_checkpoint() if was_replaying else None
        )
        next_exists: bool = (
            next_checkpoint.is_existent() if next_checkpoint is not None else False
        )
        next_terminal: bool = next_exists and next_checkpoint.is_terminal()

        next_is_step: bool = (
            next_exists
            and next_checkpoint.operation.operation_type is OperationType.STEP
        )
        # While replaying, an operation that already has a checkpoint was
        # observed in a prior invocation. If the backend says the operation
        # changed since the last invocation, notify plugins as an update rather
        # than replayed history; otherwise notify as replayed history. State
        # owns the dedup; the context owns the "only while replaying" gate.
        if was_replaying and next_exists:
            if self.state.is_operation_updated_since_last_invocation(
                next_checkpoint.operation.operation_id
            ):
                self.state.emit_operation_update_hook(next_checkpoint.operation)
            else:
                self.state.emit_operation_replay_hook(next_checkpoint.operation)
        # Deferred flip applies only to non-step resume points. For step ops we
        # flip before instead, so don't defer.
        flip_after: bool = (
            was_replaying and next_exists and not next_terminal and not next_is_step
        )
        # Before-the-op flips:
        # - brand-new next op (no checkpoint): always flip to NEW.
        # - non-terminal STEP-type op (brand-new or retrying): the user function
        #   is about to run real work, so flip to NEW before it.
        if was_replaying and (not next_exists or (next_is_step and not next_terminal)):
            self._set_replay_status_new()
        try:
            yield
        finally:
            if flip_after:
                self._set_replay_status_new()
            elif self.is_replaying() and not self._next_operation_exists():
                self._set_replay_status_new()

    # endregion replay status

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
        with self._replay_aware():
            operation_id: str = self._create_step_id()
            executor: CallbackOperationExecutor = CallbackOperationExecutor(
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    sub_type=OperationSubType.CALLBACK,
                    parent_id=self._parent_id,
                    name=name,
                ),
                config=config,
            )
            callback_id: str = executor.process()
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
        if not config:
            config = InvokeConfig[P, R]()
        with self._replay_aware():
            operation_id = self._create_step_id()
            executor: InvokeOperationExecutor[R] = InvokeOperationExecutor(
                function_name=function_name,
                payload=payload,
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    sub_type=OperationSubType.CHAINED_INVOKE,
                    parent_id=self._parent_id,
                    name=name,
                ),
                config=config,
            )
            return executor.process()

    def map(
        self,
        inputs: Sequence[U],
        func: Callable[[DurableContext, U, int, Sequence[U]], T],
        name: str | None = None,
        config: MapConfig | None = None,
    ) -> BatchResult[R]:
        """Execute a callable for each item in parallel."""
        map_name: str | None = self._resolve_step_name(name, func)

        with self._replay_aware():
            operation_id = self._create_step_id()
            operation_identifier = OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.MAP,
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

            return child_handler(
                func=map_in_child_context,
                state=self.state,
                operation_identifier=operation_identifier,
                config=ChildConfig(
                    sub_type=OperationSubType.MAP,
                    serdes=getattr(config, "serdes", None),
                ),
            )

    def parallel(
        self,
        functions: Sequence[Callable[[DurableContext], T] | ParallelBranch[T]],
        name: str | None = None,
        config: ParallelConfig | None = None,
    ) -> BatchResult[T]:
        """Execute multiple callables in parallel."""
        with self._replay_aware():
            # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
            operation_id = self._create_step_id()
            parallel_context = self.create_child_context(operation_id=operation_id)
            operation_identifier = OperationIdentifier(
                operation_id=operation_id,
                sub_type=OperationSubType.PARALLEL,
                parent_id=self._parent_id,
                name=name,
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
        with self._replay_aware():
            # _create_step_id() is thread-safe. rest of method is safe, since using local copy of parent id
            operation_id = self._create_step_id()
            sub_type = (
                config.sub_type
                if config and config.sub_type
                else OperationSubType.RUN_IN_CHILD_CONTEXT
            )

            is_virtual: bool = config.is_virtual if config else False

            def callable_with_child_context():
                return func(
                    self.create_child_context(
                        operation_id=operation_id, is_virtual=is_virtual
                    )
                )

            return child_handler(
                func=callable_with_child_context,
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    sub_type=sub_type,
                    parent_id=self._parent_id,
                    name=step_name,
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
        if not config:
            config = StepConfig()
        with self._replay_aware():
            operation_id = self._create_step_id()
            executor: StepOperationExecutor[T] = StepOperationExecutor(
                func=func,
                config=config,
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    sub_type=OperationSubType.STEP,
                    parent_id=self._parent_id,
                    name=step_name,
                ),
                context_logger=self.logger,
            )
            return executor.process()

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
        with self._replay_aware():
            operation_id = self._create_step_id()
            wait_seconds = duration.seconds
            executor: WaitOperationExecutor = WaitOperationExecutor(
                seconds=wait_seconds,
                state=self.state,
                operation_identifier=OperationIdentifier(
                    operation_id=operation_id,
                    sub_type=OperationSubType.WAIT,
                    parent_id=self._parent_id,
                    name=name,
                ),
            )
            executor.process()

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

        # run_in_child_context is generic and raises ChildContextError with the
        # inner failure on __cause__; translate to callback-typed errors here, at
        # the call site, so it re-fires identically on replay.
        try:
            return self.run_in_child_context(
                wait_in_child_context,
                step_name,
                ChildConfig(sub_type=OperationSubType.WAIT_FOR_CALLBACK),
            )
        except ChildContextError as e:
            inner: BaseException | None = e.__cause__
            # Callback failures (external/timeout/base) pass through.
            if isinstance(inner, CallbackError):
                raise inner
            # A failed submitter step surfaces as CallbackSubmitterError, keeping
            # the step's data/stack_trace (the StepError stays as its cause).
            if isinstance(inner, StepError):
                raise CallbackSubmitterError(
                    inner.message or "Callback submitter failed",
                    data=inner.data,
                    stack_trace=inner.stack_trace,
                ) from inner
            raise

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

        with self._replay_aware():
            operation_id = self._create_step_id()
            executor: WaitForConditionOperationExecutor[T] = (
                WaitForConditionOperationExecutor(
                    check=check,
                    config=config,
                    state=self.state,
                    operation_identifier=OperationIdentifier(
                        operation_id=operation_id,
                        sub_type=OperationSubType.WAIT_FOR_CONDITION,
                        parent_id=self._parent_id,
                        name=name,
                    ),
                    context_logger=self.logger,
                )
            )
            return executor.process()


# endregion Operations

from __future__ import annotations

import contextlib
import copy
import datetime
import functools
import inspect
import logging
import threading
from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, MutableMapping, Protocol, cast

from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    DurableExecutionInvocationOutput,
    ErrorObject,
    InvocationStatus as ServiceInvocationStatus,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType as ServiceOperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.types import LambdaContext


logger = logging.getLogger(__name__)


class InvocationStatus(Enum):
    """Invocation outcomes exposed to instrumentation plugins."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RETRY = "RETRY"


class OperationType(Enum):
    """Durable operation categories exposed to instrumentation plugins."""

    EXECUTION = "EXECUTION"
    CONTEXT = "CONTEXT"
    STEP = "STEP"
    WAIT = "WAIT"
    CALLBACK = "CALLBACK"
    CHAINED_INVOKE = "CHAINED_INVOKE"


def _to_invocation_status(status: ServiceInvocationStatus) -> InvocationStatus:
    return InvocationStatus(status.value)


def _to_operation_type(operation_type: ServiceOperationType) -> OperationType:
    return OperationType(operation_type.value)


def _extract_result(operation: Operation) -> str | None:
    if operation.step_details and operation.step_details.result is not None:
        return operation.step_details.result
    if operation.callback_details and operation.callback_details.result is not None:
        return operation.callback_details.result
    if (
        operation.chained_invoke_details
        and operation.chained_invoke_details.result is not None
    ):
        return operation.chained_invoke_details.result
    if operation.context_details and operation.context_details.result is not None:
        return operation.context_details.result
    return None


def _extract_error(operation: Operation) -> ErrorObject | None:
    if operation.step_details and operation.step_details.error:
        return operation.step_details.error
    if operation.callback_details and operation.callback_details.error:
        return operation.callback_details.error
    if operation.chained_invoke_details and operation.chained_invoke_details.error:
        return operation.chained_invoke_details.error
    if operation.context_details and operation.context_details.error:
        return operation.context_details.error
    return None


@dataclass(frozen=True)
class OperationInfo:
    operation_id: str
    operation_type: OperationType
    sub_type: str | None
    """The recorded subtype as a plain ``str``, or None for the execution root.
    Compare it with ``==`` against an ``OperationSubType`` member, which
    equals its value, or against a caller-defined string. Do not read
    ``.value`` or compare with ``is``."""
    name: str | None
    parent_id: str | None
    start_time: datetime.datetime | None
    is_replayed: bool
    status: OperationStatus
    end_time: datetime.datetime | None = field(default=None, kw_only=True)
    result: str | None = field(
        default=None,
        kw_only=True,
    )
    """The serialized operation result, when available."""
    error: ErrorObject | None = field(
        default=None,
        kw_only=True,
    )
    """The operation error, when available."""
    attempt: int | None = field(default=None, kw_only=True)

    @staticmethod
    def from_operation(
        operation: Operation,
        *,
        is_replayed: bool = False,
    ) -> OperationInfo:
        return OperationInfo(
            operation_id=operation.operation_id,
            operation_type=_to_operation_type(operation.operation_type),
            sub_type=operation.sub_type,
            name=operation.name,
            parent_id=operation.parent_id,
            start_time=operation.start_timestamp,
            end_time=operation.end_timestamp,
            result=_extract_result(operation),
            error=_copy_error(_extract_error(operation)),
            attempt=(
                operation.step_details.attempt if operation.step_details else None
            ),
            is_replayed=is_replayed,
            status=operation.status,
        )


def _copy_error(error: ErrorObject | None) -> ErrorObject | None:
    """Return a plugin-owned copy of an operation error.

    The checkpointed ``ErrorObject`` is handed straight to user code on replay,
    and its ``stack_trace`` is a mutable list. Without a copy a plugin reading
    ``info.operations`` could append to (or clear) that list and change the error
    the execution later raises. Only the list needs cloning -- the other fields
    are immutable strings -- so this is cheaper than a full deep copy.
    """
    if error is None:
        return None
    return ErrorObject(
        message=error.message,
        type=error.type,
        data=error.data,
        stack_trace=(
            list(error.stack_trace) if error.stack_trace is not None else None
        ),
    )


def _to_operation_info_map(
    operations: Mapping[str, Operation],
) -> dict[str, OperationInfo]:
    """Convert a map of checkpointed operations to the plugin ``OperationInfo`` view.

    ``is_replayed`` is left at its default ``False``: these entries describe the
    stored state of an operation, not a replay event for it. Replay is signalled
    through the dedicated operation hooks.
    """
    return {
        operation_id: OperationInfo.from_operation(operation)
        for operation_id, operation in operations.items()
    }


@dataclass(frozen=True)
class OperationStartInfo(OperationInfo):
    pass


@dataclass(frozen=True)
class OperationEndInfo(OperationInfo):
    pass


@dataclass(frozen=True)
class OperationChangeInfo:
    execution_arn: str | None
    updated_operations: dict[str, OperationInfo]
    operations: dict[str, OperationInfo]


class UserFunctionOutcome(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INCOMPLETE = "INCOMPLETE"

    @classmethod
    def from_error(cls, error: ErrorObject | None) -> UserFunctionOutcome:
        if error is None:
            return cls(cls.SUCCEEDED)
        return cls(cls.FAILED)


@dataclass(frozen=True)
class UserFunctionStartInfo(OperationInfo):
    is_replay_children: bool = (
        False  # True if user function is called to replay children (MAP/PARALLEL)
    )


@dataclass(frozen=True)
class UserFunctionEndInfo(OperationInfo):
    is_replay_children: (
        bool  # True if user function is called to replay children (MAP/PARALLEL)
    )
    outcome: UserFunctionOutcome

    @classmethod
    def from_start_info(
        cls,
        start_info: UserFunctionStartInfo,
        error: ErrorObject | None,
        *,
        outcome: UserFunctionOutcome | None = None,
    ) -> UserFunctionEndInfo:
        return UserFunctionEndInfo(
            operation_id=start_info.operation_id,
            operation_type=start_info.operation_type,
            sub_type=start_info.sub_type,
            name=start_info.name,
            parent_id=start_info.parent_id,
            start_time=start_info.start_time,
            is_replayed=start_info.is_replayed,
            status=start_info.status,
            is_replay_children=start_info.is_replay_children,
            attempt=start_info.attempt,
            outcome=(
                outcome
                if outcome is not None
                else UserFunctionOutcome.from_error(error)
            ),
            end_time=datetime.datetime.now(datetime.UTC),
            error=error,
        )


@dataclass(frozen=True)
class InvocationInfo:
    request_id: str | None
    execution_arn: str | None
    is_first_invocation: bool
    execution_start_time: datetime.datetime | None = None
    execution_input: Any = field(
        default=None,
        kw_only=True,
        repr=False,
        compare=False,
        hash=False,
    )
    """The deserialized execution input, when available.

    Surfaced to instrumentation plugins that need to record it (e.g. Durable
    Execution Telemetry). Mirrors the JS SDK's ``InvocationInfo.executionInput``.

    Excluded from ``repr`` on purpose: instrumentation logs hook infos wholesale
    (the bundled OTel plugins at debug level, the plugin example at info), so
    including the payload here would implicitly write customer input -- possibly
    secrets, possibly megabytes -- into logs. Read the attribute explicitly to
    record it.

    Excluded from ``__eq__`` and ``__hash__`` so adding it stays additive. The
    value is arbitrary deserialized JSON, so a dict or list payload would make a
    previously hashable info unhashable, and comparisons against infos built
    from the earlier field set would start returning False.

    Defaults to ``None`` only when the field is not populated (a hook info built
    without it); ``durable_execution()`` always populates it with the
    deserialized input payload, which is ``{}`` when the payload is empty.
    """
    operations: dict[str, OperationInfo] = field(
        default_factory=dict,
        kw_only=True,
        repr=False,
        compare=False,
        hash=False,
    )
    """Checkpointed operations for this execution, keyed by id.

    A point-in-time view of the execution's operation map: as observed at the
    start of the invocation on ``on_invocation_start``, and as observed at the
    end of the invocation on ``on_invocation_end``.

    Not a reliable signal of whether this is the first invocation: the initial
    execution state already carries the ``EXECUTION`` operation, so even a first
    invocation-start sees a non-empty map. Use
    :attr:`is_first_invocation` for that. What a first invocation lacks is prior
    non-execution operations.

    Excluded from ``repr``, ``__eq__`` and ``__hash__`` for the same reasons as
    :attr:`execution_input`: the entries carry operation results and errors that
    instrumentation would otherwise log wholesale, and a mapping-valued field
    would make a previously hashable info unhashable.
    """


@dataclass(frozen=True)
class InvocationStartInfo(InvocationInfo):
    updated_operations: dict[str, OperationInfo] = field(
        default_factory=dict,
        kw_only=True,
        repr=False,
        compare=False,
        hash=False,
    )
    """Operations updated externally while this execution was suspended.

    A wait timer that expired, a callback that was delivered, or a chained
    invoke that completed between the previous invocation and this one. This is
    the subset of :attr:`InvocationInfo.operations` named by the durable
    invocation input's ``UpdatedOperationIds``, so it is empty on the first
    invocation.

    Excluded from ``repr``, ``__eq__`` and ``__hash__`` like
    :attr:`InvocationInfo.operations`.
    """


@dataclass(frozen=True)
class InvocationEndInfo(InvocationInfo):
    status: InvocationStatus = field(kw_only=True)
    error: ErrorObject | None = field(
        default=None,
    )
    """The invocation error, when available."""
    execution_result: str | None = field(
        default=None,
        kw_only=True,
        repr=False,
        compare=False,
        hash=False,
    )
    """The serialized execution result, when available.

    A JSON string, or ``""`` when the result was checkpointed out-of-band for a
    large payload. Mirrors the JS SDK's ``InvocationEndInfo.executionResult``.
    ``None`` on failure or suspend.

    Excluded from ``repr``, ``__eq__`` and ``__hash__`` for the same reasons as
    :attr:`InvocationInfo.execution_input`: hook infos are logged wholesale by
    instrumentation, and adding the field should not change how existing infos
    compare.
    """

    @classmethod
    def from_durable_execution_invocation_output(
        cls,
        invocation_start_info: InvocationStartInfo,
        output: "DurableExecutionInvocationOutput",
        operations: dict[str, OperationInfo] | None = None,
    ):
        return InvocationEndInfo(
            request_id=invocation_start_info.request_id,
            execution_arn=invocation_start_info.execution_arn,
            is_first_invocation=invocation_start_info.is_first_invocation,
            execution_start_time=invocation_start_info.execution_start_time,
            execution_input=invocation_start_info.execution_input,
            # Default to the start-of-invocation view when the caller has no
            # fresher snapshot to offer.
            operations=(
                operations
                if operations is not None
                else invocation_start_info.operations
            ),
            status=_to_invocation_status(output.status),
            error=output.error,
            execution_result=output.result,
        )


@dataclass(frozen=True)
class PropagationInput:
    """SDK-owned identity for a new chained-invoke START propagation request.

    This contract does not contain generated service-model or telemetry types.
    """

    execution_arn: str
    operation_id: str
    target_function_name: str
    parent_operation_id: str | None = None


@dataclass(frozen=True)
class PropagationMetadata:
    """Immutable plugin contribution; members are opaque to the core SDK."""

    x_amzn_trace_id: str | None = None


class DurableInstrumentationPlugin:
    """Base class for plugins. Override only the methods you need.

    **Every hook is a synchronous call on the thread named below, and that
    thread waits for it to return.** No hook runs on the checkpoint thread,
    the one thread that sends every checkpoint of the invocation and applies
    every response. So a slow hook delays the thread it runs on, and never a
    checkpoint of another thread. A plugin that needs slow work, such as a
    network export, does it on its own thread and finishes it in
    ``on_invocation_end``.

    **Where each hook runs.**

    - ``on_invocation_start``, ``on_invocation_end``: the handler thread,
      before the handler starts, and after every operation thread has ended
      and every other hook has returned.
    - ``on_operation_start``, ``on_user_function_start``,
      ``on_user_function_end``: the thread that runs the operation.
    - ``on_operation_end``: the thread that observes the operation's terminal
      result, before the operation returns to user code. For an operation this
      invocation completes, that is the thread that ran it, after its terminal
      checkpoint is confirmed. For an operation completed outside this
      invocation (a wait that elapsed, a callback, a child execution), it is
      the thread that replays the operation when the completion arrived before
      this invocation, or the thread that reads the result when the completion
      arrives in a checkpoint response during it: a branch resumed after a
      refresh, a callback result read. A second thread that observes the same
      result while the hook runs waits for it, so no thread acts on a result
      whose end hook has not returned. The one exception is a read made from
      inside any plugin hook, on the hook's own thread: it neither runs nor
      waits for an end hook. So a result read cannot make the hook that
      makes it wait for another end hook, and a lock a plugin holds across a
      hook cannot be needed by an end hook that read starts. A child context
      that writes no terminal checkpoint (a virtual context, or one in
      ReplayChildren mode) gets it on its thread when its body returns.
    - ``on_operation_end`` for a terminal operation no thread reads in this
      invocation, such as a completion that arrives after the thread that
      would have read it stopped: the handler thread, before
      ``on_invocation_end``. A completed descendant no thread read, such as a
      callback created and never awaited, runs with its ancestor's end hook,
      before it, on the thread that observes the ancestor's result. Within one
      invocation an operation's end hook runs at most once, whichever of
      these threads runs it.
    - ``on_operation_change``: the invocation's plugin change thread.

    **Order.** For one operation, on its thread: ``on_operation_start``
    returns before ``on_user_function_start`` is called;
    ``on_user_function_end`` returns before the terminal checkpoint is sent;
    ``on_operation_end`` returns before the operation returns to user code. So
    on one thread an operation's end hook returns before the next operation's
    start hook is called, and a child's end hook returns before its parent's
    is called. Threads are not ordered with each other: parallel and map
    branches run their hooks concurrently, so a plugin guards state shared
    between hooks with a lock. ``on_operation_change`` is not ordered with the
    end hooks of the same response.

    Exception to child before parent: a map or parallel with an early
    completion policy completes while a branch still runs. A FLAT branch
    whose body then returns runs its end hook on its own thread, after the
    parent's. A branch that attempts another durable operation after the
    parent completed is stopped there and gets no end hook; so is a NESTED
    branch, whose terminal checkpoint is rejected.

    **Change notifications.** ``on_operation_change`` runs on the plugin
    change thread, in response order. One call may cover several checkpoint
    responses: while the plugins are still in the hook, the responses that
    arrive merge into one pending notification. ``updated_operations`` then
    holds the newest state of every operation whose status changed since the
    previous call, and ``operations`` the newest full map. Intermediate
    states and the number of calls are not guaranteed. The pending
    notification is delivered before ``on_invocation_end``. The single pending
    slot bounds the backlog: whatever a plugin's speed, one notification is
    being delivered and at most one is pending. A slow plugin also delays the
    plugins after it in the list for that call, because the thread runs the
    plugins one at a time. If the process cannot start the thread, the SDK
    logs a warning and delivers the one merged notification on the handler
    thread before ``on_invocation_end``.

    **Repeats.** An operation whose result is replayed from its terminal
    checkpoint gets one end hook, in the first invocation that sees the
    terminal checkpoint, and is not reported in a later invocation that
    replays it. The service decides which operations an invocation sees as
    updated; a retried invocation sees the same ones again. A child context
    whose body runs again on replay reports its end on every run: a
    ReplayChildren context (its checkpoint holds a summary, not the result, so
    the body re-runs) with ``is_replayed`` true; a virtual context (no
    checkpoint at all) with ``is_replayed`` false, because it has no
    checkpoint to derive it from.

    **Failures.** An exception a hook raises is logged and ignored, so a
    plugin cannot fail the execution. ``KeyboardInterrupt``, ``SystemExit``
    and ``GeneratorExit`` are the exceptions. On an operation thread and on
    the handler thread, the fallback delivery of a change notification
    included, they propagate from the SDK call that ran the hook. One raised
    by an end hook the handler thread runs before ``on_invocation_end`` does
    not stop the other such hooks; ``on_invocation_end`` is dispatched to
    every plugin before it re-raises. On the plugin change thread they are
    logged, the other plugins still receive the notification, and the next
    notification is still delivered; nothing is carried to another thread.
    No hook runs while the SDK holds a lock.
    """

    def provide_propagation_metadata(
        self,
        info: PropagationInput,
    ) -> PropagationMetadata | None:
        """Synchronously provide metadata without performing a durable operation.

        Called before checkpointing a new invoke START, never for its replay.
        Existing plugins can inherit the no-op default. A failed, uncommitted
        START may collect again on a later attempt.
        """
        return None

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
        Called before an operation's START checkpoint is queued, or when a
        prior non-terminal operation is replayed. This guarantees that it
        strictly precedes ``on_user_function_start``. This is called within
        the thread that runs the operation.

        Args:
            info: Information about the operation.

        """
        pass

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """
        Called when an operation reaches a terminal status, or when a child
        context without a terminal checkpoint returns. An operation with a
        terminal checkpoint is not reported again on replay; a child context
        without one is reported on every replay.

        Runs on the thread that observes the terminal result, before the
        operation returns to user code: the thread that ran the operation, or
        the thread that replays, or reads the result of, an operation
        completed outside this invocation. So a slow hook delays that thread
        only. A terminal operation no thread reads in this invocation gets it
        on the handler thread before ``on_invocation_end``. See the class
        docstring.

        Args:
            info: Information about the operation.
        """
        pass

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        """
        Called when checkpointed operations change after a checkpoint response
        is merged. Runs on the invocation's plugin change thread, in response
        order, never on the checkpoint thread, so it delays no checkpoint. One
        call may cover several responses: ``updated_operations`` holds the
        newest state of every operation changed since the previous call, and
        ``operations`` the newest full map. It is not ordered with the
        ``on_operation_end`` hooks of the same response. See the class
        docstring.

        Args:
            info: Updated operations and the full operation map for the invocation.
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


class DurableInstrumentationPluginFactory(Protocol):
    """Builds one plugin instance for one invocation.

    An object with a method rather than a bare callable, because the SDK will
    grow process-level plugin hooks -- a flush when the execution environment
    shuts down, for example. A callable type has no member to add such a hook to,
    so growing one would have to change the registration type from callable to
    object, which is a second breaking change on the same public surface. One
    method on an object leaves room for an optional second member, which is
    additive.

    Not ``@runtime_checkable``. Three facts decide it. An ``isinstance`` check
    against a runtime-checkable protocol tests only that the member name is
    present, not that it is callable, so it would accept an object whose
    ``create_plugin`` is a string; the SDK needs the stronger test. The SDK also
    has to name what an invalid entry actually was, which a boolean
    ``isinstance`` result cannot supply. And ``isinstance`` against a
    runtime-checkable protocol requires *every* declared member, so publishing
    one would make the optional second member above non-additive for any caller
    who wrote such a check. :func:`plugin_discovery._is_plugin_factory` performs
    the check instead.

    Optional registration metadata belongs to the factory class, so the SDK can
    validate it without constructing an invocation plugin. A class declaring
    ``__durable_registration_api__ = 1`` may declare ``exclusive_group`` and an
    ``on_registration_result(registered)`` callback. Unmarked same-named fields
    are ignored; inherited opted-in groups continue to apply.

    Register the factory, not a plugin::

        class MyPluginFactory:
            def __init__(self, exporter: Exporter) -> None:
                self._exporter = exporter

            def create_plugin(self, info: InvocationStartInfo) -> MyPlugin:
                return MyPlugin(self._exporter)


        plugins = [MyPluginFactory(exporter)]

    A plugin class is not a factory. ``plugins=[MyPlugin]`` used to work because
    calling a class constructs an instance, and it now fails at handler
    initialization because a class carries no ``create_plugin``. A class that
    declares ``create_plugin`` itself -- as a ``@classmethod`` -- does satisfy the
    shape, because the requirement is the member and not the kind of object.

    The factory holds what outlives an invocation: an exporter, a resolved
    configuration, a shared worker. The plugin instance holds what does not.
    Setup work that can fail or that reads the environment belongs in the
    factory's own constructor rather than in the plugin's, because a plugin
    constructor should only assign fields (see ``CONTRIBUTING.md``,
    "Initialization and conversion").
    """

    def create_plugin(
        self, info: InvocationStartInfo, /
    ) -> DurableInstrumentationPlugin:
        """Return the plugin instance that serves the described invocation.

        Called once per invocation, with that invocation's
        :class:`InvocationStartInfo` -- the same object the returned instance's
        ``on_invocation_start`` then receives -- and before any hook fires. The
        instance serves only that invocation and is dropped when it returns, so a
        plugin can hold that invocation's state in ordinary instance attributes
        without keying it by execution ARN.

        Per invocation is narrower than per execution. A durable execution spans
        as many invocations as it waits, retries or resumes, so state a plugin
        leaves in its attributes is gone by the next invocation of the same
        execution. Anything that has to survive that is rebuilt from the operation
        map the invocation hooks carry -- ``InvocationStartInfo.operations`` is a
        full snapshot, including operations that completed in an earlier
        invocation -- or kept on the factory, which outlives every invocation and
        is therefore the caller's to key and to prune.

        ``info`` is positional-only, so an implementation may name the parameter
        whatever reads best; a named protocol parameter would pin that name for
        every implementation.

        A call that raises, or that returns ``None``, is logged and skipped for
        that invocation and never disrupts the execution.

        Args:
            info: The invocation the returned plugin instance will observe.

        Returns:
            The plugin instance for this invocation.
        """
        ...


def _type_name(value: object) -> str:
    """Best available name for a value's type, for log messages.

    Reads the type rather than the value, so no instance ``__getattr__`` or
    ``__getattribute__`` runs, and wraps the lookup, because these names are built
    while a plugin failure is being contained: a name that raises would turn a
    contained failure into a failed execution.
    """
    try:
        return type(value).__qualname__
    except BaseException:  # noqa: BLE001 - a name is never worth failing a hook for
        return "<unnamed>"


def _factory_name(factory: object) -> str:
    """Best available name for a factory, for log messages.

    Read from the factory's *type*, not from the factory. This runs while a
    factory failure is being contained, and a ``getattr`` on the instance would
    call a custom ``__getattr__`` or ``__getattribute__`` -- so a factory whose
    attribute hook raises would make the containment itself raise, turning a
    contained plugin failure into a failed execution. A class registered directly
    as a factory is read through the class object, which carries its own
    ``__qualname__``.

    Wrapped as well, because diagnostics must not be the thing that fails: a name
    that cannot be produced is reported as unavailable rather than raised.
    """
    try:
        if isinstance(factory, type):
            return factory.__qualname__
    except BaseException:  # noqa: BLE001 - a name is never worth failing a hook for
        return "<unnamed factory>"
    return _type_name(factory)


# Raised out of plugin code, these three are not reports of a plugin defect but
# instructions to the thread that is running: stop. Containing one would drop the
# instruction and return a thread that was told to unwind to the work after the
# plugin. They are re-raised; every other BaseException is contained.
#
# asyncio.CancelledError is deliberately NOT here. It derives from BaseException
# and it does mean "stop" for the task that was cancelled, but the task here is
# the SDK's, not the plugin's: nothing cancels the invocation thread or the
# single-worker plugin pool. A CancelledError arriving from plugin code therefore
# came from the plugin's own asyncio use -- an awaited task it let be cancelled --
# which is a plugin defect and belongs on the contained side, or the plugin's
# failure would fail an execution it was only observing.
_PLUGIN_THREAD_CONTROL_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


def _contain_plugin_failure(
    error: BaseException, message: str, *message_args: object
) -> BaseException | None:
    """Log what plugin code raised, and return the part that must not be contained.

    Returns ``None`` when the whole failure was contained, or the part that
    instructs the calling thread to stop, which the caller re-raises.

    A :class:`BaseExceptionGroup` is split rather than tested, because it is
    neither of the two cases a plain ``isinstance`` chain covers: a group carrying
    a :class:`KeyboardInterrupt` is not an instance of one, so a tuple handler
    naming the three does not match it and a broad handler would contain the
    interrupt inside it. Plugin code produces such a group without asking for it
    -- an ``asyncio.TaskGroup`` whose task is interrupted raises one -- so the
    group is partitioned: the control leaves are returned to be re-raised, and
    what remains is logged like any other contained plugin failure.
    """
    control: BaseException | None
    contained: BaseException | None
    if isinstance(error, BaseExceptionGroup):
        control, contained = error.split(_PLUGIN_THREAD_CONTROL_EXCEPTIONS)
    elif isinstance(error, _PLUGIN_THREAD_CONTROL_EXCEPTIONS):
        control, contained = error, None
    else:
        control, contained = None, error
    if contained is not None:
        logger.error(message, *message_args, exc_info=contained)
    return control


class _RecordedEndHook:
    """One recorded end hook and who may run it.

    ``done`` is None while no thread owns the hook. It is an event once a
    thread owns it: the synchronous waiter the checkpoint loop reserved it
    for (``reserved`` True until that waiter arrives), or the thread running
    it. The event completes
    when the hook has run, or when the entry is retired unrun after a
    thread-control exception in an earlier hook of the same claim, so another
    thread that observes the same result waits for it instead of running it
    again or going on without it.

    Internal to the SDK. Not part of the public API.
    """

    __slots__ = ("done", "info", "reserved")

    def __init__(self, info: OperationEndInfo, *, reserved: bool = False) -> None:
        self.info = info
        self.reserved = reserved
        self.done: threading.Event | None = threading.Event() if reserved else None


class _EndHookClaim:
    """What a thread must do after it observed a terminal result: run or wait.

    Built under the registry lock by :meth:`PluginExecutor.take_end_hook`,
    acted on by :meth:`run` outside every lock. ``to_run`` holds the entries
    this thread now owns, descendants first; ``to_wait`` the event of a hook
    another thread owns. Both empty means there is nothing to do.

    Internal to the SDK. Not part of the public API.
    """

    __slots__ = ("_executor", "to_run", "to_wait")

    def __init__(
        self,
        executor: PluginExecutor,
        to_run: list[_RecordedEndHook],
        to_wait: threading.Event | None,
    ) -> None:
        self._executor = executor
        self.to_run = to_run
        self.to_wait = to_wait

    def run(self) -> None:
        if self.to_wait is not None:
            self.to_wait.wait()
        if self.to_run:
            self._executor._run_claimed(self.to_run)  # noqa: SLF001 - same module


def _merge_change_infos(
    pending: OperationChangeInfo, latest: OperationChangeInfo
) -> OperationChangeInfo:
    """Merge a newer change notification into the pending one.

    The result describes both checkpoint responses as one. ``updated_operations``
    holds every operation either notification reported, each in the state of
    the newest full map, so a plugin sees the latest state and never an older
    one after a newer. ``operations`` is the newest full map. The map only
    grows, so it holds every operation the pending notification reported; the
    pending state is used only for an operation the map lacks.
    """
    updated_operations = {
        operation_id: latest.operations.get(operation_id, info)
        for reported in (pending.updated_operations, latest.updated_operations)
        for operation_id, info in reported.items()
    }
    return OperationChangeInfo(
        execution_arn=latest.execution_arn,
        updated_operations=updated_operations,
        operations=latest.operations,
    )


class _ChangeDispatcher:
    """One invocation's thread for ``on_operation_change``, with one pending slot.

    The checkpoint thread submits one notification per response and runs none.
    This thread delivers them in order. A submission that arrives while a
    delivery is in progress merges into the single pending notification
    (:func:`_merge_change_infos`), so a slow plugin holds at most two
    notifications in memory, the one being delivered and the pending one,
    rather than a growing queue. A thread-control exception a hook raises
    here is logged, the other plugins still receive the notification, and the
    next notification is still delivered: this thread has no caller to stop,
    and the SDK has no channel that carries an exception from this thread to
    the handler thread.

    Built by :meth:`start`, which also starts the thread, because starting a
    thread can fail. An instance built directly has no running thread:
    :meth:`join` then delivers the pending notification on the calling thread.
    That is the fallback when the process cannot start a thread.

    Internal to the SDK. Not part of the public API.
    """

    def __init__(self, executor: PluginExecutor) -> None:
        self._executor = executor
        self._pending: OperationChangeInfo | None = None
        self._stopping = False
        self._condition = threading.Condition()
        self._thread = threading.Thread(
            target=self._run, name="plugin-change", daemon=True
        )
        self._thread_started = False

    @classmethod
    def start(cls, executor: PluginExecutor) -> _ChangeDispatcher:
        """Create a dispatcher and start its thread. Raises RuntimeError when it cannot start."""
        dispatcher = cls(executor)
        dispatcher._thread.start()
        dispatcher._thread_started = True
        return dispatcher

    def submit(self, info: OperationChangeInfo) -> None:
        """Queue a notification; merge it into the pending one if there is one."""
        with self._condition:
            self._pending = (
                info
                if self._pending is None
                else _merge_change_infos(self._pending, info)
            )
            self._condition.notify()

    def join(self) -> None:
        """Deliver what is pending, stop the thread, return once nothing is pending.

        Without a running thread, the pending notification is delivered here,
        on the calling thread, and a thread-control exception a hook raises
        propagates to the caller.
        """
        with self._condition:
            self._stopping = True
            self._condition.notify()
        if self._thread_started:
            self._thread.join()
        info = self._take()
        if info is not None:
            # On the calling thread, the handler's: a thread-control exception
            # propagates here, as every hook on that thread does.
            self._executor.execute_plugins(info, finish_fan_out=True)

    def _take(self) -> OperationChangeInfo | None:
        with self._condition:
            info, self._pending = self._pending, None
            return info

    def _run(self) -> None:
        while True:
            with self._condition:
                while self._pending is None and not self._stopping:
                    self._condition.wait()
                info, self._pending = self._pending, None
            if info is None:
                # Stopping, and nothing left to deliver.
                return
            self._deliver(info)

    def _deliver(self, info: OperationChangeInfo) -> None:
        try:
            # finish_fan_out: the other plugins still receive this notification.
            self._executor.execute_plugins(info, finish_fan_out=True)
        except BaseException:  # noqa: BLE001 - only thread control reaches here
            # execute_plugins contains every other failure. Nothing on this
            # thread can be stopped by the interrupt, so it is logged and the
            # next notification is still delivered.
            logger.exception(
                "Plugin on_operation_change hook raised on the plugin change "
                "thread; ignored"
            )


class PluginExecutor:
    """One invocation's plugin instances, metadata and dispatch.

    Scoped to a single invocation, not to the handler. Everything mutable here --
    the instances built for this invocation, the start info the end hook derives
    from, the operations provider and startup contexts -- describes one invocation,
    so a single instance shared by two of them would let each overwrite the
    other's state. Concurrent executions in one environment (Lambda Managed
    Instances) are exactly that case: they run in separate threads of one
    process, against one decorated handler. The decorator and :class:`PluginHost`
    therefore build a fresh executor in each invocation's frame.

    Single-use by construction: :meth:`run` refuses a second entry, so the
    lifetime is an invariant of the class rather than a convention its callers
    have to keep. Only the factory list is handler-lifetime, and it is copied in
    rather than shared mutably.
    """

    def __init__(self, plugins: list[DurableInstrumentationPluginFactory] | None):
        # Factories outlive this executor -- the list is copied, never aliased.
        # The instances they build do not: _plugins is populated in
        # on_invocation_start and emptied when the invocation scope exits.
        self._plugin_factories = list(plugins or [])
        self._plugins: list[DurableInstrumentationPlugin] = []
        # The subset of _plugins whose invocation-start hook has been dispatched.
        # Every later hook is dispatched to this list, so a plugin that never
        # received its start hook never receives its end hook.
        self._started: list[DurableInstrumentationPlugin] = []
        self._invocation_status: InvocationStartInfo | None = None
        self._operations_provider: Callable[[], Mapping[str, Operation]] | None = None
        self._run_entered = False
        # End hooks recorded by the checkpoint thread and not yet finished,
        # keyed by operation id, in recording order. The thread that observes
        # an operation's terminal result claims and runs its hook; a second
        # thread that observes the result meanwhile waits for that hook. What
        # no thread claims runs on the handler thread before on_invocation_end.
        self._end_hooks: dict[str, _RecordedEndHook] = {}
        # Operations whose end hook this invocation has run or retired, by any
        # path. The replay path reports a terminal operation the invocation
        # input carried, and a checkpoint response can list that operation
        # too; the set keeps the end hook to one per operation per invocation.
        self._ended: set[str] = set()
        # Held while recording end hooks and while deciding which thread runs
        # one. A reader holds it across its read of the operation state and
        # its decision, so no other thread's claim comes between the two.
        self.claim_lock = threading.Lock()
        # Per thread: how many plugin hooks the thread is inside right now.
        # execute_plugins and provide_propagation_metadata, the two calls
        # into a plugin's hook methods, count their call. A claim decision
        # made by a thread inside a hook returns at once, so a read a plugin
        # makes from any hook neither runs nor waits for an end hook. See
        # take_end_hook.
        self._hook_depth = threading.local()
        # The thread for on_operation_change. Created by the first change
        # notification, joined before on_invocation_end.
        self._dispatcher: _ChangeDispatcher | None = None
        self._dispatcher_lock = threading.Lock()

    def provide_propagation_metadata(
        self, info: PropagationInput
    ) -> PropagationMetadata:
        """Collect opaque metadata synchronously in configured plugin order.

        The first non-blank value wins. Ordinary plugin failures are isolated,
        matching lifecycle dispatch; BaseException cancellation/control signals
        retain the existing propagation policy. The caller attaches the result
        to its operation model before checkpoint serialization.
        """
        value: str | None = None
        owner: str | None = None
        conflict_count = 0
        for plugin in self._plugins:
            # Read type identity without invoking instance attribute access.
            identity = "unknown plugin"
            try:
                plugin_type = type(plugin)
                identity = type.__getattribute__(plugin_type, "__qualname__")
                module = type.__getattribute__(plugin_type, "__module__")
                if type(module) is str:
                    identity = ".".join((module, identity))
            except Exception:
                pass
            try:
                with self._in_hook_scope():
                    metadata = plugin.provide_propagation_metadata(info)
                if metadata is None:
                    continue
                if inspect.iscoroutine(metadata):
                    metadata.close()
                    raise TypeError("propagation metadata hook must be synchronous")
                if not isinstance(metadata, PropagationMetadata):
                    raise TypeError("expected PropagationMetadata or None")
                candidate = metadata.x_amzn_trace_id
                if candidate is not None:
                    if not isinstance(candidate, str):
                        raise TypeError("x_amzn_trace_id must be a string or None")
                    # Treat str subclasses as data, not executable comparison
                    # hooks during first-value/conflict aggregation.
                    candidate = str.__str__(candidate)
            except Exception:
                self._log_propagation_diagnostic(
                    logging.ERROR,
                    "Plugin %s propagation metadata failure ignored",
                    identity,
                    exc_info=True,
                )
                continue
            if candidate is None or not candidate.strip():
                continue
            if value is None:
                value, owner = candidate, identity
            elif candidate != value:
                conflict_count += 1
                self._log_propagation_diagnostic(
                    logging.WARNING,
                    "Propagation metadata conflict between %s and %s for "
                    "x_amzn_trace_id; keeping first value (conflict_count=%d)",
                    owner,
                    identity,
                    conflict_count,
                )
        return PropagationMetadata(x_amzn_trace_id=value)

    @staticmethod
    def _log_propagation_diagnostic(
        level: int,
        message: str,
        *args: object,
        exc_info: bool = False,
    ) -> None:
        """Keep ordinary diagnostic failures from changing propagation collection."""
        try:
            logger.log(level, message, *args, exc_info=exc_info)
        except Exception:
            pass

    @property
    def has_plugins(self) -> bool:
        """Whether this invocation has any registered plugin factories."""
        return bool(self._plugin_factories)

    @contextlib.contextmanager
    def run(self):
        """Open this executor's one invocation scope.

        Raises:
            RuntimeError: if entered more than once. A second entry would mean an
                executor is serving two invocations, which is the shape this
                class exists to prevent; failing loudly here keeps the bug from
                reappearing as silent crosstalk.
        """
        if self._run_entered:
            msg = (
                "PluginExecutor.run() is single-use: this executor has already "
                "served an invocation. Build one executor per invocation."
            )
            raise RuntimeError(msg)
        self._run_entered = True
        try:
            yield
        finally:
            # on_invocation_end has run the leftover hooks on every path that
            # reaches it. This covers a scope that ends before it is called.
            self._finish_hooks()
            self._invocation_status = None
            self._operations_provider = None
            # Drop this invocation's plugin instances: nothing outlives the
            # invocation, and the change thread has exited.
            self._plugins = []
            self._started = []

    def record_end_hooks(
        self, operations: Sequence[Operation], waited_for: Collection[str] = ()
    ) -> None:
        """Record the end hook of every terminal operation a checkpoint response reports.

        The checkpoint thread calls this right before the response's
        operations become readable, and runs no hook.
        Recording before publishing is what makes a later claim reliable: a
        thread that reads a terminal operation finds its hook recorded.

        ``waited_for`` names the operations whose synchronous waiter the
        response is about to wake. Their hooks are reserved for that waiter,
        which claims them in ``create_checkpoint`` before the operation
        returns; a thread that reads such a result meanwhile waits for the
        hook, and an ancestor's claim leaves it alone. Every other hook is
        unowned: the thread that reads the result claims it, or the handler
        thread at the end of the invocation (:meth:`drain_end_hooks`).

        An operation reported again while its hook is unowned keeps one entry,
        with the newest state; one reported while its hook is owned, or after
        it ran, is not recorded again.
        """
        if not self._plugin_factories:
            return
        infos = [
            self._end_info(operation)
            for operation in operations
            if self._is_terminal_status(operation.status)
        ]
        if not infos:
            return
        with self.claim_lock:
            for info in infos:
                if info.operation_id in self._ended:
                    continue
                recorded = self._end_hooks.get(info.operation_id)
                if recorded is None:
                    self._end_hooks[info.operation_id] = _RecordedEndHook(
                        info, reserved=info.operation_id in waited_for
                    )
                elif recorded.done is None:
                    recorded.info = info

    def claim_end_hook(self, operation_id: str, *, waiter: bool = False) -> None:
        """Run the recorded end hook of one operation on the calling thread, if any.

        Called by a thread that observes the operation's terminal result,
        before it acts on the result: ``waiter`` is True for the synchronous
        waiter in ``create_checkpoint``, False for a reader. An unowned hook
        runs here; so does one reserved for this waiter. A hook another
        thread owns is waited for, so no thread acts on the result before the
        hook has returned. A call from inside any plugin hook returns at
        once, running and waiting for nothing: a read a plugin makes from a
        hook inspects the result and takes no part in the ordering, so a
        hook that reads its own operation does not wait for itself, and
        hooks cannot wait on each other. No entry means nothing happens:
        that is every read of an operation completed before this invocation,
        and of one whose hook already ran.

        Unowned terminal descendants of the operation run first, deepest
        first. A response can carry a completed descendant no thread read,
        such as a callback created and not awaited, together with its
        parent's completion; running it here keeps a child's end hook before
        its parent's, as on the thread that reads each result.
        """
        with self.claim_lock:
            claim = self.take_end_hook(operation_id, waiter=waiter)
        if claim is not None:
            claim.run()

    def take_end_hook(
        self, operation_id: str, *, waiter: bool = False
    ) -> _EndHookClaim | None:
        """Decide, under ``claim_lock``, what the calling thread does for an operation.

        The caller holds ``claim_lock`` and calls :meth:`_EndHookClaim.run`
        after releasing it. ``get_checkpoint_result`` holds the lock from its
        read of the operation state through this decision, and every other
        claim is made under the same lock, so no claim is decided between a
        read and the decision that follows it. Returns None when there is
        nothing to do.

        None also for a thread that is inside any plugin hook. Such a
        thread must not wait: an end hook completes its event only when it
        returns, so two hooks that read each other's operations would wait
        on each other forever, and a change hook that waits for an end hook
        while holding a lock that hook needs would do the same. It must not
        run a hook either, which would nest one hook in another. The hook
        stays recorded for the read that consumes the result, or for the
        drain.
        """
        if not self._plugin_factories or self._in_hook():
            return None
        recorded = self._end_hooks.get(operation_id)
        if recorded is None:
            return None
        if recorded.done is not None and not (waiter and recorded.reserved):
            return _EndHookClaim(self, [], recorded.done)
        return _EndHookClaim(self, self._claim_tree_locked(recorded), None)

    def _in_hook(self) -> bool:
        """True when the calling thread is inside a plugin hook this executor dispatched."""
        return getattr(self._hook_depth, "value", 0) > 0

    @contextlib.contextmanager
    def _in_hook_scope(self) -> Iterator[None]:
        """Mark the calling thread as inside a plugin hook for the duration."""
        self._hook_depth.value = getattr(self._hook_depth, "value", 0) + 1
        try:
            yield
        finally:
            self._hook_depth.value -= 1

    def drain_end_hooks(self) -> None:
        """Run every recorded end hook no thread ran, here, on the calling thread.

        Called on the handler thread once every operation thread has ended
        and the checkpoint thread has stopped, so nothing is recorded after
        it and no owner is still coming. These are terminal operations no
        thread observed in this invocation: a completion that arrived after
        the thread that would have read it stopped, or a branch stopped by
        its parent's early completion. The earliest recorded entry goes
        first, with its unowned descendants before it, then the next.

        A thread-control exception from one hook is held, the other trees
        still run (the entries of its own tree not reached are retired, see
        :meth:`_run_claimed`), and the first one raised is re-raised once
        nothing is left. Stopping at the first would leave the other trees
        recorded, and the executor's scope exit would then run them after
        ``on_invocation_end``.
        """
        held_control: BaseException | None = None
        while True:
            with self.claim_lock:
                recorded = next(
                    (
                        entry
                        for entry in self._end_hooks.values()
                        if entry.done is None or entry.reserved
                    ),
                    None,
                )
                if recorded is None:
                    break
                claimed = self._claim_tree_locked(recorded)
            try:
                self._run_claimed(claimed)
            except BaseException as control:  # noqa: BLE001 - execute_plugins contains the rest
                if held_control is None:
                    held_control = control
        if held_control is not None:
            raise held_control

    def _claim_tree_locked(self, root: _RecordedEndHook) -> list[_RecordedEndHook]:
        """Take an entry and its unowned descendants for this thread. Caller holds the lock.

        Returns them in the order to run: every descendant before its
        ancestor, the root last. Descendants are found through the recorded
        parent ids, so only a descendant whose recorded ancestors are all
        unowned is reached. A descendant another thread owns is left to that
        thread: the two threads are not ordered with each other.
        """
        unowned = [entry for entry in self._end_hooks.values() if entry.done is None]
        ordered = [root]
        taken = {root.info.operation_id}
        index = 0
        while index < len(ordered):
            parent_id = ordered[index].info.operation_id
            for entry in unowned:
                if (
                    entry.info.parent_id == parent_id
                    and entry.info.operation_id not in taken
                ):
                    taken.add(entry.info.operation_id)
                    ordered.append(entry)
            index += 1
        for entry in ordered:
            if entry.done is None:
                entry.done = threading.Event()
            entry.reserved = False
        ordered.reverse()
        return ordered

    def _run_claimed(self, claimed: list[_RecordedEndHook]) -> None:
        """Run the hooks this thread took, then retire each entry and release its waiters.

        A thread-control exception from one hook propagates at once; the
        entries not reached are retired without running, so no thread waits
        on them forever, and they are not reported later either.
        """
        for index, entry in enumerate(claimed):
            try:
                self.execute_plugins(entry.info)
            except BaseException:
                for skipped in claimed[index + 1 :]:
                    self._retire(skipped)
                raise
            finally:
                self._retire(entry)

    def _retire(self, entry: _RecordedEndHook) -> None:
        with self.claim_lock:
            self._end_hooks.pop(entry.info.operation_id, None)
            self._ended.add(entry.info.operation_id)
        if entry.done is not None:
            entry.done.set()

    def _run_end_hook_once(self, info: OperationEndInfo) -> None:
        """Run an end hook on the calling thread unless this invocation already ran one for the operation.

        The replay path uses this: it reports an operation the invocation
        input listed as updated, which a checkpoint response may also have
        recorded. Both paths go through the registry, so the operation ends
        once, and a thread on either path waits for a hook the other is
        running.
        """
        with self.claim_lock:
            if info.operation_id in self._ended:
                return
            if info.operation_id not in self._end_hooks:
                self._end_hooks[info.operation_id] = _RecordedEndHook(info)
            claim = self.take_end_hook(info.operation_id)
        if claim is not None:
            claim.run()

    def _submit_change(self, info: OperationChangeInfo) -> None:
        """Queue a change notification for the plugin change thread. Runs nothing here."""
        with self._dispatcher_lock:
            dispatcher = self._dispatcher_locked()
        dispatcher.submit(info)

    def record_change(
        self,
        updated_operations: Sequence[Operation],
        operations: Mapping[str, Operation],
        previous_operations: Mapping[str, Operation],
    ) -> None:
        """Queue one checkpoint response's change hook for the plugin change thread.

        The checkpoint thread calls this after the response's operations are
        published and before it wakes the waiters, and runs no hook. Nothing
        is queued when no operation changed status.
        """
        info = self._change_info(updated_operations, operations, previous_operations)
        if info is not None:
            self._submit_change(info)

    def _dispatcher_locked(self) -> _ChangeDispatcher:
        """The invocation's change dispatcher, created on first use. Caller holds the lock."""
        if self._dispatcher is None:
            try:
                self._dispatcher = _ChangeDispatcher.start(self)
            except RuntimeError:
                # The process cannot start a thread. Observability must not
                # fail the execution, so the notifications merge into one that
                # on_invocation_end delivers on the handler thread.
                logger.warning(
                    "Plugin change thread could not be started; "
                    "on_operation_change runs once on the handler thread "
                    "before on_invocation_end",
                    exc_info=True,
                )
                self._dispatcher = _ChangeDispatcher(self)
        return self._dispatcher

    def _finish_hooks(self) -> None:
        """Run the unclaimed end hooks, then deliver every pending change notification.

        Called on the handler thread once the checkpoint thread has stopped,
        so nothing is recorded or submitted after this returns. The change
        thread is joined even if a drained hook raises.
        """
        try:
            self.drain_end_hooks()
        finally:
            with self._dispatcher_lock:
                dispatcher, self._dispatcher = self._dispatcher, None
            if dispatcher is not None:
                dispatcher.join()

    def _create_plugins(self, info: InvocationStartInfo) -> None:
        """Build this invocation's plugin instances from its start info.

        Called once per invocation, before the first hook is dispatched. A
        factory whose ``create_plugin`` raises or returns ``None`` is contained
        exactly as a failing hook is -- logged and skipped -- so a broken plugin
        cannot disrupt the execution. The remaining factories still produce their
        instances.

        An entry without a usable ``create_plugin`` raises ``AttributeError``
        here, which this containment then swallows once per invocation.
        :func:`plugin_discovery.load_configured_plugins` rejects such an entry
        while the handler is being initialized, so the silent case is not
        reachable through ``durable_execution()``.

        Containment covers every ``BaseException`` except the parts that instruct
        the calling thread to stop; see
        :data:`_PLUGIN_THREAD_CONTROL_EXCEPTIONS` and
        :func:`_contain_plugin_failure`. Narrowing it to ``Exception`` left the
        contract conditional on a factory never raising outside that hierarchy,
        and a factory that awaits a cancelled task raises
        ``asyncio.CancelledError``, which is outside it.
        """
        plugins: list[DurableInstrumentationPlugin] = []
        for factory in self._plugin_factories:
            try:
                plugin = factory.create_plugin(info)
                # isinstance can call a proxy's __class__ descriptor. Its failures
                # belong to the same customer-code boundary as factory creation.
                is_plugin = isinstance(plugin, DurableInstrumentationPlugin)
            except BaseException as error:  # noqa: BLE001 - a factory must not fail the execution
                control = _contain_plugin_failure(
                    error, "Plugin factory %s exception ignored", _factory_name(factory)
                )
                if control is not None:
                    raise control from None
                continue
            if plugin is None:
                logger.error(
                    "Plugin factory %s returned None; plugin ignored",
                    _factory_name(factory),
                )
                continue
            # The load-time shape check can only establish that the factory has
            # a callable create_plugin; what that call returns is knowable only
            # here. A value that is not a plugin fails every hook inside
            # _dispatch_plugin, so registering it would produce one logged error
            # per hook per invocation for the life of the function while
            # providing no telemetry. Reject it once instead.
            if not is_plugin:
                logger.error(
                    "Plugin factory %s returned %s, which is not a "
                    "DurableInstrumentationPlugin; plugin ignored",
                    _factory_name(factory),
                    _type_name(plugin),
                )
                continue
            plugins.append(plugin)
        self._plugins = plugins

    @staticmethod
    def _dispatch_plugin(plugin: DurableInstrumentationPlugin, info) -> None:
        """Invoke a plugin on the calling thread, preserving its context changes.

        Thread-control exceptions propagate; other failures are contained using
        the same rule as factory creation. Plugins own cleanup of any partial
        bindings left by a failed hook.
        """
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
                case OperationChangeInfo():
                    plugin.on_operation_change(info)
                case UserFunctionStartInfo():
                    plugin.on_user_function_start(info)
                case UserFunctionEndInfo():
                    plugin.on_user_function_end(info)
                case _:
                    raise RuntimeError(f"Unknown info type: {type(info)}")
        except BaseException as error:  # noqa: BLE001 - a hook must not fail the execution
            control = _contain_plugin_failure(
                error, "Plugin %s exception ignored", plugin.__class__.__name__
            )
            if control is not None:
                raise control from None

    def execute_plugins(self, info, *, finish_fan_out: bool = False):
        """Dispatch one hook to this invocation's plugins.

        A plugin receives a hook only once it has received the invocation-start
        hook, which makes the pairing an invariant rather than a coincidence.
        Without it one dispatch order breaks the pairing: a start hook that raises
        one of the three exceptions :data:`_PLUGIN_THREAD_CONTROL_EXCEPTIONS`
        names propagates out of this loop, so plugins later in the list never
        receive their start hook -- and the invocation-end hook that the
        propagating exception then triggers used to reach them anyway, leaving a
        plugin to tear down state it had never been told to build.

        A plugin is counted as started before its start hook is dispatched rather
        than after, because a hook that begins and then fails may already have
        allocated what its end hook releases.

        The invocation-end hook is the one hook that finishes dispatching even
        when a plugin raises one of those. Every plugin it reaches has already
        started, so cutting the loop short costs a plugin its only chance to
        finish: Telemetry would not drain, and OTel would leave spans unended. The
        first such exception is held and re-raised once every plugin has been
        called, so the thread still stops and nothing is swallowed. A caller
        passes ``finish_fan_out=True`` to get the same treatment for another
        hook; the change thread does, so that one plugin's interrupt does not
        cost the other plugins a notification. No other hook defers: stopping a
        start-hook loop early leaves later plugins with nothing to clean up,
        because the pairing rule above then withholds their end hook too.

        Anything :meth:`_dispatch_plugin` raises is already a thread-control
        failure -- it contains everything else -- so the end path catches
        ``BaseException`` rather than naming the three again. Naming them would
        miss a :class:`BaseExceptionGroup` carrying one, which is what
        :func:`_contain_plugin_failure` hands back.

        Every hook is dispatched on the calling thread. That is what lets a
        plugin set a ``ThreadLocal`` or an MDC key the SDK's own logging then
        reads, and it is what makes the pairing and re-raise rules above
        enforceable: a hook dispatched to a pool would land in a
        :class:`~concurrent.futures.Future` nobody reads, so a control exception
        raised there would be swallowed and the end-hook fan-out could not hold
        it. An earlier ``sync`` parameter offered the pool path; no caller ever
        passed it, and it is removed rather than left as a way to opt out of
        those rules. The hooks built on the checkpoint thread are not dispatched
        there: the checkpoint loop records them, and the thread that claims an
        end hook, or the plugin change thread, makes this call, so that thread
        is the caller and the pairing and re-raise rules above hold there.

        The calling thread is marked as inside a plugin hook for the whole
        fan-out, so a result read a hook makes neither runs nor waits for an
        end hook (see :meth:`take_end_hook`). This and
        :meth:`provide_propagation_metadata` are the two calls into a
        plugin's hook methods.
        """
        if not self._plugin_factories:
            return
        starting = isinstance(info, InvocationStartInfo)
        ending = finish_fan_out or isinstance(info, InvocationEndInfo)
        deferred_control: BaseException | None = None
        with self._in_hook_scope():
            for plugin in self._plugins if starting else self._started:
                if starting:
                    # Count a hook before dispatch: even a failed Start can own
                    # state that only its End hook can release. All lifecycle
                    # hooks share the invocation worker's Context, including
                    # after a failure.
                    self._started.append(plugin)
                if not ending:
                    self._dispatch_plugin(plugin, info)
                    continue
                try:
                    self._dispatch_plugin(plugin, info)
                except BaseException as control:  # noqa: BLE001 - held and re-raised below
                    if deferred_control is None:
                        deferred_control = control
        if deferred_control is not None:
            raise deferred_control

    def _snapshot_operation_infos(
        self,
        operations_provider: Callable[[], Mapping[str, Operation]] | None,
    ) -> dict[str, OperationInfo]:
        """Build the plugin ``OperationInfo`` view of the current operation map.

        Returns a plain ``dict``, matching :class:`OperationChangeInfo`. That
        matters beyond consistency: ``dataclasses.asdict()`` and ``pickle`` only
        traverse real dicts, so a custom ``Mapping`` here would leave the
        enclosing hook info unserializable for the very plugins these fields
        exist to serve.

        Built eagerly, which also pins the point in time the hook reports: a
        plugin that stashes the info and reads it later still sees the state as
        of its own hook.

        Skipped entirely when no plugins are configured -- ``durable_execution()``
        passes a provider unconditionally, so without this gate a plugin-free
        execution would pay for a view nothing can read. The gate reads the
        factory list, not the instances: this runs while the start info is being
        built, before any instance exists.
        """
        if not self._plugin_factories or operations_provider is None:
            return {}
        try:
            return _to_operation_info_map(operations_provider())
        except Exception:
            # A plugin-facing view must never break the execution.
            logger.exception("Failed to snapshot operations for plugin hook")
            return {}

    def on_invocation_start(
        self,
        execution_arn: str,
        is_first_invocation: bool,
        execution_start_time: datetime.datetime | None,
        lambda_context: LambdaContext | None,
        execution_input: Any = None,
        operations_provider: Callable[[], Mapping[str, Operation]] | None = None,
        updated_operation_ids: Sequence[str] | None = None,
    ) -> None:
        """Fire the invocation-start hook.

        Args:
            execution_arn: ARN of the durable execution.
            is_first_invocation: False when prior operations exist (a replay).
            execution_start_time: Start timestamp of the execution operation.
            lambda_context: Lambda context, for the request id.
            execution_input: The deserialized execution input event.
            operations_provider: Returns the current checkpointed operation map,
                converted here into the plugin's ``OperationInfo`` view.
            updated_operation_ids: Operation ids from the invocation input's
                ``UpdatedOperationIds`` -- those updated while suspended.
        """
        aws_request_id = lambda_context.aws_request_id if lambda_context else None
        self._operations_provider = (
            operations_provider if self._plugin_factories else None
        )
        operations = self._snapshot_operation_infos(operations_provider)
        self._invocation_status = InvocationStartInfo(
            execution_arn=execution_arn,
            request_id=aws_request_id,
            is_first_invocation=is_first_invocation,
            execution_start_time=execution_start_time,
            execution_input=self._snapshot_execution_input(execution_input),
            operations=operations,
            updated_operations={
                operation_id: operations[operation_id]
                for operation_id in (updated_operation_ids or [])
                if operation_id in operations
            },
        )
        # Build this invocation's plugin instances from the very info their first
        # hook receives, and before that hook is dispatched.
        self._create_plugins(self._invocation_status)
        self.execute_plugins(self._invocation_status)

    def _snapshot_execution_input(self, execution_input: Any) -> Any:
        """Deep-copy the execution input so the plugin view is isolated.

        ``durable_execution()`` hands the same mutable object to the user handler
        and to this hook. Without a copy the aliasing runs both ways: a plugin
        mutating ``info.execution_input`` would change the handler's event and so
        alter execution behaviour, and a handler mutating its event would change
        what this frozen info -- and the invocation-end info derived from it --
        reports afterwards.

        The copy is eager rather than deferred: the handler starts running
        immediately after this hook, so a lazily-taken snapshot could already
        have observed the handler's mutations. It is skipped when no plugins are
        configured, so non-plugin executions pay nothing.

        The snapshot is shared by all plugins for this invocation; plugins should
        still treat it as read-only with respect to each other.
        """
        if not self._plugin_factories or execution_input is None:
            return execution_input
        try:
            return copy.deepcopy(execution_input)
        except Exception:
            # Preserve handler isolation if a snapshot cannot be created.
            logger.exception(
                "Failed to copy execution input for plugins; omitting plugin input"
            )
            return None

    def on_invocation_end(
        self,
        output: "DurableExecutionInvocationOutput",
    ) -> None:
        if self._invocation_status is None:
            # on_invocation_start not called, skip
            return

        # Every other hook of this invocation has run before the end hook: the
        # operation threads have ended, the end hooks no thread claimed run
        # here, and the change thread is joined. A thread-control exception a
        # drained hook raises on this thread is held, so every plugin still
        # receives its end hook, and re-raised after it.
        held_control: BaseException | None = None
        try:
            self._finish_hooks()
        except BaseException as control:  # noqa: BLE001 - execute_plugins contains the rest
            held_control = control

        # Re-read the operation map so the end hook sees the state as of the end
        # of this invocation, not the snapshot taken at its start.
        invocation_end_info = (
            InvocationEndInfo.from_durable_execution_invocation_output(
                self._invocation_status,
                output,
                operations=self._snapshot_operation_infos(self._operations_provider),
            )
        )
        try:
            self.execute_plugins(invocation_end_info)
        except BaseException:
            if held_control is not None:
                # Both are thread control. The drained hook's came first; the
                # end hook's is chained as its context.
                raise held_control  # noqa: B904 - chained below as __context__
            raise
        if held_control is not None:
            raise held_control

    def on_user_function_start(
        self,
        operation_identifier: OperationIdentifier,
        is_replay_children: bool = False,
        attempt: int | None = None,
    ) -> UserFunctionStartInfo:
        """Execute any registered plugins for the operation when its user function starts to execute."""
        start_info = UserFunctionStartInfo(
            operation_id=operation_identifier.operation_id,
            operation_type=_to_operation_type(operation_identifier.type),
            sub_type=operation_identifier.sub_type,
            name=operation_identifier.name,
            parent_id=operation_identifier.parent_id,
            start_time=datetime.datetime.now(datetime.UTC),
            is_replayed=False,
            status=OperationStatus.STARTED,
            is_replay_children=is_replay_children,
            attempt=attempt,
        )
        self.execute_plugins(start_info)
        return start_info

    def on_user_function_end(
        self,
        start_info: UserFunctionStartInfo,
        error,
        *,
        outcome: UserFunctionOutcome | None = None,
    ) -> None:
        """Execute plugins when a user function returns, fails, or is incomplete."""
        self.execute_plugins(
            UserFunctionEndInfo.from_start_info(start_info, error, outcome=outcome),
        )

    def on_operation_action(
        self,
        update: OperationUpdate,
        operation: Operation | None = None,
        previous_operation: Operation | None = None,
    ):
        """Execute registered plugins before an operation START is queued.

        Args:
            update: The operation update being checkpointed.
            operation: the operation after the checkpoint
            previous_operation: the operation before the checkpoint
        """
        if update.action is OperationAction.START:
            # we handle only START action here because on_operation_update may not be able to see a STARTED update
            # when START is checkpointed in batch with terminal status updates.
            self.execute_plugins(
                OperationStartInfo(
                    operation_id=update.operation_id,
                    operation_type=_to_operation_type(update.operation_type),
                    sub_type=update.sub_type,
                    name=update.name,
                    parent_id=update.parent_id,
                    start_time=operation.start_timestamp if operation else None,
                    is_replayed=previous_operation is not None,
                    status=OperationStatus.STARTED,
                ),
            )

    def on_operation_replay(self, operation: Operation) -> None:
        """Execute plugins for a non-terminal operation observed during replay."""
        if self._is_terminal_status(operation.status):
            return

        start_info = OperationStartInfo(
            operation_id=operation.operation_id,
            operation_type=_to_operation_type(operation.operation_type),
            sub_type=operation.sub_type,
            name=operation.name,
            parent_id=operation.parent_id,
            start_time=operation.start_timestamp,
            is_replayed=True,
            status=operation.status,
        )
        self.execute_plugins(start_info)

    def on_child_context_end(
        self,
        operation_identifier: OperationIdentifier,
        status: OperationStatus,
        *,
        error: ErrorObject | None = None,
        is_replayed: bool = False,
    ) -> None:
        """Execute plugins for a child context that completed without a checkpoint."""
        now = datetime.datetime.now(datetime.UTC)
        self.execute_plugins(
            OperationEndInfo(
                operation_id=operation_identifier.operation_id,
                operation_type=_to_operation_type(operation_identifier.type),
                sub_type=operation_identifier.sub_type,
                name=operation_identifier.name,
                parent_id=operation_identifier.parent_id,
                start_time=None,
                end_time=now,
                status=status,
                error=_copy_error(error),
                is_replayed=is_replayed,
            ),
        )

    def on_operation_update(
        self,
        operation_or_operations: Operation | Sequence[Operation] | None,
        operations: Mapping[str, Operation] | None = None,
        previous_operations: Mapping[str, Operation] | None = None,
    ):
        """Run the end hooks for operation updates here, and queue the change hook.

        Called for an operation that changed while the execution was suspended,
        by the thread that replays it; the end hook runs on that thread. The
        change hook, built only when both operation maps are given, goes to
        the plugin change thread. Checkpoint responses during the invocation
        take the other path: the checkpoint thread records their hooks with
        :meth:`record_end_hooks` and :meth:`record_change`, and runs none.

        Updates such as STARTED might be omitted because START and completion action (e.g. SUCCEED/FAIL) may be
        checkpointed in batch and the backend returns only the terminal status (e.g. SUCCEEDED/PENDING/FAILED).

        Note: the operation may not be up-to-date if the checkpoint is called asynchronously.

        Args:
            operation_or_operations: operation or operations that were just checkpointed.
            operations: full operation map after the update, when available.
            previous_operations: operation map before the update, when available.
        """
        # Only plugins read the info built below, and the change info converts
        # every operation. So without a plugin, return before building any of it.
        if operation_or_operations is None or not self.has_plugins:
            return

        updated_operations: list[Operation] = (
            cast(list[Operation], list(operation_or_operations))
            if isinstance(operation_or_operations, list | tuple)
            else [cast(Operation, operation_or_operations)]
        )
        for operation in updated_operations:
            if self._is_terminal_status(operation.status):
                self._run_end_hook_once(self._end_info(operation))

        change_info = self._change_info(
            updated_operations, operations, previous_operations
        )
        if change_info is not None:
            self._submit_change(change_info)

    def _end_info(self, operation: Operation) -> OperationEndInfo:
        return OperationEndInfo(
            operation_id=operation.operation_id,
            operation_type=_to_operation_type(operation.operation_type),
            sub_type=operation.sub_type,
            name=operation.name,
            parent_id=operation.parent_id,
            start_time=operation.start_timestamp,
            end_time=operation.end_timestamp,
            result=_extract_result(operation),
            status=operation.status,
            error=_copy_error(self._extract_error(operation)),
            attempt=(
                operation.step_details.attempt if operation.step_details else None
            ),
            is_replayed=False,
        )

    def _change_info(
        self,
        updated_operations: Sequence[Operation],
        operations: Mapping[str, Operation] | None,
        previous_operations: Mapping[str, Operation] | None,
    ) -> OperationChangeInfo | None:
        if (
            operations is None
            or previous_operations is None
            or self._invocation_status is None
        ):
            return None

        changed_operations = [
            operation
            for operation in updated_operations
            if previous_operations.get(operation.operation_id) is None
            or previous_operations[operation.operation_id].status != operation.status
        ]
        if not changed_operations:
            return None

        return OperationChangeInfo(
            execution_arn=self._invocation_status.execution_arn,
            updated_operations={
                operation.operation_id: OperationInfo.from_operation(operation)
                for operation in changed_operations
            },
            operations=_to_operation_info_map(operations),
        )

    @staticmethod
    def _extract_error(operation: Operation):
        return _extract_error(operation)

    @staticmethod
    def _is_terminal_status(status):
        return status in [
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.TIMED_OUT,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
        ]


class PluginHost:
    """Handler-lifetime owner of the configured plugin factories.

    The factory list is the only plugin state that may span invocations: a
    factory is resolved once when the handler is initialized and is, by
    definition, environment-lifetime. Everything a factory produces is
    invocation-lifetime, so this class never holds an instance, a start info or a
    dispatch pool -- :meth:`invocation` hands out a fresh
    :class:`PluginExecutor` and the caller keeps it in the invocation's own
    frame.

    The handler holds factories and the invocation holds instances. The other
    SDKs are moving to the same split, in aws/aws-durable-execution-sdk-js#924
    (``createInvocationPluginRunner``) and aws/aws-durable-execution-sdk-java#721
    (``PluginRunner`` constructed from factories). Both are open pull requests, so
    neither shape is on those repositories' default branches: ``createPluginRunner``
    on JS and ``PluginRunner`` on Java both still hold plugin instances directly.
    """

    def __init__(self, plugins: list[DurableInstrumentationPluginFactory] | None):
        self._plugin_factories = list(plugins or [])

    @contextlib.contextmanager
    def invocation(self) -> Iterator[PluginExecutor]:
        """Open one invocation's plugin scope and yield its executor.

        The executor is created here rather than at handler-initialization time
        so that two invocations sharing this process -- concurrent executions on
        a Lambda Managed Instance, or successive executions on a warm
        environment -- never write to the same slot. Teardown on scope exit
        touches only the executor yielded here.
        """
        executor = PluginExecutor(self._plugin_factories)
        with executor.run():
            yield executor

    @property
    def handle_durable_output(self):
        """Wrap an invocation body so plugins see its outcome.

        The wrapped function receives this invocation's :class:`PluginExecutor`
        as a third argument. Passing it in, rather than closing over one, is what
        keeps the instances out of handler-lifetime state: the executor is
        reachable only from the frames of the invocation it belongs to.
        """

        def decorator(
            func: Callable[
                [Any, LambdaContext, PluginExecutor], MutableMapping[str, Any]
            ],
        ):
            @functools.wraps(func)
            def wrapper(event: Any, context: LambdaContext):
                with self.invocation() as plugin_executor:
                    # The end hook is dispatched exactly once per invocation, so
                    # the success dispatch sits outside the try. Inside it, an
                    # end hook that raised would be caught as though the handler
                    # had failed, and the hook would run a second time with a
                    # RETRY outcome -- telling later plugins the wrong thing about
                    # an invocation that succeeded, and letting an exporter export
                    # twice. A hook can raise: _dispatch_plugin re-raises the three
                    # exceptions that instruct the calling thread to stop, and
                    # from_dict below can reject an output the handler built.
                    try:
                        output = func(event, context, plugin_executor)
                        completed = DurableExecutionInvocationOutput.from_dict(output)
                    except BaseException as e:
                        # Every exit fires the end hook, not only the ones that
                        # derive from Exception. A handler that surfaces an
                        # asyncio.CancelledError -- user code that awaited a
                        # cancelled task, most simply -- leaves the invocation by
                        # a BaseException, and an invocation that ends without
                        # its end hook costs the plugins the only point at which
                        # they can finish: Telemetry never drains, so the records it
                        # holds for this execution are dropped, and OTel never
                        # ends the spans it opened, so they are never exported.
                        # The teardown below still runs either way, which is why
                        # the gap was silent rather than a leak.
                        #
                        # KeyboardInterrupt and SystemExit reach here too, and
                        # they also fire the hook. The hook is what a plugin needs
                        # to flush, and a process being torn down is when flushing
                        # matters; the cost is the same bounded work any
                        # invocation end does.
                        #
                        # The handler's exception is what the caller sees,
                        # whatever the hook does. The end-hook dispatch can raise
                        # -- it re-raises the control exceptions it holds through
                        # the fan-out -- and letting that replace the handler's
                        # failure would report an instrumentation problem as the
                        # execution's outcome and leave the real failure reachable
                        # only as __context__. Instrumentation does not decide
                        # what an execution failed with, so the hook's exception
                        # is contained here and the original is re-raised
                        # unchanged.
                        try:
                            plugin_executor.on_invocation_end(
                                output=DurableExecutionInvocationOutput.create_retry(
                                    ErrorObject.from_exception(e)
                                ),
                            )
                        except BaseException:  # noqa: BLE001 - the handler's failure wins
                            logger.exception(
                                "Plugin invocation-end hook failed while the "
                                "invocation was already failing; the original "
                                "failure is raised"
                            )
                        raise
                    plugin_executor.on_invocation_end(output=completed)
                    return output

            return wrapper

        return decorator

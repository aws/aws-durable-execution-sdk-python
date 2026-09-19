from __future__ import annotations

import contextlib
import copy
import datetime
import functools
import logging
from collections.abc import Iterator, Mapping, Sequence
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
    OperationSubType,
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
    sub_type: OperationSubType | None
    name: str | None
    parent_id: str | None
    start_time: datetime.datetime | None
    is_replayed: bool
    status: OperationStatus
    end_time: datetime.datetime | None = field(default=None, kw_only=True)
    result: str | None = field(
        default=None,
        kw_only=True,
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: The serialized operation result, when available."""
    error: ErrorObject | None = field(
        default=None,
        kw_only=True,
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: The operation error, when available."""
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
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: The deserialized execution input, when available.

    Surfaced to instrumentation plugins that need to record it (e.g. Workflow
    Insight). Mirrors the JS SDK's ``InvocationInfo.executionInput``.

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
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: Checkpointed operations for this execution, keyed by id.

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
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: Operations updated externally while this execution was suspended.

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
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: The invocation error, when available."""
    execution_result: str | None = field(
        default=None,
        kw_only=True,
        repr=False,
        compare=False,
        hash=False,
        metadata={"experimental": True},
    )
    """EXPERIMENTAL: The serialized execution result, when available.

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
        Called before an operation's START checkpoint is queued, or when a
        prior non-terminal operation is replayed. This guarantees that it
        strictly precedes ``on_user_function_start``. This is called NOT within
        the thread that runs operation.

        Args:
            info: Information about the operation.

        """
        pass

    def on_operation_end(self, info: OperationEndInfo) -> None:
        """
        Called when an operation reaches a terminal status. Terminal operations
        are not emitted again during replay. Child contexts without a terminal
        checkpoint may emit this from the thread that runs the operation.

        Args:
            info: Information about the operation.
        """
        pass

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        """
        Called when checkpointed operations change after a checkpoint response is merged.
        This is called NOT within the thread that runs operation.

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
        return type(factory).__qualname__
    except BaseException:  # noqa: BLE001 - a name is never worth failing a hook for
        return "<unnamed factory>"


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


class PluginExecutor:
    """One invocation's plugin instances, metadata and dispatch.

    Scoped to a single invocation, not to the handler. Everything mutable here --
    the instances built for this invocation, the start info the end hook derives
    from, the operations provider, the dispatch pool -- describes one invocation,
    so a single instance shared by two of them would let each overwrite the
    other's state. Concurrent executions in one environment (Lambda Managed
    Instances) are exactly that case: they run in separate threads of one
    process, against one decorated handler. :class:`PluginHost` therefore builds
    a fresh executor per invocation and holds it only in that invocation's frame.

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
            self._invocation_status = None
            self._operations_provider = None
            # Drop this invocation's plugin instances: nothing outlives the
            # invocation. Every dispatch is synchronous, so there is no queued
            # work still holding one.
            self._plugins = []
            self._started = []

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
            if not isinstance(plugin, DurableInstrumentationPlugin):
                logger.error(
                    "Plugin factory %s returned %s, which is not a "
                    "DurableInstrumentationPlugin; plugin ignored",
                    _factory_name(factory),
                    type(plugin).__qualname__,
                )
                continue
            plugins.append(plugin)
        self._plugins = plugins

    @staticmethod
    def _dispatch_plugin(plugin: DurableInstrumentationPlugin, info) -> None:
        """Invoke the appropriate plugin callback. Runs inside the thread pool.

        Contains every ``BaseException`` except the parts that instruct the calling
        thread to stop, the same rule the factory boundary uses. The thread here
        is the executor's own single worker, which nothing outside this class
        cancels or interrupts, so an exception outside the ``Exception`` hierarchy
        arriving here was raised by the plugin.
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

    def execute_plugins(self, info):
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
        finish: Insight would not drain, and OTel would leave spans unended. The
        first such exception is held and re-raised once every plugin has been
        called, so the thread still stops and nothing is swallowed. No other hook
        defers: stopping a start-hook loop early leaves later plugins with nothing
        to clean up, because the pairing rule above then withholds their end hook
        too.

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
        those rules.
        """
        if not self._plugin_factories:
            return
        starting = isinstance(info, InvocationStartInfo)
        ending = isinstance(info, InvocationEndInfo)
        deferred_control: BaseException | None = None
        for plugin in self._plugins if starting else self._started:
            if starting:
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

        # Re-read the operation map so the end hook sees the state as of the end
        # of this invocation, not the snapshot taken at its start.
        invocation_end_info = (
            InvocationEndInfo.from_durable_execution_invocation_output(
                self._invocation_status,
                output,
                operations=self._snapshot_operation_infos(self._operations_provider),
            )
        )
        self.execute_plugins(invocation_end_info)

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
                error=error,
                is_replayed=is_replayed,
            ),
        )

    def on_operation_update(
        self,
        operation_or_operations: Operation | Sequence[Operation] | None,
        operations: Mapping[str, Operation] | None = None,
        previous_operations: Mapping[str, Operation] | None = None,
    ):
        """Execute any registered plugins for operation updates.

        Updates such as STARTED might be omitted because START and completion action (e.g. SUCCEED/FAIL) may be
        checkpointed in batch and the backend returns only the terminal status (e.g. SUCCEEDED/PENDING/FAILED).

        Note: the operation may not be up-to-date if the checkpoint is called asynchronously.

        Args:
            operation_or_operations: operation or operations that were just checkpointed.
            operations: full operation map after the update, when available.
            previous_operations: operation map before the update, when available.
        """
        if operation_or_operations is None:
            return

        updated_operations: list[Operation] = (
            cast(list[Operation], list(operation_or_operations))
            if isinstance(operation_or_operations, list | tuple)
            else [cast(Operation, operation_or_operations)]
        )
        for operation in updated_operations:
            if self._is_terminal_status(operation.status):
                self.execute_plugins(
                    OperationEndInfo(
                        operation_id=operation.operation_id,
                        operation_type=_to_operation_type(operation.operation_type),
                        sub_type=operation.sub_type,
                        name=operation.name,
                        parent_id=operation.parent_id,
                        start_time=operation.start_timestamp,
                        end_time=operation.end_timestamp,
                        result=_extract_result(operation),
                        status=operation.status,
                        error=self._extract_error(operation),
                        attempt=(
                            operation.step_details.attempt
                            if operation.step_details
                            else None
                        ),
                        is_replayed=False,
                    ),
                )

        if (
            operations is None
            or previous_operations is None
            or self._invocation_status is None
        ):
            return

        changed_operations = [
            operation
            for operation in updated_operations
            if previous_operations.get(operation.operation_id) is None
            or previous_operations[operation.operation_id].status != operation.status
        ]
        if not changed_operations:
            return

        self.execute_plugins(
            OperationChangeInfo(
                execution_arn=self._invocation_status.execution_arn,
                updated_operations={
                    operation.operation_id: OperationInfo.from_operation(operation)
                    for operation in changed_operations
                },
                operations=_to_operation_info_map(operations),
            ),
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
                        # they can finish: Insight never drains, so the records it
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

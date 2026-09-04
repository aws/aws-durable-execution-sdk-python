from __future__ import annotations

import contextlib
import copy
import datetime
import functools
import logging
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, MutableMapping, cast

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

DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION = 1


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
    result: str | None = field(default=None, kw_only=True)
    """The serialized operation result, when available."""
    error: ErrorObject | None = field(default=None, kw_only=True)
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
    error: ErrorObject | None = field(default=None)
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


@dataclass(frozen=True)
class DurableInstrumentationPluginProvider:
    """Versioned factory exposed through the plugin entry-point group."""

    plugin_type: type[DurableInstrumentationPlugin]
    factory: Callable[[], DurableInstrumentationPlugin]
    plugin_api_version: int


class PluginExecutor:
    def __init__(self, plugins: list[DurableInstrumentationPlugin] | None):
        self._plugins = plugins or []
        self._executor: ThreadPoolExecutor | None = None
        self._invocation_status: InvocationStartInfo | None = None
        self._operations_provider: Callable[[], Mapping[str, Operation]] | None = None

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
            self._operations_provider = None
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
                case OperationChangeInfo():
                    plugin.on_operation_change(info)
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

        Skipped entirely when no plugins are registered -- ``durable_execution()``
        passes a provider unconditionally, so without this gate a plugin-free
        execution would pay for a view nothing can read.
        """
        if not self._plugins or operations_provider is None:
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
        self._operations_provider = operations_provider if self._plugins else None
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
        self.execute_plugins(self._invocation_status, sync=True)

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
        registered, so non-plugin executions pay nothing.

        The snapshot is shared by all plugins for this invocation; plugins should
        still treat it as read-only with respect to each other.
        """
        if not self._plugins or execution_input is None:
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
        self.execute_plugins(start_info, sync=True)
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
            sync=True,
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
                sync=True,
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
        self.execute_plugins(start_info, sync=True)

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
            sync=True,
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
                    sync=True,
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
            sync=True,
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

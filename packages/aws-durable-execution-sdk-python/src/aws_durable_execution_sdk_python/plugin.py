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

DURABLE_INSTRUMENTATION_PLUGIN_API_VERSION = 2


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
            error=_extract_error(operation),
            attempt=(
                operation.step_details.attempt if operation.step_details else None
            ),
            is_replayed=is_replayed,
            status=operation.status,
        )


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
        cls, start_info: UserFunctionStartInfo, error: ErrorObject | None
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
            outcome=UserFunctionOutcome.from_error(error),
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


@dataclass(frozen=True)
class InvocationStartInfo(InvocationInfo):
    pass


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
    ):
        return InvocationEndInfo(
            request_id=invocation_start_info.request_id,
            execution_arn=invocation_start_info.execution_arn,
            is_first_invocation=invocation_start_info.is_first_invocation,
            execution_start_time=invocation_start_info.execution_start_time,
            execution_input=invocation_start_info.execution_input,
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

    def on_invocation_start(
        self,
        execution_arn: str,
        is_first_invocation: bool,
        execution_start_time: datetime.datetime | None,
        lambda_context: LambdaContext | None,
        execution_input: Any = None,
    ) -> None:
        aws_request_id = lambda_context.aws_request_id if lambda_context else None
        self._invocation_status = InvocationStartInfo(
            execution_arn=execution_arn,
            request_id=aws_request_id,
            is_first_invocation=is_first_invocation,
            execution_start_time=execution_start_time,
            execution_input=self._snapshot_execution_input(execution_input),
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

    def on_user_function_end(self, start_info: UserFunctionStartInfo, error) -> None:
        """Execute any registered plugins for the operation when its user function finishes execution."""
        self.execute_plugins(
            UserFunctionEndInfo.from_start_info(start_info, error), sync=True
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
                operations={
                    operation_id: OperationInfo.from_operation(operation)
                    for operation_id, operation in operations.items()
                },
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

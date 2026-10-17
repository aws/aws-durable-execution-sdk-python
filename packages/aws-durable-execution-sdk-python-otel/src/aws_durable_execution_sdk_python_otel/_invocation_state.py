"""Invocation-owned tracing data, propagated by the SDK's captured Contexts."""

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
import datetime
import threading
from typing import Any, Generic, TypeVar

from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    OperationEndInfo,
    OperationInfo,
    OperationStatus,
    OperationType,
)
from opentelemetry.sdk.trace.sampling import Sampler
from opentelemetry.trace import Span, Tracer, TracerProvider

from aws_durable_execution_sdk_python_otel.context_extractors import ExtractedContext
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
)
from aws_durable_execution_sdk_python_otel.durable_sampling import DurableSamplingIntent
from aws_durable_execution_sdk_python_otel.execution_trace_context import (
    ExecutionTraceContext,
)


_Key = TypeVar("_Key")


@dataclass
class _InvocationState(Generic[_Key]):
    provider: TracerProvider
    tracer: Tracer
    id_generator: DeterministicIdGenerator
    sampling_delegate: Sampler | None = None
    execution_arn: str = ""
    execution_trace_id: int | None = None
    execution_start_time: datetime.datetime | None = None
    extracted_context: ExtractedContext | None = None
    execution_trace_context: ExecutionTraceContext | None = None
    sampling_intent: DurableSamplingIntent | None = None
    workflow_span: Span | None = None
    operation_spans: dict[_Key, Span] = field(default_factory=dict)
    context_tokens: dict[str, tuple[int, object]] = field(default_factory=dict)
    tracing_enabled: bool = False
    owner_token: Token[Any] | None = None
    owner_thread: threading.Thread | None = None


@dataclass
class ExecutionState(_InvocationState[str]):
    invocation_span: Span | None = None
    checkpointed_context_ids: set[str] = field(default_factory=set)
    ended_operation_ids: set[str] = field(default_factory=set)
    pending_operation_ends: dict[str, list[OperationEndInfo]] = field(
        default_factory=dict
    )


@dataclass
class InvocationState(_InvocationState[str | None]):
    span_time_floor_ns: int | None = None
    input_parent_refs: dict[str, OperationInfo] = field(default_factory=dict)
    pending_operation_ends: dict[str, list[OperationEndInfo]] = field(
        default_factory=dict
    )
    context_operation_replays: dict[str, bool] = field(default_factory=dict)
    incomplete_attempt_span_keys: set[str] = field(default_factory=set)


_State = TypeVar("_State", bound=_InvocationState[Any])


def completed_input_parents(info: InvocationStartInfo) -> list[OperationInfo]:
    """Known terminal contexts that may parent input or later checkpoint updates."""
    return [
        operation
        for operation in info.operations.values()
        if operation.operation_type is OperationType.CONTEXT
        and operation.status
        in (
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.TIMED_OUT,
            OperationStatus.STOPPED,
            OperationStatus.CANCELLED,
        )
    ]


def begin_state(
    variable: ContextVar[_State],
    state: _State,
    reset_previous: Callable[[], None],
) -> None:
    """Start fresh data without clearing another invocation inherited in a copy.

    A direct warm-start caller can omit its previous End; retain that cleanup
    behavior only when its token proves this is the actual owning Context.
    Thread identity alone cannot distinguish a Context from a copy of it.
    """
    previous = variable.get()
    token = previous.owner_token
    if token is not None and previous.owner_thread is threading.current_thread():
        try:
            variable.reset(token)
        except (ValueError, RuntimeError):
            # An inherited Context (or an already-ended inherited token) does
            # not own the previous invocation and must not mutate its data.
            pass
        else:
            previous.owner_token = variable.set(previous)
            reset_previous()
    state.owner_thread = threading.current_thread()
    state.owner_token = variable.set(state)


def finish_state(variable: ContextVar[_State]) -> None:
    """Restore the binding on the Context that ran Start, after normal cleanup."""
    state = variable.get()
    token = state.owner_token
    if token is not None:
        state.owner_token = None
        state.owner_thread = None
        variable.reset(token)

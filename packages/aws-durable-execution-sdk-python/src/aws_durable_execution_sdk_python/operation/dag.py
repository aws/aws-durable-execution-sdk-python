"""dag_handler: wraps register + validate + schedule inside a DAG container.

The DAG container is a child-context operation whose result is the converged
cross-language envelope (see ``dag_result.DagResultImpl.to_dict``). This module
owns the two behaviours the generic child-context executor cannot express:

* the **degradation ladder** that serializes the envelope and, if it exceeds the
  checkpoint size limit, drops ``tasks`` (setting ``ReplayChildren``) and then
  ``failedTaskNames`` -- never the counts, ``completionReason`` or
  ``startedTaskNames``; and
* the **reconstruct** replay strategy for the offloaded case, which re-runs the
  deterministic register graph (each task fast-paths from its retained child
  checkpoint) and seeds the STARTED set from the envelope so an in-flight task
  is never restarted.

.. warning::
   **Experimental.** Internal wiring for ``context.dag()``.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.constants import CHECKPOINT_SIZE_LIMIT_BYTES
from aws_durable_execution_sdk_python.dag import DagCompletionReason, DagConfig, DagResult
from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    DagCyclicDependencyError,
    DagDuplicateTaskError,
    DagExecutionError,
    DagInvalidDependencyError,
    DagInvalidTaskNameError,
    DagPredicateError,
    InvocationError,
    SuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ContextOptions,
    ErrorObject,
    OperationSubType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.operation.base import CheckResult, OperationExecutor
from aws_durable_execution_sdk_python.operation.dag_context import DagContextImpl
from aws_durable_execution_sdk_python.operation.dag_executor import DagExecutor
from aws_durable_execution_sdk_python.operation.dag_result import DagResultImpl
from aws_durable_execution_sdk_python.operation.dag_validator import validate_dag

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.dag import DagContext
    from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState

# Typed Dag* errors that ``unwrap_dag_error`` surfaces cleanly through the child
# context boundary (on the first run via ``__cause__`` and on replay via
# ``error_type``). Not all are validation errors: ``DagExecutionError`` and
# ``DagPredicateError`` are execution-time.
_DAG_VALIDATION_ERRORS = (
    DagCyclicDependencyError,
    DagInvalidTaskNameError,
    DagDuplicateTaskError,
    DagInvalidDependencyError,
    DagExecutionError,
    DagPredicateError,
)

_DAG_ERROR_BY_NAME = {cls.__name__: cls for cls in _DAG_VALIDATION_ERRORS}

_warned = False


def emit_experimental_warning_once() -> None:
    """Emit a one-time ``FutureWarning`` on first use of ``context.dag()``."""
    global _warned
    if not _warned:
        _warned = True
        warnings.warn(
            "context.dag() is an EXPERIMENTAL API and may change or be removed "
            "in a future release without a major-version bump.",
            FutureWarning,
            stacklevel=3,
        )


def _check_max_concurrency(config: DagConfig) -> None:
    if config.max_concurrency is not None and config.max_concurrency <= 0:
        msg = f"Invalid max_concurrency: {config.max_concurrency}"
        raise ValidationError(msg)


@dataclass(frozen=True)
class _ReconstructInfo:
    """The subset of the offloaded envelope the reconstruct path consumes.

    ``started_task_names`` seeds the STARTED set (never restart an in-flight
    task); ``completion_reason``/``total_count`` are taken as authoritative
    rather than re-derived from the fast-pathed results.
    """

    started_task_names: set[str]
    completion_reason: DagCompletionReason | None
    total_count: int | None


def _run_dag_body(
    dag_child_ctx: DurableContext,
    register: Callable[[DagContext], None],
    config: DagConfig,
    reconstruct: _ReconstructInfo | None = None,
) -> DagResult:
    dag_ctx = DagContextImpl(dag_child_ctx, config)
    register(dag_ctx)
    validate_dag(dag_ctx)
    executor = DagExecutor(dag_child_ctx, dag_ctx.get_tasks(), config)
    if reconstruct is None:
        return executor.run()
    return executor.run(
        reconstruct_started=reconstruct.started_task_names,
        reconstruct_reason=reconstruct.completion_reason,
        reconstruct_total=reconstruct.total_count,
    )


class DagContainerExecutor(OperationExecutor[DagResult]):
    """Checkpoint orchestration for the DAG container operation.

    Mirrors ``ChildOperationExecutor``'s START/SUCCEED/FAIL contract (so a nested
    DAG's error unwraps identically and replay reconstruction of a FAILED
    container is unchanged), but replaces the generic size branch with the
    DAG degradation ladder and the generic ``ReplayChildren`` re-execute with the
    DAG reconstruct strategy.
    """

    def __init__(
        self,
        *,
        run_body: Callable[[_ReconstructInfo | None], DagResult],
        state: ExecutionState,
        operation_identifier: OperationIdentifier,
    ) -> None:
        self._run_body = run_body
        self.state = state
        self.operation_identifier = operation_identifier
        self.sub_type = OperationSubType.DAG

    def check_result_status(self) -> CheckResult[DagResult]:
        cr: CheckpointedResult = self.state.get_checkpoint_result(
            self.operation_identifier.operation_id
        )

        # Terminal success, tasks present (not offloaded): deserialize the full
        # envelope and return. Do not read children, do not re-run the body.
        if cr.is_succeeded() and not cr.is_replay_children():
            return CheckResult.create_completed(_deserialize_inline(cr.result))

        # Terminal success, offloaded (ReplayChildren): reconstruct from the
        # retained child checkpoints plus the envelope.
        if cr.is_succeeded() and cr.is_replay_children():
            return CheckResult.create_is_ready_to_execute(cr)

        # Terminal failure: surface as ChildContextError (unwrap_dag_error maps it
        # back to the typed Dag* error), identical on first run and replay.
        if cr.is_failed():
            cr.raise_operation_error(ChildContextError)

        # Create the START checkpoint if the container has not started. Fire and
        # forget (is_sync=False), matching the child-context executor.
        if not cr.is_existent():
            start = OperationUpdate.create_context_start(
                identifier=self.operation_identifier, sub_type=self.sub_type
            )
            self.state.create_checkpoint(operation_update=start, is_sync=False)

        return CheckResult.create_is_ready_to_execute(cr)

    def execute(self, checkpointed_result: CheckpointedResult) -> DagResult:
        reconstruct: _ReconstructInfo | None = None
        if checkpointed_result.is_succeeded() and (
            checkpointed_result.is_replay_children()
        ):
            reconstruct = _reconstruct_info(checkpointed_result.result)

        try:
            result = self._run_body(reconstruct)
        except SuspendExecution:
            # The DAG suspended (a task is waiting): bubble without checkpointing.
            raise
        except Exception as e:  # noqa: BLE001
            # Retryable InvocationError: re-raise with no FAIL checkpoint so the
            # backend retry re-runs. Everything else is terminal.
            if isinstance(e, InvocationError) and e.is_retryable():
                raise
            error_object = ErrorObject.from_exception(e)
            fail = OperationUpdate.create_context_fail(
                identifier=self.operation_identifier,
                error=error_object,
                sub_type=self.sub_type,
            )
            self.state.create_checkpoint(operation_update=fail)
            error_object.raise_as_operation_error(ChildContextError)

        if reconstruct is not None:
            # Offloaded reconstruct: the container is already SUCCEEDED with the
            # offloaded envelope; do not re-checkpoint.
            return result

        self._checkpoint_with_ladder(result)  # type: ignore[arg-type]
        return result

    def _checkpoint_with_ladder(self, result: DagResultImpl) -> None:
        """Serialize the envelope and degrade until it fits, in the exact
        contract order.

        1. Full envelope with ``tasks`` -- checkpoint, no ReplayChildren.
        2. Too large: drop ``tasks``, set ReplayChildren so the backend retains
           the child operations that hold the per-task results.
        3. Still too large: drop ``failedTaskNames``.

        Counts, ``completionReason`` and ``startedTaskNames`` are never dropped:
        a DAG must never fail to checkpoint because its own summary did not fit,
        and ``startedTaskNames`` (bounded by ``max_concurrency``) is what replay
        needs to avoid restarting an in-flight task.
        """
        envelope = result.to_dict()
        payload = json.dumps(envelope)
        replay_children = False
        if _too_large(payload):
            envelope.pop("tasks", None)
            payload = json.dumps(envelope)
            replay_children = True
            if _too_large(payload):
                envelope.pop("failedTaskNames", None)
                payload = json.dumps(envelope)

        succeed = OperationUpdate.create_context_succeed(
            identifier=self.operation_identifier,
            payload=payload,
            sub_type=self.sub_type,
            context_options=ContextOptions(replay_children=replay_children),
        )
        self.state.create_checkpoint(operation_update=succeed)


def _too_large(payload: str) -> bool:
    return len(payload.encode("utf-8")) > CHECKPOINT_SIZE_LIMIT_BYTES


def _deserialize_inline(payload: str | None) -> DagResult:
    if not payload:
        # Defensive: a succeeded, non-offloaded container always carries the
        # envelope; an empty payload can only mean an empty DAG round-trip.
        return DagResultImpl({}, DagCompletionReason.ALL_COMPLETED)
    return DagResultImpl.from_dict(json.loads(payload))


def _reconstruct_info(payload: str | None) -> _ReconstructInfo:
    data = json.loads(payload) if payload else {}
    reason_value = data.get("completionReason")
    return _ReconstructInfo(
        started_task_names=set(data.get("startedTaskNames") or []),
        completion_reason=(
            DagCompletionReason(reason_value) if reason_value else None
        ),
        total_count=data.get("totalCount"),
    )


def unwrap_dag_error(exc: ChildContextError) -> None:
    """Re-raise the typed Dag* cause of a wrapped ``ChildContextError``.

    ``DagContainerExecutor`` surfaces body exceptions as ``ChildContextError``
    with the original on ``__cause__`` (first run) and ``error_type`` set to the
    original class name (both first run and replay, reconstructed from the
    checkpoint). This restores the clean typed throw for DAG validation /
    execution errors, mirroring the ``wait_for_callback`` precedent.

    On **replay** the failure is rebuilt from a checkpoint, which sets
    ``error_type`` (the original class name) but leaves ``__cause__`` as
    ``None``. In that case we reconstruct the typed Dag* error from
    ``error_type`` so a nested DAG's error surfaces identically on the first run
    and on replay. If neither path identifies a Dag* error, re-raise the
    original wrapper unchanged.
    """
    cause = exc.__cause__
    if isinstance(cause, _DAG_VALIDATION_ERRORS):
        # Re-raise the typed error, preserving ITS OWN original cause so a
        # DagPredicateError still exposes the raising predicate's exception as
        # __cause__ (contract: the original error must remain the retrievable
        # cause). ``from inner`` also suppresses the ChildContextError wrapper
        # from the traceback; when inner is None (the validation errors) this is
        # exactly the previous ``raise cause from None`` behaviour.
        inner = cause.__cause__
        raise cause from inner
    dag_cls = _DAG_ERROR_BY_NAME.get(exc.error_type or "")
    if dag_cls is not None:
        raise dag_cls(exc.message) from None
    raise exc


def dag_handler(
    ctx: DurableContext,
    name: str | None,
    register: Callable[[DagContext], None],
    config: DagConfig | None,
) -> DagResult:
    """Run a top-level DAG as a container child context and return its DagResult.

    The container takes a counter-based operation id (like
    ``run_in_child_context``), so ``_replay_aware`` bookkeeping is identical.
    """
    config = config or DagConfig()
    _check_max_concurrency(config)

    with ctx._replay_aware():
        operation_id = ctx._create_step_id()
        identifier = OperationIdentifier(
            operation_id=operation_id,
            sub_type=OperationSubType.DAG,
            parent_id=ctx._parent_id,
            name=name,
        )

        def run_body(reconstruct: _ReconstructInfo | None) -> DagResult:
            child = ctx.create_child_context(operation_id=operation_id)
            return _run_dag_body(child, register, config, reconstruct)

        executor = DagContainerExecutor(
            run_body=run_body,
            state=ctx.state,
            operation_identifier=identifier,
        )
        try:
            return executor.process()
        except ChildContextError as e:
            unwrap_dag_error(e)
            raise  # pragma: no cover - unwrap_dag_error always raises


def run_nested_dag(
    ctx: DurableContext,
    name: str,
    register: Callable[[DagContext], None],
    config: DagConfig | None,
) -> DagResult:
    """Run a nested DAG task under a name-based (``DAG_NODE_T_``) container id."""
    config = config or DagConfig()
    _check_max_concurrency(config)
    task_id = ctx._create_task_id(name)
    identifier = OperationIdentifier(
        operation_id=task_id,
        sub_type=OperationSubType.DAG,
        parent_id=ctx._parent_id,
        name=name,
    )

    def run_body(reconstruct: _ReconstructInfo | None) -> DagResult:
        child = ctx.create_child_context(operation_id=task_id)
        return _run_dag_body(child, register, config, reconstruct)

    executor = DagContainerExecutor(
        run_body=run_body,
        state=ctx.state,
        operation_identifier=identifier,
    )
    try:
        return executor.process()
    except ChildContextError as e:
        unwrap_dag_error(e)
        raise  # pragma: no cover - unwrap_dag_error always raises

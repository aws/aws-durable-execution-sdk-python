"""dag_handler: wraps register + validate + schedule inside a child context.

.. warning::
   **Experimental.** Internal wiring for ``context.dag()``.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.dag import DagConfig
from aws_durable_execution_sdk_python.exceptions import (
    CallableRuntimeError,
    DagCyclicDependencyError,
    DagDuplicateTaskError,
    DagExecutionError,
    DagInvalidDependencyError,
    DagInvalidTaskNameError,
    ValidationError,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.operation.child import child_handler
from aws_durable_execution_sdk_python.operation.dag_context import DagContextImpl
from aws_durable_execution_sdk_python.operation.dag_executor import DagExecutor
from aws_durable_execution_sdk_python.operation.dag_result import (
    create_dag_result_serdes,
)
from aws_durable_execution_sdk_python.operation.dag_validator import validate_dag

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.dag import DagContext, DagResult
    from aws_durable_execution_sdk_python.state import ExecutionState

_DAG_VALIDATION_ERRORS = (
    DagCyclicDependencyError,
    DagInvalidTaskNameError,
    DagDuplicateTaskError,
    DagInvalidDependencyError,
    DagExecutionError,
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


def _run_dag_body(
    dag_child_ctx: DurableContext,
    register: Callable[[DagContext], None],
    config: DagConfig,
) -> DagResult:
    dag_ctx = DagContextImpl(dag_child_ctx, config)
    register(dag_ctx)
    validate_dag(dag_ctx)
    return DagExecutor(dag_child_ctx, dag_ctx.get_tasks(), config).run()


def unwrap_dag_error(exc: CallableRuntimeError) -> None:
    """Re-raise the typed Dag* cause of a wrapped ``CallableRuntimeError``.

    ``child_handler`` wraps body exceptions as ``CallableRuntimeError`` with the
    original on ``__cause__`` (``raise ... from e``). This restores the clean
    typed throw for DAG validation / execution errors, mirroring the
    ``wait_for_callback`` precedent.

    On **replay**, the failure is rebuilt from a checkpoint via
    ``ErrorObject.to_callable_runtime_error()``, which sets ``error_type`` (the
    original class name) but leaves ``__cause__`` as ``None``. In that case we
    reconstruct the typed Dag* error from ``error_type`` so a nested DAG's error
    surfaces identically on the first run and on replay. If neither path
    identifies a Dag* error, re-raise the original wrapper unchanged.
    """
    cause = exc.__cause__
    if isinstance(cause, _DAG_VALIDATION_ERRORS):
        raise cause from None
    dag_cls = _DAG_ERROR_BY_NAME.get(exc.error_type or "")
    if dag_cls is not None:
        raise dag_cls(exc.message) from None
    raise exc


def dag_handler(
    run_in_child_context: Callable[..., DagResult],
    state: ExecutionState,
    name: str | None,
    register: Callable[[DagContext], None],
    config: DagConfig | None,
) -> DagResult:
    """Run a DAG as a child context and return its DagResult synchronously."""
    config = config or DagConfig()
    _check_max_concurrency(config)

    def body(dag_child_ctx: DurableContext) -> DagResult:
        return _run_dag_body(dag_child_ctx, register, config)

    child_config: ChildConfig = ChildConfig(
        sub_type=OperationSubType.DAG,
        serdes=config.serdes or create_dag_result_serdes(),
        summary_generator=config.summary_generator,
    )
    try:
        return run_in_child_context(body, name, child_config)
    except CallableRuntimeError as e:
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

    def body() -> DagResult:
        child = ctx.create_child_context(parent_id=task_id)
        return _run_dag_body(child, register, config)

    child_config: ChildConfig = ChildConfig(
        sub_type=OperationSubType.DAG,
        serdes=config.serdes or create_dag_result_serdes(),
        summary_generator=config.summary_generator,
    )
    try:
        return child_handler(
            func=body,
            state=ctx.state,
            operation_identifier=OperationIdentifier(
                operation_id=task_id,
                parent_id=ctx._parent_id,
                name=name,
            ),
            config=child_config,
        )
    except CallableRuntimeError as e:
        unwrap_dag_error(e)
        raise  # pragma: no cover

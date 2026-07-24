"""DagContextImpl: registers TaskDefs and hands back TaskHandles.

.. warning::
   **Experimental.** Internal implementation of the DAG registration phase.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.concurrency import BatchResult
from aws_durable_execution_sdk_python.config import ChildConfig
from aws_durable_execution_sdk_python.dag import (
    DagContext,
    DepsMap,
    TaskHandle,
    TriggerRule,
)
from aws_durable_execution_sdk_python.exceptions import DagInvalidTaskNameError
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.operation.child import child_handler
from aws_durable_execution_sdk_python.operation.invoke import invoke_handler
from aws_durable_execution_sdk_python.operation.map import map_handler
from aws_durable_execution_sdk_python.operation.parallel import parallel_handler
from aws_durable_execution_sdk_python.operation.wait import wait_handler
from aws_durable_execution_sdk_python.operation.wait_for_condition import (
    wait_for_condition_handler,
)
from aws_durable_execution_sdk_python.serdes import SerDes

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from aws_durable_execution_sdk_python.config import StepConfig
    from aws_durable_execution_sdk_python.dag import DagConfig
    from aws_durable_execution_sdk_python.serdes import SerDesContext
    from aws_durable_execution_sdk_python.types import DurableContext

logger = logging.getLogger(__name__)


class _BatchResultSerDes(SerDes):
    """Serialize a map/parallel task's ``BatchResult`` container payload."""

    def serialize(self, value: BatchResult, serdes_context: SerDesContext) -> str:
        return json.dumps(value.to_dict())

    def deserialize(self, data: str, serdes_context: SerDesContext) -> BatchResult:
        return BatchResult.from_dict(json.loads(data))


@dataclass
class TaskDef:
    """Internal record for one registered DAG task.

    ``inline_deps`` drive the :class:`DepsMap`; ``all_deps`` (inline ∪
    ``.after`` edges) drive readiness / trigger-rule / cycle checks. ``executor``
    binds the name-based (explicit-id) runner for this task's operation kind.
    """

    name: str
    kind: str
    inline_deps: list[TaskHandle[Any]]
    all_deps: list[TaskHandle[Any]]
    trigger_rule: TriggerRule
    run_if: Callable[[DepsMap], bool] | None
    config: Any
    executor: Callable[[DurableContext, DepsMap], Any]


def _resolve_name(name: str | None, func: Any) -> str:
    resolved = name if name else getattr(func, "_original_name", None)
    if not resolved:
        msg = (
            "Could not resolve a task name. Pass an explicit `name=` (bare "
            "lambdas have no resolvable name)."
        )
        raise DagInvalidTaskNameError(msg)
    return resolved


class DagContextImpl(DagContext):
    """Concrete DagContext used during the registration phase."""

    def __init__(self, ctx: DurableContext, config: DagConfig) -> None:
        self._ctx = ctx
        self._config = config
        self._tasks: dict[str, TaskDef] = {}
        # ordered record of every registration (incl. duplicate names) so the
        # validator can detect duplicates; the dict is for name lookup.
        self._registration_order: list[TaskDef] = []

    def get_tasks(self) -> dict[str, TaskDef]:
        """Return the registered tasks by name (last registration wins)."""
        return self._tasks

    def get_registration_order(self) -> list[TaskDef]:
        """Return every registered task in order, including duplicate names."""
        return self._registration_order

    # region builder mutations (called by TaskHandle)
    def _register_after(
        self, handle: TaskHandle[Any], deps: Sequence[TaskHandle[Any]]
    ) -> None:
        task = self._tasks[handle.name]
        for d in deps:
            if d not in task.all_deps:
                task.all_deps.append(d)

    def _register_trigger_rule(
        self, handle: TaskHandle[Any], rule: TriggerRule
    ) -> None:
        self._tasks[handle.name].trigger_rule = rule

    # endregion builder mutations

    def _add(
        self,
        name: str,
        kind: str,
        inline_deps: Sequence[TaskHandle[Any]] | None,
        trigger_rule: TriggerRule,
        run_if: Callable[[DepsMap], bool] | None,
        config: Any,
        executor: Callable[[DurableContext, DepsMap], Any],
    ) -> TaskHandle[Any]:
        inline = list(inline_deps) if inline_deps else []
        task = TaskDef(
            name=name,
            kind=kind,
            inline_deps=inline,
            all_deps=list(inline),
            trigger_rule=trigger_rule,
            run_if=run_if,
            config=config,
            executor=executor,
        )
        self._registration_order.append(task)
        self._tasks[name] = task
        return TaskHandle(_name=name, _dag=self)

    # region task kinds
    def step(
        self, func, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, func)
        cfg = config or self._default_step_config()

        def executor(ctx: DurableContext, deps_map: DepsMap):
            return ctx._run_step_with_task_id(
                task_name, lambda step_ctx: func(deps_map, step_ctx), cfg
            )

        return self._add(task_name, "step", deps, trigger_rule, run_if, cfg, executor)

    def invoke(
        self, function_name, payload_fn, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, payload_fn)

        def executor(ctx: DurableContext, deps_map: DepsMap):
            payload = payload_fn(deps_map) if callable(payload_fn) else payload_fn
            return invoke_handler(
                function_name=function_name,
                payload=payload,
                state=ctx.state,
                operation_identifier=OperationIdentifier(
                    operation_id=ctx._create_task_id(task_name),
                    parent_id=ctx._parent_id,
                    name=task_name,
                ),
                config=config,
            )

        return self._add(
            task_name, "invoke", deps, trigger_rule, run_if, config, executor
        )

    def wait_for_callback(
        self, submitter, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, submitter)

        def executor(ctx: DurableContext, deps_map: DepsMap):
            from aws_durable_execution_sdk_python.operation.callback import (
                wait_for_callback_handler,
            )

            def body(child: DurableContext):
                return wait_for_callback_handler(
                    child,
                    lambda cb_id: submitter(deps_map, cb_id, child),
                    task_name,
                    config,
                )

            return self._run_child(ctx, task_name, body, ChildConfig())

        return self._add(
            task_name, "wait_for_callback", deps, trigger_rule, run_if, config, executor
        )

    def wait(
        self, seconds, deps=None, name=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        if not name:
            msg = "wait tasks require an explicit `name=`."
            raise DagInvalidTaskNameError(msg)
        task_name = name

        def executor(ctx: DurableContext, deps_map: DepsMap):
            return wait_handler(
                seconds=seconds,
                state=ctx.state,
                operation_identifier=OperationIdentifier(
                    operation_id=ctx._create_task_id(task_name),
                    parent_id=ctx._parent_id,
                    name=task_name,
                ),
            )

        return self._add(task_name, "wait", deps, trigger_rule, run_if, None, executor)

    def wait_for_condition(
        self, check, config, deps=None, name=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, check)

        def executor(ctx: DurableContext, deps_map: DepsMap):
            return wait_for_condition_handler(
                check=lambda state, cctx: check(deps_map, state, cctx),
                config=config,
                state=ctx.state,
                operation_identifier=OperationIdentifier(
                    operation_id=ctx._create_task_id(task_name),
                    parent_id=ctx._parent_id,
                    name=task_name,
                ),
                context_logger=ctx.logger,
            )

        return self._add(
            task_name, "wait_for_condition", deps, trigger_rule, run_if, config, executor
        )

    def run_in_child_context(
        self, func, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, func)
        cfg = config or ChildConfig()

        def executor(ctx: DurableContext, deps_map: DepsMap):
            return self._run_child(
                ctx, task_name, lambda child: func(deps_map, child), cfg
            )

        return self._add(
            task_name, "child", deps, trigger_rule, run_if, cfg, executor
        )

    def map(
        self, inputs, func, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, func)

        def executor(ctx: DurableContext, deps_map: DepsMap):
            resolved = inputs(deps_map) if callable(inputs) else inputs
            serdes = config.serdes if (config and config.serdes) else _BatchResultSerDes()
            cfg = ChildConfig(sub_type=OperationSubType.MAP, serdes=serdes)

            def body(child: DurableContext):
                return map_handler(
                    items=resolved,
                    func=func,
                    config=config,
                    execution_state=ctx.state,
                    run_in_child_context=child.run_in_child_context,
                )

            return self._run_child(ctx, task_name, body, cfg)

        return self._add(task_name, "map", deps, trigger_rule, run_if, config, executor)

    def parallel(
        self, functions, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        if not name:
            msg = "parallel tasks require an explicit `name=`."
            raise DagInvalidTaskNameError(msg)
        task_name = name

        def executor(ctx: DurableContext, deps_map: DepsMap):
            serdes = config.serdes if (config and config.serdes) else _BatchResultSerDes()
            cfg = ChildConfig(sub_type=OperationSubType.PARALLEL, serdes=serdes)

            def body(child: DurableContext):
                return parallel_handler(
                    callables=functions,
                    config=config,
                    execution_state=ctx.state,
                    run_in_child_context=child.run_in_child_context,
                )

            return self._run_child(ctx, task_name, body, cfg)

        return self._add(
            task_name, "parallel", deps, trigger_rule, run_if, config, executor
        )

    def dag(
        self, register, deps=None, name=None, config=None, *,
        trigger_rule=TriggerRule.ALL_SUCCESS, run_if=None,
    ):
        task_name = _resolve_name(name, register)

        def executor(ctx: DurableContext, deps_map: DepsMap):
            # Deferred import to avoid a circular import (operation.dag imports us).
            from aws_durable_execution_sdk_python.operation.dag import (
                run_nested_dag,
            )

            return run_nested_dag(ctx, task_name, register, config)

        return self._add(task_name, "dag", deps, trigger_rule, run_if, config, executor)

    # endregion task kinds

    def _run_child(
        self,
        ctx: DurableContext,
        name: str,
        body_takes_child: Callable[[DurableContext], Any],
        config: ChildConfig,
    ) -> Any:
        task_id = ctx._create_task_id(name)

        def wrapped():
            return body_takes_child(ctx.create_child_context(parent_id=task_id))

        return child_handler(
            func=wrapped,
            state=ctx.state,
            operation_identifier=OperationIdentifier(
                operation_id=task_id,
                parent_id=ctx._parent_id,
                name=name,
            ),
            config=config,
        )

    def _default_step_config(self) -> StepConfig | None:
        from aws_durable_execution_sdk_python.config import StepConfig

        if self._config.default_retry_strategy is not None:
            return StepConfig(retry_strategy=self._config.default_retry_strategy)
        return None

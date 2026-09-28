"""End-to-end log correlation coverage for concurrent invocations.

Drives the decorated handler, not the filter in isolation. The SDK runs the
handler body on a worker thread it submits to a pool, so only a test that goes
through ``durable_execution`` exercises the path from the invocation-start hook
to a record emitted by top-level handler code.

Both invocations are held at a barrier until each has started, so a record is
only emitted while two invocations are open. That is the case the log filter
cannot resolve from the number of open invocations alone.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    ExecutionDetails,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python_otel.log_filter import OtelContextLogFilter
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig
from aws_durable_execution_sdk_python_otel.plugin_factory import (
    InvocationOtelPluginFactory,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


EXECUTION_START = datetime(2026, 8, 27, 5, 11, 47, tzinfo=UTC)
OWNERS = ("first", "second")
TOP_LEVEL_MESSAGE = "top-level-handler-log"


def _execution_arn(owner: str) -> str:
    return f"test-arn/{_execution_operation_id(owner)}"


def _execution_operation_id(owner: str) -> str:
    """The execution operation is keyed by the last segment of the ARN."""
    return f"concurrent-log-correlation-{owner}"


def _lambda_context(owner: str) -> Mock:
    context = Mock()
    context.aws_request_id = f"request-{owner}"
    context.client_context = None
    context.identity = None
    context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    context.invoked_function_arn = "test-arn"
    context.tenant_id = None
    return context


def _execution_operation(owner: str) -> Operation:
    return Operation(
        operation_id=_execution_operation_id(owner),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.STARTED,
        start_timestamp=EXECUTION_START,
        # The handler receives the parsed input payload, so the owner travels
        # through the execution input rather than the invocation event.
        execution_details=ExecutionDetails(input_payload=json.dumps({"owner": owner})),
    )


def _event(owner: str) -> dict[str, Any]:
    return {
        "DurableExecutionArn": _execution_arn(owner),
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [_execution_operation(owner).to_json_dict()],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _shared_checkpoint_store():
    """Return a checkpoint callable serving several concurrent executions.

    One mocked Lambda client is shared by both invocations, so operations are
    kept per execution ARN and mutated under a lock.
    """
    operations: dict[str, dict[str, Operation]] = {
        _execution_arn(owner): {
            _execution_operation_id(owner): _execution_operation(owner)
        }
        for owner in OWNERS
    }
    lock = threading.Lock()

    def checkpoint(
        durable_execution_arn,
        checkpoint_token,  # noqa: ARG001
        updates,
        client_token="token",  # noqa: S107, ARG001
    ) -> CheckpointOutput:
        with lock:
            execution_operations = operations.setdefault(durable_execution_arn, {})
            for update in updates:
                now = datetime.now(UTC)
                previous = execution_operations.get(update.operation_id)
                if update.action is OperationAction.START:
                    execution_operations[update.operation_id] = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                        name=update.name,
                        sub_type=update.sub_type,
                        start_timestamp=now,
                    )
                elif update.action is OperationAction.SUCCEED:
                    base = previous or Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                        name=update.name,
                        sub_type=update.sub_type,
                        start_timestamp=now,
                    )
                    execution_operations[update.operation_id] = replace(
                        base,
                        status=OperationStatus.SUCCEEDED,
                        end_timestamp=now,
                        step_details=(
                            StepDetails(result=update.payload, attempt=1)
                            if update.operation_type is OperationType.STEP
                            else base.step_details
                        ),
                    )
            snapshot = list(execution_operations.values())

        return CheckpointOutput(
            checkpoint_token="new-token",
            new_execution_state=CheckpointUpdatedExecutionState(operations=snapshot),
        )

    return checkpoint


class _RecordCollector(logging.Handler):
    """Collects the top-level handler records the filter has stamped."""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        if record.getMessage().startswith(TOP_LEVEL_MESSAGE):
            with self._lock:
                self.records.append(record)


def _remove_otel_filters(handler: logging.Handler) -> None:
    for installed in [
        f for f in handler.filters if isinstance(f, OtelContextLogFilter)
    ]:
        handler.removeFilter(installed)


def test_overlapping_invocations_stamp_top_level_logs_with_their_own_span() -> None:
    """A handler's first log carries its own invocation's trace and span ids.

    Both invocations are open when either record is emitted, and neither record
    is emitted from a thread the plugin hooks have run on, so the record can
    only be correlated if the SDK carried the invocation's context into the
    thread it runs the handler body on.
    """
    collector = _RecordCollector()
    root = logging.getLogger()
    root.addHandler(collector)
    # The record must reach the root handlers, so the emitting logger opts in to
    # INFO explicitly rather than inheriting the root level.
    probe_logger = logging.getLogger("probe.handler")
    previous_level = probe_logger.level
    probe_logger.setLevel(logging.INFO)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # enrich_logger is the documented default: the plugin installs the filter on
    # the root logger's handlers, including the collector added above.
    factory = InvocationOtelPluginFactory(
        OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
    )

    both_started = threading.Barrier(len(OWNERS), timeout=30)

    @durable_step
    def after_log(_step_context) -> str:
        return "done"

    def handler_impl(event: Any, context: DurableContext) -> str:
        owner = event["owner"]
        # Hold here until every invocation has started, so the log below is
        # emitted with more than one invocation open. Nothing durable has run.
        both_started.wait()
        logging.getLogger("probe.handler").info("%s %s", TOP_LEVEL_MESSAGE, owner)
        return context.step(after_log(), name=f"after-log-{owner}")

    handler = durable_execution(handler_impl, plugins=[factory])
    results: dict[str, Any] = {}
    failures: list[BaseException] = []
    results_lock = threading.Lock()

    def invoke(owner: str) -> None:
        try:
            result = handler(_event(owner), _lambda_context(owner))
            with results_lock:
                results[owner] = result
        except BaseException as error:  # noqa: BLE001
            with results_lock:
                failures.append(error)
            both_started.abort()

    try:
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient"
        ) as mock_client_class:
            mock_client = Mock()
            mock_client.checkpoint = _shared_checkpoint_store()
            mock_client_class.initialize_client.return_value = mock_client

            threads = [
                threading.Thread(target=invoke, args=(owner,), name=f"invoke-{owner}")
                for owner in OWNERS
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=60)

        assert not failures, failures
        assert not any(thread.is_alive() for thread in threads)
        for owner in OWNERS:
            assert results[owner]["Status"] == InvocationStatus.SUCCEEDED.value

        # Each invocation's Invocation span identifies its execution by ARN, so
        # the expected identifiers are read back from the exported spans rather
        # than recomputed.
        expected: dict[str, tuple[str, str]] = {}
        for span in exporter.get_finished_spans():
            if span.name != "Invocation":
                continue
            assert span.attributes is not None
            arn = span.attributes["durable.execution.arn"]
            assert span.context is not None
            expected[str(arn)] = (
                format(span.context.trace_id, "032x"),
                format(span.context.span_id, "016x"),
            )
        assert set(expected) == {_execution_arn(owner) for owner in OWNERS}

        stamped = {
            record.getMessage().rsplit(" ", 1)[1]: (
                getattr(record, "traceId", None),
                getattr(record, "spanId", None),
            )
            for record in collector.records
        }
        assert set(stamped) == set(OWNERS), collector.records
        assert stamped == {
            owner: expected[_execution_arn(owner)] for owner in OWNERS
        }, f"records carried {stamped}"
    finally:
        for installed_on in list(root.handlers):
            _remove_otel_filters(installed_on)
        root.removeHandler(collector)
        probe_logger.setLevel(previous_level)

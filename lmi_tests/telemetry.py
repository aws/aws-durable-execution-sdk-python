# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real public OTel plugins with immutable runtime and invocation ownership."""

from __future__ import annotations

from collections.abc import Callable
from importlib.metadata import version
import os
import threading
import time
from typing import Any
import uuid
from weakref import WeakKeyDictionary

from opentelemetry import context as otel_context, trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStartInfo,
)
from lmi_tests.shared_exporter import (
    AsyncSpanProcessor,
    CURRENT_OWNER,
    DEFAULT_FLUSH_TIMEOUT_MS,
    LedgerExporter,
    Owner,
    Trace,
    Tracker,
    owned,
)

_RUNTIMES: dict[tuple[str, str], Runtime] = {}
_RUNTIME_LOCK = threading.Lock()


class ObservedPlugin(DurableInstrumentationPlugin):
    """Forward the real public hooks, binding their immutable invocation owner."""

    def __init__(self, delegate: DurableInstrumentationPlugin, owner: Owner) -> None:
        self.delegate, self.owner = delegate, owner

    def _call(self, name: str, info: Any) -> Any:
        with owned(self.owner):
            return getattr(self.delegate, name)(info)

    def on_invocation_start(self, info) -> None:
        self._call("on_invocation_start", info)

    def on_invocation_end(self, info) -> None:
        self._call("on_invocation_end", info)

    def on_operation_start(self, info) -> None:
        self._call("on_operation_start", info)

    def on_operation_end(self, info) -> None:
        self._call("on_operation_end", info)

    def on_operation_change(self, info) -> None:
        self._call("on_operation_change", info)

    def on_user_function_start(self, info) -> None:
        self._call("on_user_function_start", info)

    def on_user_function_end(self, info) -> None:
        self._call("on_user_function_end", info)

    def provide_propagation_metadata(self, info):
        return self._call("provide_propagation_metadata", info)


class EvidenceFactory:
    __durable_registration_api__ = 1
    exclusive_group = "aws-durable-execution-otel-view"

    def __init__(self, view: str, provider: TracerProvider) -> None:
        kind = (
            InvocationOtelPluginFactory
            if view == "invocation"
            else ExecutionOtelPluginFactory
        )
        self.delegate = kind(
            OtelPluginConfig(tracer_provider=provider, enrich_logger=True)
        )
        self.instances: WeakKeyDictionary[DurableInstrumentationPlugin, str] = (
            WeakKeyDictionary()
        )
        self.lock = threading.Lock()

    def create_plugin(self, info: InvocationStartInfo) -> DurableInstrumentationPlugin:
        owner = CURRENT_OWNER.get()
        if owner is None:
            raise RuntimeError("Factory lost the invocation evidence owner")
        plugin = self.delegate.create_plugin(info)
        with self.lock:
            identity = self.instances.setdefault(plugin, uuid.uuid4().hex)
        owner.trace.emit(
            "OTEL_FACTORY",
            plugin_instance=identity,
            plugin_execution=info.execution_arn,
        )
        owner.tracker.observe(
            owner,
            "otel_factory",
            pluginInstanceId=identity,
            pluginExecutionArn=info.execution_arn,
        )
        return ObservedPlugin(plugin, owner)


class Runtime:
    """An immutable provider variant shared across invocations in one process."""

    def __init__(
        self,
        view: str,
        export_mode: str = "sync",
        *,
        flush_timeout_ms: float = DEFAULT_FLUSH_TIMEOUT_MS,
    ) -> None:
        if view not in {"invocation", "execution"} or export_mode not in {
            "sync",
            "async-bounded-flush",
        }:
            raise ValueError("Unknown OTel view or exporter mode")
        self.identity = uuid.uuid4().hex
        self.created_at_ms = time.time_ns() / 1_000_000
        self.view, self.export_mode = view, export_mode
        self.tracker = Tracker(
            self.identity, uuid.uuid4().hex, view, export_mode, flush_timeout_ms
        )
        self.exporter = LedgerExporter(self.tracker)
        self.provider = TracerProvider(sampler=ALWAYS_ON, shutdown_on_exit=False)
        self.provider.add_span_processor(
            SimpleSpanProcessor(self.exporter)
            if export_mode == "sync"
            else AsyncSpanProcessor(self.exporter)
        )
        self.factory = EvidenceFactory(view, self.provider)

    def pending_for_case(self, case_id: str) -> dict[str, Any]:
        return self.tracker.active(case_id)


def active_work(trace: Trace) -> dict[str, Any]:
    """Actual outstanding work/publication across immutable runtime variants."""
    case_id = str(trace.payload.get("caseId", trace.identity.get("marker", "")))
    result: dict[str, Any] = {
        "pendingExportIds": [],
        "pendingFlushIds": [],
        "publicationCount": 0,
        "publicationErrors": [],
    }
    with _RUNTIME_LOCK:
        runtimes = tuple(_RUNTIMES.values())
    for runtime in runtimes:
        state = runtime.pending_for_case(case_id)
        for field in ("pendingExportIds", "pendingFlushIds", "publicationErrors"):
            result[field].extend(state[field])
        result["publicationCount"] += state["publicationCount"]
    return result


def _context_snapshot() -> dict[str, str | None]:
    context = otel_trace.get_current_span().get_span_context()
    return {
        "traceId": format(context.trace_id, "032x") if context.is_valid else None,
        "spanId": format(context.span_id, "016x") if context.is_valid else None,
    }


def invoke(
    trace: Trace,
    invoke_handler: Callable[
        [list[DurableInstrumentationPluginFactory]], dict[str, Any]
    ],
) -> dict[str, Any]:
    view = trace.payload.get("otel_view", trace.payload.get("otelView"))
    export_mode = str(trace.payload.get("exportMode", "sync"))
    if view not in {"invocation", "execution"} or export_mode not in {
        "sync",
        "async-bounded-flush",
    }:
        raise ValueError("Unknown OTel view or exporter mode")
    key = (view, export_mode)
    with _RUNTIME_LOCK:
        if key not in _RUNTIMES:
            _RUNTIMES[key] = Runtime(view, export_mode)
        runtime = _RUNTIMES[key]
    owner = runtime.tracker.owner(trace)
    before, snapshot = otel_context.get_current(), _context_snapshot()
    carrier = os.environ.get("_X_AMZN_TRACE_ID")
    trace.emit(
        "OTEL_ENTER",
        view=view,
        export_mode=runtime.export_mode,
        provider=runtime.identity,
        carrier=carrier,
        carrier_source="runtime_environment",
        core_version=version("aws-durable-execution-sdk-python"),
        otel_version=version("aws-durable-execution-sdk-python-otel"),
    )
    runtime.tracker.observe(
        owner,
        "provider_observed",
        createdAtMs=runtime.created_at_ms,
        environmentId=trace.identity["environment"],
        processId=trace.identity["process"],
    )
    runtime.tracker.observe(
        owner,
        "otel_enter",
        contextBefore=snapshot,
        carrier=carrier,
        carrierSource="process-environment",
    )
    try:
        with owned(owner):
            return invoke_handler([runtime.factory])
    finally:
        try:
            # The SDK caller captures its real return before this diagnostic I/O.
            trace.emit(
                "OTEL_EXIT",
                view=view,
                provider=runtime.identity,
                context_restored=otel_context.get_current() == before,
                exporter_shutdowns=runtime.exporter.shutdown_calls,
            )
            runtime.tracker.observe(
                owner,
                "otel_exit",
                contextAfter=_context_snapshot(),
                exporterShutdowns=runtime.exporter.shutdown_calls,
            )
        finally:
            runtime.tracker.close_invocation(owner)

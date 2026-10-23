# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real OTel export into the LMI evidence ledger; no runtime carrier injection."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextvars import ContextVar
from importlib.metadata import version
import os
import threading
import uuid
from weakref import WeakKeyDictionary
from typing import Any, TYPE_CHECKING

from opentelemetry import context as otel_context
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
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

if TYPE_CHECKING:
    from lmi_tests.fixture import Trace

_CURRENT_TRACE: ContextVar[Trace | None] = ContextVar("lmi_otel_evidence", default=None)
_RUNTIMES: dict[str, Runtime] = {}
_RUNTIME_LOCK = threading.Lock()


class LedgerExporter(SpanExporter):
    """Capture the actual exported data, independently of expected attribution."""

    def __init__(self, view: str) -> None:
        self.view = view
        self.shutdown_calls = 0
        self.active_trace: Trace | None = None

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        # The checkpoint batcher may export outside the caller's ContextVar.
        # Native Python LMI has one request per process. Keep its independently
        # observed current invocation as a fallback; never infer ownership from
        # the span's potentially corrupted execution attributes.
        diagnostic = _CURRENT_TRACE.get() or self.active_trace
        if diagnostic is None:
            raise RuntimeError("OTel export lost the invocation evidence context")
        mode = diagnostic.payload.get("otel_export", "success")
        if mode == "held":
            diagnostic.emit("OTEL_EXPORT_HELD", view=self.view)
            diagnostic.gate(diagnostic.payload["otel_export_gate"])
        for span in spans:
            context = span.context
            if context is None:
                raise ValueError("Exported span has no span context")
            data = {
                "name": span.name,
                "trace_id": format(context.trace_id, "032x"),
                "span_id": format(context.span_id, "016x"),
                "parent_id": format(span.parent.span_id, "016x")
                if span.parent
                else None,
                "sampled": context.trace_flags.sampled,
                "start_ns": span.start_time,
                "end_ns": span.end_time,
                "status": span.status.status_code.name,
                "attributes": dict(span.attributes or {}),
            }
            diagnostic.emit("OTEL_EXPORT", view=self.view, outcome=mode, span=data)
        return (
            SpanExportResult.FAILURE if mode == "failure" else SpanExportResult.SUCCESS
        )

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class EvidenceFactory:
    """Observe factory ownership while returning the actual bundled OTel plugin."""

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
        trace = _CURRENT_TRACE.get()
        if trace is None:
            raise RuntimeError("Factory lost the invocation evidence context")
        plugin = self.delegate.create_plugin(info)
        with self.lock:
            identity = self.instances.setdefault(plugin, uuid.uuid4().hex)
        trace.emit(
            "OTEL_FACTORY",
            plugin_instance=identity,
            plugin_execution=info.execution_arn,
        )
        return plugin


class Runtime:
    """A provider/exporter shared within one Python worker, across invocations."""

    def __init__(self, view: str) -> None:
        self.identity = uuid.uuid4().hex
        self.exporter = LedgerExporter(view)
        self.provider = TracerProvider(sampler=ALWAYS_ON, shutdown_on_exit=False)
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.factory = EvidenceFactory(view, self.provider)


def invoke(
    trace: Trace,
    invoke_handler: Callable[
        [list[DurableInstrumentationPluginFactory]], dict[str, Any]
    ],
) -> dict[str, Any]:
    """Enter before SDK dispatch so worker-context propagation is also exercised."""
    view = trace.payload["otel_view"]
    if view not in {"invocation", "execution"}:
        raise ValueError("Unknown OTel view")
    with _RUNTIME_LOCK:
        if view not in _RUNTIMES:
            _RUNTIMES[view] = Runtime(view)
        runtime = _RUNTIMES[view]
    token = _CURRENT_TRACE.set(trace)
    runtime.exporter.active_trace = trace
    before = otel_context.get_current()
    trace.emit(
        "OTEL_ENTER",
        view=view,
        provider=runtime.identity,
        carrier=os.environ.get("_X_AMZN_TRACE_ID"),
        carrier_source="runtime_environment",
        core_version=version("aws-durable-execution-sdk-python"),
        otel_version=version("aws-durable-execution-sdk-python-otel"),
    )
    try:
        return invoke_handler([runtime.factory])
    finally:
        try:
            trace.emit(
                "OTEL_EXIT",
                view=view,
                provider=runtime.identity,
                context_restored=otel_context.get_current() == before,
                exporter_shutdowns=runtime.exporter.shutdown_calls,
            )
        finally:
            _CURRENT_TRACE.reset(token)

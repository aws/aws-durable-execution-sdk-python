# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Real processor/exporter fixtures with immutable invocation ownership.

Only diagnostic publication is deferred in synchronous mode. Export itself
remains on SimpleSpanProcessor's caller. The asynchronous mode queues the same
exporter work and bounds the SDK's flush wait independently of its completion.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import queue
import threading
import time
from typing import Any, Protocol
import uuid

from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class Trace(Protocol):
    payload: Mapping[str, Any]
    identity: Mapping[str, Any]

    def emit(self, phase: str, **fields: Any) -> Any: ...

    def observe(self, kind: str, **fields: Any) -> Any: ...

    def capture_observation(self, kind: str, **fields: Any) -> dict[str, Any]: ...

    def publish_observation(self, observation: Mapping[str, Any]) -> Any: ...

    def gate(self, name: str, **fields: Any) -> Any: ...

    def gate_release(self, name: str, **fields: Any) -> Any: ...


@dataclass(eq=False)
class Owner:
    trace: Trace
    tracker: Tracker
    export_behavior: str
    gate: str | None
    shared: bool
    case_id: str
    request_id: str
    exports: list[Export] = field(default_factory=list)
    publications: int = 0
    closed: bool = False
    quiescent: bool = False
    publication_errors: list[str] = field(default_factory=list)


CURRENT_OWNER: ContextVar[Owner | None] = ContextVar("lmi_export_owner", default=None)


@contextmanager
def owned(owner: Owner):
    token = CURRENT_OWNER.set(owner)
    try:
        yield
    finally:
        CURRENT_OWNER.reset(token)


@dataclass(eq=False)
class Export:
    owner: Owner
    identity: str
    spans: tuple[dict[str, Any], ...]
    result: SpanExportResult | None = None


@dataclass(eq=False)
class Flush:
    owner: Owner
    identity: str
    exports: tuple[Export, ...]
    result: SpanExportResult | None = None


def snapshot(span: ReadableSpan) -> dict[str, Any]:
    context = span.context
    if context is None:
        raise ValueError("Exported span has no span context")
    return {
        "name": span.name,
        "trace_id": format(context.trace_id, "032x"),
        "span_id": format(context.span_id, "016x"),
        "parent_id": format(span.parent.span_id, "016x") if span.parent else None,
        "sampled": context.trace_flags.sampled,
        "start_ns": span.start_time,
        "end_ns": span.end_time,
        "status": span.status.status_code.name,
        "kind": span.kind.name,
        "attributes": dict(span.attributes or {}),
        "service_name": span.resource.attributes.get("service.name"),
        "links": [
            {
                "trace_id": format(link.context.trace_id, "032x"),
                "span_id": format(link.context.span_id, "016x"),
            }
            for link in span.links
        ],
        "exception_event_count": (
            None
            if span.dropped_events
            else sum(event.name == "exception" for event in span.events)
        ),
    }


# Test-processor configuration, separate from the admitted acceptance budget.
DEFAULT_FLUSH_TIMEOUT_MS = 5000


class Tracker:
    """Keep SDK work and evidence publication owned until both actually settle."""

    def __init__(
        self,
        provider_id: str,
        exporter_id: str,
        view: str,
        export_mode: str,
        flush_timeout_ms: float = DEFAULT_FLUSH_TIMEOUT_MS,
    ) -> None:
        self.provider_id, self.exporter_id, self.view = provider_id, exporter_id, view
        self.export_mode = export_mode
        self.flush_timeout_ms = flush_timeout_ms
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.exports: dict[str, Export] = {}
        self.flushes: dict[str, Flush] = {}
        self.owners: dict[str, Owner] = {}
        self.publications: queue.Queue[tuple[Owner, str, dict[str, Any]] | None] = (
            queue.Queue()
        )
        self.publisher = threading.Thread(
            target=self._publish, name="lmi-otel-evidence", daemon=True
        )
        self.publisher.start()

    def owner(self, trace: Trace) -> Owner:
        payload = trace.payload
        result = Owner(
            trace=trace,
            tracker=self,
            export_behavior=str(payload.get("otel_export", "success")),
            gate=payload.get("otel_export_gate"),
            shared=bool(payload.get("coverageProfile")),
            case_id=str(payload.get("caseId", trace.identity.get("marker", ""))),
            request_id=str(trace.identity["request"]),
        )
        with self.lock:
            self.owners[result.request_id] = result
        return result

    def observe(self, owner: Owner, kind: str, **fields: Any) -> None:
        if not owner.shared:
            return
        with self.lock:
            captured = time.time_ns()
            data = {
                "atMs": captured / 1_000_000,
                "capturedAtNs": captured,
                "otelView": self.view,
                "exportMode": self.export_mode,
                "providerId": self.provider_id,
                "exporterId": self.exporter_id,
                **fields,
            }
            observation = owner.trace.capture_observation(kind, **data)
            owner.publications += 1
            owner.quiescent = False
            self.owners[owner.request_id] = owner
            self.publications.put((owner, kind, observation))

    def _publish(self) -> None:
        while (item := self.publications.get()) is not None:
            owner, kind, data = item
            try:
                owner.trace.publish_observation(data)
            except Exception as error:
                # Unknown publication remains owned, never a fabricated success.
                with self.lock:
                    owner.publication_errors.append(repr(error))
            else:
                with self.lock:
                    owner.publications -= 1
                    if (
                        kind == "export_quiescence"
                        and owner.publications == 0
                        and owner.quiescent
                    ):
                        self.owners.pop(owner.request_id, None)
                    else:
                        self._quiescence(owner)
            finally:
                self.publications.task_done()
            del item, owner, data
        self.publications.task_done()

    def pending(self, owner: Owner) -> list[str]:
        return [
            identity
            for identity, work in self.exports.items()
            if work.owner.case_id == owner.case_id and work.result is None
        ]

    def active(self, case_id: str) -> dict[str, Any]:
        with self.lock:
            owners = [
                owner for owner in self.owners.values() if owner.case_id == case_id
            ]
            return {
                "pendingExportIds": [
                    key
                    for key, work in self.exports.items()
                    if work.owner.case_id == case_id
                ],
                "pendingFlushIds": [
                    key
                    for key, work in self.flushes.items()
                    if work.owner.case_id == case_id
                ],
                "publicationCount": sum(owner.publications for owner in owners),
                "publicationErrors": [
                    error for owner in owners for error in owner.publication_errors
                ],
            }

    def close_invocation(self, owner: Owner) -> None:
        with self.lock:
            owner.closed = True
            owner.exports.clear()
            self._quiescence(owner)

    def _quiescence(self, owner: Owner) -> None:
        if (
            not owner.closed
            or owner.publications
            or owner.quiescent
            or any(work.owner is owner for work in self.exports.values())
            or any(work.owner is owner for work in self.flushes.values())
        ):
            return
        owner.quiescent = True
        if owner.shared:
            # This signal follows publication of every earlier observation.
            # Do not count the signal itself as outstanding prior publication.
            owner.publications += 1
            self.publications.put(
                (
                    owner,
                    "export_quiescence",
                    owner.trace.capture_observation(
                        "export_quiescence",
                        pendingExportIds=[],
                        pendingFlushIds=[],
                        publicationCount=0,
                    ),
                )
            )
        else:
            self.owners.pop(owner.request_id, None)

    def accept(self, spans: Sequence[ReadableSpan]) -> Export:
        owner = CURRENT_OWNER.get()
        if owner is None or owner.tracker is not self:
            raise RuntimeError(
                "Export lost its original public-plugin invocation owner"
            )
        work = Export(owner, uuid.uuid4().hex, tuple(snapshot(span) for span in spans))
        with self.lock:
            self.exports[work.identity] = work
            owner.exports.append(work)
            self.observe(owner, "export_started", exportId=work.identity)
            for span in work.spans:
                self.observe(
                    owner,
                    "otel_export_started",
                    exportId=work.identity,
                    traceId=span["trace_id"],
                    spanId=span["span_id"],
                )
        return work

    def complete(
        self, work: Export, result: SpanExportResult, error: str | None
    ) -> None:
        with self.changed:
            work.result = result
            self.observe(
                work.owner,
                "export_completed",
                exportId=work.identity,
                outcome="succeeded" if result is SpanExportResult.SUCCESS else "failed",
                **(
                    {"fixture": "bounded-export-error"}
                    if work.owner.export_behavior == "failure"
                    else {}
                ),
                **(
                    {"error": error or "SpanExportResult.FAILURE"}
                    if result is not SpanExportResult.SUCCESS
                    else {}
                ),
            )
            self.exports.pop(work.identity)
            for flush in tuple(self.flushes.values()):
                self._complete_flush(flush)
            self.changed.notify_all()
            self._quiescence(work.owner)

    def _complete_flush(self, flush: Flush) -> None:
        if flush.result is not None or any(
            work.result is None for work in flush.exports
        ):
            return
        flush.result = (
            SpanExportResult.SUCCESS
            if all(work.result is SpanExportResult.SUCCESS for work in flush.exports)
            else SpanExportResult.FAILURE
        )
        self.observe(
            flush.owner,
            "flush_completed",
            flushId=flush.identity,
            outcome="succeeded"
            if flush.result is SpanExportResult.SUCCESS
            else "failed",
            pendingExportIds=self.pending(flush.owner),
        )
        self.flushes.pop(flush.identity)
        self.changed.notify_all()

    def force_flush(self, timeout_millis: float) -> bool:
        owner = CURRENT_OWNER.get()
        if owner is None or owner.tracker is not self:
            raise RuntimeError("Flush lost its public-plugin invocation owner")
        allowed = min(float(timeout_millis), self.flush_timeout_ms)
        if allowed <= 0:
            raise ValueError(
                "Flush wait must have a positive provider and scenario budget"
            )
        deadline = time.monotonic() + allowed / 1000
        with self.changed:
            flush = Flush(owner, uuid.uuid4().hex, tuple(owner.exports))
            owner.exports.clear()
            self.flushes[flush.identity] = flush
            self.observe(
                owner,
                "flush_started",
                flushId=flush.identity,
                timeoutMs=allowed,
                configuredTimeoutMs=self.flush_timeout_ms,
                pendingExportIds=self.pending(owner),
            )
            self._complete_flush(flush)
            while (
                flush.result is None and (remaining := deadline - time.monotonic()) > 0
            ):
                self.changed.wait(remaining)
            outcome = (
                "timeout"
                if flush.result is None
                else "completed"
                if flush.result is SpanExportResult.SUCCESS
                else "error"
            )
            self.observe(
                owner,
                "flush_wait_completed",
                flushId=flush.identity,
                outcome=outcome,
                pendingExportIds=self.pending(owner),
                **({"error": "Observed export failure"} if outcome == "error" else {}),
            )
            return flush.result is SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        self.publications.join()
        self.publications.put(None)
        self.publisher.join()


class LedgerExporter(SpanExporter):
    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker
        self.shutdown_calls = 0

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self.export_owned(self.tracker.accept(spans))

    def export_owned(self, work: Export) -> SpanExportResult:
        owner, result, failure = work.owner, SpanExportResult.FAILURE, None
        try:
            if owner.export_behavior == "held":
                if not owner.gate:
                    raise ValueError("Held export requires its case-owned gate")
                owner.trace.emit("OTEL_EXPORT_HELD", view=self.tracker.view)
                if owner.shared:

                    def held(at_ms):
                        self.tracker.observe(
                            owner,
                            "export_held",
                            exportId=work.identity,
                            gateId=owner.gate,
                            atMs=at_ms,
                        )

                    def released(release):
                        if release.held_at_ms is not None:
                            self.tracker.observe(
                                owner,
                                "export_released",
                                exportId=work.identity,
                                gateId=owner.gate,
                                mode=release.mode,
                                atMs=release.observed_at_ms,
                            )

                    owner.trace.gate_release(
                        owner.gate, on_hold=held, on_release=released
                    )
                else:
                    owner.trace.gate(owner.gate)
            for span in work.spans:
                owner.trace.emit(
                    "OTEL_EXPORT",
                    view=self.tracker.view,
                    outcome=owner.export_behavior,
                    span=span,
                )
                self.tracker.observe(
                    owner,
                    "otel_export",
                    exportId=work.identity,
                    outcome=owner.export_behavior,
                    traceId=span["trace_id"],
                    spanId=span["span_id"],
                    span=span,
                )
            result = (
                SpanExportResult.FAILURE
                if owner.export_behavior == "failure"
                else SpanExportResult.SUCCESS
            )
            return result
        except Exception as error:
            failure = repr(error)
            owner.trace.emit("OTEL_EXPORT_ERROR", view=self.tracker.view, error=failure)
            return result
        finally:
            self.tracker.complete(work, result, failure)

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.tracker.shutdown()


class AsyncSpanProcessor(SpanProcessor):
    def __init__(self, exporter: LedgerExporter) -> None:
        self.exporter = exporter
        self.executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="lmi-otel-export"
        )

    def on_start(self, span, parent_context=None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        work = self.exporter.tracker.accept((span,))
        self.executor.submit(self.exporter.export_owned, work)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.exporter.tracker.force_flush(timeout_millis)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True)
        self.exporter.shutdown()

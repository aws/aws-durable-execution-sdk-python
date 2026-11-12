# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Driver-only conversion of actual OTel ledger data to the shared wire model.

This module is not part of the Lambda fixture bundle. It imports the pinned
conformance model only when converting observed spans, not on runtime startup.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any


def _canonical_span(raw: Mapping[str, Any]) -> dict[str, Any]:
    from aws_durable_execution_conformance_tests_otel.model import (
        Span,
        SpanLink,
        span_to_dict,
    )

    return span_to_dict(
        Span(
            trace_id=raw["trace_id"],
            span_id=raw["span_id"],
            name=raw["name"],
            start_time=datetime.fromtimestamp(raw["start_ns"] / 1_000_000_000, UTC),
            end_time=datetime.fromtimestamp(raw["end_ns"] / 1_000_000_000, UTC),
            parent_span_id=raw["parent_id"],
            status=raw["status"],
            kind=raw["kind"],
            attributes=raw["attributes"],
            service_name=raw["service_name"],
            links=tuple(SpanLink(**link) for link in raw["links"]),
            exception_event_count=raw["exception_event_count"],
        )
    )


def collect_telemetry(
    raw_events: Sequence[Mapping[str, Any]],
    attempts: Sequence[Mapping[str, Any]],
    *,
    view: str,
    expected_sampled: bool,
    caller_headers: Mapping[str, str],
    observed_until_ms: float,
    export_mode: str | None = None,
) -> dict[str, Any]:
    """Preserve exporter ownership and report incomplete collection explicitly.

    Expected sampling and caller headers come from the admitted driver's actual
    Invoke configuration. They are never inferred from the observed runtime
    carrier or from whether telemetry happened to arrive.
    """
    by_request = {attempt["requestId"]: attempt for attempt in attempts}
    observations = [
        (record, record["observation"])
        for record in raw_events
        if record.get("phase") == "CONFORMANCE"
        and isinstance(record.get("observation"), Mapping)
    ]
    providers: dict[str, dict[str, Any]] = {}
    bindings, carriers, spans, outcomes = [], [], [], []
    complete = bool(attempts)
    for request_id, attempt in by_request.items():
        records = [
            record for record in raw_events if record.get("request") == request_id
        ]
        local = [
            (record, observation)
            for record, observation in observations
            if record.get("request") == request_id
        ]
        numbers = [record.get("sequence") for record in records]
        complete &= bool(numbers) and all(type(number) is int for number in numbers)
        if numbers and all(type(number) is int for number in numbers):
            sequences = [number for number in numbers if isinstance(number, int)]
            complete &= sorted(sequences) == list(range(1, max(sequences) + 1))
        complete &= any(
            record.get("phase") in {"WRAPPER_RETURN", "WRAPPER_RAISE"}
            for record in records
        )
        quiescent = [
            observation
            for _, observation in local
            if observation.get("kind") == "export_quiescence"
        ]
        complete &= bool(quiescent) and all(
            item.get("pendingExportIds") == []
            and item.get("pendingFlushIds") == []
            and item.get("publicationCount") == 0
            for item in quiescent
        )
        entries = [
            (record, item) for record, item in local if item.get("kind") == "otel_enter"
        ]
        if any(item.get("otelView") != view for _, item in entries):
            raise ValueError("The observed OTel view differs from the admitted view")
        if export_mode is not None and any(
            item.get("exportMode") != export_mode for _, item in entries
        ):
            raise ValueError(
                "The observed exporter mode differs from the admitted mode"
            )
        completed = [
            item for _, item in local if item.get("kind") == "export_completed"
        ]
        started = [item for _, item in local if item.get("kind") == "export_started"]
        complete &= len(entries) == 1 and {item["exportId"] for item in started} == {
            item["exportId"] for item in completed
        }
        for _, observation in local:
            if observation.get("kind") == "provider_observed":
                provider = {
                    key: observation[key]
                    for key in (
                        "providerId",
                        "exporterId",
                        "createdAtMs",
                        "environmentId",
                        "processId",
                    )
                }
                previous = providers.setdefault(provider["providerId"], provider)
                if previous != provider:
                    raise ValueError(
                        "The observed provider identity changed across requests"
                    )
            if observation.get("kind") == "otel_export":
                raw_span = observation["span"]
                spans.append(
                    {
                        "requestId": request_id,
                        "exportId": observation["exportId"],
                        "sampled": raw_span["sampled"],
                        "span": _canonical_span(raw_span),
                    }
                )
        if len(entries) == 1:
            source, entry = entries[0]
            bindings.append(
                {"requestId": request_id, "providerId": entry["providerId"]}
            )
            carriers.append(
                {
                    "requestId": request_id,
                    "environmentId": source["environment"],
                    "processId": source["process"],
                    "workerId": source.get("workerId", source["process"]),
                    "source": entry["carrierSource"],
                    "header": entry.get("carrier"),
                    "expectedSampled": expected_sampled,
                    **(
                        {"callerHeader": caller_headers[request_id]}
                        if request_id in caller_headers
                        else {}
                    ),
                }
            )
        failures = [item for item in completed if item.get("outcome") == "failed"]
        if failures:
            failed = failures[0]
            outcomes.append(
                {
                    "requestId": request_id,
                    "status": "export-error",
                    "error": failed.get("error"),
                    "fixture": failed.get("fixture"),
                }
            )
        else:
            outcomes.append(
                {
                    "requestId": request_id,
                    "status": "delivered"
                    if started
                    else "unsampled"
                    if not expected_sampled
                    else "missing",
                }
            )
    unknown = {
        record.get("request")
        for record, item in observations
        if item.get("kind") == "otel_export"
    } - set(by_request)
    if unknown:
        raise ValueError("Telemetry contains an unjournaled original invocation")
    return {
        "providers": list(providers.values()),
        "providerBindings": bindings,
        "views": {
            view: {
                "collection": {
                    "status": "complete" if complete else "incomplete",
                    "scope": "cohort",
                    "requestIds": list(by_request),
                    "throughMs": observed_until_ms,
                },
                "carriers": carriers,
                "spans": spans,
                "exportOutcomes": outcomes,
            }
        },
    }

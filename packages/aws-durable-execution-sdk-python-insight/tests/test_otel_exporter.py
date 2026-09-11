# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``OTelExporter`` against a local HTTP server."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator

import pytest

from aws_durable_execution_sdk_python_insight import OTelExporter, OTelProtocol
from aws_durable_execution_sdk_python_insight.exporters.otel_exporter import (
    OTelExporter as OTelExporterFromModule,
)


def _record(**overrides: Any) -> dict[str, Any]:
    """A complete SUCCEEDED record; keyword arguments override fields."""
    record: dict[str, Any] = {
        "recordType": "WorkflowInsight",
        "schemaVersion": "1.0",
        "emittedAt": "2026-07-15T12:00:00.000Z",
        "executionArn": (
            "arn:aws:lambda:us-east-1:123456789012:function:fn:$LATEST"
            "/durable-execution/my-exec/inv-1"
        ),
        "executionName": "my-exec",
        "functionName": "fn",
        "functionQualifier": "$LATEST",
        "region": "us-east-1",
        "accountId": "123456789012",
        "status": "SUCCEEDED",
        "startTime": "2026-07-15T11:59:58.000Z",
        "endTime": "2026-07-15T12:00:00.000Z",
        "durationMs": 2000,
        "operations": [
            {
                "id": "op-1",
                "name": "fetch-user",
                "type": "STEP",
                "subType": "Step",
                "status": "SUCCEEDED",
                "durationMs": 12,
            }
        ],
    }
    record.update(overrides)
    return record


@dataclass
class CapturedRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


@dataclass
class HttpCapture:
    """A local HTTP server that records requests and replies with ``status``."""

    url: str
    requests: list[CapturedRequest] = field(default_factory=list)
    status: int = 200
    response_body: bytes = b""
    delay_seconds: float = 0.0
    location: str | None = None


@pytest.fixture
def http_capture() -> Iterator[HttpCapture]:
    capture = HttpCapture(url="")

    class Handler(BaseHTTPRequestHandler):
        def _handle(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            capture.requests.append(
                CapturedRequest(
                    method=self.command,
                    path=self.path,
                    headers={k.lower(): v for k, v in self.headers.items()},
                    body=body,
                )
            )
            if capture.delay_seconds:
                time.sleep(capture.delay_seconds)
            self.send_response(capture.status)
            if capture.location is not None:
                self.send_header("Location", capture.location)
            self.send_header("Content-Length", str(len(capture.response_body)))
            self.end_headers()
            self.wfile.write(capture.response_body)

        do_POST = _handle
        do_PUT = _handle

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    capture.url = f"http://127.0.0.1:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield capture
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _log_record(payload: dict[str, Any]) -> dict[str, Any]:
    resource_log = payload["resourceLogs"][0]
    return dict(resource_log["scopeLogs"][0]["logRecords"][0])


def _attrs(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {item["key"]: item["value"] for item in items}


def test_public_import_path_and_defaults() -> None:
    assert OTelExporter is OTelExporterFromModule
    exporter = OTelExporter(endpoint="http://127.0.0.1:1/v1/logs")
    assert exporter.max_record_size_bytes == 1_000_000
    assert exporter.operations_format == "array"
    assert exporter.headers == {}
    exporter.flush()  # no buffering: a no-op


def test_http_protobuf_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="http/protobuf is not yet supported"):
        OTelExporter(endpoint="http://127.0.0.1:1/v1/logs", protocol="http/protobuf")
    with pytest.raises(ValueError):
        OTelExporter(
            endpoint="http://127.0.0.1:1/v1/logs", protocol=OTelProtocol.HTTP_PROTOBUF
        )
    with pytest.raises(ValueError):
        OTelExporter(
            endpoint="http://127.0.0.1:1/v1/logs",
            protocol="grpc",  # type: ignore[arg-type]  # dynamic invalid value
        )


def test_posts_otlp_request_with_record_in_log_body(http_capture: HttpCapture) -> None:
    record = _record()
    exporter = OTelExporter(
        endpoint=f"{http_capture.url}/v1/logs", headers={"x-api-key": "k"}
    )
    exporter.export(record)

    assert len(http_capture.requests) == 1
    req = http_capture.requests[0]
    assert req.method == "POST"
    assert req.path == "/v1/logs"
    assert req.headers["content-type"] == "application/json"
    assert req.headers["x-api-key"] == "k"

    payload = json.loads(req.body)
    assert req.body == json.dumps(
        exporter.render(record), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    resource_attrs = _attrs(payload["resourceLogs"][0]["resource"]["attributes"])
    assert resource_attrs["service.name"] == {"stringValue": "fn"}
    assert resource_attrs["cloud.region"] == {"stringValue": "us-east-1"}
    assert resource_attrs["cloud.account.id"] == {"stringValue": "123456789012"}
    assert resource_attrs["faas.name"] == {"stringValue": "fn"}
    assert resource_attrs["faas.version"] == {"stringValue": "$LATEST"}
    scope = payload["resourceLogs"][0]["scopeLogs"][0]["scope"]
    assert scope == {
        "name": "aws-durable-execution-sdk-python-insight",
        "version": "1.0",
    }

    log_record = _log_record(payload)
    # 2026-07-15T12:00:00.000Z in nanoseconds since the epoch, as a string
    assert log_record["timeUnixNano"] == "1784116800000000000"
    assert log_record["severityNumber"] == 9
    assert log_record["severityText"] == "SUCCEEDED"
    body = json.loads(log_record["body"]["stringValue"])
    assert body == record
    attrs = _attrs(log_record["attributes"])
    assert attrs["workflow.execution_arn"] == {"stringValue": record["executionArn"]}
    assert attrs["workflow.execution_name"] == {"stringValue": "my-exec"}
    assert attrs["workflow.status"] == {"stringValue": "SUCCEEDED"}
    assert attrs["workflow.duration_ms"] == {"intValue": "2000"}


def test_failed_maps_to_error_severity_and_missing_fields_default(
    http_capture: HttpCapture,
) -> None:
    record = _record(status="FAILED")
    del record["executionName"]
    del record["durationMs"]
    OTelExporter(endpoint=f"{http_capture.url}/v1/logs").export(record)
    log_record = _log_record(json.loads(http_capture.requests[0].body))
    assert log_record["severityNumber"] == 17
    assert log_record["severityText"] == "FAILED"
    attrs = _attrs(log_record["attributes"])
    assert attrs["workflow.execution_name"] == {"stringValue": ""}
    assert attrs["workflow.duration_ms"] == {"intValue": "0"}


def test_unknown_status_maps_to_unspecified_severity() -> None:
    rendered = OTelExporter(endpoint="http://127.0.0.1:1/").render(
        _record(status="STOPPED")
    )
    assert _log_record(rendered)["severityNumber"] == 0


def test_by_name_format_reshapes_log_body_only(http_capture: HttpCapture) -> None:
    OTelExporter(
        endpoint=f"{http_capture.url}/v1/logs", operations_format="by-name"
    ).export(_record())
    body = json.loads(
        _log_record(json.loads(http_capture.requests[0].body))["body"]["stringValue"]
    )
    assert "operations" not in body
    assert body["operationsByName"]["fetch-user"]["count"] == 1


def test_non_2xx_response_raises(http_capture: HttpCapture) -> None:
    http_capture.status = 503
    exporter = OTelExporter(endpoint=f"{http_capture.url}/v1/logs")
    with pytest.raises(RuntimeError, match=r"OTelExporter: OTLP endpoint returned 503"):
        exporter.export(_record())


def test_time_unix_nano_keeps_microseconds() -> None:
    rendered = OTelExporter(endpoint="http://127.0.0.1:1/").render(
        _record(emittedAt="2026-07-15T12:00:00.123456Z")
    )
    assert _log_record(rendered)["timeUnixNano"] == "1784116800123456000"


def test_redirect_is_reported_as_failure(http_capture: HttpCapture) -> None:
    http_capture.status = 302
    http_capture.location = f"{http_capture.url}/other"
    exporter = OTelExporter(
        endpoint=f"{http_capture.url}/v1/logs", headers={"x-api-key": "k"}
    )
    with pytest.raises(RuntimeError, match=r"OTLP endpoint returned 302"):
        exporter.export(_record())
    assert [r.path for r in http_capture.requests] == ["/v1/logs"]

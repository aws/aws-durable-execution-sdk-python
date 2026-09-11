# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``HttpExporter`` against a local HTTP server."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator

import pytest

from aws_durable_execution_sdk_python_insight import HttpExporter, HttpMethod
from aws_durable_execution_sdk_python_insight.exporters.http_exporter import (
    HttpExporter as HttpExporterFromModule,
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


def test_public_import_path_and_defaults() -> None:
    assert HttpExporter is HttpExporterFromModule
    exporter = HttpExporter(url="http://127.0.0.1:1/insight")
    assert exporter.method is HttpMethod.POST
    assert exporter.timeout_ms == 10_000
    assert exporter.headers == {}
    assert exporter.operations_format == "array"
    assert exporter.max_record_size_bytes is None
    exporter.flush()  # no buffering: a no-op
    with pytest.raises(ValueError):
        HttpExporter(
            url="http://127.0.0.1:1/",
            method="PATCH",  # type: ignore[arg-type]  # dynamic invalid value
        )


def test_posts_compact_json_with_content_type(http_capture: HttpCapture) -> None:
    record = _record()
    exporter = HttpExporter(url=f"{http_capture.url}/insight")
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(http_capture.requests) == 1
    req = http_capture.requests[0]
    assert req.method == "POST"
    assert req.path == "/insight"
    assert req.headers["content-type"] == "application/json"
    assert req.body == json.dumps(
        record, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert isinstance(json.loads(req.body)["operations"], list)


def test_put_with_custom_headers_and_by_name_format(http_capture: HttpCapture) -> None:
    HttpExporter(
        url=f"{http_capture.url}/upsert",
        method="PUT",
        headers={"Authorization": "Bearer token123"},
        operations_format="by-name",
    ).export(_record())
    req = http_capture.requests[0]
    assert req.method == "PUT"
    assert req.headers["authorization"] == "Bearer token123"
    assert req.headers["content-type"] == "application/json"
    body = json.loads(req.body)
    assert "operations" not in body
    assert body["operationsByName"]["fetch-user"]["count"] == 1


def test_custom_header_can_override_content_type(http_capture: HttpCapture) -> None:
    HttpExporter(
        url=http_capture.url, headers={"Content-Type": "application/x-ndjson"}
    ).export(_record())
    assert http_capture.requests[0].headers["content-type"] == "application/x-ndjson"


def test_non_2xx_response_raises(http_capture: HttpCapture) -> None:
    http_capture.status = 500
    exporter = HttpExporter(url=http_capture.url)
    with pytest.raises(RuntimeError, match=r"HttpExporter: endpoint returned 500"):
        exporter.export(_record())


def test_timeout_is_enforced(http_capture: HttpCapture) -> None:
    http_capture.delay_seconds = 1.0
    exporter = HttpExporter(url=http_capture.url, timeout_ms=100)
    with pytest.raises(TimeoutError):
        exporter.export(_record())


def test_oversized_error_body_is_capped_in_the_message(
    http_capture: HttpCapture,
) -> None:
    http_capture.status = 502
    http_capture.response_body = b"x" * (200 * 1024)
    exporter = HttpExporter(url=http_capture.url)
    with pytest.raises(RuntimeError) as info:
        exporter.export(_record())
    # the message carries status and reason only; the body is not echoed
    assert str(info.value) == "HttpExporter: endpoint returned 502 Bad Gateway"


def test_large_success_body_is_not_read(http_capture: HttpCapture) -> None:
    http_capture.response_body = b"y" * (4 * 1024 * 1024)
    HttpExporter(url=http_capture.url).export(_record())
    assert len(http_capture.requests) == 1


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirects_are_not_followed(http_capture: HttpCapture, status: int) -> None:
    http_capture.status = status
    http_capture.location = f"{http_capture.url}/elsewhere"
    exporter = HttpExporter(
        url=f"{http_capture.url}/insight", headers={"Authorization": "Bearer secret"}
    )
    with pytest.raises(
        RuntimeError, match=rf"HttpExporter: endpoint returned {status}"
    ):
        exporter.export(_record())
    # exactly one request, to the configured URL, with the body; nothing was
    # re-sent to the redirect target
    assert [r.path for r in http_capture.requests] == ["/insight"]
    assert http_capture.requests[0].method == "POST"
    assert http_capture.requests[0].body

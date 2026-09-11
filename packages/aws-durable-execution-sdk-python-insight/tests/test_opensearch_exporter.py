# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for ``OpenSearchExporter`` against a local HTTP server."""

from __future__ import annotations

import base64
import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator
from urllib.parse import quote

import pytest

from aws_durable_execution_sdk_python_insight import (
    OpenSearchAuth,
    OpenSearchExporter,
)
from aws_durable_execution_sdk_python_insight.exporters.opensearch_exporter import (
    OpenSearchExporter as OpenSearchExporterFromModule,
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
    assert OpenSearchExporter is OpenSearchExporterFromModule
    exporter = OpenSearchExporter(
        endpoint="https://d.us-east-1.es.amazonaws.com/", region="us-east-1"
    )
    assert exporter.endpoint == "https://d.us-east-1.es.amazonaws.com"
    assert exporter.index_name == "workflow-insight"
    assert exporter.auth is OpenSearchAuth.SIGV4
    assert exporter.max_record_size_bytes == 10_000_000
    exporter.flush()  # no buffering: a no-op
    with pytest.raises(ValueError):
        OpenSearchExporter(
            endpoint="https://d",
            region="us-east-1",
            auth="token",  # type: ignore[arg-type]  # dynamic invalid value
        )


def test_sigv4_puts_signed_document_keyed_by_encoded_arn(
    http_capture: HttpCapture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    record = _record()
    exporter = OpenSearchExporter(endpoint=f"{http_capture.url}/", region="us-east-1")
    assert exporter.render(record) is record
    exporter.export(record)

    assert len(http_capture.requests) == 1
    req = http_capture.requests[0]
    assert req.method == "PUT"
    encoded = quote(record["executionArn"], safe="-_.!~*'()")
    assert "$" not in encoded and "/" not in encoded and ":" not in encoded
    assert req.path == f"/workflow-insight/_doc/{encoded}"
    assert req.headers["content-type"] == "application/json"
    assert req.headers["authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert "Credential=AKIAEXAMPLE/" in req.headers["authorization"]
    assert "/us-east-1/es/aws4_request" in req.headers["authorization"]
    assert "x-amz-date" in req.headers
    assert req.headers["x-amz-security-token"] == "token"
    assert req.body == json.dumps(
        record, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def test_basic_auth_sends_encoded_credentials_without_signing(
    http_capture: HttpCapture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    OpenSearchExporter(
        endpoint=http_capture.url,
        region="us-east-1",
        index_name="custom-index",
        auth="basic",
        username="admin",
        password="secret",  # noqa: S106 - test fixture value
    ).export(_record())

    req = http_capture.requests[0]
    assert req.path.startswith("/custom-index/_doc/")
    expected = "Basic " + base64.b64encode(b"admin:secret").decode("ascii")
    assert req.headers["authorization"] == expected
    assert "x-amz-date" not in req.headers


def test_non_2xx_raises_with_status_and_detail(http_capture: HttpCapture) -> None:
    http_capture.status = 403
    http_capture.response_body = b'{"message":"User is not authorized"}'
    exporter = OpenSearchExporter(
        endpoint=http_capture.url,
        region="us-east-1",
        auth=OpenSearchAuth.BASIC,
        username="u",
        password="p",  # noqa: S106 - test fixture value
    )
    with pytest.raises(RuntimeError, match=r"OpenSearch index failed: 403") as info:
        exporter.export(_record())
    assert "User is not authorized" in str(info.value)


def test_error_detail_is_truncated_to_500_chars(http_capture: HttpCapture) -> None:
    http_capture.status = 500
    http_capture.response_body = b"e" * (100 * 1024)
    exporter = OpenSearchExporter(
        endpoint=http_capture.url,
        region="us-east-1",
        auth="basic",
        username="u",
        password="p",  # noqa: S106 - test fixture value
    )
    with pytest.raises(RuntimeError) as info:
        exporter.export(_record())
    message = str(info.value)
    assert message.startswith("OpenSearch index failed: 500 Internal Server Error — ")
    assert message.endswith("e" * 500)
    assert len(message) < 600


@pytest.mark.parametrize(
    ("username", "password"),
    [(None, None), ("admin", None), (None, "secret")],
)
def test_basic_auth_requires_username_and_password(
    username: str | None, password: str | None
) -> None:
    with pytest.raises(ValueError, match="requires username and password"):
        OpenSearchExporter(
            endpoint="https://d",
            region="us-east-1",
            auth="basic",
            username=username,
            password=password,
        )


def test_basic_auth_accepts_an_empty_password(http_capture: HttpCapture) -> None:
    OpenSearchExporter(
        endpoint=http_capture.url,
        region="us-east-1",
        auth="basic",
        username="admin",
        password="",
    ).export(_record())
    expected = "Basic " + base64.b64encode(b"admin:").decode("ascii")
    assert http_capture.requests[0].headers["authorization"] == expected

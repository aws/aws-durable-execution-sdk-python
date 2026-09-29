#!/usr/bin/env python3
"""Give the local runner the Lambda Invoke endpoint the JS tests call.

CloudDurableTestRunner starts an execution by calling the Lambda ``Invoke``
API and reading ``DurableExecutionArn`` off the response (the AWS SDK maps
the ``X-Amz-Durable-Execution-Arn`` response header to that field). The
WebServer under test does not serve the Invoke API; it starts executions via
``POST /start-durable-execution``. This proxy bridges the two:

* ``POST /2015-03-31/functions/{FunctionName}/invocations`` -> translate into
  ``POST /start-durable-execution`` on the upstream WebServer, then answer with
  the new execution ARN in the ``X-Amz-Durable-Execution-Arn`` header.
* everything else (``/2025-12-01/durable-executions/*`` history/state/etc.) ->
  forward verbatim to the upstream WebServer and relay the response.

Runs on the loopback only; no request signing is validated. The account ID
must match the one lambda-shim.cjs reports, or the runner rejects chained
invokes whose target ARN names another account.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

UPSTREAM = "http://127.0.0.1:5050"
# When set, each history, state and checkpoint response body is appended to a
# file in this directory. Use it to investigate a failing example.
DUMP_DIR = os.environ.get("PROXY_DUMP_DIR", "")
ACCOUNT_ID = "123456789012"
EXECUTION_TIMEOUT_SECONDS = 300
RETENTION_DAYS = 7

_INVOKE_RE = re.compile(r"^/2015-03-31/functions/(?P<fn>[^/]+)/invocations/?$")
# Hop-by-hop headers that must not be forwarded.
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


class ProxyHandler(BaseHTTPRequestHandler):
    # HTTP/1.0 => the connection closes after each response, so each request
    # thread exits promptly. HTTP/1.1 keep-alive pins a thread per persistent
    # client connection, which accumulates under a keep-alive SDK client
    # across a long suite run and exhausts file descriptors.
    protocol_version = "HTTP/1.0"

    # Silence the default noisy logging; the run scripts capture stderr.
    def log_message(self, *args: object) -> None:  # noqa: A002
        return

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or "0")
        return self.rfile.read(length) if length else b""

    def _start_execution(self, function_name: str, body: bytes) -> None:
        payload = body.decode("utf-8") if body else ""
        start_input = {
            "AccountId": ACCOUNT_ID,
            "FunctionName": function_name,
            "FunctionQualifier": "$LATEST",
            "ExecutionName": f"{function_name}-{uuid.uuid4().hex}",
            "ExecutionTimeoutSeconds": EXECUTION_TIMEOUT_SECONDS,
            "ExecutionRetentionPeriodDays": RETENTION_DAYS,
            "InvocationId": str(uuid.uuid4()),
        }
        # An Invoke with no payload delivers an empty object to the handler
        # (matching the invoke->start translation of the real service). Passing
        # an empty/absent input instead would be normalized to JSON ``null``.
        start_input["Input"] = payload if payload != "" else "{}"

        req = Request(
            f"{UPSTREAM}/start-durable-execution",
            data=json.dumps(start_input).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=30) as resp:  # noqa: S310
                out = json.loads(resp.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read()
            self.send_response(exc.code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(detail)))
            self.end_headers()
            self.wfile.write(detail)
            return
        except URLError as exc:
            msg = json.dumps({"errorMessage": str(exc)}).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return

        arn = out.get("ExecutionArn", "")
        body_out = b""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Amz-Durable-Execution-Arn", arn)
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def _forward(self, method: str, body: bytes) -> None:
        url = f"{UPSTREAM}{self.path}"
        fwd_headers = {
            k: v for k, v in self.headers.items() if k.lower() not in _HOP_BY_HOP
        }
        req = Request(url, data=body or None, headers=fwd_headers, method=method)
        try:
            with urlopen(req, timeout=60) as resp:  # noqa: S310
                data = resp.read()
                status = resp.status
                resp_headers = list(resp.headers.items())
        except HTTPError as exc:
            data = exc.read()
            status = exc.code
            resp_headers = list(exc.headers.items()) if exc.headers else []
        except URLError as exc:
            data = json.dumps({"errorMessage": str(exc)}).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self.send_response(status)
        for k, v in resp_headers:
            if k.lower() in _HOP_BY_HOP:
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self._maybe_dump(data)

    def _maybe_dump(self, data: bytes) -> None:
        if not DUMP_DIR:
            return
        path = self.path.split("?", 1)[0]
        kind = None
        if path.endswith("/history"):
            kind = "history"
        elif path.endswith("/state"):
            kind = "state"
        elif path.endswith("/checkpoint"):
            kind = "checkpoint"
        if kind is None:
            return
        try:
            os.makedirs(DUMP_DIR, exist_ok=True)
            with open(os.path.join(DUMP_DIR, f"{kind}.ndjson"), "ab") as fh:
                fh.write(data.rstrip(b"\n") + b"\n")
        except OSError:
            pass

    def _dispatch(self, method: str) -> None:
        body = self._read_body()
        m = _INVOKE_RE.match(self.path.split("?", 1)[0])
        if method == "POST" and m:
            self._start_execution(m.group("fn"), body)
        else:
            self._forward(method, body)

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch("DELETE")


def main() -> int:
    global UPSTREAM  # noqa: PLW0603

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--upstream", default=UPSTREAM)
    args = parser.parse_args()

    UPSTREAM = args.upstream

    server = ThreadingHTTPServer(("127.0.0.1", args.port), ProxyHandler)
    print(f"invoke_proxy on :{args.port} -> {UPSTREAM}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())

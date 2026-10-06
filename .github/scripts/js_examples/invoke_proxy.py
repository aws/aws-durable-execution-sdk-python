"""Accept the Lambda Invoke call the JS tests use to start an execution.

The JS test runner (CloudDurableTestRunner) starts an execution by calling
Lambda Invoke on the function. It reads the execution ARN from the
X-Amz-Durable-Execution-Arn response header. The local runner has no Invoke
route. It starts executions with POST /start-durable-execution. So this
proxy sits between jest and the runner:

* POST /2015-03-31/functions/{name}/invocations: the proxy calls
  POST /start-durable-execution on the runner, then answers with the new
  execution ARN in the X-Amz-Durable-Execution-Arn header.
* Every other request, for example history, state and callbacks: the proxy
  forwards it to the runner unchanged and relays the response.

The proxy listens on the loopback only and does not check request
signatures. Its account ID must match the one lambda-shim.cjs reports.
Otherwise the runner rejects chained invokes whose target ARN names another
account.

run.py serves a ProxyServer on a thread for each server session.
"""

from __future__ import annotations

import json
import re
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote
from urllib.request import Request, urlopen

ACCOUNT_ID = "123456789012"
# Used only when a function's DurableConfig omits a field. The examples'
# template sets both fields on every function. The values match the runner's
# defaults for a chained child, DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS and
# DEFAULT_CHILD_RETENTION_PERIOD_DAYS in child_dispatcher.py, so a function
# gets the same settings whether it starts as a root or as a child. This
# module does not import them, because its tests run without the testing
# package installed.
DEFAULT_EXECUTION_TIMEOUT_SECONDS = 120
DEFAULT_RETENTION_DAYS = 1

INVOKE_PATH = re.compile(r"^/2015-03-31/functions/(?P<name>[^/]+)/invocations/?$")
# Hop-by-hop headers, and headers the proxy sets itself. They are not
# forwarded. send_response() always adds Date and Server. Forwarding the
# runner's copies would send each twice, and the AWS SDK reads a doubled Date
# as one invalid date: its clock-skew correction then breaks request signing.
SKIPPED_HEADERS = {
    "date",
    "server",
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


def split_identifier(identifier: str) -> tuple[str, str | None]:
    """Split a Lambda function identifier into its name and qualifier.

    Invoke accepts a name, name:qualifier, or a function ARN with or without
    a qualifier. The qualifier is None when the identifier has none.
    """
    parts = identifier.split(":")
    if parts[0] == "arn" and len(parts) > 6:
        # arn:aws:lambda:<region>:<account>:function:<name>[:<qualifier>]
        return parts[6], parts[7] if len(parts) > 7 else None
    return parts[0], parts[1] if len(parts) > 1 else None


class ProxyServer(ThreadingHTTPServer):
    """The HTTP server. Handlers read their settings from it as self.server."""

    daemon_threads = True

    def __init__(
        self,
        port: int,
        upstream: str,
        dump_dir: Path | None,
        function_configs: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        """function_configs maps a function name to {} or {"DurableConfig": {...}},
        the same configs the runner gets. An execution starts with its
        function's ExecutionTimeout and RetentionPeriodInDays."""
        super().__init__(("127.0.0.1", port), ProxyHandler)
        self.upstream = upstream.rstrip("/")
        self.dump_dir = dump_dir
        self.function_configs = dict(function_configs or {})
        if dump_dir is not None:
            dump_dir.mkdir(parents=True, exist_ok=True)


class ProxyHandler(BaseHTTPRequestHandler):
    # HTTP/1.0 closes the connection after each response, so each request
    # thread ends promptly. With HTTP/1.1 keep-alive, every idle client
    # connection would hold a thread and a file descriptor for the whole run.
    protocol_version = "HTTP/1.0"
    server: ProxyServer

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        # The default logs every request to stderr. The run is too noisy for
        # that to help.
        return

    def do_GET(self) -> None:  # noqa: N802
        self.dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self.dispatch()

    def do_PUT(self) -> None:  # noqa: N802
        self.dispatch()

    def do_DELETE(self) -> None:  # noqa: N802
        self.dispatch()

    def dispatch(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        path, _, query = self.path.partition("?")
        match = INVOKE_PATH.match(path)
        if self.command == "POST" and match:
            # A client URL-encodes the ":" in a qualified name or ARN.
            name, qualifier = split_identifier(unquote(match["name"]))
            # Invoke also takes the qualifier as ?Qualifier=. Lambda rejects a
            # request whose two qualifiers differ; the proxy uses the path's.
            query_qualifier = parse_qs(query).get("Qualifier", [None])[0]
            self.start_execution(
                name,
                qualifier or query_qualifier or "$LATEST",
                self.headers.get("X-Amz-Tenant-Id"),
                body,
            )
        else:
            self.forward(body)

    def start_execution(
        self,
        function_name: str,
        qualifier: str,
        tenant_id: str | None,
        payload: bytes,
    ) -> None:
        durable = self.server.function_configs.get(function_name) or {}
        durable_config = durable.get("DurableConfig") or {}
        start_input = {
            "AccountId": ACCOUNT_ID,
            "FunctionName": function_name,
            "FunctionQualifier": qualifier,
            "ExecutionName": f"{function_name}-{uuid.uuid4().hex}",
            "ExecutionTimeoutSeconds": durable_config.get(
                "ExecutionTimeout", DEFAULT_EXECUTION_TIMEOUT_SECONDS
            ),
            "ExecutionRetentionPeriodDays": durable_config.get(
                "RetentionPeriodInDays", DEFAULT_RETENTION_DAYS
            ),
            "InvocationId": str(uuid.uuid4()),
            # Lambda delivers an empty object to a handler invoked with no
            # payload. The runner would read an empty input as JSON null.
            "Input": payload.decode("utf-8") or "{}",
        }
        # Invoke names the tenant of a tenant-isolated function in a header.
        # The runner then sends it on every invocation of the handler.
        if tenant_id:
            start_input["TenantId"] = tenant_id
        request = Request(
            f"{self.server.upstream}/start-durable-execution",
            data=json.dumps(start_input).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        status, headers, data = self.call(request, timeout=30)
        # The runner answers a started execution with 201 Created.
        if not 200 <= status < 300:
            self.respond(status, headers, data)
            return
        arn = json.loads(data)["ExecutionArn"]
        self.respond(200, [("X-Amz-Durable-Execution-Arn", arn)], b"")

    def forward(self, body: bytes) -> None:
        headers = {
            k: v for k, v in self.headers.items() if k.lower() not in SKIPPED_HEADERS
        }
        request = Request(
            f"{self.server.upstream}{self.path}",
            data=body or None,
            headers=headers,
            method=self.command,
        )
        status, response_headers, data = self.call(request, timeout=60)
        self.respond(status, response_headers, data)
        self.dump(data)

    def call(
        self, request: Request, timeout: float
    ) -> tuple[int, list[tuple[str, str]], bytes]:
        """Send a request to the runner. Return status, headers and body.

        An HTTP error status is returned like any other response. A runner
        that cannot be reached becomes a 502.
        """
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310
                return response.status, list(response.headers.items()), response.read()
        except HTTPError as exc:
            headers = list(exc.headers.items()) if exc.headers else []
            return exc.code, headers, exc.read()
        except URLError as exc:
            data = json.dumps({"errorMessage": str(exc)}).encode("utf-8")
            return 502, [("Content-Type", "application/json")], data

    def respond(self, status: int, headers: list[tuple[str, str]], data: bytes) -> None:
        self.send_response(status)
        for key, value in headers:
            if key.lower() not in SKIPPED_HEADERS:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def dump(self, data: bytes) -> None:
        """Append history, state and checkpoint bodies to --dump-dir, if set."""
        if self.server.dump_dir is None:
            return
        kind = self.path.split("?", 1)[0].rsplit("/", 1)[-1]
        if kind not in ("history", "state", "checkpoint"):
            return
        with (self.server.dump_dir / f"{kind}.ndjson").open("ab") as fh:
            fh.write(data.rstrip(b"\n") + b"\n")

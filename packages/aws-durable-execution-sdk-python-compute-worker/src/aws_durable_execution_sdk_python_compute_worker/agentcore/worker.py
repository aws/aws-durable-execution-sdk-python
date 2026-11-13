# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The HTTP server that receives jobs inside an AgentCore Runtime session.

AgentCore Runtime sends each ``InvokeAgentRuntime`` request to
``POST /invocations`` in the agent's container, and checks the session with
``GET /ping``. This module answers both, runs each job in its own thread, and
reports the job's outcome to its callback.

The callback reporter, the heartbeats, and the job document parser come from
the MicroVM worker. The job document is the MicroVM job request without
``microvmId``.
"""

from __future__ import annotations

import inspect
import logging
import os
import socketserver
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python_compute_worker.microvm.callback_reporter import (
    CallbackReporter,
    ResultSerializationError,
    ResultTooLargeError,
    is_terminal_callback_error,
)
from aws_durable_execution_sdk_python_compute_worker.microvm.heartbeats import (
    Heartbeats,
    validate_heartbeat_interval,
)
from aws_durable_execution_sdk_python_compute_worker.microvm.logger import (
    MicrovmWorkerLogger,
    describe,
    safe_logger,
)
from aws_durable_execution_sdk_python_compute_worker.microvm.payload import (
    InvalidRunHookPayloadError,
    MicrovmJobDocument,
    MicrovmJobRequest,
)
from aws_durable_execution_sdk_python_compute_worker.microvm.worker import (
    MAX_FINISHED_JOBS,
    MAX_JOB_BODY_BYTES,
    _Exchange,
)


if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = Any


AGENTCORE_PORT = 8080
"""AgentCore Runtime sends requests to port 8080 of the agent's container,
unless the agent runtime's ``protocolConfiguration`` sets another port."""

AGENTCORE_HOST = "0.0.0.0"  # noqa: S104
"""The address that the worker listens on inside AgentCore Runtime.

The AgentCore Runtime service contract requires the agent to listen on
``0.0.0.0:8080``. The runtime publishes no port. Only the AgentCore data
plane reaches it, after it has authorized the ``InvokeAgentRuntime`` caller.
So the worker listens on all interfaces only inside AgentCore Runtime.
"""

LOCAL_HOST = "127.0.0.1"
"""The address that the worker listens on outside AgentCore Runtime.

The worker has no authentication. On a laptop or a dev host, a bind on all
interfaces would let any machine on the network send it a job. So outside
AgentCore Runtime the worker listens on loopback only.
"""

RUNTIME_URL_ENV = "AGENTCORE_RUNTIME_URL"
"""The environment variable that marks the AgentCore Runtime environment.

A live run in a runtime with direct code deployment found this variable
set, and found no ``/.dockerenv`` file and no ``DOCKER_CONTAINER``
variable. So the worker uses this variable, not the Docker markers, to
choose the default host.
"""


def default_host() -> str:
    """The default host: :data:`AGENTCORE_HOST` when
    :data:`RUNTIME_URL_ENV` is set, and :data:`LOCAL_HOST` otherwise."""
    return AGENTCORE_HOST if os.environ.get(RUNTIME_URL_ENV) else LOCAL_HOST


INVOCATIONS_PATH = "/invocations"
"""The path of the ``InvokeAgentRuntime`` requests."""

PING_PATH = "/ping"
"""The path of AgentCore's health checks."""

SESSION_ID_HEADER = "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id"
"""The header in which AgentCore passes the runtime session ID."""

DEFAULT_LOGGER_NAME = "aws_durable_execution_sdk_python_compute_worker.agentcore"
"""The name of the standard library logger that the worker uses by default."""

_SERVER_POLL_SECONDS = 0.1


def _now() -> int:
    """The current Unix time in whole seconds. Tests replace it."""
    return int(time.time())


_REQUEST_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True, kw_only=True)
class AgentcoreJobContext:
    """What the job handler receives besides its input."""

    callback_id: str
    """The callback ID that the worker completes with the handler's result."""
    session_id: str | None
    """The AgentCore runtime session ID, from the request header. ``None``
    when the request had no such header."""
    region: str
    """The Region of the durable function."""
    cancelled: threading.Event
    """Set when a heartbeat finds that the durable function no longer
    accepts the result, for example after a timeout.

    The worker learns it only from a heartbeat answer. So it is set only for
    a job with a heartbeat timeout. Python cannot stop a running thread. So
    the handler should check the event and stop its work when it is set.
    """
    logger: MicrovmWorkerLogger
    """The worker's logger."""


AgentcoreJobHandler = Callable[[Any, AgentcoreJobContext], Any]
"""The job to run.

The handler receives the job's input and an :class:`AgentcoreJobContext`.
Its return value becomes the durable function's result, and must be
JSON-serializable. An exception fails the durable operation with the
exception's class name and message.
"""


class _JobRunner:
    """Answers the requests of AgentCore Runtime, and runs the jobs.

    :func:`start_agentcore_worker` describes the requests and the answers.
    """

    def __init__(
        self,
        *,
        handler: AgentcoreJobHandler,
        create_client: Callable[[str], LambdaClient] | None,
        heartbeat_interval_seconds: float | None,
        logger: MicrovmWorkerLogger,
    ) -> None:
        self._handler = handler
        self._create_client = create_client
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._logger = logger
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._running: set[str] = set()
        # The accepted callback IDs whose jobs have ended, oldest first.
        self._finished: OrderedDict[str, None] = OrderedDict()
        # The Unix time, in seconds, of the last change between Healthy and
        # HealthyBusy. The worker's start counts as a change.
        self._last_status_change = _now()

    def ping(self) -> dict[str, object]:
        """The answer to ``GET /ping``.

        AgentCore documents ``time_of_last_update`` as optional. It must
        hold the time of the last status change, not the time of the ping.
        The issue aws/bedrock-agentcore-sdk-python#471 reports that a
        ``HealthyBusy`` session without the field can end at the idle
        timeout. So the answer always carries the field, and the field
        changes only when the status changes.
        """
        with self._lock:
            return {
                "status": "HealthyBusy" if self._running else "Healthy",
                "time_of_last_update": self._last_status_change,
            }

    def idle(self, timeout: float | None) -> bool:
        """Wait until no job runs or reports its outcome."""
        with self._changed:
            return self._changed.wait_for(lambda: not self._running, timeout)

    def handle(self, request: BaseHTTPRequestHandler) -> None:
        """Answer one request."""
        exchange = _Exchange(request, self._logger)
        try:
            if exchange.method == "GET" and exchange.path == PING_PATH:
                exchange.respond(200, self.ping())
            elif exchange.method == "POST" and exchange.path == INVOCATIONS_PATH:
                self._handle_invocation(exchange)
            else:
                exchange.respond(404, {"error": "not found"})
        except Exception as error:  # noqa: BLE001
            self._logger.error(
                "could not handle the request", extra={"error": describe(error)}
            )
            exchange.respond(500, {"error": "internal error"})

    def _handle_invocation(self, exchange: _Exchange) -> None:
        try:
            body = exchange.read_json(MAX_JOB_BODY_BYTES)
            request = MicrovmJobRequest.from_dict(body)
        except InvalidRunHookPayloadError as error:
            # A 400 makes InvokeAgentRuntime fail. The default retry strategy
            # does not retry a 4xx answer. So the durable function fails at
            # once with AgentcoreDeliveryError.
            self._logger.warning(
                "rejected an invalid job request", extra={"error": describe(error)}
            )
            exchange.respond(400, {"error": str(error)})
            return
        job = request.job
        session_id = exchange.header(SESSION_ID_HEADER)
        # The job thread waits for this event before it runs any job code.
        # A handler that holds the GIL would otherwise delay the answer.
        answered = threading.Event()
        thread = threading.Thread(
            target=self._run_job,
            args=(job, request.region, session_id, answered),
            name=f"agentcore-job-{job.callback_id[:16]}",
            daemon=True,
        )
        # Registration and the thread start are one commit under the lock.
        # A duplicate request waits for the lock. So it sees the callback
        # ID only after the thread started, and never answers 200 for a job
        # whose thread failed to start.
        with self._changed:
            known = job.callback_id in self._running or job.callback_id in (
                self._finished
            )
            if not known:
                if not self._running:
                    self._last_status_change = _now()
                self._running.add(job.callback_id)
                try:
                    thread.start()
                except BaseException:
                    # The job never ran. AgentCore returns the 500 to the
                    # request step as an error that the default retry
                    # strategy does not retry. So the operation fails with
                    # AgentcoreDeliveryError. The job must not stay known,
                    # so that a new run of the durable function can deliver
                    # it.
                    self._end_job_locked(job.callback_id, finished=False)
                    raise
        if known:
            self._logger.info(
                "ignored a repeated job", extra={"callbackId": job.callback_id}
            )
            exchange.respond(200, {"accepted": True})
            return
        try:
            exchange.respond(200, {"accepted": True})
        finally:
            # The job is accepted even when the answer could not be sent.
            # A retried request then gets the answer for a known job.
            answered.set()

    def _end_job(self, callback_id: str, *, finished: bool) -> None:
        with self._changed:
            self._end_job_locked(callback_id, finished=finished)

    def _end_job_locked(self, callback_id: str, *, finished: bool) -> None:
        """Forget a running job. The caller holds the lock."""
        self._running.discard(callback_id)
        if not self._running:
            self._last_status_change = _now()
        if finished:
            self._finished[callback_id] = None
            if len(self._finished) > MAX_FINISHED_JOBS:
                self._finished.popitem(last=False)
        self._changed.notify_all()

    def _run_job(
        self,
        job: MicrovmJobDocument,
        region: str,
        session_id: str | None,
        answered: threading.Event,
    ) -> None:
        answered.wait()
        fields: dict[str, object] = {
            "callbackId": job.callback_id,
            "runtimeSessionId": session_id,
        }
        try:
            self._run_job_and_report(job, region, session_id, fields)
        except Exception as error:  # noqa: BLE001
            self._logger.error(
                "job failed inside the worker",
                extra={**fields, "error": describe(error)},
            )
        finally:
            self._end_job(job.callback_id, finished=True)

    def _run_job_and_report(
        self,
        job: MicrovmJobDocument,
        region: str,
        session_id: str | None,
        fields: dict[str, object],
    ) -> None:
        reporter = (
            CallbackReporter.create(job.callback_id, region, logger=self._logger)
            if self._create_client is None
            else CallbackReporter(
                job.callback_id, self._create_client(region), logger=self._logger
            )
        )
        try:
            cancelled = threading.Event()
            try:
                heartbeats = Heartbeats.start(
                    reporter,
                    heartbeat_timeout_seconds=job.heartbeat_timeout_seconds,
                    on_callback_gone=lambda _error: cancelled.set(),
                    heartbeat_interval_seconds=self._heartbeat_interval_seconds,
                    logger=self._logger,
                )
            except Exception as error:  # noqa: BLE001
                # The job has its answer, so no later delivery runs it. Fail
                # the callback, as the MicroVM worker does, instead of leaving
                # the job to its heartbeat timeout or its timeout.
                self._logger.error(
                    "could not start the job's heartbeats",
                    extra={**fields, "error": describe(error)},
                )
                self._report(reporter, None, error, fields)
                return
            try:
                self._logger.info("job started", extra=fields)
                context = AgentcoreJobContext(
                    callback_id=job.callback_id,
                    session_id=session_id,
                    region=region,
                    cancelled=cancelled,
                    logger=self._logger,
                )
                value: Any = None
                failure: BaseException | None = None
                try:
                    value = self._handler(job.input, context)
                except BaseException as error:  # noqa: BLE001
                    # SystemExit from the handler would otherwise end the
                    # thread without a report.
                    failure = error
                # Heartbeats keep running while the outcome is reported. A
                # completion attempt can stall for up to 30 seconds.
                if heartbeats.handler_settled():
                    self._report(reporter, value, failure, fields)
                else:
                    self._logger.warning(
                        "job outcome not reported: the callback is gone",
                        extra=fields,
                    )
            finally:
                heartbeats.stop()
        finally:
            reporter.close()

    def _report(
        self,
        reporter: CallbackReporter,
        value: Any,
        failure: BaseException | None,
        fields: dict[str, object],
    ) -> None:
        try:
            if failure is None:
                try:
                    reporter.succeed(value)
                except (ResultTooLargeError, ResultSerializationError) as error:
                    # The result was never sent. Report it as a failure, so
                    # the durable function learns why instead of waiting for
                    # its timeout.
                    reporter.fail(error)
                    self._logger.info(
                        "job failed: its result cannot be sent",
                        extra={**fields, "error": describe(error)},
                    )
                    return
                self._logger.info("job succeeded", extra=fields)
                return
            reporter.fail(failure)
            self._logger.info(
                "job failed", extra={**fields, "error": describe(failure)}
            )
        except Exception as error:  # noqa: BLE001
            self._logger.error(
                "job outcome could not be reported",
                extra={
                    **fields,
                    "terminal": is_terminal_callback_error(error),
                    "error": describe(error),
                },
            )


def start_agentcore_worker(
    *,
    handler: AgentcoreJobHandler,
    logger: MicrovmWorkerLogger | None = None,
    create_client: Callable[[str], LambdaClient] | None = None,
    heartbeat_interval_seconds: float | None = None,
    host: str | None = None,
    port: int = AGENTCORE_PORT,
) -> AgentcoreWorker:
    """Start the HTTP server that AgentCore Runtime sends the jobs to.

    The server runs in a daemon thread. So end the container's entry point
    with :meth:`AgentcoreWorker.wait`.

    On ``POST /invocations`` the worker:

    1. Parses the job document, and checks its ``version``. A document that
       does not match the contract gets HTTP 400.
    2. Answers ``{"accepted": true}`` for a callback ID that it has already
       accepted, and does nothing else. ``InvokeAgentRuntime`` has no
       idempotency token, so a retried request can deliver the same job
       again. The worker remembers the last 1,000 finished callback IDs,
       in process memory only. So a job runs once per process. A new
       process after the idle timeout runs a repeated job again, so the
       handler's side effects must be idempotent.
    3. Otherwise registers the callback ID and starts the job's thread, as
       one step under a lock. A thread that cannot start gets HTTP 500, and
       the callback ID is forgotten. A duplicate request that arrives
       meanwhile waits for the lock, so it never gets 200 for a job that
       never ran.
    4. Answers ``{"accepted": true}``. Only then does the job's thread run
       the handler. So a handler that holds the GIL does not delay the
       answer.

    The job's thread sends heartbeats when the job has a heartbeat timeout,
    and then the success or the failure callback.

    ``GET /ping`` answers ``{"status": "HealthyBusy"}`` while a job runs or
    reports, and ``{"status": "Healthy"}`` otherwise. AgentCore keeps a
    ``HealthyBusy`` session alive. The answer also carries
    ``time_of_last_update``: the Unix time, in seconds, of the worker's start
    or of the last change between the two statuses. Every other request gets HTTP 404.

    Args:
        handler: The job to run for each job request.
        logger: Receives the worker's log lines. Defaults to the standard
            library logger ``aws_durable_execution_sdk_python_compute_worker.agentcore``.
        create_client: Creates the Lambda client for the callback calls of
            one job, from the durable function's Region. Defaults to a boto3
            client with the default credential chain, which resolves the
            agent runtime's execution role.
        heartbeat_interval_seconds: An explicit heartbeat interval. It is cut
            to a third of the job's heartbeat timeout.
        host: The address to listen on. ``None`` picks ``0.0.0.0``, which
            the AgentCore Runtime service contract requires, when the
            ``AGENTCORE_RUNTIME_URL`` environment variable is set. Otherwise
            it picks ``127.0.0.1``. See :func:`default_host`.
        port: The port to listen on. Defaults to 8080, the port that
            AgentCore Runtime sends to. 0 picks a free port.

    Raises:
        TypeError: When ``handler`` is not a plain function.
        ValueError: When ``heartbeat_interval_seconds`` is not above 0 and at
            most 900.
        OSError: When the port cannot be bound.
    """
    if not callable(handler):
        msg = f"a job handler must be callable. Got {type(handler).__name__}."
        raise TypeError(msg)
    # An async function returns a coroutine, which the worker would report
    # as a result that is not JSON-serializable. So it is refused here.
    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(type(handler), "__call__", None)
    ):
        msg = "a job handler must be a plain function, not an async function"
        raise TypeError(msg)
    validate_heartbeat_interval(heartbeat_interval_seconds)
    runner = _JobRunner(
        handler=handler,
        create_client=create_client,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        logger=safe_logger(
            logger if logger is not None else logging.getLogger(DEFAULT_LOGGER_NAME)
        ),
    )
    server = _Server((default_host() if host is None else host, port), runner)
    threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": _SERVER_POLL_SECONDS},
        name="agentcore-worker-server",
        daemon=True,
    ).start()
    return AgentcoreWorker(runner=runner, server=server)


class AgentcoreWorker:
    """A running worker server.

    Create it with :func:`start_agentcore_worker`. See that function for the
    requests that the worker answers.
    """

    def __init__(self, *, runner: _JobRunner, server: ThreadingHTTPServer) -> None:
        self._runner = runner
        self._server = server
        self._lock = threading.Lock()
        self._closed = threading.Event()

    @property
    def port(self) -> int:
        """The port the server listens on."""
        return int(self._server.server_address[1])

    def idle(self, timeout: float | None = None) -> bool:
        """Wait until every job started so far has reported its outcome.

        Returns:
            ``True`` when no job runs. ``False`` when the timeout ended first.
        """
        return self._runner.idle(timeout)

    def wait(self, timeout: float | None = None) -> bool:
        """Block until :meth:`close` is called.

        The server runs in a daemon thread, so the process would otherwise
        end when the main thread ends. Call this method at the end of the
        container's entry point.

        Returns:
            ``True`` when the worker is closed. ``False`` when the timeout
            ended first.
        """
        return self._closed.wait(timeout)

    def close(self) -> None:
        """Stop accepting requests. Running jobs continue."""
        with self._lock:
            if self._closed.is_set():
                return
            self._server.shutdown()
            self._server.server_close()
            self._closed.set()


class _RequestHandler(BaseHTTPRequestHandler):
    """Passes every GET and POST request to the server's :class:`_JobRunner`."""

    server: _Server
    timeout = _REQUEST_TIMEOUT_SECONDS
    # HTTP/1.1, as the MicroVM worker. Every answer closes its connection.
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        self.server.runner.handle(self)

    def do_POST(self) -> None:  # noqa: N802
        self.server.runner.handle(self)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # The base class writes every request to stderr. The worker logs what
        # matters through its own logger.
        return

    def version_string(self) -> str:
        # The base class names the exact Python version in the Server header.
        return "aws-durable-execution-sdk-python-compute-worker"


class _Server(ThreadingHTTPServer):
    """The worker's HTTP server."""

    daemon_threads = True
    # socketserver listens with a backlog of 5. A connection beyond the
    # backlog can get a TCP reset. So the worker uses Node's backlog of 511,
    # as the MicroVM worker does. The kernel caps it at net.core.somaxconn.
    request_queue_size = 511

    def __init__(self, address: tuple[str, int], runner: _JobRunner) -> None:
        self.runner = runner
        super().__init__(address, _RequestHandler)

    def server_bind(self) -> None:
        # HTTPServer.server_bind looks up the host's fully qualified name,
        # which can wait for DNS. The worker never uses the name. So the bind
        # skips the lookup.
        socketserver.TCPServer.server_bind(self)
        self.server_name = "localhost"
        self.server_port = int(self.server_address[1])

    def handle_error(self, request: Any, client_address: Any) -> None:
        # A client that resets the connection while it sends the request
        # line or the headers ends here. It needs no traceback on stderr.
        if isinstance(sys.exception(), ConnectionError):
            return
        super().handle_error(request, client_address)

# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The HTTP listener that receives jobs inside the MicroVM.

Lambda calls the MicroVM's lifecycle hooks over HTTP. The durable function
sends a job in the ``run`` hook, or in an HTTP request to a job route. This
module answers both, runs each job in its own thread, and reports the job's
outcome to its callback.
"""

from __future__ import annotations

import contextlib
import inspect
import json
import os
import re
import socket
import socketserver
import sys
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import cached_property
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python_microvm_worker.callback_reporter import (
    CallbackReporter,
    ResultSerializationError,
    ResultTooLargeError,
    is_terminal_callback_error,
)
from aws_durable_execution_sdk_python_microvm_worker.heartbeats import (
    Heartbeats,
    validate_heartbeat_interval,
)
from aws_durable_execution_sdk_python_microvm_worker.logger import (
    MicrovmWorkerLogger,
    describe,
    safe_logger,
)
from aws_durable_execution_sdk_python_microvm_worker.payload import (
    InvalidRunHookPayloadError,
    MicrovmJobDocument,
    MicrovmJobRequest,
    RunHookRequest,
    loads_strict,
)


if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = Any


HOOK_PATH_PREFIX = "/aws/lambda-microvms/runtime/v1/"
"""Lambda sends the lifecycle hooks to paths under this prefix."""

MICROVM_JOB_PATH = "/durable-execution/v1/job"
"""The route that receives a job over HTTP when the job names no route.

The durable function sends a job here when the job is too large for the
``run`` hook, and for an invoke without a path. The worker runs ``handler``
for it. The value must equal ``DEFAULT_MICROVM_JOB_PATH`` in the durable
function's MicroVM operations.
"""

DEFAULT_PORT = 8080
"""The default port. It is also the port that the MicroVM endpoint forwards to."""

MAX_HOOK_BODY_BYTES = 64 * 1024
"""The largest ``run`` hook body the worker reads. The payload is at most 16 KB."""

MAX_JOB_BODY_BYTES = 6 * 1024 * 1024
"""The largest job request body the worker reads."""

MAX_FINISHED_JOBS = 1_000
"""How many finished callback IDs the worker remembers.

The durable function can deliver a job again after it finished. Its request
step runs again on replay when its result was not checkpointed, and it
resends a job whose 202 answer was lost. Each callback ID belongs to one job.
So the worker ignores a callback ID that it already ran. A callback ID is a
short string, so 1,000 of them use well under 1 MB.
"""

_REQUEST_TIMEOUT_SECONDS = 60.0
"""How long the worker waits for each read of a request.

A client that stops sending would otherwise hold its request thread forever.
Lambda allows the ``run`` hook at most 60 seconds.
"""

_SERVER_POLL_SECONDS = 0.1
"""How often the server checks for a shutdown request. It bounds ``close()``."""

_UNKNOWN_MICROVM_ID = "unknown"

# At most 19 digits, so that int() stays under Python's limit on digits, and
# any declared length still exceeds every body limit.
_CONTENT_LENGTH = re.compile(r"[0-9]{1,19}")

_CHUNK_SIZE = re.compile(rb"[0-9A-Fa-f]+")

_MAX_CHUNK_LINE_BYTES = 1024

_DISCARD_SECONDS = 10.0
"""How long the worker reads and drops a body that it answered with 400."""

_MAX_DISCARD_BYTES = 4 * MAX_JOB_BODY_BYTES
"""How many bytes of such a body the worker reads and drops at most."""

_DISCARD_READ_BYTES = 64 * 1024


@dataclass(frozen=True, kw_only=True)
class MicrovmJobContext:
    """What the job handler receives besides its input."""

    callback_id: str
    """The callback ID that the worker completes with the handler's result."""
    microvm_id: str
    """The MicroVM identifier.

    The worker learns it from the ``run`` hook, or from a job request that
    arrives first. It is ``"unknown"`` until then.
    """
    region: str
    """The Region of the durable function."""
    cancelled: threading.Event
    """Set when the durable function stops accepting the result.

    This happens, for example, after a callback timeout or a heartbeat
    timeout. The worker learns it from a heartbeat answer. So the event is set
    only for a job with a heartbeat timeout, and only while the handler runs.
    Python cannot stop a running thread. So the handler should check the
    event, or wait on it, and stop its work when it is set.
    """
    logger: MicrovmWorkerLogger
    """The worker's logger."""


MicrovmJobHandler = Callable[[Any, MicrovmJobContext], Any]
"""The job to run.

The handler receives the job's input and a :class:`MicrovmJobContext`. Its
return value becomes the durable function's result, and must be
JSON-serializable. An exception fails the durable operation with the
exception's class name and message.
"""


class MicrovmWorkerListener:
    """Answers the MicroVM lifecycle hooks and the job routes.

    Create it with :meth:`create`. :func:`start_microvm_worker` creates one
    and serves it. Use this class directly to serve the hooks from your own
    ``http.server`` server, through :attr:`request_handler_class`.

    On ``POST .../run`` the listener:

    1. Validates the body and its run hook payload.
    2. Answers HTTP 200 at once. Lambda allows the ``run`` hook at most 60
       seconds, and a slow or failed hook can terminate the MicroVM. So the
       job starts after the answer.
    3. Runs ``handler`` for the payload's job, when the payload has one. A
       payload without a job belongs to a MicroVM whose jobs arrive over
       HTTP.

    On ``POST <route>`` the listener validates the job request, answers HTTP
    202, and runs the route's handler. ``POST`` to :data:`MICROVM_JOB_PATH`
    runs ``handler``.

    For every job, the listener sends heartbeats until the job's outcome is
    reported, when the job sets a heartbeat timeout. When the handler
    returns, the listener completes the callback with the result. When the
    handler raises, the listener fails the callback with the exception.

    A second delivery of a callback ID is answered like the first and
    ignored, while that job runs and after it ended. The listener remembers
    the last 1,000 finished callback IDs.

    A document that does not match the contract gets HTTP 400. If the
    document still names a callback, the listener fails that callback. So
    the durable function fails at once instead of at its timeout.

    Every other lifecycle hook gets HTTP 200. This includes the ``ready``
    image hook, which the service requires whenever the ``run`` hook is
    enabled. Other paths get HTTP 404.
    """

    def __init__(
        self,
        *,
        handler: MicrovmJobHandler | None,
        routes: Mapping[str, MicrovmJobHandler],
        create_client: Callable[[str], LambdaClient] | None,
        heartbeat_interval_seconds: float | None,
        logger: MicrovmWorkerLogger,
    ) -> None:
        self._handler = handler
        self._routes = dict(routes)
        self._create_client = create_client
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._logger = logger
        # Guards every field below. Each request and each job runs in its own
        # thread. The condition wakes idle() when a job or a report ends.
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        # The callback IDs of the jobs that run now.
        self._jobs: set[str] = set()
        # The callback IDs of jobs that ended, oldest first.
        self._finished: OrderedDict[str, None] = OrderedDict()
        # Callback failure reports in flight, for invalid requests.
        self._reports = 0
        # Set by the run hook. A job request that arrives first sets it too.
        self._microvm_id = _UNKNOWN_MICROVM_ID
        # Lambda sends one run hook per MicroVM. The MicroVM endpoint also
        # forwards a request to the hook path from any caller that holds an
        # auth token for the MicroVM. So the listener acts only on the first
        # valid run hook, and ignores every later one.
        self._run_hook_accepted = False

    @classmethod
    def create(
        cls,
        *,
        handler: MicrovmJobHandler | None = None,
        routes: Mapping[str, MicrovmJobHandler] | None = None,
        create_client: Callable[[str], LambdaClient] | None = None,
        heartbeat_interval_seconds: float | None = None,
        logger: MicrovmWorkerLogger | None = None,
    ) -> MicrovmWorkerListener:
        """Create a listener.

        Args:
            handler: The job to run when a job names no route. The durable
                function sends such a job in the ``run`` hook, or over HTTP
                to :data:`MICROVM_JOB_PATH` when the job is too large for the
                ``run`` hook.
            routes: The jobs to run when a job arrives over HTTP on a named
                route, keyed by path.
            create_client: Creates the Lambda client for a Region. The
                listener calls it for each job, after the job arrives, and to
                fail the callback of an invalid request. It never closes a
                client that this function returns. Defaults to a client of
                its own per job, from the default credential chain. The
                listener closes that client when the job ends.
            heartbeat_interval_seconds: The heartbeat interval. Defaults to a
                third of the job's heartbeat timeout, at most 15 minutes. It
                is cut to a third of the job's heartbeat timeout.
            logger: The worker's logger. Defaults to the package's standard
                library logger.

        Raises:
            TypeError: When there is neither a handler nor a route, or a
                handler is not a plain function. An ``async def`` function
                is not supported.
            ValueError: When a route path is not valid, or
                ``heartbeat_interval_seconds`` is not above 0 and at most
                900.
        """
        routes = dict(routes or {})
        if handler is None and not routes:
            msg = "the MicroVM worker needs a handler or routes"
            raise TypeError(msg)
        for job_handler in [handler, *routes.values()]:
            _check_handler(job_handler)
        for path in routes:
            if not path.startswith("/") or path.startswith(HOOK_PATH_PREFIX):
                msg = (
                    f'route "{path}" must start with "/" and must not use the '
                    "lifecycle hook prefix"
                )
                raise ValueError(msg)
            if path == MICROVM_JOB_PATH:
                msg = (
                    f'route "{path}" is reserved for jobs that name no route; '
                    "set handler instead"
                )
                raise ValueError(msg)
        validate_heartbeat_interval(heartbeat_interval_seconds)
        return cls(
            handler=handler,
            routes=routes,
            create_client=create_client,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            # A logger that raises must not end a request thread or a job
            # thread. A job could then never report its outcome.
            logger=safe_logger(logger),
        )

    @cached_property
    def request_handler_class(self) -> type[BaseHTTPRequestHandler]:
        """A request handler class for ``http.server`` that serves this listener.

        Pass it to a ``ThreadingHTTPServer``. A single-threaded server serves
        one request at a time. So one slow client, which the listener waits
        for up to 60 seconds per read, would hold back every other request,
        including the ``run`` hook.

        Also give the server a backlog of 511, as :func:`start_microvm_worker`
        does. A session can send many jobs at once, and the default backlog
        of 5 resets the connections beyond it. The server's constructor
        already listens, so setting ``request_queue_size`` on a server object
        changes nothing. Set it as a class attribute of a subclass instead::

            class Server(ThreadingHTTPServer):
                request_queue_size = 511
        """
        listener = self

        class _Handler(_RequestHandler):
            def handle_exchange(self) -> None:
                listener.handle_request(self)

            def handle_expect_100(self) -> bool:
                return listener.handle_expect_100(self)

        return _Handler

    def handle_request(self, request: BaseHTTPRequestHandler) -> None:
        """Handle one request that ``http.server`` received.

        :attr:`request_handler_class` calls it for every request. An
        unexpected error is logged, and gets HTTP 500 when no answer was sent.
        """
        exchange = _Exchange(request, self._logger)
        try:
            self._dispatch(exchange)
        except Exception as error:
            self._logger.error(
                "request handling failed", extra={"error": describe(error)}
            )
            if not exchange.responded:
                exchange.respond(500, {"error": "internal error"})
        finally:
            exchange.discard_unread_body()

    def handle_expect_100(self, request: BaseHTTPRequestHandler) -> bool:
        """Answer a request that sent ``Expect: 100-continue``.

        :attr:`request_handler_class` calls it before it reads the body.

        1. A request whose ``Content-Length`` is over the path's limit gets
           its HTTP 400 now, instead of ``100 Continue``. RFC 9110 section
           10.1.1 allows a final answer in place of ``100 Continue``. The
           client then does not send the body.
        2. Every other request gets ``100 Continue``.

        Returns:
            Whether the base class goes on to read the request.
        """
        exchange = _Exchange(request, self._logger)
        limit = self._body_limit(exchange)
        length = exchange.declared_length()
        if limit is not None and length is not None and length > limit:
            self._reject(exchange.too_large(limit), exchange)
            exchange.discard_unread_body()
            return False
        return BaseHTTPRequestHandler.handle_expect_100(request)

    def idle(self, timeout: float | None = None) -> bool:
        """Wait until every job started so far has ended.

        The wait also covers the failure reports of invalid requests.

        Args:
            timeout: The longest wait in seconds. ``None`` waits until the
                jobs end.

        Returns:
            ``True`` when no job runs. ``False`` when the timeout ended first.
        """
        with self._changed:
            return self._changed.wait_for(
                lambda: not self._jobs and self._reports == 0, timeout
            )

    def _dispatch(self, exchange: _Exchange) -> None:
        path = exchange.path
        if path.startswith(HOOK_PATH_PREFIX) and exchange.method == "POST":
            hook = path[len(HOOK_PATH_PREFIX) :]
            if hook == "run":
                self._handle_run_hook(exchange)
                return
            self._logger.info("lifecycle hook", extra={"hook": hook})
            exchange.respond(200, {})
            return
        handler = self._job_handler(path)
        if handler is not None and exchange.method == "POST":
            self._handle_job_request(handler, exchange)
            return
        exchange.respond(404, {"error": "not found"})

    def _job_handler(self, path: str) -> MicrovmJobHandler | None:
        """The handler for a job route, or ``None`` for another path."""
        return self._handler if path == MICROVM_JOB_PATH else self._routes.get(path)

    def _body_limit(self, exchange: _Exchange) -> int | None:
        """The largest body that the request's path reads.

        ``None`` for a request whose body the worker does not read.
        """
        if exchange.method != "POST":
            return None
        if exchange.path == f"{HOOK_PATH_PREFIX}run":
            return MAX_HOOK_BODY_BYTES
        if self._job_handler(exchange.path) is not None:
            return MAX_JOB_BODY_BYTES
        return None

    def _handle_run_hook(self, exchange: _Exchange) -> None:
        try:
            parsed = RunHookRequest.from_dict(exchange.read_json(MAX_HOOK_BODY_BYTES))
        except Exception as error:  # noqa: BLE001
            self._reject(error, exchange)
            return
        payload = parsed.payload
        job = None if payload is None else payload.job
        # The checks and the job's registration run under one lock. Requests
        # run in parallel threads, so two deliveries of one callback ID could
        # otherwise both start the job.
        with self._lock:
            later = self._run_hook_accepted
            if not later:
                self._run_hook_accepted = True
                self._microvm_id = parsed.microvm_id
            microvm_id = self._microvm_id
            duplicate = job is not None and self._is_known(job.callback_id)
            new_job = None if later or duplicate else job
            if new_job is not None and self._handler is not None:
                self._jobs.add(new_job.callback_id)

        if payload is not None and new_job is not None and self._handler is None:
            # The report starts before the answer, so that idle() covers it
            # once the caller has the answer.
            self._fail_in_background(
                InvalidRunHookPayloadError(
                    "the run hook delivered a job, and this worker has no "
                    "handler for run hook jobs"
                ),
                new_job.callback_id,
                payload.region,
            )
        start_error: Exception | None = None
        if payload is not None and new_job is not None and self._handler is not None:
            # The thread starts before the answer, so that idle() covers the
            # job once the caller has the answer.
            try:
                self._start_job(self._handler, new_job, payload.region)
            except Exception as error:  # noqa: BLE001
                start_error = error
                with self._lock:
                    self._reports += 1
        exchange.respond(200, {})
        if payload is not None and new_job is not None and start_error is not None:
            # Lambda sends the run hook once. So no later delivery comes, and
            # the job would wait for its heartbeat timeout or its timeout. A
            # new thread cannot start either. So this request thread, whose
            # answer has gone out, fails the callback itself.
            self._logger.error(
                "could not start the run hook job",
                extra={
                    "microvmId": microvm_id,
                    "callbackId": new_job.callback_id,
                    "error": describe(start_error),
                },
            )
            self._send_failure(start_error, new_job.callback_id, payload.region)
            return
        if new_job is not None:
            return

        if later:
            self._logger.warning(
                "later run hook ignored",
                extra={"microvmId": microvm_id, "hookMicrovmId": parsed.microvm_id},
            )
        elif payload is None:
            self._logger.warning(
                "run hook without runHookPayload", extra={"microvmId": microvm_id}
            )
        elif job is not None and duplicate:
            self._logger.warning(
                "duplicate job ignored",
                extra={"microvmId": microvm_id, "callbackId": job.callback_id},
            )

    def _handle_job_request(
        self, handler: MicrovmJobHandler, exchange: _Exchange
    ) -> None:
        try:
            request = MicrovmJobRequest.from_dict(
                exchange.read_json(MAX_JOB_BODY_BYTES)
            )
        except Exception as error:  # noqa: BLE001
            self._reject(error, exchange)
            return
        job = request.job
        with self._lock:
            if (
                self._microvm_id == _UNKNOWN_MICROVM_ID
                and request.microvm_id is not None
            ):
                # The endpoint can accept a request before Lambda has sent the
                # run hook. The run hook sets the same identifier when it
                # arrives.
                self._microvm_id = request.microvm_id
            microvm_id = self._microvm_id
            duplicate = self._is_known(job.callback_id)
            if not duplicate:
                self._jobs.add(job.callback_id)
        if duplicate:
            exchange.respond(202, {})
            self._logger.warning(
                "duplicate job ignored",
                extra={"microvmId": microvm_id, "callbackId": job.callback_id},
            )
            return
        # The thread starts before the answer. A thread that cannot start
        # raises, and the caller gets HTTP 500, which the durable function
        # retries. After a 202, the durable function does not deliver the job
        # again.
        self._start_job(handler, job, request.region)
        exchange.respond(202, {})

    def _reject(self, error: BaseException, exchange: _Exchange) -> None:
        """Answer 400 to a request that the worker could not accept.

        Only the worker's own validation messages are sent back. They describe
        the request, not the worker. Any other error, such as a connection
        that failed while the body was read, is logged, and the caller gets a
        generic message. So no internal error text reaches the HTTP response.
        """
        if not isinstance(error, InvalidRunHookPayloadError):
            self._logger.error(
                "could not read the request", extra={"error": describe(error)}
            )
            exchange.respond(400, {"error": "invalid request"})
            return
        message = str(error)
        self._logger.error("invalid request", extra={"error": message})
        if error.callback_id is not None:
            region = error.region or os.environ.get("AWS_REGION")
            if region:
                # The report starts before the answer, so that idle() covers
                # it once the caller has the answer.
                self._fail_in_background(error, error.callback_id, region)
        exchange.respond(400, {"error": message})

    def _is_known(self, callback_id: str) -> bool:
        """Whether a job with this callback ID runs now, or ran recently.

        The caller holds the lock.
        """
        return callback_id in self._jobs or callback_id in self._finished

    def _start_job(
        self, handler: MicrovmJobHandler, job: MicrovmJobDocument, region: str
    ) -> None:
        """Run a job in its own thread. The caller registered it in ``_jobs``."""
        thread = threading.Thread(
            target=self._run_job,
            args=(handler, job, region),
            name=f"microvm-job-{job.callback_id[:16]}",
            daemon=True,
        )
        try:
            thread.start()
        except BaseException:
            # The job never ran. A job request then answers 500, so the
            # durable function delivers the job again, and the retry may run
            # it. The run hook fails the callback instead, because Lambda
            # sends it only once.
            with self._changed:
                self._jobs.discard(job.callback_id)
                self._changed.notify_all()
            raise

    def _run_job(
        self, handler: MicrovmJobHandler, job: MicrovmJobDocument, region: str
    ) -> None:
        try:
            self._run_job_and_report(handler, job, region)
        except Exception as error:  # noqa: BLE001
            self._logger.error(
                "job failed inside the worker",
                extra={"callbackId": job.callback_id, "error": describe(error)},
            )
        finally:
            with self._changed:
                self._jobs.discard(job.callback_id)
                self._finished[job.callback_id] = None
                if len(self._finished) > MAX_FINISHED_JOBS:
                    self._finished.popitem(last=False)
                self._changed.notify_all()

    def _run_job_and_report(
        self, handler: MicrovmJobHandler, job: MicrovmJobDocument, region: str
    ) -> None:
        callback_id = job.callback_id
        with self._lock:
            microvm_id = self._microvm_id
        fields = {"microvmId": microvm_id, "callbackId": callback_id}
        try:
            reporter = self._reporter_for(callback_id, region)
        except Exception as error:  # noqa: BLE001
            # Without a client, the worker cannot report anything. The durable
            # function fails the job at its heartbeat timeout or its timeout.
            self._logger.error(
                "could not create the Lambda client for the job",
                extra={**fields, "error": describe(error)},
            )
            return
        try:
            cancelled = threading.Event()
            gone: list[BaseException] = []

            def on_callback_gone(error: BaseException) -> None:
                gone.append(error)
                cancelled.set()

            try:
                heartbeats = Heartbeats.start(
                    reporter,
                    heartbeat_timeout_seconds=job.heartbeat_timeout_seconds,
                    on_callback_gone=on_callback_gone,
                    heartbeat_interval_seconds=self._heartbeat_interval_seconds,
                    logger=self._logger,
                )
            except Exception as error:  # noqa: BLE001
                # The job has its answer, so no later delivery runs it. Fail
                # the callback, instead of leaving the job to its heartbeat
                # timeout.
                self._logger.error(
                    "could not start the job's heartbeats",
                    extra={**fields, "error": describe(error)},
                )
                self._report(reporter, _Outcome(error=error), fields)
                return
            try:
                self._logger.info("job started", extra=fields)
                context = MicrovmJobContext(
                    callback_id=callback_id,
                    microvm_id=microvm_id,
                    region=region,
                    cancelled=cancelled,
                    logger=self._logger,
                )
                outcome = _Outcome.of(lambda: handler(job.input, context))
                # Heartbeats keep running while the outcome is reported. A
                # completion attempt can stall for up to 30 seconds. Without
                # heartbeats, a short heartbeat timeout would expire before
                # the retry.
                if heartbeats.handler_settled():
                    self._report(reporter, outcome, fields)
                else:
                    # The heartbeat thread decides under a lock, and calls
                    # on_callback_gone after it. So the reason can still be
                    # missing here.
                    self._logger.warning(
                        "job outcome not reported: the callback is gone",
                        extra={
                            **fields,
                            "reason": describe(gone[0]) if gone else None,
                        },
                    )
            finally:
                heartbeats.stop()
        finally:
            reporter.close()

    def _report(
        self,
        reporter: CallbackReporter,
        outcome: _Outcome,
        fields: Mapping[str, object],
    ) -> None:
        try:
            if outcome.error is None:
                try:
                    reporter.succeed(outcome.value)
                except (ResultTooLargeError, ResultSerializationError) as error:
                    # The result was never sent. Report it as a failure, so the
                    # durable function learns why instead of waiting for its
                    # timeout.
                    reporter.fail(error)
                    self._logger.info(
                        "job failed: its result cannot be sent",
                        extra={**fields, "error": describe(error)},
                    )
                    return
                self._logger.info("job succeeded", extra=fields)
                return
            reporter.fail(outcome.error)
            self._logger.info(
                "job failed", extra={**fields, "error": describe(outcome.error)}
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

    def _fail_in_background(
        self, error: BaseException, callback_id: str, region: str
    ) -> None:
        """Fail a callback in its own thread. :meth:`idle` waits for it."""
        with self._lock:
            self._reports += 1
        thread = threading.Thread(
            target=self._send_failure,
            args=(error, callback_id, region),
            name=f"microvm-failure-{callback_id[:16]}",
            daemon=True,
        )
        try:
            thread.start()
        except BaseException:
            self._report_ended()
            raise

    def _send_failure(
        self, error: BaseException, callback_id: str, region: str
    ) -> None:
        fields = {"callbackId": callback_id}
        reporter: CallbackReporter | None = None
        try:
            reporter = self._reporter_for(callback_id, region)
            reporter.fail(error)
        except Exception as report_error:  # noqa: BLE001
            if is_terminal_callback_error(report_error):
                # The callback has already ended, for example at its timeout.
                # The durable function no longer waits for this report.
                self._logger.info(
                    "the callback no longer accepts a failure",
                    extra={**fields, "error": describe(report_error)},
                )
            else:
                self._logger.error(
                    "could not fail the callback",
                    extra={**fields, "error": describe(report_error)},
                )
        finally:
            try:
                if reporter is not None:
                    reporter.close()
            except Exception as close_error:  # noqa: BLE001
                self._logger.error(
                    "could not close the Lambda client",
                    extra={**fields, "error": describe(close_error)},
                )
            self._report_ended()

    def _report_ended(self) -> None:
        with self._changed:
            self._reports -= 1
            self._changed.notify_all()

    def _reporter_for(self, callback_id: str, region: str) -> CallbackReporter:
        # Each job gets its own client. A client kept across jobs would keep
        # its connections across a suspend of the MicroVM, and those can be
        # dead after the resume.
        if self._create_client is None:
            return CallbackReporter.create(callback_id, region, logger=self._logger)
        return CallbackReporter(
            callback_id, self._create_client(region), logger=self._logger
        )


class MicrovmWorker:
    """A running worker server.

    Create it with :func:`start_microvm_worker`.
    """

    def __init__(
        self,
        *,
        listener: MicrovmWorkerListener,
        server: ThreadingHTTPServer,
    ) -> None:
        self._listener = listener
        self._server = server
        self._lock = threading.Lock()
        self._closed = threading.Event()

    @classmethod
    def start(
        cls, listener: MicrovmWorkerListener, *, port: int = DEFAULT_PORT
    ) -> MicrovmWorker:
        """Serve ``listener`` on ``port``, in a background thread.

        Raises:
            OSError: When the port cannot be bound.
        """
        server = _create_server(port, listener.request_handler_class)
        threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": _SERVER_POLL_SECONDS},
            name="microvm-worker-server",
            daemon=True,
        ).start()
        return cls(listener=listener, server=server)

    @property
    def port(self) -> int:
        """The port the server listens on."""
        return int(self._server.server_address[1])

    def idle(self, timeout: float | None = None) -> bool:
        """Wait until every job started so far has ended.

        Returns:
            ``True`` when no job runs. ``False`` when the timeout ended first.
        """
        return self._listener.idle(timeout)

    def wait(self, timeout: float | None = None) -> bool:
        """Block until :meth:`close` is called.

        The server runs in a daemon thread, so the process would otherwise
        end when the main thread ends. Call this method at the end of the
        image's entry point.

        Returns:
            ``True`` when the worker is closed. ``False`` when the timeout
            ended first.
        """
        return self._closed.wait(timeout)

    def close(self) -> None:
        """Stop accepting requests. Running jobs continue.

        Call :meth:`idle` first to let the running jobs report their outcome.
        """
        with self._lock:
            if self._closed.is_set():
                return
            self._server.shutdown()
            self._server.server_close()
            self._closed.set()


def start_microvm_worker(
    *,
    handler: MicrovmJobHandler | None = None,
    routes: Mapping[str, MicrovmJobHandler] | None = None,
    create_client: Callable[[str], LambdaClient] | None = None,
    heartbeat_interval_seconds: float | None = None,
    logger: MicrovmWorkerLogger | None = None,
    port: int = DEFAULT_PORT,
) -> MicrovmWorker:
    """Start an HTTP server for the MicroVM lifecycle hooks and the job routes.

    Start it when the image's process starts. Lambda snapshots the process
    after the ``ready`` hook, so the worker must already be listening. The
    image's hook port must match ``port``. The server runs in a daemon thread.
    So end the entry point with :meth:`MicrovmWorker.wait`.

    See :meth:`MicrovmWorkerListener.create` for the other arguments, and
    :class:`MicrovmWorkerListener` for the request handling.

    Args:
        port: The port to listen on. Defaults to 8080, the MicroVM default.
            0 picks a free port.

    Raises:
        TypeError: As :meth:`MicrovmWorkerListener.create`.
        ValueError: As :meth:`MicrovmWorkerListener.create`.
        OSError: When the port cannot be bound.
    """
    listener = MicrovmWorkerListener.create(
        handler=handler,
        routes=routes,
        create_client=create_client,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        logger=logger,
    )
    return MicrovmWorker.start(listener, port=port)


def _check_handler(handler: object) -> None:
    if handler is None:
        return
    if not callable(handler):
        msg = f"a job handler must be callable. Got {type(handler).__name__}."
        raise TypeError(msg)
    # An async function returns a coroutine, which the worker would report as
    # a result that is not JSON-serializable. So it is refused here. An object
    # whose __call__ is async is an async function too, and
    # iscoroutinefunction does not detect it on the object itself.
    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(type(handler), "__call__", None)
    ):
        msg = "a job handler must be a plain function, not an async function"
        raise TypeError(msg)


@dataclass(frozen=True, kw_only=True)
class _Outcome:
    """What the handler returned, or what it raised."""

    value: Any = None
    error: BaseException | None = None

    @classmethod
    def of(cls, run: Callable[[], Any]) -> _Outcome:
        """Run the handler, and keep its result or its exception.

        ``BaseException`` is kept too. A handler that calls ``sys.exit()``
        raises ``SystemExit``, which would otherwise end the job thread
        without a report.

        A handler can still return an awaitable, for example a plain function
        that calls an async function and returns its coroutine. The worker
        cannot run it. So the outcome is a ``TypeError`` that names the
        cause, and a coroutine is closed, so that Python does not warn that
        it was never awaited.
        """
        try:
            value = run()
        except BaseException as error:  # noqa: BLE001
            return cls(error=error)
        if inspect.isawaitable(value):
            if inspect.iscoroutine(value):
                # close() raises when the coroutine has started and ignores
                # GeneratorExit, for example with an await in a finally
                # block. The TypeError below must still be reported.
                with contextlib.suppress(Exception):
                    value.close()
            return cls(
                error=TypeError(
                    "the job handler returned an awaitable. A job handler must "
                    "be a plain function that returns its result."
                )
            )
        return cls(value=value)


class _RequestHandler(BaseHTTPRequestHandler):
    """Passes every request to :meth:`handle_exchange`.

    The listener's subclass implements it.
    """

    timeout = _REQUEST_TIMEOUT_SECONDS
    # HTTP/1.1 makes the base class answer "Expect: 100-continue". Under
    # HTTP/1.0 it does not, and a client such as curl then waited 1 second
    # before it sent the body. Every answer closes its connection, so a body
    # that the worker did not read cannot be parsed as the next request.
    protocol_version = "HTTP/1.1"

    def handle_exchange(self) -> None:  # pragma: no cover
        raise NotImplementedError

    def do_POST(self) -> None:  # noqa: N802
        self.handle_exchange()

    def do_GET(self) -> None:  # noqa: N802
        self.handle_exchange()

    def do_PUT(self) -> None:  # noqa: N802
        self.handle_exchange()

    def do_PATCH(self) -> None:  # noqa: N802
        self.handle_exchange()

    def do_DELETE(self) -> None:  # noqa: N802
        self.handle_exchange()

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # The base class writes every request to stderr. The worker logs what
        # matters through its own logger.
        return

    def version_string(self) -> str:
        # The base class names the exact Python version in the Server header.
        # Every caller of the MicroVM endpoint would learn it, and no caller
        # needs it.
        return "aws-durable-execution-sdk-python-microvm-worker"


class _Exchange:
    """One request and its answer."""

    def __init__(
        self, request: BaseHTTPRequestHandler, logger: MicrovmWorkerLogger
    ) -> None:
        self._request = request
        self._logger = logger
        self.responded = False
        # Set when the worker answers before it has read the whole body.
        self._body_unread = False

    @property
    def method(self) -> str:
        return self._request.command

    @property
    def path(self) -> str:
        return self._request.path.split("?", 1)[0]

    def declared_length(self) -> int | None:
        """The body length from ``Content-Length``.

        ``None`` when the body is chunked, or the header is absent or not
        valid. Reading the body reports an invalid header.
        """
        headers = self._request.headers
        if "chunked" in headers.get("Transfer-Encoding", "").lower():
            return None
        raw = headers.get("Content-Length")
        if raw is None or not _CONTENT_LENGTH.fullmatch(raw.strip()):
            return None
        return int(raw)

    def read_json(self, max_bytes: int) -> Any:
        """Read the body, and parse it as JSON.

        Raises:
            InvalidRunHookPayloadError: When the body is larger than
                ``max_bytes``, its framing is not valid, or it is not JSON.
            OSError: When the connection fails or ends before the body does.
        """
        body = self._read_body(max_bytes)
        try:
            return loads_strict(body)
        except ValueError as error:
            msg = "request body is not valid JSON"
            raise InvalidRunHookPayloadError(msg) from error

    def too_large(
        self, max_bytes: int, part: str = "request body"
    ) -> InvalidRunHookPayloadError:
        """Return the error for a request part over ``max_bytes``.

        The worker answers such a request before it reads the rest of the
        body. So this method also marks the body unread.
        """
        self._body_unread = True
        return InvalidRunHookPayloadError(f"{part} exceeds {max_bytes} bytes")

    def respond(self, status: int, body: Mapping[str, object]) -> None:
        """Send the answer. A client that has gone away is logged."""
        self.responded = True
        data = json.dumps(body, separators=(",", ":")).encode()
        request = self._request
        try:
            request.send_response(status)
            request.send_header("Content-Type", "application/json")
            request.send_header("Content-Length", str(len(data)))
            # The header also makes the base class close the connection.
            request.send_header("Connection", "close")
            request.end_headers()
            request.wfile.write(data)
        except OSError as error:
            self._logger.warning(
                "could not send the response",
                extra={"status": status, "error": describe(error)},
            )

    def discard_unread_body(self) -> None:
        """Read and drop the rest of a body that the worker did not read.

        1. The worker answers a body over the limit without reading it.
        2. The connection then closes with unread bytes in its receive
           buffer. So the kernel sends a TCP reset instead of a normal close.
        3. A client that is still sending gets the reset before it reads the
           answer. So the client never sees the 400.
        4. The durable function fails at once on a 400, and retries a reset.
           So the reset turns a clear rejection into retries.

        So after the answer, the worker ends its side of the connection, and
        reads and drops input until the client closes. The read stops after
        :data:`_DISCARD_SECONDS` or :data:`_MAX_DISCARD_BYTES`. A client that
        sends more still gets the reset.
        """
        if not self._body_unread:
            return
        connection = self._request.connection
        try:
            connection.shutdown(socket.SHUT_WR)
            deadline = time.monotonic() + _DISCARD_SECONDS
            discarded = 0
            while discarded < _MAX_DISCARD_BYTES:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                connection.settimeout(remaining)
                data = connection.recv(_DISCARD_READ_BYTES)
                if not data:
                    return
                discarded += len(data)
        except OSError:
            # The client has gone, or the time ran out in recv.
            return

    def _read_body(self, max_bytes: int) -> bytes:
        headers = self._request.headers
        if "chunked" in headers.get("Transfer-Encoding", "").lower():
            return self._read_chunked(max_bytes)
        if headers.get("Content-Length") is None:
            return b""
        length = self.declared_length()
        if length is None:
            msg = "request Content-Length is not valid"
            raise InvalidRunHookPayloadError(msg)
        if length > max_bytes:
            raise self.too_large(max_bytes)
        return self._read_exactly(length)

    def _read_chunked(self, max_bytes: int) -> bytes:
        """Read a body with chunked transfer encoding.

        ``http.server`` does not decode it, and Node's server does. A durable
        function in another language, or Lambda's own hook client, can send
        one. So the worker decodes it.

        RFC 9112 defines a chunk size as one or more hex digits. ``int(x, 16)``
        also accepts a ``0x`` prefix, a sign, and underscores. So the size is
        matched against the RFC form first, as ``Content-Length`` is.

        The size lines, their extensions, and the trailers do not count as
        body data. Without a limit of their own, they could make the worker
        read without bound. Node fixed the same issue in CVE-2024-22019. So
        they get their own limit of ``max_bytes``, and the data limit keeps
        its meaning.
        """
        chunks: list[bytes] = []
        size = 0
        framing = 0

        def read_framing_line() -> bytes:
            nonlocal framing
            line = self._request.rfile.readline(_MAX_CHUNK_LINE_BYTES)
            framing += len(line)
            if framing > max_bytes:
                raise self.too_large(
                    max_bytes, "the chunked framing of the request body"
                )
            return line

        while True:
            line = read_framing_line()
            if not line.endswith(b"\n"):
                raise _malformed_chunks() if line else _ended_early()
            token = line.split(b";", 1)[0].strip()
            if not _CHUNK_SIZE.fullmatch(token):
                raise _malformed_chunks()
            chunk_size = int(token, 16)
            if chunk_size == 0:
                # The trailer lines end at an empty line.
                while read_framing_line() not in {b"\r\n", b"\n", b""}:
                    pass
                return b"".join(chunks)
            size += chunk_size
            if size > max_bytes:
                raise self.too_large(max_bytes)
            chunks.append(self._read_exactly(chunk_size))
            if self._read_exactly(2) != b"\r\n":
                raise _malformed_chunks()

    def _read_exactly(self, length: int) -> bytes:
        data = self._request.rfile.read(length)
        if len(data) < length:
            raise _ended_early()
        return data


def _malformed_chunks() -> InvalidRunHookPayloadError:
    return InvalidRunHookPayloadError("request body has malformed chunked encoding")


def _ended_early() -> ConnectionError:
    return ConnectionError("the connection ended before the request body did")


class _Server(ThreadingHTTPServer):
    """The worker's HTTP server."""

    # socketserver listens with a backlog of 5, and Node's server with 511.
    # A session can send many jobs at once. A connection beyond the backlog
    # waits for a SYN retransmit, or gets a TCP reset. So the worker uses
    # Node's backlog. The kernel caps it at net.core.somaxconn.
    request_queue_size = 511

    def server_bind(self) -> None:
        # HTTPServer.server_bind looks up the host's fully qualified name,
        # which can wait for DNS. The worker never uses the name. So the bind
        # skips the lookup.
        socketserver.TCPServer.server_bind(self)
        self.server_name = "localhost"
        self.server_port = int(self.server_address[1])

    def handle_error(self, request: Any, client_address: Any) -> None:
        # A client that resets the connection while it sends the request line
        # or the headers ends here, before the listener gets the request. The
        # base class prints a traceback to stderr. A client that went away
        # needs none.
        if isinstance(sys.exception(), ConnectionError):
            return
        super().handle_error(request, client_address)


class _DualStackServer(_Server):
    """Listens on IPv6 and IPv4 at once.

    Lambda calls the hooks on ``localhost``. That name can resolve to ``::1``
    or to ``127.0.0.1``. A server on one address family would refuse the
    other, and a refused ``run`` hook loses the job. Node's server listens on
    both by default.
    """

    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        # _create_server uses this class only when has_dualstack_ipv6() is
        # true. That check makes this same call on a new socket. So the call
        # fails only on a fault, and the worker then fails to start. A
        # suppressed failure would leave a server that refuses IPv4 on a host
        # whose IPv6 sockets start IPv6-only.
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


def _create_server(
    port: int, handler_class: type[BaseHTTPRequestHandler]
) -> ThreadingHTTPServer:
    if socket.has_dualstack_ipv6():
        return _DualStackServer(("::", port), handler_class)
    return _Server(("0.0.0.0", port), handler_class)  # noqa: S104

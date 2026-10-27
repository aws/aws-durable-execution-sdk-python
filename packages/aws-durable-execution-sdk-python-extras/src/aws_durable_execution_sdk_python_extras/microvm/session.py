# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``microvm_session`` operation: run several jobs in one AWS Lambda
MicroVM."""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypeVar

from aws_durable_execution_sdk_python.exceptions import (
    ChildContextError,
    ExecutionError,
    InvocationError,
    ValidationError,
)

from aws_durable_execution_sdk_python_extras.microvm.config import (
    MicrovmInvokeOptions,
    MicrovmSessionConfig,
)
from aws_durable_execution_sdk_python_extras.microvm.errors import MicrovmError
from aws_durable_execution_sdk_python_extras.microvm.job import (
    run_hook_payload_without_job,
)
from aws_durable_execution_sdk_python_extras.microvm.operation import (
    HTTP_DELIVERY,
    LaunchResult,
    _MicrovmScope,
    _validate_base,
    _validate_heartbeat,
    _validate_input,
    _validate_region,
    _validate_request,
    _validate_suspend_after,
    _validate_timeout,
    create_job_callback,
    job_error,
    step_retry_strategy,
    unknown_delivery_error,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import DurableContext


_T = TypeVar("_T")

DEFAULT_AUTO_SUSPEND_IDLE_SECONDS = 60
"""The default :attr:`MicrovmSessionConfig.auto_suspend_idle_time`, in seconds.

A resume took about 1 second in us-east-1. So 60 seconds keeps back-to-back
jobs from paying for a suspend and a resume that they do not need. A long
wait costs at most 60 seconds of compute before the MicroVM suspends. A
session whose ``timeout`` is shorter uses its ``timeout``. A session whose
``timeout`` is under 10 seconds does not suspend by default.
"""

_WARNED_SESSIONS: OrderedDict[str, None] = OrderedDict()
"""The IDs of the sessions that warned about a worker that ignores
``suspend_after``, oldest first."""

_MAX_WARNED_SESSIONS = 1_000
"""How many session IDs :data:`_WARNED_SESSIONS` keeps. A session ID is a
short string, so 1,000 of them use well under 1 MB."""

_WARNED_LOCK = threading.Lock()

_MIN_AUTO_SUSPEND_IDLE_SECONDS = 10
"""The shortest idle time a session accepts.

A resumed MicroVM runs its ``resume`` hook, reports ``RUNNING`` at the next
state poll, and then receives the job after a new auth token. That path can
take a few seconds. A shorter idle time could suspend the MicroVM again
before its job arrives.
"""


def microvm_session(
    context: DurableContext,
    name: str,
    *,
    config: MicrovmSessionConfig,
    handler: Callable[[MicrovmSession, DurableContext], _T],
) -> _T:
    """Run a handler that sends several jobs to one AWS Lambda MicroVM, and
    return the handler's value.

    The jobs share the MicroVM's files and processes. So a later job can use
    what an earlier job wrote. The operation runs its durable operations in
    one child context named ``name``:

    1. ``<name>.session`` generates a session ID and checkpoints it.
    2. ``<name>.launch`` calls ``RunMicrovm``. The client token is the
       SHA-256 hex digest of the session ID. The session ID is random, so the
       token is unique per session. It is checkpointed, so every retry and
       replay of the launch returns the same MicroVM.
    3. The handler runs with a :class:`MicrovmSession` and the child context.
       Each :meth:`MicrovmSession.invoke` delivers one job over HTTP and
       waits for its result. The handler can use the child context for
       durable operations between jobs, such as waits and callbacks.
    4. ``<name>.terminate`` calls ``TerminateMicrovm`` after the handler
       returns or raises.

    The session sends its first job right after the launch. The MicroVM
    endpoint holds or refuses requests until the ``run`` hook returns, and
    the request step retries a refused request inside its own attempt. So the
    session needs no separate readiness wait.

    With ``config.auto_suspend_on_idle``, the worker in the MicroVM suspends
    its own MicroVM when no job has run for ``auto_suspend_idle_time``. With
    that setting or an ``idle_policy``, the request step of each job calls
    ``GetMicrovm`` first. It resumes a suspended MicroVM, and it returns at
    once for a new MicroVM that is still booting. The launch checkpoint
    records whether the MicroVM can be suspended. Replay follows that record,
    not the current config.

    A job with ``suspend_after`` asks the worker to suspend the MicroVM after
    the job. Each later job of the handle then checks the state before
    delivery, as with ``auto_suspend_on_idle``. That record is kept in
    memory only. So every job of every session also checks the state after
    a sign of a suspended MicroVM: no answer, or an HTTP 502 with
    ``x-aws-proxy-error: MICROVM_CONNECT_FAILED``. A boot gives no such
    sign, unless the worker takes more than 10 seconds to start listening.
    Then the first delivery gets no answer, and the operation makes one
    ``GetMicrovm`` call.

    A failed :meth:`MicrovmSession.invoke` raises inside the handler. The
    handler can catch the error and continue with the same MicroVM. An
    error that leaves the handler ends the session, and the MicroVM is
    terminated:

    1. A MicroVM error, such as :class:`MicrovmJobFailedError`, that the
       handler raises itself reaches the caller with its own class.
    2. Any other error reaches the caller as ``ChildContextError``, as from
       ``run_in_child_context``. This includes a MicroVM error that leaves
       a child context that the handler created: that child context records
       ``ChildContextError`` as its error type. The caller then gets the
       MicroVM error's message, but not its class.

    The session's value is the handler's return value, checkpointed as the
    child context result. So it must be serializable by the SDK's default
    serdes, and it counts toward the checkpoint size limit.

    Pass the context that the call runs in. ``invoke`` takes its context the
    same way:

    .. code-block:: python

        def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
            build = vm.invoke(
                ctx,
                "build",
                job_input=event,
                options=MicrovmInvokeOptions(
                    path="/build", timeout=Duration.from_minutes(15)
                ),
            )
            return vm.invoke(
                ctx,
                "test",
                job_input=build,
                options=MicrovmInvokeOptions(
                    path="/test", timeout=Duration.from_minutes(10)
                ),
            )


        result = microvm_session(context, "pipeline", config=CONFIG, handler=pipeline)

    Args:
        context: The context to create the durable operations in.
        name: The child context name. It also prefixes the session's own
            operation names.
        config: The MicroVM. ``timeout`` bounds the MicroVM's lifetime.
        handler: Sends the jobs. It receives the session handle and the
            session's child context.

    Returns:
        The handler's return value.

    Raises:
        ValidationError: When ``name`` or ``config`` is not valid, or the
            Region cannot be determined. No durable operation starts.
        MicrovmLaunchError: When ``RunMicrovm`` fails after all retries, or
            rejects the config, for example an empty ``image_identifier``.
        MicrovmError: A MicroVM error that the handler did not catch.
        ChildContextError: Any other error that the handler raised. A
            ``handler`` that is not callable raises Python's ``TypeError``
            after the launch, inside the child context. So it also reaches
            the caller as ``ChildContextError``, after the terminate step.
    """
    scope = _SessionScope.create(context, name, config)

    def run(child: DurableContext) -> _T:
        return scope.run_session(child, handler)

    try:
        return context.run_in_child_context(run, name=name)
    except ChildContextError as error:
        # The handler is caller code. So only a MicroVM error is restored to
        # its class. Every other error keeps the SDK's ChildContextError.
        if isinstance(error.__cause__, MicrovmError):
            raise MicrovmError.from_child_context_error(name, error) from error
        raise


class MicrovmSession:
    """The handle that a :func:`microvm_session` handler receives.

    The handle holds no context of its own. Each :meth:`invoke` creates its
    durable operations in the context that the caller passes. So one handle
    works in the session's child context, in a nested child context, and in
    ``map`` and ``parallel`` items.
    """

    def __init__(
        self,
        scope: _SessionScope,
        launched: LaunchResult,
        *,
        resume_before_delivery: bool,
        session_id: str,
    ) -> None:
        self._scope = scope
        self._launched = launched
        self._resume_before_delivery = resume_before_delivery
        # Set by the first suspend_after job of this handle. Later jobs then
        # check the state before delivery. The flag lives in memory only. A
        # replay of a completed job, map item, or child context does not run
        # its invoke() again, so it does not set the flag. The check after a
        # sign of a suspended MicroVM covers that case.
        self._may_be_suspended = False
        self._session_id = session_id

    @property
    def microvm_id(self) -> str:
        """The ID of the session's MicroVM."""
        return self._launched.microvm_id

    def _resume_mode(self) -> bool:
        """Whether a job checks the MicroVM state before delivery.

        The request step reads it at the start of each attempt. So a step
        retry, and a ``map`` item that starts after a sibling set the flag,
        see the current value. The flag only changes from ``False`` to
        ``True``.
        """
        return self._resume_before_delivery or self._may_be_suspended

    def _claim_suspend_after_warning(self) -> bool:
        """Return ``True`` the first time, so the session warns once about a
        worker that does not support ``suspend_after``.

        Each invocation creates a new handle. So the record is kept per
        process, by session ID. A later invocation in the same execution
        environment then logs no second warning.
        """
        with _WARNED_LOCK:
            if self._session_id in _WARNED_SESSIONS:
                return False
            _WARNED_SESSIONS[self._session_id] = None
            if len(_WARNED_SESSIONS) > _MAX_WARNED_SESSIONS:
                _WARNED_SESSIONS.popitem(last=False)
            return True

    def invoke(
        self,
        context: DurableContext,
        name: str,
        *,
        job_input: Any,
        options: MicrovmInvokeOptions,
    ) -> Any:
        """Send one job to the session's MicroVM, and return the job's result.

        The job goes over HTTP to ``options.path``. Without a path, it goes to
        :data:`DEFAULT_MICROVM_JOB_PATH`, which the worker serves with its
        ``handler``.

        Each call creates a child context named ``name`` in ``context``, with
        a callback ``<name>.callback`` and a request step ``<name>.request``.
        Pass the context that the call runs in: the session's child context,
        or a nested one, such as a ``map`` item's context.

        Args:
            context: The context to create the job's durable operations in.
            name: The job's child context name.
            job_input: The job's input. It must be JSON-serializable, and the
                same on every replay.
            options: The route, the timeouts, and ``suspend_after`` of this
                job.

        Returns:
            The value that the MicroVM passed to
            ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

        Raises:
            TypeError: When ``context`` is the context that
                ``microvm_session`` was called with, or ``job_input`` is not
                JSON-serializable.
            AttributeError: When ``context`` is not a durable context, for
                example a job name passed as the first argument. Python
                raises it at ``context.run_in_child_context``.
            ValueError: When the JSON of ``job_input`` is over
                ``MAX_JOB_INPUT_BYTES`` UTF-8 bytes.
            ValidationError: When ``name`` or ``options`` is not valid, for
                example a ``suspend_after`` that is not a ``bool``.
            MicrovmNotRunningError: When the session's MicroVM has ended. The
                session reports it when it checks the MicroVM state before
                each job, and for every job when ``CreateMicrovmAuthToken``
                reports the MicroVM removed. A check after a sign of a
                suspended MicroVM reports it too.
            MicrovmDeliveryError: When the delivery fails after all retries,
                or the route rejects the job. A suspended MicroVM whose
                ``ResumeMicrovm`` is denied fails the delivery at once.
            MicrovmJobFailedError: When the MicroVM reports a failure, or
                sends a result that is not valid JSON.
            MicrovmTimeoutError: When no result or heartbeat arrives in time.
        """
        scope = self._scope
        # The caller's own context is often in scope in the handler's
        # closure. A job created in it lands outside the session's child
        # context. The core SDK does not refuse that, and the next replay
        # fails with NonDeterministicExecutionError after the job has run.
        # So the most likely wrong context is refused here. A full ancestry
        # check would need support in the core SDK.
        if context is scope.outer_context:
            msg = (
                f'MicroVM session "{scope.name}": invoke received the context '
                "that microvm_session was called with. Pass the context that "
                "the handler receives, or a context nested in it."
            )
            raise TypeError(msg)
        if not isinstance(name, str) or not name:
            msg = (
                f'MicroVM session "{scope.name}": invoke requires a non-empty job name'
            )
            raise ValidationError(msg)
        retry_window_seconds = _validate_request(name, options)
        timeout_seconds = _validate_timeout(name, "timeout", options.timeout)
        heartbeat_timeout_seconds = _validate_heartbeat(name, options.heartbeat_timeout)
        suspend_after = _validate_suspend_after(name, options.suspend_after)
        _validate_input(name, job_input)
        launched = self._launched
        if suspend_after:
            # The flag is set before the child context. A replay of a
            # completed job returns its result without running the child
            # context's function. So the flag is set on every replay of this
            # call too.
            self._may_be_suspended = True

        def run(job_context: DurableContext) -> Any:
            callback = create_job_callback(
                job_context, name, timeout_seconds, heartbeat_timeout_seconds
            )
            scope.deliver(
                job_context,
                job_name=name,
                launched=launched,
                callback_id=callback.callback_id,
                job_input=job_input,
                heartbeat_timeout_seconds=heartbeat_timeout_seconds,
                request=options,
                retry_window_seconds=retry_window_seconds,
                resume=self._resume_mode,
                suspend_check=True,
                suspend_after=suspend_after,
                claim_suspend_after_warning=(
                    self._claim_suspend_after_warning if suspend_after else None
                ),
            )
            try:
                return callback.result()
            except (ExecutionError, InvocationError):
                # An SDK execution or invocation error is not a job outcome.
                # The core SDK records neither as the child context's failure.
                raise
            except Exception as error:
                # SuspendExecution is a BaseException, so a wait for the
                # callback passes through here.
                raise job_error(name, callback, error) from error

        try:
            return context.run_in_child_context(run, name=name)
        except ChildContextError as error:
            raise MicrovmError.from_child_context_error(name, error) from error


@dataclass(frozen=True, kw_only=True)
class _SessionScope(_MicrovmScope):
    """The validated values of one :func:`microvm_session` call."""

    auto_suspend_idle_seconds: int | None
    idle_policy: Mapping[str, Any] | None
    outer_context: DurableContext = field(compare=False, repr=False)
    """The context that ``microvm_session`` was called with. A job created
    in it would be outside the session's child context."""

    @property
    def resume_from_config(self) -> bool:
        """Whether the current config asks each job to check the MicroVM
        state, and resume it.

        The worker suspends an idle MicroVM, and an idle policy can suspend
        it too. Either way, each job must resume it first. A session launch
        records this value as its resume mode.
        """
        return (
            self.auto_suspend_idle_seconds is not None or self.idle_policy is not None
        )

    @classmethod
    def create(
        cls, context: DurableContext, name: str, config: MicrovmSessionConfig
    ) -> _SessionScope:
        """Validate the call, before any durable operation starts.

        Raises:
            ValidationError: When ``name`` or ``config`` is not valid, or the
                Region cannot be determined.
        """
        timeout_seconds = _validate_base(name, config.timeout)
        auto_suspend_idle_seconds = _validate_auto_suspend(
            name, config, timeout_seconds
        )
        partition, region = _validate_region(context, name)
        return cls(
            name=name,
            config=config,
            partition=partition,
            region=region,
            timeout_seconds=timeout_seconds,
            retry_strategy=step_retry_strategy(config),
            auto_suspend_idle_seconds=auto_suspend_idle_seconds,
            idle_policy=config.idle_policy,
            outer_context=context,
        )

    def run_session(
        self,
        child: DurableContext,
        handler: Callable[[MicrovmSession, DurableContext], _T],
    ) -> _T:
        """Run the session's durable operations in its child context."""
        session_id: str = child.step(
            lambda _: str(uuid.uuid4()), name=f"{self.name}.session"
        )
        launched = self.launch(
            child,
            token_source=session_id,
            payload=run_hook_payload_without_job(
                self.region, self.auto_suspend_idle_seconds
            ),
            delivery=HTTP_DELIVERY,
            idle_policy=self.idle_policy,
            resume_before_delivery=self.resume_from_config,
        )
        if launched.delivery != HTTP_DELIVERY:
            # A launch checkpoint from a release with another delivery. Every
            # session job goes over HTTP, so this release cannot run one.
            self.terminate(child, launched.microvm_id)
            raise unknown_delivery_error(self.name, launched)
        # 1. The worker reads its idle time once, from the run hook payload of
        #    the launch. RunMicrovm sets the idle policy once, at the launch.
        # 2. The launch step's result comes from the checkpoint on replay.
        # 3. So a deploy that changes auto_suspend_on_idle or idle_policy does
        #    not change what the running MicroVM does.
        # 4. A job that skips the state check of a suspended MicroVM gets a
        #    502, or no answer. The request step then checks the state, and
        #    resumes the MicroVM. That costs a failed request per job.
        # 5. So each job follows the resume mode that the launch recorded,
        #    not the current config.
        # Every session launch records a boolean. So only `True` turns the
        # state check on. A missing field, or any other value, reads as False.
        try:
            result = handler(
                MicrovmSession(
                    self,
                    launched,
                    resume_before_delivery=launched.resume_before_delivery is True,
                    session_id=session_id,
                ),
                child,
            )
        except ExecutionError:
            # A replay that does not match the history raises
            # NonDeterministicExecutionError, and the execution fails without
            # a retry. The core SDK lets an ExecutionError leave a child
            # context without a checkpoint. A terminate step recorded now
            # would add an operation to that history, or take the ID of an
            # operation already in it. So the error passes through, and the
            # MicroVM runs until its maximum duration, as in JS.
            raise
        except InvocationError as error:
            # A retryable InvocationError, such as RetryableSerDesError, makes
            # Lambda run the invocation again. The child context records no
            # outcome for it, and the handler replays on the same MicroVM.
            # A terminate step recorded now would take the position of the
            # handler's next operation, and the replay would fail with
            # NonDeterministicExecutionError. So the session terminates only
            # for an error that ends the session.
            if not error.is_retryable():
                self.terminate(child, launched.microvm_id)
            raise
        except Exception:
            # SuspendExecution is a BaseException. A handler that waits, for
            # a job or a durable wait, passes through here without
            # terminating the MicroVM.
            self.terminate(child, launched.microvm_id)
            raise
        self.terminate(child, launched.microvm_id)
        return result


def _validate_auto_suspend(
    name: str, config: MicrovmSessionConfig, timeout_seconds: int
) -> int | None:
    """Validate the auto-suspend settings.

    Returns:
        The idle time in seconds, or ``None`` when the MicroVM must not
        suspend itself.

    Raises:
        ValidationError: When a setting is not valid.
    """
    # Suspending is opt-in. It needs lambda:GetMicrovm and
    # lambda:ResumeMicrovm in the function's role, and lambda:SuspendMicrovm
    # in the MicroVM's role. It also stops background processes between jobs.
    if config.auto_suspend_on_idle is not True:
        if config.auto_suspend_idle_time is not None:
            msg = (
                f'MicroVM session "{name}": auto_suspend_idle_time is set, but '
                "auto_suspend_on_idle is not True"
            )
            raise ValidationError(msg)
        return None
    if config.auto_suspend_idle_time is None:
        # A session shorter than the minimum idle time does not suspend. Its
        # MicroVM would suspend again before a resumed job arrives.
        if timeout_seconds < _MIN_AUTO_SUSPEND_IDLE_SECONDS:
            return None
        return min(DEFAULT_AUTO_SUSPEND_IDLE_SECONDS, timeout_seconds)
    idle_seconds = config.auto_suspend_idle_time.to_seconds()
    if idle_seconds < _MIN_AUTO_SUSPEND_IDLE_SECONDS:
        msg = (
            f'MicroVM session "{name}": auto_suspend_idle_time must be at least '
            f"{_MIN_AUTO_SUSPEND_IDLE_SECONDS} seconds"
        )
        raise ValidationError(msg)
    if idle_seconds > timeout_seconds:
        msg = (
            f'MicroVM session "{name}": auto_suspend_idle_time must not be longer '
            "than timeout"
        )
        raise ValidationError(msg)
    return idle_seconds

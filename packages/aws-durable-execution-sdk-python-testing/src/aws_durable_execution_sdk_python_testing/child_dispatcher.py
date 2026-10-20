"""Dispatch of chained-invoke targets.

A CHAINED_INVOKE operation names a function to invoke. The in-process
runner resolves the name from registered handlers; the web runner
resolves it from a function configuration file and invokes at a
Lambda-compatible endpoint. A :class:`ChildDispatcher` owns that
difference and reports one of three results:

* ``StartChild``: a durable target; the executor creates, links, and
  launches a child durable execution;
* ``RunInvocation``: a non-durable target; one invocation whose return
  value is the outcome;
* ``KnownOutcome``: the target could not be dispatched.

A child execution completes the parent's operation through the
executor's terminal-transition hook; no dispatcher waits for it.
"""

from __future__ import annotations

import json
import logging
import pathlib
import re
import threading
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

from botocore.exceptions import ClientError, ReadTimeoutError  # type: ignore

from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime

    from aws_durable_execution_sdk_python_testing.clock import Clock

logger = logging.getLogger(__name__)

# Default execution settings for a chained child execution when the
# registration does not specify them.
DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS = 120
DEFAULT_CHILD_RETENTION_PERIOD_DAYS = 1

# Error surfaced on a CHAINED_INVOKE operation that timed out: a durable
# child whose execution timed out, or a plain target whose single
# invocation ran past the invocation timeout. The service reports this
# fixed type; a stopped or failed child instead surfaces its own error
# object, and a target that could not be invoked surfaces the Lambda
# API error code.
CHAINED_INVOKE_TIMEOUT_ERROR_TYPE: Final[str] = "ChainedInvoke.Timeout"
# Error type Lambda reports for a function that ran past its timeout.
FUNCTION_TIMEOUT_ERROR_TYPE: Final[str] = "Sandbox.Timedout"

# Lambda API error code for an Invoke of a function that does not exist.
FUNCTION_NOT_FOUND_ERROR_TYPE: Final[str] = "ResourceNotFoundException"

# Chained-invoke payload limits, as the service enforces them: the
# input a parent sends, the result a durable child returns, and the
# result a plain target returns are each capped at 1 MiB.
MAX_CHAINED_INVOKE_PAYLOAD_BYTES: Final[int] = 1_048_576
CHAINED_INVOKE_INPUT_TOO_LARGE_MESSAGE: Final[str] = (
    "CHAINED_INVOKE input payload size must be less than or equal to "
    f"{MAX_CHAINED_INVOKE_PAYLOAD_BYTES} bytes."
)
CHAINED_INVOKE_OUTPUT_TOO_LARGE_MESSAGE: Final[str] = (
    "CHAINED_INVOKE output payload size must be less than or equal to "
    f"{MAX_CHAINED_INVOKE_PAYLOAD_BYTES} bytes."
)
CHILD_EXECUTION_OUTPUT_TOO_LARGE_MESSAGE: Final[str] = (
    "Execution output payload size must be less than or equal to "
    f"{MAX_CHAINED_INVOKE_PAYLOAD_BYTES} bytes."
)
CHAINED_INVOKE_UTF8_DECODING_MESSAGE: Final[str] = (
    "CHAINED_INVOKE response could not be decoded as UTF-8."
)

# A chained-invoke target is a Lambda function name in any form Invoke
# accepts: bare name, name with qualifier, partial ARN, or full ARN.
# Grammar and length are those of ChainedInvokeOptions.FunctionName.
MAX_FUNCTION_NAME_LENGTH: Final[int] = 256
_FUNCTION_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?P<arn_prefix>arn:(aws[a-zA-Z-]*)?:lambda:)?"
    r"((?P<region>(eusc-)?[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\d{1}):)?"
    r"((?P<account_id>\d{12}):)?"
    r"(function:)?"
    r"(?P<name>[a-zA-Z0-9-_\.]+)"
    r"(:(?P<qualifier>\$LATEST(\.PUBLISHED)?|[a-zA-Z0-9-_]+))?"
)
INVALID_FUNCTION_ARN_MESSAGE_FORMAT: Final[str] = "Invalid function ARN '{}'"

# ChainedInvokeOptions.TenantId: 1 to 256 characters from this set.
MAX_TENANT_ID_LENGTH: Final[int] = 256
_TENANT_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"[a-zA-Z0-9\._:\/=+\-@ ]+")
INVALID_TENANT_ID_MESSAGE: Final[str] = (
    "TenantId must be 1 to 256 characters matching [a-zA-Z0-9._:/=+-@ ]."
)


def is_valid_tenant_id(tenant_id: str) -> bool:
    """Whether ``tenant_id`` satisfies the ChainedInvokeOptions.TenantId constraint."""
    return (
        0 < len(tenant_id) <= MAX_TENANT_ID_LENGTH
        and _TENANT_ID_PATTERN.fullmatch(tenant_id) is not None
    )


CHAINED_INVOKE_DIFFERENT_ACCOUNT_MESSAGE: Final[str] = (
    "Cannot start a CHAINED_INVOKE on a function in another account."
)
CHAINED_INVOKE_DIFFERENT_REGION_MESSAGE: Final[str] = (
    "Cannot start a CHAINED_INVOKE on a function in another region."
)


def chained_invoke_timeout_message(timeout_seconds: int) -> str:
    """The service's message for a chained invoke that timed out."""
    return f"CHAINED_INVOKE timed out after {timeout_seconds} seconds."


def chained_invoke_timeout_error(timeout_seconds: int) -> ErrorObject:
    """The error a timed-out CHAINED_INVOKE operation carries."""
    return ErrorObject(
        message=chained_invoke_timeout_message(timeout_seconds),
        type=CHAINED_INVOKE_TIMEOUT_ERROR_TYPE,
        data=None,
        stack_trace=None,
    )


def payload_size_bytes(payload: str) -> int:
    """Size of ``payload`` as the service measures it: UTF-8 bytes."""
    return len(payload.encode("utf-8"))


@dataclass(frozen=True)
class FunctionTarget:
    """A parsed chained-invoke target."""

    name: str
    qualifier: str | None
    account_id: str | None
    region: str | None


def parse_function_target(function_name: str) -> FunctionTarget:
    """Parse a Lambda function name into its parts.

    Accepts the four forms Invoke accepts (bare name, name with
    qualifier, partial ARN, full ARN). Raises ``ValueError`` for any
    other string, including a full ARN missing its region or account.
    """
    match = (
        _FUNCTION_NAME_PATTERN.fullmatch(function_name)
        if 0 < len(function_name) <= MAX_FUNCTION_NAME_LENGTH
        else None
    )
    if match is None:
        raise ValueError(function_name)
    region: str | None = match.group("region")
    account_id: str | None = match.group("account_id")
    if match.group("arn_prefix") and (region is None or account_id is None):
        raise ValueError(function_name)
    return FunctionTarget(
        name=match.group("name"),
        qualifier=match.group("qualifier"),
        account_id=account_id,
        region=region,
    )


@dataclass(frozen=True)
class ChainedInvokeRequest:
    """A chained-invoke dispatch request in wire form.

    ``function_name`` is the target's bare name and ``qualifier`` its
    alias or version, if the parent gave one.
    """

    parent_execution_arn: str
    operation_id: str
    function_name: str
    tenant_id: str | None
    payload: str | None
    account_id: str
    trace_fields: dict | None = None
    qualifier: str | None = None

    def lookup_keys(self) -> tuple[str, ...]:
        """Keys the target's registration or configuration may sit under,
        most specific first. See :func:`target_lookup_keys`."""
        return target_lookup_keys(self.function_name, self.qualifier)

    def child_qualifier(self) -> str:
        """Qualifier the child execution records."""
        return self.qualifier if self.qualifier is not None else "$LATEST"

    def invoke_identifier(self) -> str:
        """The function identifier to invoke, exactly as the parent wrote
        it: ``name``, or ``name:qualifier`` including an explicit $LATEST."""
        if self.qualifier is None:
            return self.function_name
        return f"{self.function_name}:{self.qualifier}"


def target_lookup_keys(function_name: str, qualifier: str | None) -> tuple[str, ...]:
    """Keys a target's registration or configuration may sit under, most
    specific first.

    The runner has one registration per key, not versions: a qualified
    key wins when present, otherwise the bare name serves every
    qualifier. The key selects the registration only; what is invoked
    is the identifier as the parent wrote it.
    """
    if qualifier is None:
        return (function_name,)
    return (f"{function_name}:{qualifier}", function_name)


def invoke_identifier(function_name: str, qualifier: str) -> str:
    """The identifier a durable execution's handler is invoked by:
    ``name`` for $LATEST, else ``name:qualifier``."""
    if qualifier == "$LATEST":
        return function_name
    return f"{function_name}:{qualifier}"


@dataclass(frozen=True)
class ChildOutcome:
    """Terminal outcome of a single-invocation chained target.

    ``timed_out`` marks an Invoke the endpoint did not answer within the
    invocation timeout; ``error`` then carries the service's timeout
    error and the operation ends TIMED_OUT rather than FAILED.
    """

    result: str | None = None
    error: ErrorObject | None = None
    timed_out: bool = False


def timed_out_outcome(timeout_seconds: int) -> ChildOutcome:
    """Outcome of a plain target the endpoint did not answer for within ``timeout_seconds``."""
    return ChildOutcome(
        error=chained_invoke_timeout_error(timeout_seconds), timed_out=True
    )


def function_timeout_outcome(
    timeout_seconds: int, request_id: str, now: datetime
) -> ChildOutcome:
    """Outcome of a plain target that ran past its function timeout.

    Lambda ends the function and answers the Invoke with a function
    error, so the operation ends FAILED with that error, as it does at
    the service. The message follows Lambda's own.
    """
    stamp: str = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return ChildOutcome(
        error=ErrorObject(
            message=f"{stamp} {request_id} Task timed out after {timeout_seconds:.2f} seconds",
            type=FUNCTION_TIMEOUT_ERROR_TYPE,
            data=None,
            stack_trace=None,
        )
    )


def result_outcome(result: str | None) -> ChildOutcome:
    """Outcome of a plain target that returned ``result``, size-checked."""
    if (
        result is not None
        and payload_size_bytes(result) > MAX_CHAINED_INVOKE_PAYLOAD_BYTES
    ):
        return ChildOutcome(
            error=ErrorObject.from_message(CHAINED_INVOKE_OUTPUT_TOO_LARGE_MESSAGE)
        )
    return ChildOutcome(result=result)


@dataclass(frozen=True)
class StartChild:
    """Start a new child durable execution.

    ``child_start.lambda_endpoint`` is ``None``. A per-execution endpoint
    serves one function alone, and the child is another function, so
    the parent's own endpoint is not the child's. The executor pins the
    child to the endpoint the parent's chained targets go to
    (``Invoker.inherit_endpoint``).
    """

    child_start: StartDurableExecutionInput


@dataclass(frozen=True)
class RunInvocation:
    """Run the target as a single invocation off the worker lanes.

    The callable blocks for at most one invocation of a non-durable
    target and returns that invocation's outcome.
    """

    invocation: Callable[[], ChildOutcome]


@dataclass(frozen=True)
class KnownOutcome:
    """The terminal outcome is already known."""

    outcome: ChildOutcome


DispatchResult = StartChild | RunInvocation | KnownOutcome


def failed_to_start(message: str, error_type: str | None = None) -> KnownOutcome:
    """Build the outcome for a target that could not be dispatched.

    ``error_type`` is the Lambda API error code when one applies (for
    example ``ResourceNotFoundException``); a dispatch failure with no
    API counterpart carries a message only, as the service does.
    """
    return KnownOutcome(
        outcome=ChildOutcome(
            error=ErrorObject(
                message=message,
                type=error_type,
                data=None,
                stack_trace=None,
            )
        )
    )


def function_not_found(function_name: str, hint: str = "") -> KnownOutcome:
    """Build the outcome for a target that is not a known function."""
    return failed_to_start(
        f"Function not found: {function_name}.{hint}",
        error_type=FUNCTION_NOT_FOUND_ERROR_TYPE,
    )


def invoke_function(
    lambda_client: Any,
    function_name: str,
    payload: str | None,
    tenant_id: str | None,
    invocation_timeout_seconds: int,
) -> ChildOutcome:
    """Invoke non-durable ``function_name`` once at a Lambda-compatible endpoint.

    The response payload (or function error) is the outcome. An Invoke
    API error fails the operation with that error's code and message,
    as the service reports it. A read timeout means the target ran past
    ``invocation_timeout_seconds``, which the service reports as a
    chained-invoke timeout.
    """
    try:
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "InvocationType": "RequestResponse",
            "Payload": payload if payload is not None else "{}",
        }
        if tenant_id is not None:
            kwargs["TenantId"] = tenant_id
        response: dict[str, Any] = lambda_client.invoke(**kwargs)
        # The response headers arrive when the target returns; the body
        # streams after them and can time out on its own.
        raw_body: bytes = response["Payload"].read()
    except ReadTimeoutError:
        logger.info("Chained invoke of %s timed out", function_name)
        return timed_out_outcome(invocation_timeout_seconds)
    except ClientError as err:
        logger.info("Chained invoke of %s failed to start: %s", function_name, err)
        api_error: dict[str, Any] = err.response.get("Error", {})
        return ChildOutcome(
            error=ErrorObject(
                message=api_error.get("Message") or str(err),
                type=api_error.get("Code"),
                data=None,
                stack_trace=None,
            )
        )
    except Exception as err:  # noqa: BLE001 — dispatch failure fails the operation
        logger.info("Chained invoke of %s failed to start: %s", function_name, err)
        return ChildOutcome(
            error=ErrorObject(
                message=f"Failed to invoke {function_name}: {err}",
                type=None,
                data=None,
                stack_trace=None,
            )
        )

    try:
        body: str = raw_body.decode("utf-8")
    except UnicodeDecodeError:
        return ChildOutcome(
            error=ErrorObject.from_message(CHAINED_INVOKE_UTF8_DECODING_MESSAGE)
        )
    if "FunctionError" in response:
        return ChildOutcome(error=_error_from_invoke_body(body))
    return result_outcome(body if body else None)


class ChildDispatcher(Protocol):
    """Dispatches a chained-invoke target for one environment."""

    def preflight(self, target: FunctionTarget) -> ChildOutcome | None:
        """The outcome of ``target`` known before anything is dispatched,
        or ``None`` when dispatch may proceed.

        The service resolves a target before it schedules anything. A
        target it cannot resolve comes back FAILED in the checkpoint
        response itself, so the handler learns of it without
        suspending. This is the runner's counterpart. It consults only
        what the runner already holds (registrations or configurations)
        and never the endpoint, so it returns at once.
        """
        ...

    def dispatch(self, request: ChainedInvokeRequest) -> DispatchResult:
        """Dispatch ``request`` and report how it will complete.

        Runs off the parent execution's worker lane. May block for the
        duration of a single invocation of a non-durable target, but
        must not block on a durable child reaching a terminal state.
        """
        ...


@dataclass(frozen=True)
class RegisteredFunction:
    """A function registered with the in-process runner."""

    handler: Callable[..., Any]
    is_durable: bool
    execution_timeout_seconds: int = DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS
    retention_period_days: int = DEFAULT_CHILD_RETENTION_PERIOD_DAYS


class FunctionRegistry:
    """Name-to-handler registry for the in-process runner."""

    def __init__(self) -> None:
        self._functions: dict[str, RegisteredFunction] = {}

    def register(
        self,
        function_name: str,
        handler: Callable[..., Any],
        *,
        is_durable: bool,
        execution_timeout_seconds: int = DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS,
        retention_period_days: int = DEFAULT_CHILD_RETENTION_PERIOD_DAYS,
    ) -> None:
        """Register ``handler`` under ``function_name``."""
        self._functions[function_name] = RegisteredFunction(
            handler=handler,
            is_durable=is_durable,
            execution_timeout_seconds=execution_timeout_seconds,
            retention_period_days=retention_period_days,
        )

    def get(self, function_name: str) -> RegisteredFunction | None:
        """Return the registration for ``function_name`` if present."""
        return self._functions.get(function_name)


class InProcessChildDispatcher:
    """Resolves chained-invoke targets from registered handlers.

    A durable registration becomes a child durable execution start; a
    non-durable registration runs as a single handler call whose
    payload and result cross the seam in serialized form; an unknown
    name fails to start.
    """

    def __init__(
        self,
        registry: FunctionRegistry,
        context_factory: Callable[[ChainedInvokeRequest], Any],
        invocation_timeout_seconds: int,
        clock: Clock,
    ) -> None:
        """``context_factory`` builds the Lambda context a plain target
        receives; ``clock`` stamps a function-timeout error."""
        self._registry = registry
        self._context_factory = context_factory
        self._invocation_timeout_seconds = invocation_timeout_seconds
        self._clock = clock

    def _find_registration(
        self, function_name: str, qualifier: str | None
    ) -> RegisteredFunction | None:
        for key in target_lookup_keys(function_name, qualifier):
            registration: RegisteredFunction | None = self._registry.get(key)
            if registration is not None:
                return registration
        return None

    @staticmethod
    def _not_found(function_name: str) -> KnownOutcome:
        return function_not_found(
            function_name,
            hint=" Register it with register_durable_function or register_function.",
        )

    def preflight(self, target: FunctionTarget) -> ChildOutcome | None:
        """Fail a target no handler is registered for."""
        if self._find_registration(target.name, target.qualifier) is None:
            return self._not_found(target.name).outcome
        return None

    def dispatch(self, request: ChainedInvokeRequest) -> DispatchResult:
        """Dispatch ``request`` against the registered functions."""
        registration: RegisteredFunction | None = self._find_registration(
            request.function_name, request.qualifier
        )
        if registration is None:
            return self._not_found(request.function_name)

        if registration.is_durable:
            return StartChild(
                child_start=StartDurableExecutionInput(
                    account_id=request.account_id,
                    function_name=request.function_name,
                    function_qualifier=request.child_qualifier(),
                    execution_name=str(uuid.uuid4()),
                    execution_timeout_seconds=registration.execution_timeout_seconds,
                    execution_retention_period_days=registration.retention_period_days,
                    invocation_id=None,
                    trace_fields=request.trace_fields,
                    tenant_id=request.tenant_id,
                    input=request.payload,
                    lambda_endpoint=None,
                )
            )

        handler: Callable[..., Any] = registration.handler
        return RunInvocation(
            invocation=lambda: self._run_single_invocation(handler, request)
        )

    def _run_single_invocation(
        self, handler: Callable[..., Any], request: ChainedInvokeRequest
    ) -> ChildOutcome:
        """Run ``handler`` once, bounded by the invocation timeout.

        The handler runs on its own thread so the bound can be enforced.
        A handler still running at the deadline is abandoned and the
        outcome is Lambda's function-timeout error, as the runner does
        for a handler invocation that exceeds the timeout. Python cannot
        terminate a thread, so the abandoned handler keeps running until
        it returns and its result is discarded. Lambda stops the sandbox
        at the timeout; the in-process runner cannot, for this handler
        as for its durable handlers.
        """
        outcome: list[ChildOutcome] = []
        context: Any = self._context_factory(request)

        def run() -> None:
            try:
                payload: str | None = request.payload
                event: Any = json.loads(payload) if payload else None
                result: Any = handler(event, context)
                outcome.append(result_outcome(json.dumps(result)))
            except Exception as err:  # noqa: BLE001 — the outcome carries the error
                logger.info("Chained invoke target raised: %s", err)
                outcome.append(ChildOutcome(error=ErrorObject.from_exception(err)))

        thread = threading.Thread(
            target=run, name="durable-chained-invoke-target", daemon=True
        )
        thread.start()
        thread.join(self._invocation_timeout_seconds)
        if thread.is_alive():
            logger.info("Chained invoke target exceeded the invocation timeout")
            return function_timeout_outcome(
                self._invocation_timeout_seconds,
                str(getattr(context, "aws_request_id", "")),
                self._clock.now(),
            )
        return outcome[0]


def _error_from_invoke_body(body: str) -> ErrorObject:
    """Build an ErrorObject from a function-error invoke response body."""
    try:
        data: dict[str, Any] = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return ErrorObject.from_message(body or "Chained invoke failed.")
    return ErrorObject(
        message=data.get("errorMessage"),
        type=data.get("errorType"),
        data=data.get("errorData"),
        stack_trace=data.get("stackTrace"),
    )


@dataclass(frozen=True)
class FunctionConfig:
    """Durability configuration for a chained-invoke target function.

    Mirrors the Lambda function configuration: a function is durable
    when it carries a ``DurableConfig``, whose ``ExecutionTimeout`` and
    ``RetentionPeriodInDays`` bound its executions. A function without
    one is a plain function.
    """

    is_durable: bool
    execution_timeout_seconds: int = DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS
    retention_period_days: int = DEFAULT_CHILD_RETENTION_PERIOD_DAYS

    @classmethod
    def from_dict(cls, data: Any, name: str | None = None) -> FunctionConfig:
        """Build from one entry of the function configurations.

        ``{}`` or ``null`` is a plain function. ``{"DurableConfig": {...}}``
        is a durable function; fields the ``DurableConfig`` omits take
        the runner defaults. Any other shape raises ``ValueError`` naming
        ``name``: a malformed entry must not turn a durable target plain
        or fail with a bare traceback.
        """
        if data is None:
            return cls(is_durable=False)
        if not isinstance(data, dict):
            raise ValueError(
                _malformed(name, "the entry must be a JSON object or null")
            )
        durable_config: Any = data.get("DurableConfig")
        if durable_config is None:
            return cls(is_durable=False)
        if not isinstance(durable_config, dict):
            raise ValueError(_malformed(name, "DurableConfig must be a JSON object"))
        return cls(
            is_durable=True,
            execution_timeout_seconds=_positive_int(
                durable_config,
                "ExecutionTimeout",
                DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS,
                name,
            ),
            retention_period_days=_positive_int(
                durable_config,
                "RetentionPeriodInDays",
                DEFAULT_CHILD_RETENTION_PERIOD_DAYS,
                name,
            ),
        )


def _malformed(name: str | None, detail: str) -> str:
    """Message for a malformed function configuration entry."""
    subject = (
        f"Function configuration for {name!r}" if name else "Function configuration"
    )
    return f"{subject}: {detail}."


def _positive_int(
    config: dict[str, Any], field: str, default: int, name: str | None
) -> int:
    """``config[field]`` as a positive integer, or ``default`` when absent."""
    value: Any = config.get(field, default)
    # bool is an int subclass; true/false are not durations.
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        msg = _malformed(name, f"DurableConfig.{field} must be a positive integer")
        raise ValueError(msg)
    return value


FILE_URL_PREFIX: Final[str] = "file://"


@dataclass(frozen=True)
class FunctionConfigs:
    """The functions a durable function may invoke, by name.

    Each key is a function name a parent may pass to ``context.invoke``;
    each value is that function's :class:`FunctionConfig`. Built from
    the ``--function-configs`` value with :meth:`from_value`, or from
    an already parsed mapping with :meth:`from_dict`.
    """

    by_name: Mapping[str, FunctionConfig]

    @classmethod
    def from_value(cls, value: str) -> FunctionConfigs:
        """Parse the ``--function-configs`` value.

        ``value`` is a JSON object mapping function names to entries in
        the shape :meth:`FunctionConfig.from_dict` reads, or
        ``file://<path>`` naming a file that holds one. Raises
        ``ValueError`` for malformed JSON or a non-object, and ``OSError``
        for an unreadable file.
        """
        text: str = value
        if value.startswith(FILE_URL_PREFIX):
            text = pathlib.Path(value[len(FILE_URL_PREFIX) :]).read_text(
                encoding="utf-8"
            )
        raw: Any = json.loads(text)
        if not isinstance(raw, dict):
            msg = "--function-configs must be a JSON object mapping function names to configurations."
            raise ValueError(msg)
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> FunctionConfigs:
        """Build from a parsed name-to-entry mapping; a malformed entry raises ``ValueError``."""
        return cls(
            {name: FunctionConfig.from_dict(entry, name) for name, entry in raw.items()}
        )

    def resolve(self, request: ChainedInvokeRequest) -> FunctionConfig | None:
        """The configuration for ``request``'s target, if any."""
        return self.lookup(request.function_name, request.qualifier)

    def lookup(
        self, function_name: str, qualifier: str | None
    ) -> FunctionConfig | None:
        """The configuration for a target, if any.

        The entry under the qualified identifier wins; otherwise the
        bare name's entry serves every qualifier.
        """
        for key in target_lookup_keys(function_name, qualifier):
            config: FunctionConfig | None = self.by_name.get(key)
            if config is not None:
                return config
        return None


class UnconfiguredChildDispatcher:
    """Fails every chained invoke because no function configuration was given.

    The web runner cannot learn a target's durability from the endpoint,
    so without the configuration it cannot dispatch; the error names the
    option.
    """

    def preflight(self, target: FunctionTarget) -> ChildOutcome | None:
        """Every target fails: the runner cannot resolve any."""
        return self._unconfigured(target.name).outcome

    def dispatch(self, request: ChainedInvokeRequest) -> DispatchResult:
        """Fail ``request`` with a message naming the missing option."""
        return self._unconfigured(request.function_name)

    @staticmethod
    def _unconfigured(function_name: str) -> KnownOutcome:
        return failed_to_start(
            f"Cannot invoke {function_name}: the local runner has no "
            "function configurations. Start it with --function-configs "
            "mapping each function name to its configuration."
        )


class EndpointChildDispatcher:
    """Resolves chained-invoke targets against configured functions served
    by a Lambda-compatible endpoint.

    A durable target becomes a child durable execution start: this
    runner creates the child and invokes its handler at the endpoint by
    function name through the normal invocation path. A non-durable
    target runs as one RequestResponse invoke at the endpoint. An
    unknown name fails to start with ``ResourceNotFoundException``.
    """

    def __init__(
        self,
        function_configs: FunctionConfigs,
        client_provider: Callable[[str], Any],
        invocation_timeout_seconds: int,
    ) -> None:
        """``client_provider`` maps a parent execution ARN to the Lambda
        client its non-durable targets are invoked with, so a target goes
        to the endpoint the parent is invoked at."""
        self._function_configs = function_configs
        self._client_provider = client_provider
        self._invocation_timeout_seconds = invocation_timeout_seconds

    def preflight(self, target: FunctionTarget) -> ChildOutcome | None:
        """Fail a target no configuration names."""
        if self._function_configs.lookup(target.name, target.qualifier) is None:
            return function_not_found(target.name).outcome
        return None

    def dispatch(self, request: ChainedInvokeRequest) -> DispatchResult:
        """Dispatch ``request`` against the configured functions."""
        config: FunctionConfig | None = self._function_configs.resolve(request)
        if config is None:
            return function_not_found(request.function_name)

        if config.is_durable:
            return StartChild(
                child_start=StartDurableExecutionInput(
                    account_id=request.account_id,
                    function_name=request.function_name,
                    function_qualifier=request.child_qualifier(),
                    execution_name=str(uuid.uuid4()),
                    execution_timeout_seconds=config.execution_timeout_seconds,
                    execution_retention_period_days=config.retention_period_days,
                    invocation_id=None,
                    trace_fields=request.trace_fields,
                    tenant_id=request.tenant_id,
                    input=request.payload,
                    lambda_endpoint=None,
                )
            )

        client: Any = self._client_provider(request.parent_execution_arn)
        function_name: str = request.invoke_identifier()
        payload: str | None = request.payload
        tenant_id: str | None = request.tenant_id
        timeout_seconds: int = self._invocation_timeout_seconds
        return RunInvocation(
            invocation=lambda: invoke_function(
                client, function_name, payload, tenant_id, timeout_seconds
            )
        )

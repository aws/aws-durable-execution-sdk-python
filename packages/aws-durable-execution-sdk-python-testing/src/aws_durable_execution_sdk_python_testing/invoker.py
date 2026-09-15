from __future__ import annotations

import json
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

import boto3  # type: ignore
from botocore.config import Config  # type: ignore

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    DEFAULT_MAX_INVOCATION_PAGE_BYTES,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.execution import OperationPaginatorState
from aws_durable_execution_sdk_python_testing.model import (
    LambdaContext,
    executed_version,
)


if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient
    from aws_durable_execution_sdk_python_testing.execution import Execution


# Every Invoke the runner sends is one Lambda invocation: a handler
# invocation, or a chained invoke of a non-durable target. The client
# waits for it to return, so the read timeout exceeds the emulated
# function timeout (``--invocation-timeout``, default 900 s) by a fixed
# headroom for the network round-trip and RIE startup.
DEFAULT_INVOCATION_TIMEOUT_SECONDS = 900
LAMBDA_READ_TIMEOUT_HEADROOM_SECONDS = 60


def read_timeout_for(invocation_timeout_seconds: int) -> int:
    """Client read timeout that outlasts one invocation of ``invocation_timeout_seconds``."""
    return invocation_timeout_seconds + LAMBDA_READ_TIMEOUT_HEADROOM_SECONDS


DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS = read_timeout_for(
    DEFAULT_INVOCATION_TIMEOUT_SECONDS
)

# Request header on every handler invocation. To a Lambda-compatible
# endpoint, a caller's Invoke of a durable function starts a new
# execution. A handler invocation is an Invoke of that function whose
# payload is a durable invocation input; the endpoint must run the
# handler once with it and return the handler's output. This header is
# how the runner says so. Endpoints that never start executions ignore it.
INVOCATION_MARKER_HEADER = "X-Dex-Handler-Invoke"
INVOCATION_MARKER_VALUE = "true"


def _add_invocation_marker(params: dict[str, Any], **_kwargs: Any) -> None:
    """botocore ``before-call`` hook: mark the request as a handler invocation."""
    params.setdefault("headers", {})[INVOCATION_MARKER_HEADER] = INVOCATION_MARKER_VALUE


def create_lambda_client(
    endpoint_url: str | None,
    region_name: str,
    read_timeout_seconds: int = DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS,
    *,
    mark_invocations: bool = True,
) -> Any:
    """Create a boto3 Lambda client for the runner's Invoke calls.

    ``read_timeout_seconds`` bounds one Invoke. With ``mark_invocations``
    every Invoke carries :data:`INVOCATION_MARKER_HEADER`; the client
    that invokes non-durable chained targets passes ``False`` so those
    look like any caller's Invoke.
    """

    client: Any = boto3.client(
        "lambda",
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(
            read_timeout=read_timeout_seconds,
            retries={"max_attempts": 0},
        ),
    )
    if mark_invocations:
        client.meta.events.register("before-call.lambda.Invoke", _add_invocation_marker)
    return client


@dataclass(frozen=True)
class InvokeResponse:
    """Response from invoking a durable function."""

    invocation_output: DurableExecutionInvocationOutput
    request_id: str


# Values the in-process Lambda context reports when the caller gives none.
DEFAULT_TEST_REGION = "us-west-2"
DEFAULT_TEST_ACCOUNT_ID = "123456789012"
DEFAULT_TEST_FUNCTION_NAME = "test-function"


def create_test_lambda_context(
    *,
    region: str = DEFAULT_TEST_REGION,
    account_id: str = DEFAULT_TEST_ACCOUNT_ID,
    function_name: str = DEFAULT_TEST_FUNCTION_NAME,
    tenant_id: str | None = None,
) -> LambdaContext:
    """Build the Lambda context handed to an in-process handler.

    ``function_name`` is the identifier the function is invoked by:
    ``name`` or ``name:qualifier``. Lambda fills ``function_name`` with
    the bare name, ``function_version`` with the version that runs, and
    ``invoked_function_arn`` with the ARN as invoked, so target code that
    reads them sees the same values here. The runner keeps no versions:
    a numeric qualifier is reported as the version, anything else
    (no qualifier, ``$LATEST``, an alias) as ``$LATEST``.

    ``tenant_id`` is the execution's or invoke's tenant; ``None`` means
    the invocation had none, as in Lambda.
    """
    bare_name, _, qualifier = function_name.partition(":")
    function_version: str = executed_version(qualifier)
    # Create client context as a dictionary, not as objects
    # LambdaContext.__init__ expects dictionaries and will create the objects internally
    client_context_dict = {
        "custom": {"test_key": "test_value"},
        "env": {"platform": "test", "make": "test", "model": "test"},
        "client": {
            "installation_id": "test-installation-123",
            "app_title": "TestApp",
            "app_version_name": "1.0.0",
            "app_version_code": "100",
            "app_package_name": "com.test.app",
        },
    }

    cognito_identity_dict = {
        "cognitoIdentityId": "test-cognito-identity-123",
        "cognitoIdentityPoolId": "us-west-2:test-pool-456",
    }

    return LambdaContext(
        aws_request_id="test-invoke-12345",
        client_context=client_context_dict,
        identity=cognito_identity_dict,
        function_name=bare_name,
        function_version=function_version,
        invoked_function_arn=(
            f"arn:aws:lambda:{region}:{account_id}:function:{function_name}"
        ),
        tenant_id=tenant_id,
    )


class Invoker(Protocol):
    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput: ...  # pragma: no cover

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,
        tenant_id: str | None = None,
        account_id: str | None = None,
        region_name: str | None = None,
    ) -> InvokeResponse: ...  # pragma: no cover

    def update_endpoint(
        self, endpoint_url: str, region_name: str
    ) -> None: ...  # pragma: no cover

    def inherit_endpoint(
        self, child_execution_arn: str, parent_execution_arn: str
    ) -> None: ...  # pragma: no cover


class InProcessInvoker(Invoker):
    def __init__(
        self,
        handler: Callable,
        service_client: InMemoryServiceClient,
        max_page_bytes: int = DEFAULT_MAX_INVOCATION_PAGE_BYTES,
        region: str = DEFAULT_TEST_REGION,
    ):
        self.handler = handler
        self._region = region
        self.service_client = service_client
        self._max_page_bytes = max_page_bytes
        # Named handlers for chained-invoke targets. The root handler
        # remains the fallback for the execution under test.
        self._handlers: dict[str, Callable] = {}

    def register(self, function_name: str, handler: Callable) -> None:
        """Register ``handler`` to be resolved by ``function_name``."""
        self._handlers[function_name] = handler

    def _resolve_handler(self, function_name: str) -> Callable:
        """The handler for ``function_name``, which may carry a qualifier.

        A registration under the qualified identifier wins; otherwise
        the bare name's registration serves every qualifier; otherwise
        the runner's own handler.
        """
        handler: Callable | None = self._handlers.get(function_name)
        if handler is None:
            handler = self._handlers.get(function_name.split(":", 1)[0])
        return handler if handler is not None else self.handler

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        paginator = OperationPaginatorState.pin(execution)
        page_operations, next_marker = paginator.page(None, self._max_page_bytes)
        return DurableExecutionInvocationInputWithClient(
            durable_execution_arn=execution.durable_execution_arn,
            # TODO: this needs better logic - use existing if not used yet, vs create new
            checkpoint_token=execution.get_new_checkpoint_token(),
            initial_execution_state=InitialExecutionState(
                operations=page_operations,
                next_marker=next_marker or "",
            ),
            updated_operation_ids=list(execution.updated_operation_ids),
            service_client=self.service_client,
        )

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,  # noqa: ARG002
        tenant_id: str | None = None,
        account_id: str | None = None,
        region_name: str | None = None,  # noqa: ARG002 — the context reports the runner's
    ) -> InvokeResponse:
        input_with_client = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
            input, self.service_client
        )
        context = create_test_lambda_context(
            region=self._region,
            account_id=account_id or DEFAULT_TEST_ACCOUNT_ID,
            function_name=function_name,
            tenant_id=tenant_id,
        )
        handler: Callable = self._resolve_handler(function_name)
        response_dict = handler(input_with_client, context)
        output = DurableExecutionInvocationOutput.from_dict(response_dict)
        return InvokeResponse(
            invocation_output=output, request_id=context.aws_request_id
        )

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """No-op for in-process invoker."""

    def inherit_endpoint(
        self, child_execution_arn: str, parent_execution_arn: str
    ) -> None:
        """No-op for in-process invoker: there is no endpoint to inherit."""


class LambdaInvoker(Invoker):
    def __init__(
        self,
        lambda_client: Any,
        max_page_bytes: int = DEFAULT_MAX_INVOCATION_PAGE_BYTES,
        read_timeout_seconds: int = DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS,
        endpoint_url: str = "",
        region_name: str = "",
    ) -> None:
        """``endpoint_url`` and ``region_name`` describe ``lambda_client``."""
        self.lambda_client = lambda_client
        self._max_page_bytes = max_page_bytes
        # Applied to every client this invoker creates for another endpoint.
        self._read_timeout_seconds = read_timeout_seconds
        # Clients are keyed by (endpoint URL, region): the region is the
        # signing region, so the same URL in another region is another
        # client. Marked clients invoke handlers; unmarked clients invoke
        # non-durable chained targets.
        self._endpoint_clients: dict[tuple[str, str], Any] = {}
        self._unmarked_clients: dict[tuple[str, str], Any] = {}
        # An execution without its own endpoint is pinned, at its first
        # invocation, to the endpoint current at that moment. Every later
        # handler invocation and every chained target it dispatches use
        # the pinned endpoint, so an update_endpoint call while it runs
        # does not split it across endpoints. A child it starts inherits
        # the pin (inherit_endpoint). An execution with its own endpoint
        # is pinned at its first chained dispatch instead: the pin then
        # covers only its chained targets and children, and its own
        # endpoint is signed in the execution's region.
        self._execution_endpoints: dict[str, tuple[str, str]] = {}
        # Endpoint and region for executions not yet pinned.
        self._current: tuple[str, str] = (endpoint_url, region_name)
        if endpoint_url:
            self._endpoint_clients[self._current] = lambda_client
        self._lock = Lock()

    @staticmethod
    def create(
        endpoint_url: str,
        region_name: str,
        max_page_bytes: int = DEFAULT_MAX_INVOCATION_PAGE_BYTES,
        read_timeout_seconds: int = DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS,
    ) -> LambdaInvoker:
        """Create with the boto lambda client."""
        return LambdaInvoker(
            create_lambda_client(endpoint_url, region_name, read_timeout_seconds),
            max_page_bytes=max_page_bytes,
            read_timeout_seconds=read_timeout_seconds,
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """Update the Lambda endpoint and region for executions not yet pinned."""
        key: tuple[str, str] = (endpoint_url, region_name)
        with self._lock:
            self.lambda_client = self._marked_client(key)
            self._current = key

    def _marked_client(self, key: tuple[str, str]) -> Any:
        """Client that marks its invokes as handler invocations. Caller holds the lock."""
        client: Any = self._endpoint_clients.get(key)
        if client is None:
            client = create_lambda_client(
                key[0] or None, key[1], self._read_timeout_seconds
            )
            self._endpoint_clients[key] = client
        return client

    def _pinned_endpoint(self, durable_execution_arn: str) -> tuple[str, str]:
        """The (endpoint, region) ``durable_execution_arn`` is pinned to, pinning it now if needed."""
        with self._lock:
            pinned: tuple[str, str] | None = self._execution_endpoints.get(
                durable_execution_arn
            )
            if pinned is None:
                pinned = self._current
                self._execution_endpoints[durable_execution_arn] = pinned
            return pinned

    def inherit_endpoint(
        self, child_execution_arn: str, parent_execution_arn: str
    ) -> None:
        """Pin a chained child to the endpoint its parent's chained targets go to.

        The parent is pinned now if it has no pin yet. So the parent's
        plain targets, the child's handler invocations, and the child's
        own chained dispatches all use one endpoint and region, whatever
        update_endpoint sets for executions started later.
        """
        pinned: tuple[str, str] = self._pinned_endpoint(parent_execution_arn)
        with self._lock:
            self._execution_endpoints[child_execution_arn] = pinned

    def unmarked_client_for(self, durable_execution_arn: str) -> Any:
        """Client for invoking the non-durable chained targets of one execution.

        The client targets the endpoint and region the execution is
        pinned to, so a target goes where the execution's own handler
        invocations go. It carries no invocation marker, so the endpoint
        runs the target as any caller's Invoke.
        """
        key: tuple[str, str] = self._pinned_endpoint(durable_execution_arn)
        with self._lock:
            client: Any = self._unmarked_clients.get(key)
            if client is None:
                client = create_lambda_client(
                    key[0] or None,
                    key[1],
                    self._read_timeout_seconds,
                    mark_invocations=False,
                )
                self._unmarked_clients[key] = client
            return client

    def _get_client_for_execution(
        self,
        durable_execution_arn: str,
        lambda_endpoint: str | None = None,
        region_name: str | None = None,
    ) -> Any:
        """Get the appropriate client for this execution.

        An execution with its own ``lambda_endpoint`` is invoked there,
        signed in ``region_name``, the execution's region. That endpoint
        serves the execution's function alone (under sam it is the
        function's own container), so it is never recorded as the
        execution's pin: the execution's chained targets are other
        functions and go to the pinned endpoint, which routes by name.
        Any other execution is invoked at the endpoint it is pinned to.
        """
        if lambda_endpoint:
            with self._lock:
                return self._marked_client(
                    (lambda_endpoint, region_name or "us-east-1")
                )

        key: tuple[str, str] = self._pinned_endpoint(durable_execution_arn)
        if not key[0]:
            # Built with a client and no endpoint: nothing else to pick.
            return self.lambda_client
        with self._lock:
            return self._marked_client(key)

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        paginator = OperationPaginatorState.pin(execution)
        page_operations, next_marker = paginator.page(None, self._max_page_bytes)
        return DurableExecutionInvocationInput(
            durable_execution_arn=execution.durable_execution_arn,
            checkpoint_token=execution.get_new_checkpoint_token(),
            initial_execution_state=InitialExecutionState(
                operations=page_operations,
                next_marker=next_marker or "",
            ),
            updated_operation_ids=list(execution.updated_operation_ids),
        )

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,
        tenant_id: str | None = None,
        account_id: str | None = None,  # noqa: ARG002 — identity is the endpoint's
        region_name: str | None = None,
    ) -> InvokeResponse:
        """Invoke AWS Lambda function and return durable execution result.

        Args:
            function_name: Name of the Lambda function to invoke
            input: Durable execution invocation input
            endpoint_url: The execution's own Lambda endpoint, if it has one
            tenant_id: The execution's tenant, sent as the Invoke TenantId
            account_id: The execution's account; unused, the endpoint owns identity
            region_name: The execution's region; signs an Invoke at ``endpoint_url``

        Returns:
            InvokeResponse: Response containing invocation output and request ID

        Raises:
            ResourceNotFoundException: If function does not exist
            InvalidParameterValueException: If parameters are invalid
            DurableFunctionsTestError: For other invocation failures
        """

        # Parameter validation
        if not function_name or not function_name.strip():
            msg = "Function name is required"
            raise InvalidParameterValueException(msg)

        # Get the client for this execution
        client = self._get_client_for_execution(
            input.durable_execution_arn, endpoint_url, region_name
        )

        invoke_kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "InvocationType": "RequestResponse",  # Synchronous invocation
            "Payload": json.dumps(input.to_json_dict()),
        }
        if tenant_id is not None:
            invoke_kwargs["TenantId"] = tenant_id

        try:
            # Invoke AWS Lambda function using standard invoke method
            response = client.invoke(**invoke_kwargs)

            # Check HTTP status code
            status_code = response.get("StatusCode")
            if status_code not in (200, 202, 204):
                msg = f"Lambda invocation failed with status code: {status_code}"
                raise DurableFunctionsTestError(msg)

            # Check for function errors
            if "FunctionError" in response:
                error_payload = response["Payload"].read().decode("utf-8")
                msg = f"Lambda invocation failed with status {status_code}: {error_payload}"
                raise DurableFunctionsTestError(msg)

            # Parse response payload
            response_payload = response["Payload"].read().decode("utf-8")
            response_dict = json.loads(response_payload)

            # Extract request ID from response headers (x-amzn-RequestId or x-amzn-request-id)
            headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
            request_id = (
                headers.get("x-amzn-RequestId")
                or headers.get("x-amzn-request-id")
                or f"local-{uuid4()}"
            )

            # Convert to DurableExecutionInvocationOutput
            output = DurableExecutionInvocationOutput.from_dict(response_dict)
            return InvokeResponse(invocation_output=output, request_id=request_id)

        except client.exceptions.ResourceNotFoundException as e:
            msg = f"Function not found: {function_name}"
            raise ResourceNotFoundException(msg) from e
        except client.exceptions.InvalidParameterValueException as e:
            msg = f"Invalid parameter: {e}"
            raise InvalidParameterValueException(msg) from e
        except (
            client.exceptions.TooManyRequestsException,
            client.exceptions.ServiceException,
            client.exceptions.ResourceConflictException,
            client.exceptions.InvalidRequestContentException,
            client.exceptions.RequestTooLargeException,
            client.exceptions.UnsupportedMediaTypeException,
            client.exceptions.InvalidRuntimeException,
            client.exceptions.InvalidZipFileException,
            client.exceptions.ResourceNotReadyException,
            client.exceptions.SnapStartTimeoutException,
            client.exceptions.SnapStartNotReadyException,
            client.exceptions.SnapStartException,
            client.exceptions.RecursiveInvocationException,
        ) as e:
            msg = f"Lambda invocation failed: {e}"
            raise DurableFunctionsTestError(msg) from e
        except (
            client.exceptions.InvalidSecurityGroupIDException,
            client.exceptions.EC2ThrottledException,
            client.exceptions.EFSMountConnectivityException,
            client.exceptions.SubnetIPAddressLimitReachedException,
            client.exceptions.EC2UnexpectedException,
            client.exceptions.InvalidSubnetIDException,
            client.exceptions.EC2AccessDeniedException,
            client.exceptions.EFSIOException,
            client.exceptions.ENILimitReachedException,
            client.exceptions.EFSMountTimeoutException,
            client.exceptions.EFSMountFailureException,
        ) as e:
            msg = f"Lambda infrastructure error: {e}"
            raise DurableFunctionsTestError(msg) from e
        except (
            client.exceptions.KMSAccessDeniedException,
            client.exceptions.KMSDisabledException,
            client.exceptions.KMSNotFoundException,
            client.exceptions.KMSInvalidStateException,
        ) as e:
            msg = f"Lambda KMS error: {e}"
            raise DurableFunctionsTestError(msg) from e
        except Exception as e:
            # Handle any remaining exceptions, including custom ones like DurableExecutionAlreadyStartedException
            if "DurableExecutionAlreadyStartedException" in str(type(e)):
                msg = f"Durable execution already started: {e}"
                raise DurableFunctionsTestError(msg) from e
            msg = f"Unexpected error during Lambda invocation: {e}"
            raise DurableFunctionsTestError(msg) from e

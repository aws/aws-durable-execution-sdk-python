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

from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsTestError,
    InvalidParameterValueException,
    ResourceNotFoundException,
)
from aws_durable_execution_sdk_python_testing.model import LambdaContext


if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python_testing.client import InMemoryServiceClient
    from aws_durable_execution_sdk_python_testing.execution import Execution


# Max Lambda function timeout is 15 minutes (900s); we give headroom for
# network round-trip and RIE startup.
_LAMBDA_READ_TIMEOUT_SECONDS = 960
_LAMBDA_CLIENT_CONFIG = Config(
    read_timeout=_LAMBDA_READ_TIMEOUT_SECONDS,
    retries={"max_attempts": 0},
)


def create_lambda_client(endpoint_url: str | None, region_name: str) -> Any:
    """Create a boto3 Lambda client configured for durable function invocations."""

    return boto3.client(
        "lambda",
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=_LAMBDA_CLIENT_CONFIG,
    )


@dataclass(frozen=True)
class InvokeResponse:
    """Response from invoking a durable function."""

    invocation_output: DurableExecutionInvocationOutput
    request_id: str


def create_test_lambda_context() -> LambdaContext:
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
        invoked_function_arn="arn:aws:lambda:us-west-2:123456789012:function:test-function",
        tenant_id="test-tenant-789",
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
    ) -> InvokeResponse: ...  # pragma: no cover

    def update_endpoint(
        self, endpoint_url: str, region_name: str
    ) -> None: ...  # pragma: no cover


class InProcessInvoker(Invoker):
    def __init__(self, handler: Callable, service_client: InMemoryServiceClient):
        self.handler = handler
        self.service_client = service_client

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInputWithClient(
            durable_execution_arn=execution.durable_execution_arn,
            # TODO: this needs better logic - use existing if not used yet, vs create new
            checkpoint_token=execution.get_new_checkpoint_token(),
            initial_execution_state=InitialExecutionState(
                operations=execution.operations,
                next_marker="",
            ),
            service_client=self.service_client,
        )

    def invoke(
        self,
        function_name: str,  # noqa: ARG002
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,  # noqa: ARG002
    ) -> InvokeResponse:
        # TODO: reasses if function_name will be used in future
        input_with_client = DurableExecutionInvocationInputWithClient.from_durable_execution_invocation_input(
            input, self.service_client
        )
        context = create_test_lambda_context()
        response_dict = self.handler(input_with_client, context)
        output = DurableExecutionInvocationOutput.from_dict(response_dict)
        return InvokeResponse(
            invocation_output=output, request_id=context.aws_request_id
        )

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """No-op for in-process invoker."""


class LambdaInvoker(Invoker):
    def __init__(self, lambda_client: Any) -> None:
        self.lambda_client = lambda_client
        # Maps execution_arn -> endpoint for that execution
        # Maps endpoint -> client to reuse clients across executions
        self._execution_endpoints: dict[str, str] = {}
        self._endpoint_clients: dict[str, Any] = {}
        self._current_endpoint: str = ""  # Track current endpoint for new executions
        self._lock = Lock()

    @staticmethod
    def create(endpoint_url: str, region_name: str) -> LambdaInvoker:
        """Create with the boto lambda client."""
        invoker = LambdaInvoker(create_lambda_client(endpoint_url, region_name))
        invoker._current_endpoint = endpoint_url
        invoker._endpoint_clients[endpoint_url] = invoker.lambda_client
        return invoker

    def update_endpoint(self, endpoint_url: str, region_name: str) -> None:
        """Update the Lambda client endpoint."""
        # Cache client by endpoint to reuse across executions
        with self._lock:
            if endpoint_url not in self._endpoint_clients:
                self._endpoint_clients[endpoint_url] = create_lambda_client(
                    endpoint_url, region_name
                )
            self.lambda_client = self._endpoint_clients[endpoint_url]
        self._current_endpoint = endpoint_url

    def _get_client_for_execution(
        self,
        durable_execution_arn: str,
        lambda_endpoint: str | None = None,
        region_name: str | None = None,
    ) -> Any:
        """Get the appropriate client for this execution."""
        # Use provided endpoint or fall back to cached endpoint for this execution
        if lambda_endpoint:
            if lambda_endpoint not in self._endpoint_clients:
                self._endpoint_clients[lambda_endpoint] = create_lambda_client(
                    lambda_endpoint, region_name or "us-east-1"
                )
            return self._endpoint_clients[lambda_endpoint]

        # Fallback to cached endpoint
        if durable_execution_arn not in self._execution_endpoints:
            with self._lock:
                if durable_execution_arn not in self._execution_endpoints:
                    self._execution_endpoints[durable_execution_arn] = (
                        self._current_endpoint
                    )

        endpoint = self._execution_endpoints[durable_execution_arn]

        # If no endpoint configured, fall back to default client
        if not endpoint:
            return self.lambda_client

        return self._endpoint_clients[endpoint]

    def create_invocation_input(
        self, execution: Execution
    ) -> DurableExecutionInvocationInput:
        return DurableExecutionInvocationInput(
            durable_execution_arn=execution.durable_execution_arn,
            checkpoint_token=execution.get_new_checkpoint_token(),
            initial_execution_state=InitialExecutionState(
                operations=execution.operations,
                next_marker="",
            ),
        )

    def invoke(
        self,
        function_name: str,
        input: DurableExecutionInvocationInput,
        endpoint_url: str | None = None,
    ) -> InvokeResponse:
        """Invoke AWS Lambda function and return durable execution result.

        Args:
            function_name: Name of the Lambda function to invoke
            input: Durable execution invocation input
            endpoint_url: Lambda endpoint url

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
            input.durable_execution_arn, endpoint_url
        )

        try:
            # Invoke AWS Lambda function using standard invoke method
            response = client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",  # Synchronous invocation
                Payload=json.dumps(input.to_json_dict()),
            )

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

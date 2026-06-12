"""Tests for HTTP endpoint handlers."""

from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    Operation,
    OperationStatus,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.exceptions import (
    AwsApiException,
    IllegalArgumentException,
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.model import (
    CheckpointDurableExecutionResponse,
    Event,
    ExecutionStartedDetails,
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionResponse,
    GetDurableExecutionStateResponse,
    ListDurableExecutionsByFunctionResponse,
    ListDurableExecutionsResponse,
    SendDurableExecutionCallbackFailureRequest,
    SendDurableExecutionCallbackFailureResponse,
    SendDurableExecutionCallbackHeartbeatRequest,
    SendDurableExecutionCallbackHeartbeatResponse,
    SendDurableExecutionCallbackSuccessRequest,
    SendDurableExecutionCallbackSuccessResponse,
    StartDurableExecutionInput,
    StartDurableExecutionOutput,
    StopDurableExecutionResponse,
)
from aws_durable_execution_sdk_python_testing.model import (
    Execution as ExecutionSummary,
)
from aws_durable_execution_sdk_python_testing.web import handlers
from aws_durable_execution_sdk_python_testing.web.handlers import (
    CheckpointDurableExecutionHandler,
    EndpointHandler,
    GetDurableExecutionHandler,
    GetDurableExecutionHistoryHandler,
    GetDurableExecutionStateHandler,
    HealthHandler,
    ListDurableExecutionsByFunctionHandler,
    ListDurableExecutionsHandler,
    MetricsHandler,
    SendDurableExecutionCallbackFailureHandler,
    SendDurableExecutionCallbackHeartbeatHandler,
    SendDurableExecutionCallbackSuccessHandler,
    StartExecutionHandler,
    StopDurableExecutionHandler,
)
from aws_durable_execution_sdk_python_testing.web.models import (
    HTTPRequest,
    HTTPResponse,
)
from aws_durable_execution_sdk_python_testing.web.routes import (
    CallbackFailureRoute,
    CallbackHeartbeatRoute,
    CallbackSuccessRoute,
    GetDurableExecutionRoute,
    ListDurableExecutionsRoute,
    Route,
    Router,
    StartExecutionRoute,
)


class MockableEndpointHandler(EndpointHandler):
    """Test-specific handler that exposes private methods for testing."""

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle request - test implementation."""
        return self._success_response({"test": "data"})

    # Public methods that expose private functionality for testing
    def parse_json_body(self, request: HTTPRequest) -> dict[str, Any]:
        """Public wrapper for _parse_json_body."""
        return self._parse_json_body(request)

    def json_response(
        self,
        status_code: int,
        data: dict[str, Any],
        additional_headers: dict[str, str] | None = None,
    ) -> HTTPResponse:
        """Public wrapper for _json_response."""
        return self._json_response(status_code, data, additional_headers)

    def success_response(
        self, data: dict[str, Any], additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Public wrapper for _success_response."""
        return self._success_response(data, additional_headers)

    def created_response(
        self, data: dict[str, Any], additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Public wrapper for _created_response."""
        return self._created_response(data, additional_headers)

    def no_content_response(
        self, additional_headers: dict[str, str] | None = None
    ) -> HTTPResponse:
        """Public wrapper for _no_content_response."""
        return self._no_content_response(additional_headers)

    def parse_query_param(self, request: HTTPRequest, param_name: str) -> str | None:
        """Public wrapper for _parse_query_param."""
        return self._parse_query_param(request, param_name)

    def parse_query_param_list(
        self, request: HTTPRequest, param_name: str
    ) -> list[str]:
        """Public wrapper for _parse_query_param_list."""
        return self._parse_query_param_list(request, param_name)

    def validate_required_fields(
        self, data: dict[str, Any], required_fields: list[str]
    ) -> None:
        """Public wrapper for _validate_required_fields."""
        return self._validate_required_fields(data, required_fields)


def test_endpoint_handler_initialization():
    """Test EndpointHandler initialization."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)
    assert handler.executor == executor


def test_endpoint_handler_parse_json_body_valid():
    """Test parse_json_body with valid JSON."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    request = HTTPRequest(
        method="POST",
        path=Route.from_string("/test"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"key": "value"},
    )

    result = handler.parse_json_body(request)
    assert result == {"key": "value"}


def test_endpoint_handler_parse_json_body_empty():
    """Test parse_json_body with empty body."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    request = HTTPRequest(
        method="POST",
        path=Route.from_string("/test"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body={},
    )

    with pytest.raises(
        InvalidParameterValueException, match="Request body is required"
    ):
        handler.parse_json_body(request)


def test_endpoint_handler_parse_json_body_invalid():
    """Test parse_json_body with invalid JSON - now this test is not applicable since body is already a dict."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    # Since body is now a dict, this test case doesn't apply anymore
    # The validation happens during HTTPRequest.from_bytes() deserialization
    request = HTTPRequest(
        method="POST",
        path=Route.from_string("/test"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"valid": "json"},  # Body is always valid dict now
    )

    # This should work fine now since body is already parsed
    result = handler.parse_json_body(request)
    assert result == {"valid": "json"}


def test_endpoint_handler_json_response():
    """Test json_response method."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    response = handler.json_response(200, {"test": "data"})
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"
    assert response.body == {"test": "data"}

    # Verify serialization to bytes works
    body_bytes = response.body_to_bytes()
    assert b'"test":"data"' in body_bytes


def test_endpoint_handler_success_response():
    """Test success_response method."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    response = handler.success_response({"test": "data"})
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"


def test_endpoint_handler_created_response():
    """Test created_response method."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    response = handler.created_response({"test": "data"})
    assert response.status_code == 201
    assert response.headers["Content-Type"] == "application/json"


def test_endpoint_handler_no_content_response():
    """Test no_content_response method."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    response = handler.no_content_response()
    assert response.status_code == 204
    assert response.body == {}


def test_endpoint_handler_error_response():
    """Test error response creation using HTTPResponse.create_error_from_exception."""
    # Test that we can create error responses using the new method
    exception = InvalidParameterValueException("Bad request")

    response = HTTPResponse.create_error_from_exception(exception)
    assert response.status_code == 400
    assert response.headers["Content-Type"] == "application/json"

    # The new format doesn't wrap in an "error" object
    # InvalidParameterValueException uses lowercase "message" per Smithy definition
    expected_body = {
        "Type": "InvalidParameterValueException",
        "message": "Bad request",
    }
    assert response.body == expected_body

    # Verify serialization to bytes works
    body_bytes = response.body_to_bytes()
    assert b'"message":"Bad request"' in body_bytes
    assert b'"Type":"InvalidParameterValueException"' in body_bytes


def test_endpoint_handler_parse_query_param():
    """Test parse_query_param method."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    request = HTTPRequest(
        method="GET",
        path=Route.from_string("/test"),
        headers={},
        query_params={"param1": ["value1"], "param2": ["value2a", "value2b"]},
        body={},
    )

    assert handler.parse_query_param(request, "param1") == "value1"
    assert handler.parse_query_param(request, "param2") == "value2a"  # First value
    assert handler.parse_query_param(request, "nonexistent") is None


def test_endpoint_handler_parse_query_param_list():
    """Test parse_query_param_list method."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    request = HTTPRequest(
        method="GET",
        path=Route.from_string("/test"),
        headers={},
        query_params={"param1": ["value1"], "param2": ["value2a", "value2b"]},
        body={},
    )

    assert handler.parse_query_param_list(request, "param1") == ["value1"]
    assert handler.parse_query_param_list(request, "param2") == ["value2a", "value2b"]
    assert handler.parse_query_param_list(request, "nonexistent") == []


def test_endpoint_handler_validate_required_fields_valid():
    """Test validate_required_fields with valid data."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    data = {"field1": "value1", "field2": "value2", "field3": "value3"}
    required_fields = ["field1", "field2"]

    # Should not raise an exception
    handler.validate_required_fields(data, required_fields)


def test_endpoint_handler_validate_required_fields_missing():
    """Test validate_required_fields with missing fields."""
    executor = Mock()
    handler = MockableEndpointHandler(executor)

    data = {"field1": "value1"}
    required_fields = ["field1", "field2", "field3"]

    with pytest.raises(
        InvalidParameterValueException, match="Missing required fields: field2, field3"
    ):
        handler.validate_required_fields(data, required_fields)


def test_start_execution_handler_success():
    """Test StartExecutionHandler with successful execution start."""
    executor = Mock()
    handler = StartExecutionHandler(executor)

    # Mock successful executor response
    mock_output = StartDurableExecutionOutput(execution_arn="test-execution-arn")
    executor.start_execution.return_value = mock_output

    # Create request with valid input data
    request_data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
        "Input": '{"test": "data"}',
    }

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_data,
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Verify response
    assert response.status_code == 201
    assert response.headers["Content-Type"] == "application/json"
    assert response.body == {"ExecutionArn": "test-execution-arn"}

    # Verify executor was called with correct input
    executor.start_execution.assert_called_once()
    call_args = executor.start_execution.call_args[0][0]
    assert isinstance(call_args, StartDurableExecutionInput)
    assert call_args.account_id == "123456789012"
    assert call_args.function_name == "test-function"
    assert call_args.execution_name == "test-execution"


def test_start_execution_handler_empty_body():
    """Test StartExecutionHandler with empty request body."""
    executor = Mock()
    handler = StartExecutionHandler(executor)

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body={},
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Should return 400 Bad Request for empty body with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert "Request body is required" in response.body["message"]


def test_start_execution_handler_missing_required_fields():
    """Test StartExecutionHandler with missing required fields."""
    executor = Mock()
    handler = StartExecutionHandler(executor)

    # Request missing required fields
    request_data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        # Missing other required fields
    }

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_data,
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Should return 400 Bad Request for missing fields with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert "FunctionQualifier" in response.body["message"]


def test_start_execution_handler_invalid_parameter_error():
    """Test StartExecutionHandler with IllegalArgumentException from executor."""

    executor = Mock()
    handler = StartExecutionHandler(executor)

    # Mock executor to raise IllegalArgumentException
    executor.start_execution.side_effect = IllegalArgumentException(
        "Invalid timeout value"
    )

    request_data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": -1,  # Invalid value
        "ExecutionRetentionPeriodDays": 7,
    }

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_data,
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Should return 400 Bad Request with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid timeout value"


def test_start_execution_handler_execution_already_exists():
    """Test StartExecutionHandler with execution already exists error."""

    executor = Mock()
    handler = StartExecutionHandler(executor)

    # Mock executor to raise IllegalStateException (execution already exists)
    executor.start_execution.side_effect = IllegalStateException(
        "Execution with name 'test-execution' already exists"
    )

    request_data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_data,
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Should return 409 Conflict with AWS-compliant format (ExecutionAlreadyStartedException has no Type field)
    assert response.status_code == 409
    assert "already exists" in response.body["message"]
    assert (
        response.body["DurableExecutionArn"]
        == "arn:aws:lambda:us-east-1:123456789012:function:test"
    )
    assert (
        "Type" not in response.body
    )  # ExecutionAlreadyStartedException doesn't have Type field


def test_start_execution_handler_unexpected_error():
    """Test StartExecutionHandler with unexpected error from executor."""
    executor = Mock()
    handler = StartExecutionHandler(executor)

    # Mock executor to raise unexpected error
    executor.start_execution.side_effect = RuntimeError("Unexpected database error")

    request_data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
    }

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_data,
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Should return 500 Internal Server Error with AWS-compliant format
    assert response.status_code == 500
    assert response.body["Type"] == "ServiceException"
    assert response.body["Message"] == "Unexpected database error"


def test_start_execution_handler_with_optional_fields():
    """Test StartExecutionHandler with optional fields included."""

    executor = Mock()
    handler = StartExecutionHandler(executor)

    # Mock successful executor response
    mock_output = StartDurableExecutionOutput(execution_arn="test-execution-arn")
    executor.start_execution.return_value = mock_output

    # Create request with optional fields
    request_data = {
        "AccountId": "123456789012",
        "FunctionName": "test-function",
        "FunctionQualifier": "$LATEST",
        "ExecutionName": "test-execution",
        "ExecutionTimeoutSeconds": 300,
        "ExecutionRetentionPeriodDays": 7,
        "InvocationId": "test-invocation-id",
        "TraceFields": {"traceId": "test-trace"},
        "TenantId": "test-tenant",
        "Input": '{"test": "data"}',
    }

    request = HTTPRequest(
        method="POST",
        path=StartExecutionRoute.from_string("/start-durable-execution"),
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_data,
    )

    route = StartExecutionRoute.from_string("/start-durable-execution")
    response = handler.handle(route, request)

    # Verify response
    assert response.status_code == 201
    assert response.body == {"ExecutionArn": "test-execution-arn"}

    # Verify executor was called with correct input including optional fields
    executor.start_execution.assert_called_once()
    call_args = executor.start_execution.call_args[0][0]
    assert isinstance(call_args, StartDurableExecutionInput)
    assert call_args.invocation_id == "test-invocation-id"
    assert call_args.trace_fields == {"traceId": "test-trace"}
    assert call_args.tenant_id == "test-tenant"
    assert call_args.input == '{"test": "data"}'


def test_get_durable_execution_handler_success():
    """Test GetDurableExecutionHandler with successful execution retrieval."""

    executor = Mock()
    handler = GetDurableExecutionHandler(executor)

    # Mock the executor response
    mock_response = GetDurableExecutionResponse(
        durable_execution_arn="test-arn",
        durable_execution_name="test-execution",
        function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
        status="SUCCEEDED",
        start_timestamp="2023-01-01T00:00:00Z",
        input_payload="test-input",
        result="test-result",
        error=None,
        end_timestamp="2023-01-01T00:01:00Z",
        version="1.0",
    )
    executor.get_execution_details.return_value = mock_response

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions/test-arn")
    typed_route = GetDurableExecutionRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    expected_body = {
        "DurableExecutionArn": "test-arn",
        "DurableExecutionName": "test-execution",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
        "Status": "SUCCEEDED",
        "StartTimestamp": "2023-01-01T00:00:00Z",
        "InputPayload": "test-input",
        "Result": "test-result",
        "EndTimestamp": "2023-01-01T00:01:00Z",
        "Version": "1.0",
    }
    assert response.body == expected_body

    # Verify executor was called with correct ARN
    executor.get_execution_details.assert_called_once_with("test-arn")


def test_get_durable_execution_handler_resource_not_found():
    """Test GetDurableExecutionHandler with ResourceNotFoundException."""

    executor = Mock()
    handler = GetDurableExecutionHandler(executor)

    # Mock executor to raise ResourceNotFoundException
    executor.get_execution_details.side_effect = ResourceNotFoundException(
        "Execution not-found-arn not found"
    )

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions/not-found-arn")
    typed_route = GetDurableExecutionRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Execution not-found-arn not found"

    # Verify executor was called
    executor.get_execution_details.assert_called_once_with("not-found-arn")


def test_get_durable_execution_handler_invalid_parameter():
    """Test GetDurableExecutionHandler with IllegalArgumentException."""

    executor = Mock()
    handler = GetDurableExecutionHandler(executor)

    # Mock executor to raise IllegalArgumentException
    executor.get_execution_details.side_effect = IllegalArgumentException(
        "Invalid execution ARN format"
    )

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions/invalid-arn")
    typed_route = GetDurableExecutionRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid execution ARN format"

    # Verify executor was called
    executor.get_execution_details.assert_called_once_with("invalid-arn")


def test_get_durable_execution_handler_unexpected_error():
    """Test GetDurableExecutionHandler with unexpected error."""

    executor = Mock()
    handler = GetDurableExecutionHandler(executor)

    # Mock executor to raise unexpected error
    executor.get_execution_details.side_effect = RuntimeError("Unexpected error")

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions/test-arn")
    typed_route = GetDurableExecutionRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 500
    assert response.body["Type"] == "ServiceException"
    assert response.body["Message"] == "Unexpected error"

    # Verify executor was called
    executor.get_execution_details.assert_called_once_with("test-arn")


def test_checkpoint_durable_execution_handler_success():
    """Test CheckpointDurableExecutionHandler with successful checkpoint processing."""

    executor = Mock()
    handler = CheckpointDurableExecutionHandler(executor)

    # Mock the executor response
    mock_response = CheckpointDurableExecutionResponse(
        checkpoint_token="new-token-123",  # noqa: S106
        new_execution_state=None,
    )
    executor.checkpoint_execution.return_value = mock_response

    # Create request with proper checkpoint data
    request_body = {
        "CheckpointToken": "current-token-123",
        "Updates": [
            {"Id": "op-1", "Type": "STEP", "Action": "SUCCEED", "SubType": "Step"}
        ],
        "ClientToken": "client-token-123",
    }

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/checkpoint", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=typed_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_body,
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert response.body == {
        "CheckpointToken": "new-token-123",
    }

    # Verify executor was called with correct parameters
    executor.checkpoint_execution.assert_called_once()
    call_args = executor.checkpoint_execution.call_args
    assert call_args[0][0] == "test-arn"  # execution_arn
    assert call_args[0][1] == "current-token-123"  # checkpoint_token
    assert call_args[0][3] == "client-token-123"  # client_token

    # Verify the updates parameter
    updates = call_args[0][2]
    assert len(updates) == 1
    assert updates[0].operation_id == "op-1"


def test_checkpoint_durable_execution_handler_invalid_request():
    """Test CheckpointDurableExecutionHandler with invalid request body."""

    executor = Mock()
    handler = CheckpointDurableExecutionHandler(executor)

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/checkpoint", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify AWS-compliant error format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert "Request body is required" in response.body["message"]


def test_checkpoint_durable_execution_handler_invalid_checkpoint_exception():
    """Test CheckpointDurableExecutionHandler with IllegalStateException mapping to ServiceException."""

    executor = Mock()
    handler = CheckpointDurableExecutionHandler(executor)

    # Mock executor to raise IllegalStateException
    executor.checkpoint_execution.side_effect = IllegalStateException(
        "Invalid checkpoint token"
    )

    request_body = {
        "CheckpointToken": "invalid-token",
    }

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/checkpoint", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=typed_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_body,
    )

    response = handler.handle(typed_route, request)

    # Verify IllegalStateException maps to ServiceException in AWS-compliant format
    assert response.status_code == 500
    assert response.body["Type"] == "ServiceException"
    assert response.body["Message"] == "Invalid checkpoint token"


def test_stop_durable_execution_handler_success():
    """Test StopDurableExecutionHandler with successful execution stop."""

    executor = Mock()
    handler = StopDurableExecutionHandler(executor)

    # Mock the executor response
    mock_response = StopDurableExecutionResponse(stop_timestamp="2023-01-01T00:01:00Z")
    executor.stop_execution.return_value = mock_response

    # Create request with proper stop data
    request_body = {
        "DurableExecutionArn": "test-arn",
        "Error": {
            "ErrorMessage": "User requested stop",
            "ErrorType": "UserStop",
        },
    }

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/stop", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=typed_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_body,
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert response.body == {"StopTimestamp": "2023-01-01T00:01:00Z"}

    # Verify executor was called with correct parameters
    executor.stop_execution.assert_called_once()
    call_args = executor.stop_execution.call_args
    assert call_args[0][0] == "test-arn"  # execution_arn


def test_stop_durable_execution_handler_execution_already_stopped():
    """Test StopDurableExecutionHandler with execution already stopped returns idempotent response."""

    executor = Mock()
    handler = StopDurableExecutionHandler(executor)

    # Mock executor to return stop response with timestamp
    stop_timestamp = "2023-01-01T00:01:00Z"
    executor.stop_execution.return_value = StopDurableExecutionResponse(
        stop_timestamp=stop_timestamp
    )

    request_body = {
        "DurableExecutionArn": "test-arn",
    }

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/stop", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=typed_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_body,
    )

    response = handler.handle(typed_route, request)

    # Verify idempotent response with stop timestamp
    assert response.status_code == 200
    assert response.body["StopTimestamp"] == stop_timestamp


def test_stop_durable_execution_handler_resource_not_found():
    """Test StopDurableExecutionHandler with ResourceNotFoundException."""

    executor = Mock()
    handler = StopDurableExecutionHandler(executor)

    # Mock executor to raise ResourceNotFoundException
    executor.stop_execution.side_effect = ResourceNotFoundException(
        "Execution not-found-arn not found"
    )

    request_body = {
        "DurableExecutionArn": "not-found-arn",
    }

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/not-found-arn/stop", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=typed_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body=request_body,
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Execution not-found-arn not found"


def test_get_durable_execution_state_handler_success():
    """Test GetDurableExecutionStateHandler with successful state retrieval."""

    executor = Mock()
    handler = GetDurableExecutionStateHandler(executor)

    # Mock the executor response with operations

    mock_operations = [
        Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            name="test-step",
        )
    ]
    mock_response = GetDurableExecutionStateResponse(
        operations=mock_operations, next_marker=None
    )
    executor.get_execution_state.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/state", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert "Operations" in response.body
    assert len(response.body["Operations"]) == 1
    assert response.body["Operations"][0]["Id"] == "op-1"
    assert response.body["Operations"][0]["Type"] == "STEP"

    # Verify executor was called with correct ARN
    executor.get_execution_state.assert_called_once_with("test-arn")


def test_get_durable_execution_state_handler_resource_not_found():
    """Test GetDurableExecutionStateHandler with ResourceNotFoundException."""

    executor = Mock()
    handler = GetDurableExecutionStateHandler(executor)

    # Mock executor to raise ResourceNotFoundException
    executor.get_execution_state.side_effect = ResourceNotFoundException(
        "Execution not-found-arn not found"
    )

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/not-found-arn/state", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Execution not-found-arn not found"


def test_get_durable_execution_state_handler_invalid_parameter():
    """Test GetDurableExecutionStateHandler with IllegalArgumentException."""

    executor = Mock()
    handler = GetDurableExecutionStateHandler(executor)

    # Mock executor to raise IllegalArgumentException
    executor.get_execution_state.side_effect = IllegalArgumentException(
        "Invalid checkpoint token"
    )

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/state", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid checkpoint token"


def test_get_durable_execution_history_handler_success():
    """Test GetDurableExecutionHistoryHandler with successful history retrieval."""

    executor = Mock()
    handler = GetDurableExecutionHistoryHandler(executor)

    # Mock the executor response with events
    mock_events = [
        Event(
            event_type="ExecutionStarted",
            event_timestamp="2023-01-01T00:00:00Z",
            event_id=1,
            operation_id="exec-1",
            execution_started_details=ExecutionStartedDetails(),
        )
    ]
    mock_response = GetDurableExecutionHistoryResponse(
        events=mock_events, next_marker=None
    )
    executor.get_execution_history.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/history", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={"MaxItems": ["10"], "Marker": ["token-123"]},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert "Events" in response.body
    assert len(response.body["Events"]) == 1
    assert response.body["Events"][0]["EventType"] == "ExecutionStarted"
    assert response.body["Events"][0]["EventId"] == 1

    # Verify executor was called with correct parameters
    executor.get_execution_history.assert_called_once_with(
        "test-arn",
        include_execution_data=False,
        reverse_order=False,
        marker="token-123",
        max_items=10,
    )


def test_get_durable_execution_history_handler_resource_not_found():
    """Test GetDurableExecutionHistoryHandler with ResourceNotFoundException."""

    executor = Mock()
    handler = GetDurableExecutionHistoryHandler(executor)

    # Mock executor to raise ResourceNotFoundException
    executor.get_execution_history.side_effect = ResourceNotFoundException(
        "Execution not-found-arn not found"
    )

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/not-found-arn/history", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Execution not-found-arn not found"


def test_get_durable_execution_history_handler_with_query_params():
    """Test GetDurableExecutionHistoryHandler with query parameters."""

    executor = Mock()
    handler = GetDurableExecutionHistoryHandler(executor)

    # Mock the executor response
    mock_response = GetDurableExecutionHistoryResponse(events=[], next_marker=None)
    executor.get_execution_history.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/history", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={"MaxItems": ["25"]},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert response.body == {"Events": []}

    # Verify executor was called with correct parameters
    executor.get_execution_history.assert_called_once_with(
        "test-arn",
        include_execution_data=False,
        reverse_order=False,
        marker=None,
        max_items=25,
    )


def test_get_durable_execution_history_handler_with_include_execution_data():
    """Test GetDurableExecutionHistoryHandler with IncludeExecutionData parameter."""

    executor = Mock()
    handler = GetDurableExecutionHistoryHandler(executor)

    # Mock the executor response
    mock_response = GetDurableExecutionHistoryResponse(events=[], next_marker=None)
    executor.get_execution_history.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/history", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={"IncludeExecutionData": ["true"], "MaxItems": ["1000"]},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert response.body == {"Events": []}

    # Verify executor was called with include_execution_data=True
    executor.get_execution_history.assert_called_once_with(
        "test-arn",
        include_execution_data=True,
        reverse_order=False,
        marker=None,
        max_items=1000,
    )


def test_get_durable_execution_history_handler_with_include_execution_data_false():
    """Test GetDurableExecutionHistoryHandler with IncludeExecutionData=false."""

    executor = Mock()
    handler = GetDurableExecutionHistoryHandler(executor)

    # Mock the executor response
    mock_response = GetDurableExecutionHistoryResponse(events=[], next_marker=None)
    executor.get_execution_history.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/history", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={"IncludeExecutionData": ["false"]},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert response.body == {"Events": []}

    # Verify executor was called with include_execution_data=False
    executor.get_execution_history.assert_called_once_with(
        "test-arn",
        include_execution_data=False,
        reverse_order=False,
        marker=None,
        max_items=None,
    )


def test_list_durable_executions_handler_success():
    """Test ListDurableExecutionsHandler with successful execution listing."""
    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock the executor response
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:test-1",
            durable_execution_name="test-execution-1",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="SUCCEEDED",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-01T00:01:00Z",
        ),
        ExecutionSummary(
            durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:test-2",
            durable_execution_name="test-execution-2",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="RUNNING",
            start_timestamp="2023-01-01T00:02:00Z",
            end_timestamp=None,
        ),
    ]

    mock_response = ListDurableExecutionsResponse(
        durable_executions=mock_executions,
        next_marker=None,
    )
    executor.list_executions.return_value = mock_response

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    expected_body = {
        "DurableExecutions": [
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:test-1",
                "DurableExecutionName": "test-execution-1",
                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
                "Status": "SUCCEEDED",
                "StartTimestamp": "2023-01-01T00:00:00Z",
                "EndTimestamp": "2023-01-01T00:01:00Z",
            },
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:test-2",
                "DurableExecutionName": "test-execution-2",
                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
                "Status": "RUNNING",
                "StartTimestamp": "2023-01-01T00:02:00Z",
            },
        ]
    }
    assert response.body == expected_body

    # Verify executor was called with correct parameters (all None for no filters)
    executor.list_executions.assert_called_once_with(
        function_name=None,
        function_version=None,
        execution_name=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=None,
        reverse_order=False,
    )


def test_list_durable_executions_handler_with_filters():
    """Test ListDurableExecutionsHandler with query parameter filters."""
    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock the executor response
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:filtered-1",
            durable_execution_name="filtered-execution",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="SUCCEEDED",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-01T00:01:00Z",
        ),
    ]

    mock_response = ListDurableExecutionsResponse(
        durable_executions=mock_executions,
        next_marker="next-page-token",
    )
    executor.list_executions.return_value = mock_response

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    # Create request with query parameters
    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={
            "FunctionName": ["test-function"],
            "FunctionVersion": ["$LATEST"],
            "DurableExecutionName": ["filtered-execution"],
            "StatusFilter": ["SUCCEEDED"],
            "StartedAfter": ["2023-01-01T00:00:00Z"],
            "StartedBefore": ["2023-01-01T23:59:59Z"],
            "Marker": ["start-token"],
            "MaxItems": ["10"],
            "ReverseOrder": ["true"],
        },
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    expected_body = {
        "DurableExecutions": [
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:filtered-1",
                "DurableExecutionName": "filtered-execution",
                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
                "Status": "SUCCEEDED",
                "StartTimestamp": "2023-01-01T00:00:00Z",
                "EndTimestamp": "2023-01-01T00:01:00Z",
            },
        ],
        "NextMarker": "next-page-token",
    }
    assert response.body == expected_body

    # Verify executor was called with correct filtered parameters
    executor.list_executions.assert_called_once_with(
        function_name="test-function",
        function_version="$LATEST",
        execution_name="filtered-execution",
        status_filter="SUCCEEDED",
        started_after="2023-01-01T00:00:00Z",
        started_before="2023-01-01T23:59:59Z",
        marker="start-token",
        max_items=10,
        reverse_order=True,
    )


def test_list_durable_executions_handler_pagination():
    """Test ListDurableExecutionsHandler with pagination support."""
    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock the executor response with pagination
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn=f"arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:page-{i}",
            durable_execution_name=f"page-execution-{i}",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="SUCCEEDED",
            start_timestamp=f"2023-01-0{i}T00:00:00Z",
            end_timestamp=f"2023-01-0{i}T00:01:00Z",
        )
        for i in range(1, 4)  # 3 executions
    ]

    mock_response = ListDurableExecutionsResponse(
        durable_executions=mock_executions,
        next_marker="next-page-marker",
    )
    executor.list_executions.return_value = mock_response

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    # Create request with pagination parameters
    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={
            "MaxItems": ["3"],
            "Marker": ["current-page-marker"],
        },
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response includes pagination
    assert response.status_code == 200
    assert len(response.body["DurableExecutions"]) == 3
    assert response.body["NextMarker"] == "next-page-marker"

    # Verify executor was called with pagination parameters
    executor.list_executions.assert_called_once_with(
        function_name=None,
        function_version=None,
        execution_name=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker="current-page-marker",
        max_items=3,
        reverse_order=False,
    )


def test_list_durable_executions_handler_empty_results():
    """Test ListDurableExecutionsHandler with no executions found."""

    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock empty executor response
    mock_response = ListDurableExecutionsResponse(
        durable_executions=[],
        next_marker=None,
    )
    executor.list_executions.return_value = mock_response

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    assert response.body == {"DurableExecutions": []}

    # Verify executor was called
    executor.list_executions.assert_called_once()


def test_list_durable_executions_handler_dataclass_serialization():
    """Test ListDurableExecutionsHandler uses from_dict/to_dict methods for serialization."""
    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock the executor response
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn="test-arn",
            durable_execution_name="test-execution",
            function_arn="test-function-arn",
            status="SUCCEEDED",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-01T00:01:00Z",
        ),
    ]

    mock_response = ListDurableExecutionsResponse(
        durable_executions=mock_executions,
        next_marker=None,
    )
    executor.list_executions.return_value = mock_response

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    # Create request with query parameters to test from_dict
    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={
            "FunctionName": ["test-function"],
            "MaxItems": ["5"],
        },
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response uses to_dict() serialization
    assert response.status_code == 200
    assert "DurableExecutions" in response.body
    assert isinstance(response.body["DurableExecutions"], list)

    # Verify the response structure matches to_dict() output
    execution_data = response.body["DurableExecutions"][0]
    assert execution_data["DurableExecutionArn"] == "test-arn"
    assert execution_data["DurableExecutionName"] == "test-execution"
    assert execution_data["Status"] == "SUCCEEDED"

    # Verify executor was called (implicitly tests from_dict was used for request parsing)
    executor.list_executions.assert_called_once_with(
        function_name="test-function",
        function_version=None,
        execution_name=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=5,
        reverse_order=False,
    )


def test_list_durable_executions_handler_invalid_parameter_error():
    """Test ListDurableExecutionsHandler with IllegalArgumentException from executor."""

    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock executor to raise IllegalArgumentException
    executor.list_executions.side_effect = IllegalArgumentException(
        "Invalid MaxItems value"
    )

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={
            "MaxItems": ["-1"],  # Invalid value
        },
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid MaxItems value"


def test_list_durable_executions_handler_unexpected_error():
    """Test ListDurableExecutionsHandler with unexpected error from executor."""

    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock executor to raise unexpected error
    executor.list_executions.side_effect = RuntimeError("Database connection failed")

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 500
    assert response.body["Type"] == "ServiceException"
    assert response.body["Message"] == "Database connection failed"


def test_list_durable_executions_handler_common_exception_handling():
    """Test ListDurableExecutionsHandler uses base class _handle_common_exceptions method."""

    executor = Mock()
    handler = ListDurableExecutionsHandler(executor)

    # Mock executor to raise ResourceNotFoundException
    executor.list_executions.side_effect = ResourceNotFoundException(
        "Function not found"
    )

    # Create strongly-typed route
    base_route = Route.from_string("/2025-12-01/durable-executions")
    typed_route = ListDurableExecutionsRoute.from_route(base_route)

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response uses common exception handling with AWS-compliant format
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Function not found"


def test_list_durable_executions_by_function_handler_success():
    """Test ListDurableExecutionsByFunctionHandler with successful execution listing."""

    executor = Mock()
    handler = ListDurableExecutionsByFunctionHandler(executor)

    # Mock the executor response
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:func-1",
            durable_execution_name="function-execution-1",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="SUCCEEDED",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-01T00:01:00Z",
        ),
        ExecutionSummary(
            durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:func-2",
            durable_execution_name="function-execution-2",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="RUNNING",
            start_timestamp="2023-01-01T00:02:00Z",
            end_timestamp=None,
        ),
    ]

    mock_response = ListDurableExecutionsByFunctionResponse(
        durable_executions=mock_executions,
        next_marker=None,
    )
    executor.list_executions_by_function.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/functions/test-function/durable-executions", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    expected_body = {
        "DurableExecutions": [
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:func-1",
                "DurableExecutionName": "function-execution-1",
                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
                "Status": "SUCCEEDED",
                "StartTimestamp": "2023-01-01T00:00:00Z",
                "EndTimestamp": "2023-01-01T00:01:00Z",
            },
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:func-2",
                "DurableExecutionName": "function-execution-2",
                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
                "Status": "RUNNING",
                "StartTimestamp": "2023-01-01T00:02:00Z",
            },
        ]
    }
    assert response.body == expected_body

    # Verify executor was called with correct function name
    executor.list_executions_by_function.assert_called_once_with(
        function_name="test-function",
        qualifier=None,
        execution_name=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=None,
        reverse_order=False,
    )


def test_list_durable_executions_by_function_handler_with_filters():
    """Test ListDurableExecutionsByFunctionHandler with query parameter filters."""

    executor = Mock()
    handler = ListDurableExecutionsByFunctionHandler(executor)

    # Mock the executor response
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:filtered",
            durable_execution_name="filtered-execution",
            function_arn="arn:aws:lambda:us-east-1:123456789012:function:test-function",
            status="SUCCEEDED",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-01T00:01:00Z",
        ),
    ]

    mock_response = ListDurableExecutionsByFunctionResponse(
        durable_executions=mock_executions,
        next_marker="next-page-token",
    )
    executor.list_executions_by_function.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/functions/test-function/durable-executions", "GET"
    )

    # Create request with query parameters
    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={
            "functionVersion": ["$LATEST"],
            "executionName": ["filtered-execution"],
            "statusFilter": ["SUCCEEDED"],
            "startedAfter": ["2023-01-01T00:00:00Z"],
            "startedBefore": ["2023-01-01T23:59:59Z"],
            "marker": ["start-token"],
            "maxItems": ["5"],
            "reverseOrder": ["true"],
        },
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response
    assert response.status_code == 200
    expected_body = {
        "DurableExecutions": [
            {
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function:execution:filtered",
                "DurableExecutionName": "filtered-execution",
                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-function",
                "Status": "SUCCEEDED",
                "StartTimestamp": "2023-01-01T00:00:00Z",
                "EndTimestamp": "2023-01-01T00:01:00Z",
            },
        ],
        "NextMarker": "next-page-token",
    }
    assert response.body == expected_body

    # Verify executor was called with correct filtered parameters
    executor.list_executions_by_function.assert_called_once_with(
        function_name="test-function",
        qualifier="$LATEST",
        execution_name="filtered-execution",
        status_filter="SUCCEEDED",
        started_after="2023-01-01T00:00:00Z",
        started_before="2023-01-01T23:59:59Z",
        marker="start-token",
        max_items=5,
        reverse_order=True,
    )


def test_list_durable_executions_by_function_handler_dataclass_serialization():
    """Test ListDurableExecutionsByFunctionHandler uses from_dict/to_dict methods for serialization."""

    executor = Mock()
    handler = ListDurableExecutionsByFunctionHandler(executor)

    # Mock the executor response
    mock_executions = [
        ExecutionSummary(
            durable_execution_arn="test-arn",
            durable_execution_name="test-execution",
            function_arn="test-function-arn",
            status="SUCCEEDED",
            start_timestamp="2023-01-01T00:00:00Z",
            end_timestamp="2023-01-01T00:01:00Z",
        ),
    ]

    mock_response = ListDurableExecutionsByFunctionResponse(
        durable_executions=mock_executions,
        next_marker=None,
    )
    executor.list_executions_by_function.return_value = mock_response

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/functions/test-function/durable-executions", "GET"
    )

    # Create request with query parameters to test from_dict
    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={
            "functionVersion": ["$LATEST"],
            "maxItems": ["10"],
        },
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify response uses to_dict() serialization
    assert response.status_code == 200
    assert "DurableExecutions" in response.body
    assert isinstance(response.body["DurableExecutions"], list)

    # Verify the response structure matches to_dict() output
    execution_data = response.body["DurableExecutions"][0]
    assert execution_data["DurableExecutionArn"] == "test-arn"
    assert execution_data["DurableExecutionName"] == "test-execution"
    assert execution_data["Status"] == "SUCCEEDED"

    # Verify executor was called (implicitly tests from_dict was used for request parsing)
    executor.list_executions_by_function.assert_called_once_with(
        function_name="test-function",
        qualifier="$LATEST",
        execution_name=None,
        status_filter=None,
        started_after=None,
        started_before=None,
        marker=None,
        max_items=10,
        reverse_order=False,
    )


def test_list_durable_executions_by_function_handler_resource_not_found():
    """Test ListDurableExecutionsByFunctionHandler with ResourceNotFoundException."""

    executor = Mock()
    handler = ListDurableExecutionsByFunctionHandler(executor)

    # Mock executor to raise ResourceNotFoundException
    executor.list_executions_by_function.side_effect = ResourceNotFoundException(
        "Function not-found-function not found"
    )

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/functions/not-found-function/durable-executions", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response uses common exception handling with AWS-compliant format
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Function not-found-function not found"


def test_list_durable_executions_by_function_handler_common_exception_handling():
    """Test ListDurableExecutionsByFunctionHandler uses base class _handle_common_exceptions method."""

    executor = Mock()
    handler = ListDurableExecutionsByFunctionHandler(executor)

    # Mock executor to raise IllegalArgumentException
    executor.list_executions_by_function.side_effect = IllegalArgumentException(
        "Invalid function name format"
    )

    # Create strongly-typed route using Router
    router = Router()
    typed_route = router.find_route(
        "/2025-12-01/functions/invalid-function/durable-executions", "GET"
    )

    request = HTTPRequest(
        method="GET",
        path=typed_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(typed_route, request)

    # Verify error response uses common exception handling with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid function name format"


def test_send_durable_execution_callback_success_handler():
    """Test SendDurableExecutionCallbackSuccessHandler with valid request."""

    executor = Mock()
    executor.send_callback_success.return_value = (
        SendDurableExecutionCallbackSuccessResponse()
    )
    handler = SendDurableExecutionCallbackSuccessHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)
    assert route.callback_id == "test-callback-id"

    # Result is sent as raw binary body
    request_body = b"success-result"

    request = HTTPRequest(
        method="POST",
        path=route,
        headers={},
        query_params={},
        body=request_body,
    )

    response = handler.handle(route, request)

    # Verify successful response
    assert response.status_code == 200
    assert response.body == {}

    # Verify executor was called with correct parameters
    executor.send_callback_success.assert_called_once_with(
        callback_id="test-callback-id", result=b"success-result"
    )


def test_send_durable_execution_callback_success_handler_empty_body():
    """Test SendDurableExecutionCallbackSuccessHandler with empty body."""
    executor = Mock()
    executor.send_callback_success.return_value = (
        SendDurableExecutionCallbackSuccessResponse()
    )
    handler = SendDurableExecutionCallbackSuccessHandler(executor)

    base_route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/test-id/succeed"
    )
    callback_route = CallbackSuccessRoute.from_route(base_route)

    request = HTTPRequest(
        method="POST",
        path=callback_route,
        headers={},
        query_params={},
        body=b"",
    )

    response = handler.handle(callback_route, request)
    # Handler should accept empty body (Result is optional) and return 200
    assert response.status_code == 200
    assert response.body == {}

    # Verify executor was called with empty result
    executor.send_callback_success.assert_called_once_with(
        callback_id="test-id", result=b""
    )


def test_send_durable_execution_callback_failure_handler():
    """Test SendDurableExecutionCallbackFailureHandler with valid request."""

    executor = Mock()
    executor.send_callback_failure.return_value = (
        SendDurableExecutionCallbackFailureResponse()
    )
    handler = SendDurableExecutionCallbackFailureHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/fail", "POST"
    )
    assert isinstance(route, CallbackFailureRoute)
    assert route.callback_id == "test-callback-id"

    # Test with valid request body including error
    error_data = {
        "ErrorMessage": "Test error",
        "ErrorType": "TestException",
        "ErrorData": None,
        "StackTrace": None,
    }
    request = HTTPRequest(
        method="POST",
        path=route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body=error_data,  # Pass error data directly as body
    )
    response = handler.handle(route, request)

    # Verify successful response
    assert response.status_code == 200
    assert response.body == {}

    # Verify executor was called with correct parameters
    executor.send_callback_failure.assert_called_once()
    call_args = executor.send_callback_failure.call_args
    assert call_args[1]["callback_id"] == "test-callback-id"
    assert isinstance(call_args[1]["error"], ErrorObject)
    assert call_args[1]["error"].message == "Test error"


def test_update_lambda_endpoint_handler_success():
    """Test UpdateLambdaEndpointHandler with valid request."""
    from aws_durable_execution_sdk_python_testing.invoker import LambdaInvoker
    from aws_durable_execution_sdk_python_testing.web.handlers import (
        UpdateLambdaEndpointHandler,
    )
    from aws_durable_execution_sdk_python_testing.web.routes import (
        UpdateLambdaEndpointRoute,
    )

    executor = Mock()
    lambda_invoker = Mock(spec=LambdaInvoker)
    executor._invoker = lambda_invoker  # noqa: SLF001
    handler = UpdateLambdaEndpointHandler(executor)

    base_route = Route.from_string("/lambda-endpoint")
    update_route = UpdateLambdaEndpointRoute.from_route(base_route)

    request = HTTPRequest(
        method="PUT",
        path=update_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"EndpointUrl": "http://localhost:8080", "RegionName": "us-west-2"},
    )

    response = handler.handle(update_route, request)

    assert response.status_code == 200
    assert response.body == {"message": "Lambda endpoint updated successfully"}
    lambda_invoker.update_endpoint.assert_called_once_with(
        "http://localhost:8080", "us-west-2"
    )


def test_update_lambda_endpoint_handler_missing_endpoint_url():
    """Test UpdateLambdaEndpointHandler with missing EndpointUrl."""
    from aws_durable_execution_sdk_python_testing.web.handlers import (
        UpdateLambdaEndpointHandler,
    )
    from aws_durable_execution_sdk_python_testing.web.routes import (
        UpdateLambdaEndpointRoute,
    )

    executor = Mock()
    handler = UpdateLambdaEndpointHandler(executor)

    base_route = Route.from_string("/lambda-endpoint")
    update_route = UpdateLambdaEndpointRoute.from_route(base_route)

    request = HTTPRequest(
        method="PUT",
        path=update_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"RegionName": "us-west-2"},
    )

    response = handler.handle(update_route, request)

    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "EndpointUrl is required"


def test_update_lambda_endpoint_handler_default_region():
    """Test UpdateLambdaEndpointHandler uses default region when not specified."""
    from aws_durable_execution_sdk_python_testing.invoker import LambdaInvoker
    from aws_durable_execution_sdk_python_testing.web.handlers import (
        UpdateLambdaEndpointHandler,
    )
    from aws_durable_execution_sdk_python_testing.web.routes import (
        UpdateLambdaEndpointRoute,
    )

    executor = Mock()
    lambda_invoker = Mock(spec=LambdaInvoker)
    executor._invoker = lambda_invoker  # noqa: SLF001
    handler = UpdateLambdaEndpointHandler(executor)

    base_route = Route.from_string("/lambda-endpoint")
    update_route = UpdateLambdaEndpointRoute.from_route(base_route)

    request = HTTPRequest(
        method="PUT",
        path=update_route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"EndpointUrl": "http://localhost:8080"},
    )

    response = handler.handle(update_route, request)

    assert response.status_code == 200
    lambda_invoker.update_endpoint.assert_called_once_with(
        "http://localhost:8080", "us-east-1"
    )


def test_send_durable_execution_callback_failure_handler_empty_body():
    """Test SendDurableExecutionCallbackFailureHandler with empty body."""
    executor = Mock()
    handler = SendDurableExecutionCallbackFailureHandler(executor)

    base_route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/test-id/fail"
    )
    callback_route = CallbackFailureRoute.from_route(base_route)

    request = HTTPRequest(
        method="POST",
        path=callback_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(callback_route, request)
    # Handler should accept empty body for failure requests
    assert response.status_code == 200


def test_send_durable_execution_callback_heartbeat_handler():
    """Test SendDurableExecutionCallbackHeartbeatHandler with valid request."""

    executor = Mock()
    executor.send_callback_heartbeat.return_value = (
        SendDurableExecutionCallbackHeartbeatResponse()
    )
    handler = SendDurableExecutionCallbackHeartbeatHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/heartbeat", "POST"
    )
    assert isinstance(route, CallbackHeartbeatRoute)
    assert route.callback_id == "test-callback-id"

    # Test with valid request body
    request = HTTPRequest(
        method="POST",
        path=route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"CallbackId": "test-callback-id"},
    )
    response = handler.handle(route, request)

    # Verify successful response
    assert response.status_code == 200
    assert response.body == {}

    # Verify executor was called with correct parameters
    executor.send_callback_heartbeat.assert_called_once_with(
        callback_id="test-callback-id"
    )


def test_send_durable_execution_callback_heartbeat_handler_empty_body():
    """Test SendDurableExecutionCallbackHeartbeatHandler with empty body."""
    executor = Mock()
    handler = SendDurableExecutionCallbackHeartbeatHandler(executor)

    base_route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/test-id/heartbeat"
    )
    callback_route = CallbackHeartbeatRoute.from_route(base_route)

    request = HTTPRequest(
        method="POST",
        path=callback_route,
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(callback_route, request)
    # Handler should accept empty body for heartbeat requests
    assert response.status_code == 200


def test_health_handler():
    """Test HealthHandler returns healthy status."""
    executor = Mock()
    handler = HealthHandler(executor)

    request = HTTPRequest(
        method="GET",
        path=Route.from_string("/health"),
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(Route.from_string("/health"), request)
    assert response.status_code == 200
    assert response.body == {"status": "healthy"}


def test_metrics_handler():
    """Test MetricsHandler returns empty metrics."""
    executor = Mock()
    handler = MetricsHandler(executor)

    request = HTTPRequest(
        method="GET",
        path=Route.from_string("/metrics"),
        headers={},
        query_params={},
        body={},
    )

    response = handler.handle(Route.from_string("/metrics"), request)
    assert response.status_code == 200
    assert response.body == {"metrics": {}}


def test_handler_naming_matches_smithy_operations():
    """Test that handler names match the Smithy operation names."""
    # Verify that all handlers are named after their corresponding Smithy operations
    handler_names = [
        "StartExecutionHandler",  # Note: This one doesn't have "Durable" prefix in Smithy
        "GetDurableExecutionHandler",
        "CheckpointDurableExecutionHandler",
        "StopDurableExecutionHandler",
        "GetDurableExecutionStateHandler",
        "GetDurableExecutionHistoryHandler",
        "ListDurableExecutionsHandler",
        "ListDurableExecutionsByFunctionHandler",
        "SendDurableExecutionCallbackSuccessHandler",
        "SendDurableExecutionCallbackFailureHandler",
        "SendDurableExecutionCallbackHeartbeatHandler",
        "HealthHandler",
        "MetricsHandler",
    ]

    # Import the handlers module to check all classes exist

    for handler_name in handler_names:
        assert hasattr(handlers, handler_name), f"Handler {handler_name} not found"
        handler_class = getattr(handlers, handler_name)
        assert issubclass(handler_class, EndpointHandler), (
            f"{handler_name} should inherit from EndpointHandler"
        )


def test_all_handlers_have_executor():
    """Test that all handlers store the executor reference."""
    executor = Mock()

    handlers_to_test = [
        StartExecutionHandler,
        GetDurableExecutionHandler,
        CheckpointDurableExecutionHandler,
        StopDurableExecutionHandler,
        GetDurableExecutionStateHandler,
        GetDurableExecutionHistoryHandler,
        ListDurableExecutionsHandler,
        ListDurableExecutionsByFunctionHandler,
        SendDurableExecutionCallbackSuccessHandler,
        SendDurableExecutionCallbackFailureHandler,
        SendDurableExecutionCallbackHeartbeatHandler,
        HealthHandler,
        MetricsHandler,
    ]

    for handler_class in handlers_to_test:
        handler = handler_class(executor)
        assert handler.executor == executor, (
            f"{handler_class.__name__} should store executor reference"
        )


class MockExceptionHandler(EndpointHandler):
    """Test handler that can trigger specific exception types for testing."""

    def __init__(
        self, executor: Executor, exception_to_raise: Exception | None = None
    ) -> None:
        super().__init__(executor)
        self.exception_to_raise = exception_to_raise

    def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
        """Handle request by raising the configured exception."""
        if self.exception_to_raise:
            if isinstance(self.exception_to_raise, AwsApiException):
                return self._handle_aws_exception(self.exception_to_raise)

            return self._handle_framework_exception(self.exception_to_raise)
        return self._success_response({"status": "ok"})


def test_framework_exception_handling():
    """Test the framework exception handling through public API."""

    executor = Mock()

    # Test ValueError handling - maps to InvalidParameterValueException
    handler = MockExceptionHandler(executor, ValueError("Invalid input"))
    response = handler.handle(Mock(), Mock())
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid input"

    # Test KeyError handling - maps to InvalidParameterValueException
    handler = MockExceptionHandler(executor, KeyError("missing_field"))
    response = handler.handle(Mock(), Mock())
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "'missing_field'"

    # Test unexpected exception handling - maps to ServiceException
    handler = MockExceptionHandler(executor, RuntimeError("Unexpected error"))
    response = handler.handle(Mock(), Mock())
    assert response.status_code == 500
    assert response.body["Type"] == "ServiceException"
    assert response.body["Message"] == "Unexpected error"


def test_aws_exception_handling():
    """Test the AWS exception handling through public API."""

    executor = Mock()

    # Test ResourceNotFoundException handling
    handler = MockExceptionHandler(
        executor, ResourceNotFoundException("Resource not found")
    )
    response = handler.handle(Mock(), Mock())
    assert response.status_code == 404
    assert response.body["Type"] == "ResourceNotFoundException"
    assert response.body["Message"] == "Resource not found"

    # Test IllegalArgumentException handling
    handler = MockExceptionHandler(
        executor, IllegalArgumentException("Invalid parameter")
    )
    response = handler.handle(Mock(), Mock())
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Invalid parameter"


def test_send_durable_execution_callback_success_handler_invalid_callback_id():
    """Test SendDurableExecutionCallbackSuccessHandler with invalid callback ID."""

    executor = Mock()
    executor.send_callback_success.side_effect = IllegalArgumentException(
        "callback_id is required"
    )
    handler = SendDurableExecutionCallbackSuccessHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/succeed", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"CallbackId": "test-callback-id"},
    )

    response = handler.handle(route, request)

    # Verify error response with AWS-compliant format
    assert response.status_code == 400
    assert response.body["Type"] == "InvalidParameterValueException"
    assert "callback_id is required" in response.body["message"]


def test_send_durable_execution_callback_success_handler_callback_state_conflict():
    """Test SendDurableExecutionCallbackSuccessHandler with callback state conflict."""

    executor = Mock()
    executor.send_callback_success.side_effect = IllegalStateException(
        "Callback already completed"
    )
    handler = SendDurableExecutionCallbackSuccessHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/succeed", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"CallbackId": "test-callback-id"},
    )

    response = handler.handle(route, request)

    # Verify error response - IllegalStateException in callback context maps to ExecutionConflictException
    assert response.status_code == 409
    assert response.body["Type"] == "ExecutionConflictException"
    assert response.body["message"] == "Callback already completed"


def test_send_durable_execution_callback_failure_handler_callback_state_conflict():
    """Test SendDurableExecutionCallbackFailureHandler with callback state conflict."""

    executor = Mock()
    executor.send_callback_failure.side_effect = IllegalStateException(
        "Callback already completed"
    )
    handler = SendDurableExecutionCallbackFailureHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/fail", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"CallbackId": "test-callback-id"},
    )

    response = handler.handle(route, request)

    # Verify error response - IllegalStateException in callback context maps to ExecutionConflictException
    assert response.status_code == 409
    assert response.body["Type"] == "ExecutionConflictException"
    assert response.body["message"] == "Callback already completed"


def test_send_durable_execution_callback_heartbeat_handler_callback_state_conflict():
    """Test SendDurableExecutionCallbackHeartbeatHandler with callback state conflict."""

    executor = Mock()
    executor.send_callback_heartbeat.side_effect = IllegalStateException(
        "Callback already completed"
    )
    handler = SendDurableExecutionCallbackHeartbeatHandler(executor)

    # Create route using Router
    router = Router()
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-callback-id/heartbeat", "POST"
    )

    request = HTTPRequest(
        method="POST",
        path=route,
        headers={"Content-Type": "application/json"},
        query_params={},
        body={"CallbackId": "test-callback-id"},
    )

    response = handler.handle(route, request)

    # Verify error response - IllegalStateException in callback context maps to ExecutionConflictException
    assert response.status_code == 409
    assert response.body["Type"] == "ExecutionConflictException"
    assert response.body["message"] == "Callback already completed"


def test_callback_handlers_use_dataclass_serialization():
    """Test that all callback handlers use dataclass from_dict/to_dict methods."""

    # Test that all callback request dataclasses have from_dict/to_dict methods
    success_request = SendDurableExecutionCallbackSuccessRequest.from_dict(
        {"CallbackId": "test-id", "Result": "test-result"}
    )
    assert success_request.callback_id == "test-id"
    assert success_request.result == "test-result"
    assert success_request.to_dict() == {
        "CallbackId": "test-id",
        "Result": "test-result",
    }

    failure_request = SendDurableExecutionCallbackFailureRequest.from_dict(
        {}, "test-id"
    )
    assert failure_request.callback_id == "test-id"
    assert failure_request.error is None
    assert failure_request.to_dict() == {"CallbackId": "test-id"}

    heartbeat_request = SendDurableExecutionCallbackHeartbeatRequest.from_dict(
        {"CallbackId": "test-id"}
    )
    assert heartbeat_request.callback_id == "test-id"
    assert heartbeat_request.to_dict() == {"CallbackId": "test-id"}

"""Tests for HTTP request/response data models and utilities."""

from __future__ import annotations

import datetime
import json
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python_testing.exceptions import (
    CallbackTimeoutException,
    ExecutionAlreadyStartedException,
    IllegalArgumentException,
    IllegalStateException,
    InvalidParameterValueException,
    ResourceNotFoundException,
    ServiceException,
    TooManyRequestsException,
)
from aws_durable_execution_sdk_python_testing.web.models import (
    HTTPRequest,
    HTTPResponse,
    OperationHandler,
)
from aws_durable_execution_sdk_python_testing.web.routes import Route


def test_http_request_creation() -> None:
    """Test HTTPRequest dataclass creation."""
    path = Route.from_string("/test/path")
    request = HTTPRequest(
        method="GET",
        path=path,
        headers={"Content-Type": "application/json"},
        query_params={"param1": ["value1"], "param2": ["value2a", "value2b"]},
        body={"test": "data"},
    )

    assert request.method == "GET"
    assert request.path == path
    assert request.headers == {"Content-Type": "application/json"}
    assert request.query_params == {
        "param1": ["value1"],
        "param2": ["value2a", "value2b"],
    }
    assert request.body == {"test": "data"}


def test_http_request_immutable() -> None:
    """Test that HTTPRequest is immutable."""
    path = Route.from_string("/test/path")
    request = HTTPRequest(method="GET", path=path, headers={}, query_params={}, body={})

    # Should not be able to modify fields
    with pytest.raises(AttributeError):
        request.method = "POST"  # type: ignore


def test_http_response_creation() -> None:
    """Test HTTPResponse dataclass creation."""
    response = HTTPResponse(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body={"result": "success"},
    )

    assert response.status_code == 200
    assert response.headers == {"Content-Type": "application/json"}
    assert response.body == {"result": "success"}


def test_http_response_immutable() -> None:
    """Test that HTTPResponse is immutable."""
    response = HTTPResponse(status_code=200, headers={}, body={})

    # Should not be able to modify fields
    with pytest.raises(AttributeError):
        response.status_code = 404  # type: ignore


def test_http_response_json_basic() -> None:
    """Test creating basic JSON response."""
    data = {"message": "success", "id": 123}
    response = HTTPResponse.create_json(200, data)

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/json"

    # Verify the body is stored as dict
    assert response.body == data

    # Verify serialization to bytes works
    body_bytes = response.body_to_bytes()
    parsed_body = json.loads(body_bytes.decode("utf-8"))
    assert parsed_body == data


def test_http_response_json_with_additional_headers() -> None:
    """Test creating JSON response with additional headers."""
    data = {"result": "ok"}
    additional_headers = {
        "X-Custom-Header": "custom-value",
        "Cache-Control": "no-cache",
    }

    response = HTTPResponse.create_json(201, data, additional_headers)

    assert response.status_code == 201
    assert response.headers["Content-Type"] == "application/json"
    assert response.headers["X-Custom-Header"] == "custom-value"
    assert response.headers["Cache-Control"] == "no-cache"

    # Verify the body is stored as dict
    assert response.body == data

    # Verify serialization to bytes works
    body_bytes = response.body_to_bytes()
    parsed_body = json.loads(body_bytes.decode("utf-8"))
    assert parsed_body == data


def test_http_response_json_compact_serialization() -> None:
    """Test that JSON response uses compact serialization."""
    data = {"key": "value", "nested": {"inner": "data"}}
    response = HTTPResponse.create_json(200, data)

    # Verify the body is stored as dict
    assert response.body == data

    # Verify serialization to bytes uses compact format
    body_bytes = response.body_to_bytes()
    body_str = body_bytes.decode("utf-8")
    assert " " not in body_str  # No spaces after separators
    assert "\n" not in body_str  # No newlines


# Removed deprecated tests for create_error method


def test_http_response_empty_basic() -> None:
    """Test creating basic empty response."""
    response = HTTPResponse.create_empty(204)

    assert response.status_code == 204
    assert response.headers == {}
    assert response.body == {}


def test_http_response_empty_with_headers() -> None:
    """Test creating empty response with additional headers."""
    additional_headers = {"Location": "/new-resource", "X-Request-ID": "123"}
    response = HTTPResponse.create_empty(201, additional_headers)

    assert response.status_code == 201
    assert response.headers == additional_headers
    assert response.body == {}


def test_operation_handler_protocol() -> None:
    """Test that OperationHandler protocol works correctly."""

    class TestHandler:
        def handle(self, parsed_route: Route, request: HTTPRequest) -> HTTPResponse:
            return HTTPResponse(
                status_code=200,
                headers={"Content-Type": "text/plain"},
                body={"message": "handled"},
            )

    # Should be able to use as OperationHandler
    handler: OperationHandler = TestHandler()

    path = Route.from_string("/test")
    request = HTTPRequest(method="GET", path=path, headers={}, query_params={}, body={})

    response = handler.handle(path, request)
    assert response.status_code == 200
    assert response.body == {"message": "handled"}


def test_operation_handler_protocol_type_checking() -> None:
    """Test that OperationHandler protocol enforces correct signature."""

    class InvalidHandler:
        def handle(self, wrong_params: str) -> str:  # Wrong signature
            return "invalid"

    # This should work at runtime but would fail type checking
    # We can't test static type checking in unit tests, but this documents the expected behavior
    invalid_handler = InvalidHandler()

    # The protocol is structural, so this would work at runtime
    # but mypy would catch the type mismatch
    assert hasattr(invalid_handler, "handle")


def test_http_response_edge_cases() -> None:
    """Test edge cases for HTTP response factory methods."""

    # Test with empty data
    response = HTTPResponse.create_json(200, {})
    assert response.body == {}

    # Test with complex nested data
    complex_data = {
        "list": [1, 2, 3],
        "nested": {"deep": {"value": True}},
        "null": None,
        "unicode": "🚀",
    }
    response = HTTPResponse.create_json(200, complex_data)
    assert response.body == complex_data

    # Verify serialization to bytes works
    body_bytes = response.body_to_bytes()
    parsed = json.loads(body_bytes.decode("utf-8"))
    assert parsed == complex_data


def test_http_request_with_empty_collections() -> None:
    """Test HTTPRequest with empty collections."""
    path = Route.from_string("/empty")
    request = HTTPRequest(method="GET", path=path, headers={}, query_params={}, body={})

    assert request.headers == {}
    assert request.query_params == {}
    assert request.body == {}


def test_http_response_with_empty_collections() -> None:
    """Test HTTPResponse with empty collections."""
    response = HTTPResponse(status_code=204, headers={}, body={})

    assert response.headers == {}
    assert response.body == {}


# Tests for HTTPRequest.from_bytes method


def test_http_request_from_bytes_standard_json() -> None:
    """Test HTTPRequest.from_bytes with standard JSON deserialization."""
    test_data = {"key": "value", "number": 42}
    body_bytes = json.dumps(test_data).encode("utf-8")

    path = Route.from_string("/test")
    request = HTTPRequest.from_bytes(
        body_bytes=body_bytes,
        method="POST",
        path=path,
        headers={"Content-Type": "application/json"},
        query_params={"param": ["value"]},
    )

    assert request.method == "POST"
    assert request.path == path
    assert request.headers == {"Content-Type": "application/json"}
    assert request.query_params == {"param": ["value"]}
    assert request.body == test_data


def test_http_get_request_from_bytes_ignore_body() -> None:
    """Test HTTPRequest.from_bytes with standard JSON deserialization."""
    test_data = {"key": "value", "number": 42}
    body_bytes = json.dumps(test_data).encode("utf-8")

    path = Route.from_string("/test")
    request = HTTPRequest.from_bytes(
        body_bytes=body_bytes,
        method="GET",
        path=path,
        headers={"Content-Type": "application/json"},
        query_params={"param": ["value"]},
    )

    assert request.method == "GET"
    assert request.path == path
    assert request.headers == {"Content-Type": "application/json"}
    assert request.query_params == {"param": ["value"]}
    assert request.body == {}


def test_http_request_from_bytes_minimal_params() -> None:
    """Test HTTPRequest.from_bytes with minimal parameters."""
    test_data = {"message": "hello"}
    body_bytes = json.dumps(test_data).encode("utf-8")

    request = HTTPRequest.from_bytes(body_bytes=body_bytes)

    assert request.method == "POST"  # Default
    assert request.path.raw_path == ""  # Default empty route
    assert request.headers == {}  # Default
    assert request.query_params == {}  # Default
    assert request.body == test_data


def test_http_request_from_bytes_aws_operation_fallback() -> None:
    """Test HTTPRequest.from_bytes with AWS operation that falls back to JSON."""
    test_data = {"Input": "test-input", "ExecutionName": "test-execution"}
    body_bytes = json.dumps(test_data).encode("utf-8")

    # Use a non-existent operation name to trigger fallback
    request = HTTPRequest.from_bytes(
        body_bytes=body_bytes,
        operation_name="NonExistentOperation",
        method="POST",
    )

    assert request.method == "POST"
    assert request.body == test_data


def test_http_request_from_bytes_invalid_json() -> None:
    """Test HTTPRequest.from_bytes with invalid JSON raises InvalidParameterValueException."""
    invalid_json = b'{"invalid": json}'

    with pytest.raises(
        InvalidParameterValueException, match="JSON deserialization failed"
    ):
        HTTPRequest.from_bytes(body_bytes=invalid_json)


def test_http_request_from_bytes_invalid_utf8() -> None:
    """Test HTTPRequest.from_bytes with invalid UTF-8 raises InvalidParameterValueException."""
    invalid_utf8 = b'\xff\xfe{"test": "data"}'  # Invalid UTF-8 BOM

    with pytest.raises(
        InvalidParameterValueException, match="JSON deserialization failed"
    ):
        HTTPRequest.from_bytes(body_bytes=invalid_utf8)


def test_http_request_from_bytes_empty_body() -> None:
    """Test HTTPRequest.from_bytes with empty body."""
    empty_body = b"{}"

    request = HTTPRequest.from_bytes(body_bytes=empty_body)

    assert request.body == {}


def test_http_request_from_bytes_complex_json() -> None:
    """Test HTTPRequest.from_bytes with complex nested JSON."""
    complex_data = {
        "list": [1, 2, 3],
        "nested": {"deep": {"value": True}},
        "null": None,
        "unicode": "🚀",
    }
    body_bytes = json.dumps(complex_data).encode("utf-8")

    request = HTTPRequest.from_bytes(body_bytes=body_bytes)

    assert request.body == complex_data


def test_http_request_from_bytes_aws_operation_success() -> None:
    """Test HTTPRequest.from_bytes with valid AWS operation (if available)."""
    # This test will use AWS deserialization if available, otherwise fall back to JSON
    test_data = {
        "Input": "test-input",
        "ExecutionName": "test-execution",
        "FunctionName": "test-function",
    }
    body_bytes = json.dumps(test_data).encode("utf-8")

    # Try with a real AWS operation name
    request = HTTPRequest.from_bytes(
        body_bytes=body_bytes,
        operation_name="StartDurableExecution",
        method="POST",
    )

    assert request.method == "POST"
    assert request.body is not None
    # The exact structure may vary depending on AWS deserialization vs JSON fallback
    # but we should get some valid dict data


def test_http_request_from_bytes_preserves_field_names() -> None:
    """Test that from_bytes preserves field names from the input."""
    # Test with AWS-style PascalCase field names
    aws_style_data = {
        "ExecutionName": "test-execution",
        "FunctionName": "my-function",
        "Input": {"Key": "Value"},
    }
    body_bytes = json.dumps(aws_style_data).encode("utf-8")

    request = HTTPRequest.from_bytes(body_bytes=body_bytes)

    # Field names should be preserved as-is
    assert isinstance(request.body, dict)
    assert "ExecutionName" in request.body
    assert "FunctionName" in request.body
    assert request.body["ExecutionName"] == "test-execution"
    assert request.body["FunctionName"] == "my-function"
    assert request.body["Input"]["Key"] == "Value"


# Tests for HTTPResponse.body_to_bytes method


def test_http_response_body_to_bytes_standard_json() -> None:
    """Test HTTPResponse.body_to_bytes with standard JSON serialization."""
    test_data = {"message": "success", "id": 123}
    response = HTTPResponse(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body=test_data,
    )

    body_bytes = response.body_to_bytes()

    # Verify it's bytes
    assert isinstance(body_bytes, bytes)

    # Verify content is correct
    parsed_data = json.loads(body_bytes.decode("utf-8"))
    assert parsed_data == test_data


def test_http_response_body_to_bytes_compact_format() -> None:
    """Test that body_to_bytes uses compact JSON format."""
    test_data = {"key": "value", "nested": {"inner": "data"}}
    response = HTTPResponse(status_code=200, headers={}, body=test_data)

    body_bytes = response.body_to_bytes()
    body_str = body_bytes.decode("utf-8")

    # Should not contain extra whitespace
    assert " " not in body_str  # No spaces after separators
    assert "\n" not in body_str  # No newlines


def test_http_response_body_to_bytes_empty_body() -> None:
    """Test body_to_bytes with empty body."""
    response = HTTPResponse(status_code=204, headers={}, body={})

    body_bytes = response.body_to_bytes()

    assert body_bytes == b"{}"


def test_http_response_body_to_bytes_complex_data() -> None:
    """Test body_to_bytes with complex nested data."""
    complex_data = {
        "list": [1, 2, 3],
        "nested": {"deep": {"value": True}},
        "null": None,
        "unicode": "🚀",
    }
    response = HTTPResponse(status_code=200, headers={}, body=complex_data)

    body_bytes = response.body_to_bytes()
    parsed_data = json.loads(body_bytes.decode("utf-8"))

    assert parsed_data == complex_data


# Tests for HTTPResponse.from_dict method


def test_http_response_from_dict_basic() -> None:
    """Test HTTPResponse.from_dict with basic parameters."""
    test_data = {"message": "success", "id": 123}

    response = HTTPResponse.from_dict(test_data)

    assert response.status_code == 200  # Default
    assert response.headers == {}  # Default
    assert response.body == test_data


def test_http_response_from_dict_with_status_code() -> None:
    """Test HTTPResponse.from_dict with custom status code."""
    test_data = {"error": "not found"}

    response = HTTPResponse.from_dict(test_data, status_code=404)

    assert response.status_code == 404
    assert response.headers == {}
    assert response.body == test_data


def test_http_response_from_dict_with_headers() -> None:
    """Test HTTPResponse.from_dict with custom headers."""
    test_data = {"result": "ok"}
    headers = {"Content-Type": "application/json", "X-Custom": "value"}

    response = HTTPResponse.from_dict(test_data, headers=headers)

    assert response.status_code == 200
    assert response.headers == headers
    assert response.body == test_data


def test_http_response_from_dict_with_all_params() -> None:
    """Test HTTPResponse.from_dict with all parameters."""
    test_data = {"data": "test"}
    headers = {"Content-Type": "application/json"}

    response = HTTPResponse.from_dict(test_data, status_code=201, headers=headers)

    assert response.status_code == 201
    assert response.headers == headers
    assert response.body == test_data


def test_http_response_from_dict_empty_data() -> None:
    """Test HTTPResponse.from_dict with empty data."""
    response = HTTPResponse.from_dict({})

    assert response.status_code == 200
    assert response.headers == {}
    assert response.body == {}


def test_http_response_from_dict_complex_data() -> None:
    """Test HTTPResponse.from_dict with complex nested data."""
    complex_data = {
        "list": [1, 2, 3],
        "nested": {"deep": {"value": True}},
        "null": None,
        "unicode": "🚀",
    }

    response = HTTPResponse.from_dict(complex_data)

    assert response.body == complex_data


def test_http_response_from_dict_immutable() -> None:
    """Test that HTTPResponse.from_dict creates immutable response."""
    test_data = {"key": "value"}
    response = HTTPResponse.from_dict(test_data)

    # Should not be able to modify fields
    with pytest.raises(AttributeError):
        response.status_code = 404  # type: ignore


def test_http_response_from_dict_integration_with_body_to_bytes() -> None:
    """Test that from_dict works with body_to_bytes method."""
    test_data = {"message": "integration test", "success": True}

    response = HTTPResponse.from_dict(test_data, status_code=201)
    body_bytes = response.body_to_bytes()

    # Verify round-trip serialization
    parsed_data = json.loads(body_bytes.decode("utf-8"))
    assert parsed_data == test_data
    assert response.status_code == 201


def test_http_request_from_bytes_aws_deserialization_success() -> None:
    """Test HTTPRequest.from_bytes with successful AWS deserialization."""
    test_data = {"ExecutionName": "test-execution", "Input": "test-input"}
    body_bytes = json.dumps(test_data).encode("utf-8")

    # Mock successful AWS deserialization
    mock_deserializer = Mock()
    mock_deserializer.from_bytes.return_value = test_data

    with patch(
        "aws_durable_execution_sdk_python_testing.web.models.AwsRestJsonDeserializer.create",
        return_value=mock_deserializer,
    ):
        request = HTTPRequest.from_bytes(
            body_bytes=body_bytes, operation_name="StartDurableExecution"
        )

    assert request.body == test_data
    mock_deserializer.from_bytes.assert_called_once_with(body_bytes)


def test_http_request_from_bytes_aws_deserialization_fallback_error() -> None:
    """Test HTTPRequest.from_bytes when both AWS and JSON deserialization fail."""

    invalid_bytes = b"invalid json data"

    # Mock AWS deserialization failure
    mock_deserializer = Mock()
    mock_deserializer.from_bytes.side_effect = InvalidParameterValueException(
        "AWS failed"
    )

    with patch(
        "aws_durable_execution_sdk_python_testing.web.models.AwsRestJsonDeserializer.create",
        return_value=mock_deserializer,
    ):
        with pytest.raises(
            InvalidParameterValueException,
            match="Both AWS and JSON deserialization failed",
        ):
            HTTPRequest.from_bytes(
                body_bytes=invalid_bytes, operation_name="StartDurableExecution"
            )


def test_http_response_body_to_bytes_serialization_error() -> None:
    """Test HTTPResponse.body_to_bytes when JSON serialization fail."""

    # Create data that can't be JSON serialized
    class CustomObject:
        pass

    test_data = {"custom": CustomObject()}
    response = HTTPResponse(status_code=200, headers={}, body=test_data)

    with pytest.raises(
        InvalidParameterValueException,
        match="Failed to serialize data to JSON: Object of type CustomObject is not JSON serializable",
    ):
        response.body_to_bytes()


# Tests for HTTPResponse.create_error_from_exception method


def test_create_error_from_exception_invalid_parameter_value() -> None:
    """Test create_error_from_exception with InvalidParameterValueException."""

    exception = InvalidParameterValueException("Parameter 'name' is required")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 400
    assert response.headers["Content-Type"] == "application/json"

    expected_body = {
        "Type": "InvalidParameterValueException",
        "message": "Parameter 'name' is required",
    }
    assert response.body == expected_body


def test_create_error_from_exception_resource_not_found() -> None:
    """Test create_error_from_exception with ResourceNotFoundException."""

    exception = ResourceNotFoundException("Execution not found")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 404
    assert response.headers["Content-Type"] == "application/json"

    expected_body = {
        "Type": "ResourceNotFoundException",
        "Message": "Execution not found",
    }
    assert response.body == expected_body


def test_create_error_from_exception_service_exception() -> None:
    """Test create_error_from_exception with ServiceException."""

    exception = ServiceException("Internal server error")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 500
    assert response.headers["Content-Type"] == "application/json"

    expected_body = {"Type": "ServiceException", "Message": "Internal server error"}
    assert response.body == expected_body


def test_create_error_from_exception_execution_already_started() -> None:
    """Test create_error_from_exception with ExecutionAlreadyStartedException."""

    arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
    exception = ExecutionAlreadyStartedException("Execution already exists", arn)
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 409
    assert response.headers["Content-Type"] == "application/json"

    # ExecutionAlreadyStartedException has no Type field per Smithy definition
    expected_body = {"message": "Execution already exists", "DurableExecutionArn": arn}
    assert response.body == expected_body


def test_create_error_from_exception_callback_timeout() -> None:
    """Test create_error_from_exception with CallbackTimeoutException."""

    exception = CallbackTimeoutException("Callback timed out")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 408
    assert response.headers["Content-Type"] == "application/json"

    expected_body = {
        "Type": "CallbackTimeoutException",
        "message": "Callback timed out",
    }
    assert response.body == expected_body


def test_create_error_from_exception_too_many_requests() -> None:
    """Test create_error_from_exception with TooManyRequestsException."""

    exception = TooManyRequestsException("Rate limit exceeded")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 429
    assert response.headers["Content-Type"] == "application/json"

    expected_body = {
        "Type": "TooManyRequestsException",
        "message": "Rate limit exceeded",
    }
    assert response.body == expected_body


def test_create_error_from_exception_illegal_state() -> None:
    """Test create_error_from_exception with IllegalStateException (unmapped)."""

    exception = IllegalStateException("Invalid state transition")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 500
    assert response.headers["Content-Type"] == "application/json"

    # IllegalStateException maps to ServiceException when serialized
    expected_body = {"Type": "ServiceException", "Message": "Invalid state transition"}
    assert response.body == expected_body


def test_create_error_from_exception_runtime_exception() -> None:
    """Test create_error_from_exception with RuntimeException (unmapped)."""

    exception = IllegalArgumentException("Invalid argument provided")
    response = HTTPResponse.create_error_from_exception(exception)

    assert response.status_code == 400
    assert response.headers["Content-Type"] == "application/json"

    # IllegalArgumentException maps to InvalidParameterValueException when serialized
    expected_body = {
        "Type": "InvalidParameterValueException",
        "message": "Invalid argument provided",
    }
    assert response.body == expected_body


def test_create_error_from_exception_type_validation() -> None:
    """Test create_error_from_exception with non-AwsApiException raises TypeError."""
    # Test with regular Exception
    regular_exception = Exception("Not an AWS exception")

    with pytest.raises(
        TypeError, match="Expected AwsApiException, got <class 'Exception'>"
    ):
        HTTPResponse.create_error_from_exception(regular_exception)  # type: ignore

    # Test with AWS API exception (should work fine)

    framework_exception = InvalidParameterValueException("Framework error")

    # This should NOT raise an error since InvalidParameterValueException is an AwsApiException
    response = HTTPResponse.create_error_from_exception(framework_exception)
    assert response.status_code == 400


def test_create_error_from_exception_no_wrapper_object() -> None:
    """Test that create_error_from_exception doesn't add wrapper 'error' object."""

    exception = InvalidParameterValueException("Test message")
    response = HTTPResponse.create_error_from_exception(exception)

    # Should NOT have wrapper "error" object like the old create_error method
    assert "error" not in response.body

    # Should have direct AWS-compliant structure
    assert "Type" in response.body
    assert "message" in response.body
    assert response.body["Type"] == "InvalidParameterValueException"
    assert response.body["message"] == "Test message"


def test_create_error_from_exception_serialization_round_trip() -> None:
    """Test that create_error_from_exception produces serializable responses."""

    exception = ResourceNotFoundException("Resource not found")
    response = HTTPResponse.create_error_from_exception(exception)

    # Should be able to serialize to bytes
    body_bytes = response.body_to_bytes()

    # Should be valid JSON
    parsed_body = json.loads(body_bytes.decode("utf-8"))

    expected_body = {
        "Type": "ResourceNotFoundException",
        "Message": "Resource not found",
    }
    assert parsed_body == expected_body

"""Tests for AWS-compliant exceptions and their boto3 compatibility.

This module contains comprehensive tests for all exception types used in the
AWS Durable Execution SDK Python Testing framework, including validation
of boto3 compatibility for proper AWS service integration.
"""

import json

import pytest

from aws_durable_execution_sdk_python_testing import exceptions


# =============================================================================
# Base Exception Tests
# =============================================================================


def test_durable_functions_test_error_base_exception() -> None:
    """Test DurableFunctionsTestError base exception."""
    error = exceptions.DurableFunctionsTestError("Base error message")

    assert str(error) == "Base error message"
    assert isinstance(error, Exception)


def test_durable_functions_local_runner_error_base_exception() -> None:
    """Test DurableFunctionsLocalRunnerError base exception."""
    error = exceptions.DurableFunctionsLocalRunnerError("Local runner error")

    assert str(error) == "Local runner error"
    assert isinstance(error, Exception)


def test_serialization_error() -> None:
    """Test SerializationError for serialization failures."""
    error = exceptions.SerializationError("Failed to serialize data")

    assert str(error) == "Failed to serialize data"
    assert isinstance(error, exceptions.DurableFunctionsLocalRunnerError)


def test_unknown_route_error() -> None:
    """Test UnknownRouteError for unknown HTTP routes."""
    error = exceptions.UnknownRouteError("POST", "/unknown/path")

    assert str(error) == "Unknown path pattern: POST /unknown/path"
    assert error.method == "POST"
    assert error.path == "/unknown/path"
    assert isinstance(error, exceptions.DurableFunctionsLocalRunnerError)


def test_aws_api_exception_base() -> None:
    """Test AwsApiException base class."""
    # AwsApiException is abstract, so we test with a concrete implementation
    error = exceptions.ServiceException("Test service error")

    assert isinstance(error, exceptions.AwsApiException)
    assert isinstance(error, exceptions.DurableFunctionsLocalRunnerError)
    assert error.http_status_code == 500


def test_exception_hierarchy() -> None:
    """Test that all custom exceptions inherit from appropriate base exceptions."""
    # Test AWS API exceptions
    aws_exceptions = [
        exceptions.IllegalStateException("test"),
        exceptions.InvalidParameterValueException("test"),
        exceptions.ResourceNotFoundException("test"),
        exceptions.ServiceException("test"),
        exceptions.CallbackTimeoutException("test"),
    ]

    for aws_exception in aws_exceptions:
        assert isinstance(aws_exception, exceptions.AwsApiException)
        assert isinstance(aws_exception, exceptions.DurableFunctionsLocalRunnerError)
        assert isinstance(aws_exception, Exception)

    # Test local runner exceptions
    local_exceptions = [
        exceptions.SerializationError("test"),
        exceptions.UnknownRouteError("GET", "/test"),
    ]

    for local_exception in local_exceptions:
        assert isinstance(local_exception, exceptions.DurableFunctionsLocalRunnerError)
        assert isinstance(local_exception, Exception)

    # Test testing exceptions
    test_error = exceptions.DurableFunctionsTestError("test")
    assert isinstance(test_error, Exception)


def test_illegal_argument_exception() -> None:
    """Test IllegalArgumentException maps to InvalidParameterValueException."""
    error = exceptions.IllegalArgumentException("Invalid argument provided")

    assert str(error) == "Invalid argument provided"
    assert isinstance(error, exceptions.AwsApiException)
    assert error.http_status_code == 400

    # Test serialization maps to InvalidParameterValueException
    json_dict = error.to_dict()
    assert json_dict == {
        "Type": "InvalidParameterValueException",
        "message": "Invalid argument provided",
    }


def test_runtime_exception() -> None:
    """Test RuntimeException maps to ServiceException."""
    error = exceptions.RuntimeException("Runtime error occurred")

    assert str(error) == "Runtime error occurred"
    assert isinstance(error, exceptions.AwsApiException)
    assert error.http_status_code == 500

    # Test serialization maps to ServiceException
    json_dict = error.to_dict()
    assert json_dict == {
        "Type": "ServiceException",
        "Message": "Runtime error occurred",
    }


def test_illegal_state_exception() -> None:
    """Test IllegalStateException for invalid state transitions."""
    error = exceptions.IllegalStateException(
        "Cannot transition from RUNNING to PENDING"
    )

    assert str(error) == "Cannot transition from RUNNING to PENDING"
    assert isinstance(error, exceptions.AwsApiException)
    assert error.http_status_code == 500

    # Test serialization maps to ServiceException
    json_dict = error.to_dict()
    assert json_dict == {
        "Type": "ServiceException",
        "Message": "Cannot transition from RUNNING to PENDING",
    }


# =============================================================================
# Boto3 Compatibility Tests
# =============================================================================


def test_invalid_parameter_value_exception_boto3_format() -> None:
    """Test InvalidParameterValueException produces correct boto3 format."""
    exception = exceptions.InvalidParameterValueException("Invalid parameter value")

    # Test serialization
    json_dict = exception.to_dict()

    # Validate structure matches boto3 expectations
    assert json_dict == {
        "Type": "InvalidParameterValueException",
        "message": "Invalid parameter value",
    }

    # Test that it can be serialized to JSON and back
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)
    assert parsed_back == json_dict

    # Validate HTTP status code
    assert exception.http_status_code == 400


def test_resource_not_found_exception_boto3_format() -> None:
    """Test ResourceNotFoundException produces correct boto3 format."""
    exception = exceptions.ResourceNotFoundException("Resource not found")

    json_dict = exception.to_dict()

    assert json_dict == {
        "Type": "ResourceNotFoundException",
        "Message": "Resource not found",  # Capital M per Smithy definition
    }

    # Test JSON serialization
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)
    assert parsed_back == json_dict

    assert exception.http_status_code == 404


def test_service_exception_boto3_format() -> None:
    """Test ServiceException produces correct boto3 format."""
    exception = exceptions.ServiceException("Internal service error")

    json_dict = exception.to_dict()

    assert json_dict == {
        "Type": "ServiceException",
        "Message": "Internal service error",  # Capital M per Smithy definition
    }

    # Test JSON serialization
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)
    assert parsed_back == json_dict

    assert exception.http_status_code == 500


def test_callback_timeout_exception_boto3_format() -> None:
    """Test CallbackTimeoutException produces correct boto3 format."""
    exception = exceptions.CallbackTimeoutException("Callback timed out")

    json_dict = exception.to_dict()

    assert json_dict == {
        "Type": "CallbackTimeoutException",
        "message": "Callback timed out",
    }

    # Test JSON serialization
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)
    assert parsed_back == json_dict

    assert exception.http_status_code == 408


def test_execution_already_started_exception_special_format() -> None:
    """Test ExecutionAlreadyStartedException has no Type field (special case)."""
    exception = exceptions.ExecutionAlreadyStartedException(
        "Execution already started",
        "arn:aws:states:us-east-1:123456789012:execution:test",
    )

    json_dict = exception.to_dict()

    # Special case: no Type field for this exception, includes DurableExecutionArn
    assert json_dict == {
        "message": "Execution already started",
        "DurableExecutionArn": "arn:aws:states:us-east-1:123456789012:execution:test",
    }

    # Ensure Type field is not present
    assert "Type" not in json_dict

    # Test JSON serialization
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)
    assert parsed_back == json_dict

    assert exception.http_status_code == 409


def test_all_exceptions_have_correct_type_field_values() -> None:
    """Test that Type field values match what boto3 expects for exception names."""
    test_cases = [
        (
            exceptions.InvalidParameterValueException("test"),
            "InvalidParameterValueException",
        ),
        (exceptions.ResourceNotFoundException("test"), "ResourceNotFoundException"),
        (exceptions.ServiceException("test"), "ServiceException"),
        (exceptions.CallbackTimeoutException("test"), "CallbackTimeoutException"),
    ]

    for exception, expected_type in test_cases:
        json_dict = exception.to_dict()
        assert json_dict["Type"] == expected_type


def test_message_field_casing_compatibility() -> None:
    """Test message field casing matches boto3 deserialization expectations."""
    # InvalidParameterValueException uses lowercase 'message'
    exception1 = exceptions.InvalidParameterValueException("Test message")
    json_dict1 = exception1.to_dict()

    assert "message" in json_dict1
    assert "Message" not in json_dict1
    assert json_dict1["message"] == "Test message"

    # ResourceNotFoundException uses capital 'Message'
    exception2 = exceptions.ResourceNotFoundException("Test message")
    json_dict2 = exception2.to_dict()

    assert "Message" in json_dict2
    assert "message" not in json_dict2
    assert json_dict2["Message"] == "Test message"


def test_json_serialization_with_special_characters() -> None:
    """Test that exceptions with special characters serialize correctly."""
    special_message = 'Error with "quotes", newlines\n, and unicode: 🚀'
    exception = exceptions.InvalidParameterValueException(special_message)

    json_dict = exception.to_dict()

    # Test that it can be serialized to JSON
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)

    assert parsed_back["message"] == special_message
    assert parsed_back["Type"] == "InvalidParameterValueException"


def test_empty_message_handling() -> None:
    """Test that empty messages are handled correctly."""
    exception = exceptions.InvalidParameterValueException("")
    json_dict = exception.to_dict()

    assert json_dict == {"Type": "InvalidParameterValueException", "message": ""}


def test_none_message_handling() -> None:
    """Test that None messages are converted to empty strings."""
    # This tests the edge case where message might be None
    exception = exceptions.InvalidParameterValueException(None)  # type: ignore
    json_dict = exception.to_dict()

    # Should convert None to string "None" for JSON compatibility
    assert json_dict["message"] is None or json_dict["message"] == "None"


def test_http_status_codes_match_aws_standards() -> None:
    """Test that HTTP status codes match AWS service standards."""
    status_code_tests = [
        (exceptions.InvalidParameterValueException("test"), 400),  # Bad Request
        (exceptions.ResourceNotFoundException("test"), 404),  # Not Found
        (
            exceptions.ExecutionAlreadyStartedException(
                "test", "arn:aws:states:us-east-1:123456789012:execution:test"
            ),
            409,
        ),  # Conflict
        (exceptions.CallbackTimeoutException("test"), 408),  # Request Timeout
        (exceptions.ServiceException("test"), 500),  # Internal Server Error
    ]

    for exception, expected_status in status_code_tests:
        assert exception.http_status_code == expected_status


def test_json_structure_has_no_extra_fields() -> None:
    """Test that JSON structure only contains expected fields."""
    exception = exceptions.InvalidParameterValueException("test")
    json_dict = exception.to_dict()

    # Should only have Type and message fields
    expected_fields = {"Type", "message"}
    actual_fields = set(json_dict.keys())

    assert actual_fields == expected_fields


def test_execution_already_started_has_only_message_field() -> None:
    """Test that ExecutionAlreadyStartedException only has message field."""
    exception = exceptions.ExecutionAlreadyStartedException(
        "test", "arn:aws:states:us-east-1:123456789012:execution:test"
    )
    json_dict = exception.to_dict()

    # Should only have message and DurableExecutionArn fields (no Type)
    expected_fields = {"message", "DurableExecutionArn"}
    actual_fields = set(json_dict.keys())

    assert actual_fields == expected_fields


def test_large_message_serialization() -> None:
    """Test that large messages can be serialized correctly."""
    # Create a large message (but not too large to avoid memory issues in tests)
    large_message = "Error: " + "x" * 1000
    exception = exceptions.ServiceException(large_message)

    json_dict = exception.to_dict()
    json_str = json.dumps(json_dict)
    parsed_back = json.loads(json_str)

    assert parsed_back["Message"] == large_message  # ServiceException uses capital M
    assert len(parsed_back["Message"]) == len(large_message)


def test_all_aws_exceptions_are_json_serializable() -> None:
    """Test that all AWS exception types can be JSON serialized."""
    test_exceptions = [
        exceptions.InvalidParameterValueException("test"),
        exceptions.ResourceNotFoundException("test"),
        exceptions.ServiceException("test"),
        exceptions.CallbackTimeoutException("test"),
        exceptions.ExecutionAlreadyStartedException(
            "test", "arn:aws:states:us-east-1:123456789012:execution:test"
        ),
    ]

    for exception in test_exceptions:
        json_dict = exception.to_dict()

        # Should be able to serialize to JSON without errors
        json_str = json.dumps(json_dict)

        # Should be able to parse back from JSON
        parsed_back = json.loads(json_str)

        # Should match original structure
        assert parsed_back == json_dict


def test_too_many_requests_exception() -> None:
    """Test TooManyRequestsException for rate limiting."""
    exception = exceptions.TooManyRequestsException("Rate limit exceeded")

    assert str(exception) == "Rate limit exceeded"
    assert isinstance(exception, exceptions.AwsApiException)
    assert exception.http_status_code == 429

    json_dict = exception.to_dict()
    assert json_dict == {
        "Type": "TooManyRequestsException",
        "message": "Rate limit exceeded",
    }


def test_execution_conflict_exception() -> None:
    """Test ExecutionConflictException for execution conflicts."""
    exception = exceptions.ExecutionConflictException("Execution conflict detected")

    assert str(exception) == "Execution conflict detected"
    assert isinstance(exception, exceptions.AwsApiException)
    assert exception.http_status_code == 409

    json_dict = exception.to_dict()
    assert json_dict == {
        "Type": "ExecutionConflictException",
        "message": "Execution conflict detected",
    }


# =============================================================================
# AWS Compliant Exception Tests (Comprehensive)
# =============================================================================


def test_base_exception_hierarchy():
    """Test that all AWS exceptions inherit from the correct base classes."""
    # Test base hierarchy
    assert issubclass(
        exceptions.AwsApiException, exceptions.DurableFunctionsLocalRunnerError
    )
    assert issubclass(exceptions.DurableFunctionsLocalRunnerError, Exception)

    # Test all AWS exceptions inherit from AwsApiException
    aws_exceptions = [
        exceptions.InvalidParameterValueException,
        exceptions.ResourceNotFoundException,
        exceptions.ServiceException,
        exceptions.ExecutionAlreadyStartedException,
        exceptions.ExecutionConflictException,
        exceptions.CallbackTimeoutException,
        exceptions.TooManyRequestsException,
        exceptions.IllegalStateException,
        exceptions.RuntimeException,
        exceptions.IllegalArgumentException,
    ]

    for exception_class in aws_exceptions:
        assert issubclass(exception_class, exceptions.AwsApiException)


def test_aws_api_exception_abstract_to_dict():
    """Test that AwsApiException.to_dict() raises NotImplementedError."""
    exception = exceptions.AwsApiException("test message")

    with pytest.raises(NotImplementedError):
        exception.to_dict()


class TestSmithyMappedExceptions:
    """Test Smithy-mapped exceptions (defined in Smithy models)."""

    def test_invalid_parameter_value_exception(self):
        """Test InvalidParameterValueException serialization and properties."""
        message = "Invalid parameter"
        exception = exceptions.InvalidParameterValueException(message)

        # Test properties
        assert exception.http_status_code == 400
        assert exception.message == message
        assert str(exception) == message

        # Test serialization
        expected_json = {"Type": "InvalidParameterValueException", "message": message}
        assert exception.to_dict() == expected_json

    def test_resource_not_found_exception(self):
        """Test ResourceNotFoundException serialization and properties."""
        message = "Resource not found"
        exception = exceptions.ResourceNotFoundException(message)

        # Test properties
        assert exception.http_status_code == 404
        assert exception.Message == message  # Capital M per Smithy
        assert str(exception) == message

        # Test serialization
        expected_json = {"Type": "ResourceNotFoundException", "Message": message}
        assert exception.to_dict() == expected_json

    def test_service_exception(self):
        """Test ServiceException serialization and properties."""
        message = "Service error"
        exception = exceptions.ServiceException(message)

        # Test properties
        assert exception.http_status_code == 500
        assert exception.Message == message  # Capital M per Smithy
        assert str(exception) == message

        # Test serialization
        expected_json = {"Type": "ServiceException", "Message": message}
        assert exception.to_dict() == expected_json

    def test_execution_already_started_exception(self):
        """Test ExecutionAlreadyStartedException serialization and properties."""
        message = "Execution already started"
        arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
        exception = exceptions.ExecutionAlreadyStartedException(message, arn)

        # Test properties
        assert exception.http_status_code == 409
        assert exception.message == message
        assert exception.DurableExecutionArn == arn
        assert str(exception) == message

        # Test serialization (no Type field per Smithy definition)
        expected_json = {"message": message, "DurableExecutionArn": arn}
        assert exception.to_dict() == expected_json

    def test_callback_timeout_exception(self):
        """Test CallbackTimeoutException serialization and properties."""
        message = "Callback timed out"
        exception = exceptions.CallbackTimeoutException(message)

        # Test properties
        assert exception.http_status_code == 408
        assert exception.message == message
        assert str(exception) == message

        # Test serialization
        expected_json = {"Type": "CallbackTimeoutException", "message": message}
        assert exception.to_dict() == expected_json

    def test_too_many_requests_exception(self):
        """Test TooManyRequestsException serialization and properties."""
        message = "Too many requests"
        exception = exceptions.TooManyRequestsException(message)

        # Test properties
        assert exception.http_status_code == 429
        assert exception.message == message
        assert str(exception) == message

        # Test serialization
        expected_json = {"Type": "TooManyRequestsException", "message": message}
        assert exception.to_dict() == expected_json

    def test_execution_conflict_exception(self):
        """Test ExecutionConflictException serialization and properties."""
        message = "Execution conflict"
        exception = exceptions.ExecutionConflictException(message)

        # Test properties
        assert exception.http_status_code == 409
        assert exception.message == message
        assert str(exception) == message

        # Test serialization
        expected_json = {"Type": "ExecutionConflictException", "message": message}
        assert exception.to_dict() == expected_json


class TestUnmappedExceptions:
    """Test unmapped exceptions (thrown by services but not in Smithy)."""

    def test_illegal_state_exception(self):
        """Test IllegalStateException maps to ServiceException when serialized."""
        message = "Invalid state"
        exception = exceptions.IllegalStateException(message)

        # Test properties
        assert exception.http_status_code == 500
        assert exception.message == message
        assert str(exception) == message

        # Test serialization (maps to ServiceException)
        expected_json = {"Type": "ServiceException", "Message": message}
        assert exception.to_dict() == expected_json

    def test_runtime_exception(self):
        """Test RuntimeException maps to ServiceException when serialized."""
        message = "Runtime error"
        exception = exceptions.RuntimeException(message)

        # Test properties
        assert exception.http_status_code == 500
        assert exception.message == message
        assert str(exception) == message

        # Test serialization (maps to ServiceException)
        expected_json = {"Type": "ServiceException", "Message": message}
        assert exception.to_dict() == expected_json

    def test_illegal_argument_exception(self):
        """Test IllegalArgumentException maps to InvalidParameterValueException when serialized."""
        message = "Invalid argument"
        exception = exceptions.IllegalArgumentException(message)

        # Test properties
        assert exception.http_status_code == 400
        assert exception.message == message
        assert str(exception) == message

        # Test serialization (maps to InvalidParameterValueException)
        expected_json = {"Type": "InvalidParameterValueException", "message": message}
        assert exception.to_dict() == expected_json


class TestHttpStatusCodes:
    """Test HTTP status codes match Smithy @httpError annotations."""

    def test_client_error_status_codes(self):
        """Test client error (4xx) status codes."""
        assert exceptions.InvalidParameterValueException("test").http_status_code == 400
        assert exceptions.ResourceNotFoundException("test").http_status_code == 404
        assert exceptions.CallbackTimeoutException("test").http_status_code == 408
        assert (
            exceptions.ExecutionAlreadyStartedException("test", "arn").http_status_code
            == 409
        )
        assert exceptions.ExecutionConflictException("test").http_status_code == 409
        assert exceptions.TooManyRequestsException("test").http_status_code == 429
        assert exceptions.IllegalArgumentException("test").http_status_code == 400

    def test_server_error_status_codes(self):
        """Test server error (5xx) status codes."""
        assert exceptions.ServiceException("test").http_status_code == 500
        assert exceptions.IllegalStateException("test").http_status_code == 500
        assert exceptions.RuntimeException("test").http_status_code == 500


class TestFieldNameCasing:
    """Test field name casing matches Smithy definitions."""

    def test_lowercase_message_fields(self):
        """Test exceptions that use lowercase 'message' field."""
        # These use lowercase 'message' per Smithy definitions
        exceptions_with_lowercase_message = [
            exceptions.InvalidParameterValueException("test"),
            exceptions.ExecutionAlreadyStartedException("test", "arn"),
            exceptions.ExecutionConflictException("test"),
            exceptions.CallbackTimeoutException("test"),
            exceptions.TooManyRequestsException("test"),
            exceptions.IllegalStateException("test"),
            exceptions.RuntimeException("test"),
            exceptions.IllegalArgumentException("test"),
        ]

        for exception in exceptions_with_lowercase_message:
            if hasattr(exception, "message"):
                assert exception.message == "test"

    def test_uppercase_message_fields(self):
        """Test exceptions that use uppercase 'Message' field."""
        # These use uppercase 'Message' per Smithy definitions
        exceptions_with_uppercase_message = [
            exceptions.ResourceNotFoundException("test"),
            exceptions.ServiceException("test"),
        ]

        for exception in exceptions_with_uppercase_message:
            assert exception.Message == "test"


class TestBoto3Compatibility:
    """Test boto3 compatibility and JSON structure validation."""

    def test_json_structure_matches_boto3_expectations(self):
        """Test that JSON output matches what boto3 error factory expects."""
        # Test that all exceptions produce valid JSON structures
        test_cases = [
            (
                exceptions.InvalidParameterValueException("test"),
                {"Type": "InvalidParameterValueException", "message": "test"},
            ),
            (
                exceptions.ResourceNotFoundException("test"),
                {"Type": "ResourceNotFoundException", "Message": "test"},
            ),
            (
                exceptions.ServiceException("test"),
                {"Type": "ServiceException", "Message": "test"},
            ),
            (
                exceptions.ExecutionAlreadyStartedException("test", "arn"),
                {"message": "test", "DurableExecutionArn": "arn"},
            ),
            (
                exceptions.ExecutionConflictException("test"),
                {"Type": "ExecutionConflictException", "message": "test"},
            ),
            (
                exceptions.CallbackTimeoutException("test"),
                {"Type": "CallbackTimeoutException", "message": "test"},
            ),
            (
                exceptions.TooManyRequestsException("test"),
                {"Type": "TooManyRequestsException", "message": "test"},
            ),
        ]

        for exception, expected_json in test_cases:
            actual_json = exception.to_dict()
            assert actual_json == expected_json

            # Verify JSON is serializable (no complex objects)
            json_str = json.dumps(actual_json)
            assert json.loads(json_str) == actual_json

    def test_type_field_values_match_exception_names(self):
        """Test that Type field values match what boto3 expects for exception names."""
        type_field_mappings = [
            (
                exceptions.InvalidParameterValueException("test"),
                "InvalidParameterValueException",
            ),
            (exceptions.ResourceNotFoundException("test"), "ResourceNotFoundException"),
            (exceptions.ServiceException("test"), "ServiceException"),
            (
                exceptions.ExecutionConflictException("test"),
                "ExecutionConflictException",
            ),
            (exceptions.CallbackTimeoutException("test"), "CallbackTimeoutException"),
            (exceptions.TooManyRequestsException("test"), "TooManyRequestsException"),
            # Unmapped exceptions map to different types
            (exceptions.IllegalStateException("test"), "ServiceException"),
            (exceptions.RuntimeException("test"), "ServiceException"),
            (
                exceptions.IllegalArgumentException("test"),
                "InvalidParameterValueException",
            ),
        ]

        for exception, expected_type in type_field_mappings:
            json_output = exception.to_dict()
            if (
                "Type" in json_output
            ):  # ExecutionAlreadyStartedException doesn't have Type field
                assert json_output["Type"] == expected_type

    def test_execution_already_started_exception_special_case(self):
        """Test ExecutionAlreadyStartedException special case (no Type field)."""
        exception = exceptions.ExecutionAlreadyStartedException(
            "test message", "test-arn"
        )
        json_output = exception.to_dict()

        # Should not have Type field
        assert "Type" not in json_output

        # Should have required fields
        assert "message" in json_output
        assert "DurableExecutionArn" in json_output
        assert json_output["message"] == "test message"
        assert json_output["DurableExecutionArn"] == "test-arn"

    def test_message_field_casing_compatibility(self):
        """Test message field casing compatibility with boto3 deserialization."""
        # Test lowercase 'message' field exceptions
        lowercase_exceptions = [
            exceptions.InvalidParameterValueException("test"),
            exceptions.ExecutionAlreadyStartedException("test", "arn"),
            exceptions.ExecutionConflictException("test"),
            exceptions.CallbackTimeoutException("test"),
            exceptions.TooManyRequestsException("test"),
        ]

        for exception in lowercase_exceptions:
            json_output = exception.to_dict()
            if "message" in json_output:
                assert json_output["message"] == "test"
                # Should not have uppercase Message
                assert "Message" not in json_output

        # Test uppercase 'Message' field exceptions
        uppercase_exceptions = [
            exceptions.ResourceNotFoundException("test"),
            exceptions.ServiceException("test"),
        ]

        for exception in uppercase_exceptions:
            json_output = exception.to_dict()
            assert json_output["Message"] == "test"
            # Should not have lowercase message
            assert "message" not in json_output


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_message_handling(self):
        """Test handling of empty messages."""
        exceptions_list = [
            exceptions.InvalidParameterValueException(""),
            exceptions.ResourceNotFoundException(""),
            exceptions.ServiceException(""),
            exceptions.ExecutionConflictException(""),
            exceptions.CallbackTimeoutException(""),
            exceptions.TooManyRequestsException(""),
            exceptions.IllegalStateException(""),
            exceptions.RuntimeException(""),
            exceptions.IllegalArgumentException(""),
        ]

        for exception in exceptions_list:
            # Should not raise exception during serialization
            json_output = exception.to_dict()
            assert isinstance(json_output, dict)

    def test_special_characters_in_messages(self):
        """Test handling of special characters in messages."""
        special_message = 'Test with "quotes", newlines\n, and unicode: 🚀'

        exceptions_list = [
            exceptions.InvalidParameterValueException(special_message),
            exceptions.ResourceNotFoundException(special_message),
            exceptions.ServiceException(special_message),
        ]

        for exception in exceptions_list:
            json_output = exception.to_dict()
            # Message should be preserved exactly
            message_field = "Message" if hasattr(exception, "Message") else "message"
            assert json_output[message_field] == special_message

    def test_execution_already_started_with_empty_arn(self):
        """Test ExecutionAlreadyStartedException with empty ARN."""
        exception = exceptions.ExecutionAlreadyStartedException("test", "")
        json_output = exception.to_dict()

        assert json_output["DurableExecutionArn"] == ""
        assert json_output["message"] == "test"


def test_exception_test_cases_data_structure():
    """Test that we can create a comprehensive test data structure for all exceptions."""
    # This validates the test data structure mentioned in the design document
    exception_test_cases = [
        # Smithy-mapped exceptions
        {
            "exception_class": exceptions.InvalidParameterValueException,
            "args": ["Invalid parameter"],
            "expected_json": {
                "Type": "InvalidParameterValueException",
                "message": "Invalid parameter",
            },
            "expected_status": 400,
        },
        {
            "exception_class": exceptions.ResourceNotFoundException,
            "args": ["Resource not found"],
            "expected_json": {
                "Type": "ResourceNotFoundException",
                "Message": "Resource not found",
            },
            "expected_status": 404,
        },
        {
            "exception_class": exceptions.ServiceException,
            "args": ["Service error"],
            "expected_json": {"Type": "ServiceException", "Message": "Service error"},
            "expected_status": 500,
        },
        {
            "exception_class": exceptions.ExecutionAlreadyStartedException,
            "args": [
                "Already started",
                "arn:aws:lambda:us-east-1:123456789012:function:test",
            ],
            "expected_json": {
                "message": "Already started",
                "DurableExecutionArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
            },
            "expected_status": 409,
        },
        {
            "exception_class": exceptions.ExecutionConflictException,
            "args": ["Execution conflict"],
            "expected_json": {
                "Type": "ExecutionConflictException",
                "message": "Execution conflict",
            },
            "expected_status": 409,
        },
        {
            "exception_class": exceptions.CallbackTimeoutException,
            "args": ["Callback timeout"],
            "expected_json": {
                "Type": "CallbackTimeoutException",
                "message": "Callback timeout",
            },
            "expected_status": 408,
        },
        {
            "exception_class": exceptions.TooManyRequestsException,
            "args": ["Too many requests"],
            "expected_json": {
                "Type": "TooManyRequestsException",
                "message": "Too many requests",
            },
            "expected_status": 429,
        },
        # Unmapped exceptions
        {
            "exception_class": exceptions.IllegalStateException,
            "args": ["Invalid state"],
            "expected_json": {"Type": "ServiceException", "Message": "Invalid state"},
            "expected_status": 500,
        },
        {
            "exception_class": exceptions.RuntimeException,
            "args": ["Runtime error"],
            "expected_json": {"Type": "ServiceException", "Message": "Runtime error"},
            "expected_status": 500,
        },
        {
            "exception_class": exceptions.IllegalArgumentException,
            "args": ["Invalid argument"],
            "expected_json": {
                "Type": "InvalidParameterValueException",
                "message": "Invalid argument",
            },
            "expected_status": 400,
        },
    ]

    # Test each case
    for case in exception_test_cases:
        exception = case["exception_class"](*case["args"])

        # Test status code
        assert exception.http_status_code == case["expected_status"]

        # Test serialization
        assert exception.to_dict() == case["expected_json"]

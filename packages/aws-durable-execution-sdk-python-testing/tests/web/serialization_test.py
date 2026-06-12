"""Tests for serialization interfaces and AWS boto integration."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import json
from datetime import datetime, timezone

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.web.serialization import (
    JSONSerializer,
    AwsRestJsonDeserializer,
    AwsRestJsonSerializer,
)


def test_aws_rest_json_serializer_should_initialize_and_serialize_data():
    """Test that serializer initializes and can serialize data through public API."""
    # Arrange
    operation_name = "StartDurableExecution"
    mock_serializer = Mock()
    mock_operation_model = Mock()
    mock_serializer.serialize_to_request.return_value = {"body": '{"test": "data"}'}

    # Act
    serializer = AwsRestJsonSerializer(
        operation_name, mock_serializer, mock_operation_model
    )
    result = serializer.to_bytes({"test": "data"})

    # Assert - Test public behavior only
    assert isinstance(result, bytes)
    assert result == b'{"test": "data"}'
    mock_serializer.serialize_to_request.assert_called_once_with(
        {"test": "data"}, mock_operation_model
    )


@patch("aws_durable_execution_sdk_python_testing.web.serialization.create_serializer")
@patch("aws_durable_execution_sdk_python_testing.web.serialization.ServiceModel")
@patch(
    "aws_durable_execution_sdk_python_testing.web.serialization.botocore.loaders.Loader"
)
@patch("aws_durable_execution_sdk_python_testing.web.serialization.os.path.dirname")
def test_aws_rest_json_serializer_should_create_serializer_with_boto_components(
    mock_dirname,
    mock_loader_class,
    mock_service_model_class,
    mock_create_serializer,
):
    """Test that create method sets up boto components correctly."""
    # Arrange
    operation_name = "StartDurableExecution"
    mock_package_path = "/path/to/package"
    mock_dirname.return_value = mock_package_path

    mock_loader = Mock()
    mock_loader_class.return_value = mock_loader
    mock_raw_model = {"operations": {}}
    mock_loader.load_service_model.return_value = mock_raw_model

    mock_service_model = Mock()
    mock_service_model_class.return_value = mock_service_model
    mock_operation_model = Mock()
    mock_service_model.operation_model.return_value = mock_operation_model

    mock_serializer = Mock()
    mock_create_serializer.return_value = mock_serializer

    # Act
    result = AwsRestJsonSerializer.create(operation_name)

    # Assert - Test public behavior only
    assert isinstance(result, AwsRestJsonSerializer)

    # Test that the created serializer can actually serialize data
    mock_serializer.serialize_to_request.return_value = {"body": '{"test": "value"}'}
    serialized_data = result.to_bytes({"test": "value"})
    assert isinstance(serialized_data, bytes)
    assert serialized_data == b'{"test": "value"}'

    # Verify boto setup calls
    mock_loader.load_service_model.assert_called_once_with("lambda", "service-2")
    mock_service_model_class.assert_called_once_with(mock_raw_model)
    mock_create_serializer.assert_called_once_with("rest-json", include_validation=True)
    mock_service_model.operation_model.assert_called_once_with(operation_name)


@patch("aws_durable_execution_sdk_python_testing.web.serialization.create_serializer")
def test_aws_rest_json_serializer_should_raise_serialization_error_when_create_fails(
    mock_create_serializer,
):
    """Test that create method raises InvalidParameterValueException when boto setup fails."""
    # Arrange
    operation_name = "StartDurableExecution"
    mock_create_serializer.side_effect = Exception("Boto error")

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        AwsRestJsonSerializer.create(operation_name)

    assert "Failed to create serializer for StartDurableExecution" in str(
        exc_info.value
    )


def test_aws_rest_json_serializer_should_serialize_data_to_bytes():
    """Test that to_bytes method serializes data using boto serializer."""
    # Arrange
    mock_serializer = Mock()
    mock_operation_model = Mock()
    serializer = AwsRestJsonSerializer("test", mock_serializer, mock_operation_model)

    test_data = {"key": "value"}
    serialized_response = {"body": '{"key": "value"}'}
    mock_serializer.serialize_to_request.return_value = serialized_response

    # Act
    result = serializer.to_bytes(test_data)

    # Assert
    assert result == b'{"key": "value"}'
    mock_serializer.serialize_to_request.assert_called_once_with(
        test_data, mock_operation_model
    )


def test_aws_rest_json_serializer_should_handle_bytes_body_in_serialization():
    """Test that to_bytes method handles bytes body from boto serializer."""
    # Arrange
    mock_serializer = Mock()
    mock_operation_model = Mock()
    serializer = AwsRestJsonSerializer("test", mock_serializer, mock_operation_model)

    test_data = {"key": "value"}
    serialized_response = {"body": b'{"key": "value"}'}
    mock_serializer.serialize_to_request.return_value = serialized_response

    # Act
    result = serializer.to_bytes(test_data)

    # Assert
    assert result == b'{"key": "value"}'


def test_aws_rest_json_serializer_should_handle_empty_body_in_serialization():
    """Test that to_bytes method handles empty body from boto serializer."""
    # Arrange
    mock_serializer = Mock()
    mock_operation_model = Mock()
    serializer = AwsRestJsonSerializer("test", mock_serializer, mock_operation_model)

    test_data = {"key": "value"}
    serialized_response = {}
    mock_serializer.serialize_to_request.return_value = serialized_response

    # Act
    result = serializer.to_bytes(test_data)

    # Assert
    assert result == b""


def test_aws_rest_json_serializer_should_raise_error_when_serializer_not_initialized():
    """Test that to_bytes raises error when serializer is not initialized."""
    # Arrange
    serializer = AwsRestJsonSerializer("test", None, None)
    test_data = {"key": "value"}

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        serializer.to_bytes(test_data)

    assert "Serializer not initialized for test" in str(exc_info.value)


def test_aws_rest_json_serializer_should_raise_error_when_serialization_fails():
    """Test that to_bytes raises InvalidParameterValueException when boto serialization fails."""
    # Arrange
    mock_serializer = Mock()
    mock_operation_model = Mock()
    serializer = AwsRestJsonSerializer("test", mock_serializer, mock_operation_model)

    test_data = {"key": "value"}
    mock_serializer.serialize_to_request.side_effect = Exception("Serialization failed")

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        serializer.to_bytes(test_data)

    assert "Failed to serialize data for test" in str(exc_info.value)


def test_aws_rest_json_deserializer_should_initialize_and_deserialize_data():
    """Test that deserializer initializes and can deserialize data through public API."""
    # Arrange
    operation_name = "StartDurableExecution"
    mock_parser = Mock()
    mock_operation_model = Mock()
    mock_output_shape = Mock()
    mock_operation_model.output_shape = mock_output_shape
    mock_parser.parse.return_value = {"test": "data"}

    # Act
    deserializer = AwsRestJsonDeserializer(
        operation_name, mock_parser, mock_operation_model
    )
    result = deserializer.from_bytes(b'{"test": "data"}')

    # Assert - Test public behavior only
    assert isinstance(result, dict)
    assert result == {"test": "data"}
    expected_response_dict = {
        "body": b'{"test": "data"}',
        "headers": {"content-type": "application/json"},
        "status_code": 200,
    }
    mock_parser.parse.assert_called_once_with(expected_response_dict, mock_output_shape)


@patch("aws_durable_execution_sdk_python_testing.web.serialization.create_parser")
@patch("aws_durable_execution_sdk_python_testing.web.serialization.ServiceModel")
@patch(
    "aws_durable_execution_sdk_python_testing.web.serialization.botocore.loaders.Loader"
)
@patch("aws_durable_execution_sdk_python_testing.web.serialization.os.path.dirname")
def test_aws_rest_json_deserializer_should_create_deserializer_with_boto_components(
    mock_dirname,
    mock_loader_class,
    mock_service_model_class,
    mock_create_parser,
):
    """Test that create method sets up boto components correctly."""
    # Arrange
    operation_name = "StartDurableExecution"
    mock_package_path = "/path/to/package"
    mock_dirname.return_value = mock_package_path

    mock_loader = Mock()
    mock_loader_class.return_value = mock_loader
    mock_raw_model = {"operations": {}}
    mock_loader.load_service_model.return_value = mock_raw_model

    mock_service_model = Mock()
    mock_service_model_class.return_value = mock_service_model
    mock_operation_model = Mock()
    mock_service_model.operation_model.return_value = mock_operation_model

    mock_parser = Mock()
    mock_create_parser.return_value = mock_parser

    # Act
    result = AwsRestJsonDeserializer.create(operation_name)

    # Assert - Test public behavior only
    assert isinstance(result, AwsRestJsonDeserializer)

    # Test that the created deserializer can actually deserialize data
    mock_output_shape = Mock()
    mock_operation_model.output_shape = mock_output_shape
    mock_parser.parse.return_value = {"test": "value"}
    deserialized_data = result.from_bytes(b'{"test": "value"}')
    assert isinstance(deserialized_data, dict)
    assert deserialized_data == {"test": "value"}

    # Verify boto setup calls
    mock_loader.load_service_model.assert_called_once_with("lambda", "service-2")
    mock_service_model_class.assert_called_once_with(mock_raw_model)
    mock_create_parser.assert_called_once_with("rest-json")
    mock_service_model.operation_model.assert_called_once_with(operation_name)


@patch("aws_durable_execution_sdk_python_testing.web.serialization.create_parser")
def test_aws_rest_json_deserializer_should_raise_serialization_error_when_create_fails(
    mock_create_parser,
):
    """Test that create method raises InvalidParameterValueException when boto setup fails."""
    # Arrange
    operation_name = "StartDurableExecution"
    mock_create_parser.side_effect = Exception("Boto error")

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        AwsRestJsonDeserializer.create(operation_name)

    assert "Failed to create deserializer for StartDurableExecution" in str(
        exc_info.value
    )


def test_aws_rest_json_deserializer_should_deserialize_bytes_with_output_shape():
    """Test that from_bytes method deserializes data using boto parser with output shape."""
    # Arrange
    mock_parser = Mock()
    mock_operation_model = Mock()
    mock_output_shape = Mock()
    mock_operation_model.output_shape = mock_output_shape
    deserializer = AwsRestJsonDeserializer("test", mock_parser, mock_operation_model)

    test_bytes = b'{"key": "value"}'
    parsed_data = {"key": "value"}
    mock_parser.parse.return_value = parsed_data

    # Act
    result = deserializer.from_bytes(test_bytes)

    # Assert
    assert result == parsed_data
    expected_response_dict = {
        "body": test_bytes,
        "headers": {"content-type": "application/json"},
        "status_code": 200,
    }
    mock_parser.parse.assert_called_once_with(expected_response_dict, mock_output_shape)


def test_aws_rest_json_deserializer_should_deserialize_bytes_without_output_shape():
    """Test that from_bytes method falls back to JSON parsing when no output shape."""
    # Arrange
    mock_parser = Mock()
    mock_operation_model = Mock()
    mock_operation_model.output_shape = None
    deserializer = AwsRestJsonDeserializer("test", mock_parser, mock_operation_model)

    test_bytes = b'{"key": "value"}'

    # Act
    result = deserializer.from_bytes(test_bytes)

    # Assert
    assert result == {"key": "value"}
    mock_parser.parse.assert_not_called()


def test_aws_rest_json_deserializer_should_raise_error_when_parser_not_initialized():
    """Test that from_bytes raises error when parser is not initialized."""
    # Arrange
    deserializer = AwsRestJsonDeserializer("test", None, None)
    test_bytes = b'{"key": "value"}'

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        deserializer.from_bytes(test_bytes)

    assert "Parser not initialized for test" in str(exc_info.value)


def test_aws_rest_json_deserializer_should_raise_error_when_deserialization_fails():
    """Test that from_bytes raises InvalidParameterValueException when boto parsing fails."""
    # Arrange
    mock_parser = Mock()
    mock_operation_model = Mock()
    mock_output_shape = Mock()
    mock_operation_model.output_shape = mock_output_shape
    deserializer = AwsRestJsonDeserializer("test", mock_parser, mock_operation_model)

    test_bytes = b'{"key": "value"}'
    mock_parser.parse.side_effect = Exception("Parsing failed")

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        deserializer.from_bytes(test_bytes)

    assert "Failed to deserialize data for test" in str(exc_info.value)


def test_aws_rest_json_deserializer_should_raise_error_when_json_parsing_fails():
    """Test that from_bytes raises InvalidParameterValueException when JSON parsing fails."""
    # Arrange
    mock_parser = Mock()
    mock_operation_model = Mock()
    mock_operation_model.output_shape = None
    deserializer = AwsRestJsonDeserializer("test", mock_parser, mock_operation_model)

    test_bytes = b"invalid json"

    # Act & Assert
    with pytest.raises(InvalidParameterValueException) as exc_info:
        deserializer.from_bytes(test_bytes)

    assert "Failed to deserialize data for test" in str(exc_info.value)


def test_serialize_simple_dict():
    """Test serialization of simple dictionary."""
    serializer = JSONSerializer()
    data = {"key": "value", "number": 42}
    result = serializer.to_bytes(data)

    expected = b'{"key":"value","number":42}'
    assert result == expected
    assert isinstance(result, bytes)
    assert json.loads(result.decode("utf-8")) == data


def test_serialize_datetime():
    """Test serialization of datetime objects."""
    serializer = JSONSerializer()
    now = datetime(2025, 11, 5, 16, 30, 9, 895000, tzinfo=timezone.utc)
    data = {"timestamp": now}

    result = serializer.to_bytes(data)
    expected = b'{"timestamp":1762360209.895}'

    assert result == expected
    assert isinstance(result, bytes)

    deserialized = json.loads(result.decode("utf-8"))
    assert deserialized["timestamp"] == now.timestamp()


def test_serialize_nested_datetime():
    """Test serialization of nested structures with datetime."""
    serializer = JSONSerializer()
    now = datetime(2025, 11, 5, 16, 30, 9, tzinfo=timezone.utc)
    data = {
        "event": "user_login",
        "timestamp": now,
        "metadata": {"created_at": now, "updated_at": now},
    }

    result = serializer.to_bytes(data)
    expected = (
        b'{"event":"user_login",'
        b'"timestamp":1762360209.0,'
        b'"metadata":{"created_at":1762360209.0,'
        b'"updated_at":1762360209.0}}'
    )

    assert result == expected

    deserialized = json.loads(result.decode("utf-8"))
    assert deserialized["timestamp"] == now.timestamp()
    assert deserialized["metadata"]["created_at"] == now.timestamp()


def test_serialize_list_with_datetime():
    """Test serialization of list containing datetime."""
    serializer = JSONSerializer()
    now = datetime(2025, 11, 5, 16, 30, 9, tzinfo=timezone.utc)
    data = {
        "events": [{"time": now, "action": "login"}, {"time": now, "action": "logout"}]
    }

    result = serializer.to_bytes(data)
    expected = (
        b'{"events":['
        b'{"time":1762360209.0,"action":"login"},'
        b'{"time":1762360209.0,"action":"logout"}'
        b"]}"
    )

    assert result == expected

    deserialized = json.loads(result.decode("utf-8"))
    assert deserialized["events"][0]["time"] == now.timestamp()
    assert deserialized["events"][1]["time"] == now.timestamp()


def test_serialize_mixed_types():
    """Test serialization of mixed data types."""
    serializer = JSONSerializer()
    now = datetime(2025, 11, 5, 16, 30, 9, tzinfo=timezone.utc)
    data = {
        "string": "test",
        "number": 42,
        "float": 3.14,
        "boolean": True,
        "null": None,
        "list": [1, 2, 3],
        "datetime": now,
    }

    result = serializer.to_bytes(data)
    expected = (
        b'{"string":"test",'
        b'"number":42,'
        b'"float":3.14,'
        b'"boolean":true,'
        b'"null":null,'
        b'"list":[1,2,3],'
        b'"datetime":1762360209.0}'
    )

    assert result == expected

    deserialized = json.loads(result.decode("utf-8"))
    assert deserialized["string"] == "test"
    assert deserialized["number"] == 42
    assert deserialized["float"] == 3.14
    assert deserialized["boolean"] is True
    assert deserialized["null"] is None
    assert deserialized["list"] == [1, 2, 3]
    assert deserialized["datetime"] == now.timestamp()


def test_serialize_returns_bytes():
    """Test that serialization returns bytes."""
    serializer = JSONSerializer()
    data = {"test": "value"}
    result = serializer.to_bytes(data)
    expected = b'{"test":"value"}'

    assert result == expected
    assert isinstance(result, bytes)


def test_serialize_non_serializable_object_raises_exception():
    """Test that non-serializable objects raise InvalidParameterValueException."""
    serializer = JSONSerializer()

    class CustomObject:
        pass

    data = {"custom": CustomObject()}

    with pytest.raises(InvalidParameterValueException) as exc_info:
        serializer.to_bytes(data)

    assert (
        "Failed to serialize data to JSON: Object of type CustomObject is not JSON serializable"
        in str(exc_info.value)
    )


def test_serialize_circular_reference_raises_exception():
    """Test that circular references raise InvalidParameterValueException."""
    serializer = JSONSerializer()
    data = {"key": "value"}
    data["self"] = data  # Create circular reference

    with pytest.raises(InvalidParameterValueException) as exc_info:
        serializer.to_bytes(data)

    assert "Failed to serialize data to JSON" in str(exc_info.value)


def test_serialize_datetime_with_microseconds():
    """Test serialization of datetime with microseconds."""
    serializer = JSONSerializer()
    now = datetime(2025, 11, 5, 16, 30, 9, 123456, tzinfo=timezone.utc)
    data = {"timestamp": now}

    result = serializer.to_bytes(data)
    expected = b'{"timestamp":1762360209.123456}'

    assert result == expected


def test_serialize_datetime_without_microseconds():
    """Test serialization of datetime without microseconds."""
    serializer = JSONSerializer()
    now = datetime(2025, 11, 5, 16, 30, 9, tzinfo=timezone.utc)
    data = {"timestamp": now}

    result = serializer.to_bytes(data)
    expected = b'{"timestamp":1762360209.0}'

    assert result == expected


def test_serialize_multiple_datetimes():
    """Test multiple datetime objects."""
    serializer = JSONSerializer()
    dt1 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    dt2 = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

    data = {"start": dt1, "end": dt2}
    result = serializer.to_bytes(data)
    expected = b'{"start":1735689600.0,"end":1767225599.0}'

    assert result == expected

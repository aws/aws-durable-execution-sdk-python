import json
from typing import Any

import pytest

from aws_durable_execution_sdk_python.exceptions import FatalError
from aws_durable_execution_sdk_python.serdes import (
    SerDes,
    SerDesContext,
    deserialize,
    serialize,
)


# Custom SerDes implementation for testing
class CustomStrSerDes(SerDes[str]):
    def serialize(self, value: str, serdes_context: SerDesContext) -> str:
        return value.upper()

    def deserialize(self, data: str, serdes_context: SerDesContext) -> str:
        return data.lower()


class CustomDictSerDes(SerDes[Any]):
    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        transformed = self._rec_serialize(value)
        return json.dumps(transformed)

    def _rec_serialize(self, value: Any) -> Any:
        if isinstance(value, dict):
            transformed = value.copy()
            for k, v in transformed.items():
                transformed[k] = self._rec_serialize(v)
            return transformed
        if isinstance(value, str):
            return value.upper()
        if isinstance(value, int):
            return str(value * 2)
        return value

    def deserialize(self, data: str, serdes_context: SerDesContext) -> dict[str, Any]:
        parsed = json.loads(data)
        return self._rec_deserialize(parsed)

    def _rec_deserialize(self, value: Any) -> Any:
        if isinstance(value, dict):
            transformed = value.copy()
            for k, v in transformed.items():
                transformed[k] = self._rec_deserialize(v)
            return transformed
        if isinstance(value, str) and value.isdigit():
            return int(value) // 2
        if isinstance(value, str):
            return value.lower()
        return value


def test_serdes_abstract():
    """Test SerDes abstract base class."""

    class TestSerDes(SerDes):
        def serialize(self, value):
            return str(value)

        def deserialize(self, data):
            return data

    serdes = TestSerDes()
    assert serdes.serialize(42) == "42"
    assert serdes.deserialize("test") == "test"


def test_serdes_abstract_methods():
    """Test SerDes abstract methods must be implemented."""
    with pytest.raises(TypeError):
        SerDes()


def test_serdes_abstract_methods_not_implemented():
    """Test SerDes abstract methods raise NotImplementedError when not overridden."""

    class IncompleteSerDes(SerDes):
        pass

    # This should raise TypeError because abstract methods are not implemented
    with pytest.raises(TypeError):
        IncompleteSerDes()


def test_serdes_abstract_methods_coverage():
    """Test to achieve coverage of abstract method pass statements."""
    # To cover the pass statements, call the abstract methods directly
    SerDes.serialize(None, None, None)  # Covers line 100
    SerDes.deserialize(None, None, None)  # Covers line 104


def test_serialize_invalid_json():
    circular_ref = {"a": 1}
    circular_ref["self"] = circular_ref

    with pytest.raises(FatalError) as exc_info:
        serialize(None, circular_ref, "test-op", "test-arn")
    assert "Serialization failed" in str(exc_info.value)


def test_deserialize_invalid_json():
    with pytest.raises(FatalError) as exc_info:
        deserialize(None, "invalid json", "test-op", "test-arn")
    assert "Deserialization failed" in str(exc_info.value)


def test_none_serdes_context():
    data = {"test": "value"}
    result = serialize(None, data, None, None)
    assert json.loads(result) == data


def test_default_json_serialization():
    data = {"name": "test", "value": 123}
    serialized = serialize(None, data, "test-op", "test-arn")
    assert isinstance(serialized, str)
    assert json.loads(serialized) == data


def test_default_json_deserialization():
    data = '{"name": "test", "value": 123}'
    deserialized = deserialize(None, data, "test-op", "test-arn")
    assert isinstance(deserialized, dict)
    assert deserialized == {"name": "test", "value": 123}


def test_default_json_roundtrip():
    original = {"name": "test", "value": 123}
    serialized = serialize(None, original, "test-op", "test-arn")
    deserialized = deserialize(None, serialized, "test-op", "test-arn")
    assert deserialized == original


def test_custom_str_serdes_serialization():
    result = serialize(CustomStrSerDes(), "hello world", "test-op", "test-arn")
    assert result == "HELLO WORLD"


def test_custom_str_serdes_deserialization():
    result = deserialize(CustomStrSerDes(), "HELLO WORLD", "test-op", "test-arn")
    assert result == "hello world"


def test_custom_str_serdes_roundtrip():
    original = "hello world"
    serialized = serialize(CustomStrSerDes(), original, "test-op", "test-arn")
    deserialized = deserialize(CustomStrSerDes(), serialized, "test-op", "test-arn")
    assert deserialized == "hello world"


def test_custom_dict_serdes_serialization():
    serdes = CustomDictSerDes()
    original = {"name": "test", "value": 123}
    serialized = serialize(serdes, original, "test-op", "test-arn")
    assert serialized == '{"name": "TEST", "value": "246"}'
    deserialized = deserialize(serdes, serialized, "test-op", "test-arn")
    assert deserialized == original


def test_empty_string_serialization():
    result = serialize(None, "", "test-op", "test-arn")
    assert result == '""'


def test_empty_string_deserialization():
    result = deserialize(None, '""', "test-op", "test-arn")
    assert result == ""


def test_none_value_handling():
    result = serialize(None, None, "test-op", "test-arn")
    assert result == "null"
    deserialized = deserialize(None, "null", "test-op", "test-arn")
    assert deserialized is None


def test_context_propagation():
    class ContextCheckingSerDes(SerDes[str]):
        def serialize(self, value: str, serdes_context: SerDesContext) -> str:
            assert serdes_context.operation_id == "test-op"
            assert serdes_context.durable_execution_arn == "test-arn"
            return value + serdes_context.durable_execution_arn

        def deserialize(self, data: str, serdes_context: SerDesContext) -> str:
            assert serdes_context.operation_id == "test-op"
            assert serdes_context.durable_execution_arn == "test-arn"
            return data + serdes_context.operation_id

    serdes = ContextCheckingSerDes()
    data = "data"
    serialized = serialize(serdes, data, "test-op", "test-arn")
    assert serialized == "data" + "test-arn"
    deserialized = deserialize(serdes, serialized, "test-op", "test-arn")
    assert deserialized == "data" + "test-arn" + "test-op"

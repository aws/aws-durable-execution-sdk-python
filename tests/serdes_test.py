import base64
import json
import math
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from aws_durable_execution_sdk_python.concurrency import (
    BatchItem,
    BatchItemStatus,
    BatchResult,
    CompletionReason,
)
from aws_durable_execution_sdk_python.exceptions import (
    DurableExecutionsError,
    ExecutionError,
    SerDesError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.serdes import (
    BytesCodec,
    ContainerCodec,
    DateTimeCodec,
    DecimalCodec,
    EncodedValue,
    ExtendedTypeSerDes,
    JsonSerDes,
    PrimitiveCodec,
    SerDes,
    SerDesContext,
    TypeCodec,
    TypeTag,
    UuidCodec,
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


# region Abstract SerDes Tests
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


# endregion


# region JsonSerDes Tests
def test_serialize_invalid_json():
    circular_ref = {"a": 1}
    circular_ref["self"] = circular_ref

    with pytest.raises(ExecutionError) as exc_info:
        serialize(None, circular_ref, "test-op", "test-arn")
    assert "Serialization failed" in str(exc_info.value)


def test_deserialize_invalid_json():
    with pytest.raises(ExecutionError) as exc_info:
        deserialize(None, "invalid json", "test-op", "test-arn")
    assert "Deserialization failed" in str(exc_info.value)


def test_none_serdes_context():
    data = {"test": "value"}
    result = serialize(None, data, None, None)
    # Dict uses envelope format, so roundtrip through deserialize
    deserialized = deserialize(None, result, None, None)
    assert deserialized == data


def test_default_json_serialization():
    data = {"name": "test", "value": 123}
    serialized = serialize(None, data, "test-op", "test-arn")
    assert isinstance(serialized, str)
    # Dict uses envelope format, so roundtrip through deserialize
    deserialized = deserialize(None, serialized, "test-op", "test-arn")
    assert deserialized == data


def test_default_json_deserialization():
    # Use a simple list that can be plain JSON
    data = "[1, 2, 3]"
    deserialized = deserialize(None, data, "test-op", "test-arn")
    assert isinstance(deserialized, list)
    assert deserialized == [1, 2, 3]


def test_default_json_roundtrip():
    original = {"name": "test", "value": 123}
    serialized = serialize(None, original, "test-op", "test-arn")
    deserialized = deserialize(None, serialized, "test-op", "test-arn")
    assert deserialized == original


# endregion


# region Custom SerDes Tests
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
    assert not result


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


# endregion


# region EnvelopeSerDes Basic Tests
def _roundtrip_envelope(value: Any) -> Any:
    """Helper for envelope round-trip testing."""
    serdes: ExtendedTypeSerDes[Any] = ExtendedTypeSerDes()
    context = SerDesContext(
        "test-op", "arn:aws:lambda:us-east-1:123456789012:function:test"
    )
    serialized = serdes.serialize(value, context)
    return serdes.deserialize(serialized, context)


def test_envelope_none_roundtrip():
    assert _roundtrip_envelope(None) is None


def test_envelope_bool_roundtrip():
    assert _roundtrip_envelope(True) is True
    assert _roundtrip_envelope(False) is False


def test_envelope_int_roundtrip():
    values = [0, 1, -1, 42, -999, 2**63 - 1, -(2**63)]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_float_roundtrip():
    values = [0.0, 1.5, -math.pi, 1e10, -1e-10, float("inf"), float("-inf")]
    for val in values:
        result = _roundtrip_envelope(val)
        if val != val:  # NaN check  # noqa: PLR0124
            assert result != result  # NaN != NaN  # noqa: PLR0124
        else:
            assert result == val


def test_envelope_float_nan_roundtrip():
    nan_val = float("nan")
    result = _roundtrip_envelope(nan_val)
    assert result != result  # NaN != NaN is True  # noqa: PLR0124


def test_envelope_str_roundtrip():
    values = ["", "hello", "🚀", "line1\nline2", "tab\there", '"quotes"', "\\backslash"]
    for val in values:
        assert _roundtrip_envelope(val) == val


# endregion


# region EnvelopeSerDes Extended Types
def test_envelope_datetime_roundtrip():
    values = [
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        datetime(1970, 1, 1, tzinfo=UTC),
        datetime.now(UTC),
        datetime.now(UTC),
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_date_roundtrip():
    values = [
        date(2024, 1, 1),
        date(1970, 1, 1),
        date(9999, 12, 31),
        date.today(),  # noqa: DTZ011
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_decimal_roundtrip():
    values = [
        Decimal(0),
        Decimal("3.14159"),
        Decimal("-999.999"),
        Decimal("1e10"),
        Decimal("1e-28"),
        Decimal("123456789.123456789"),
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_uuid_roundtrip():
    values = [
        uuid.uuid4(),
        uuid.UUID("12345678-1234-5678-1234-123456789abc"),
        uuid.UUID(int=0),
        uuid.UUID(int=2**128 - 1),
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_bytes_roundtrip():
    values = [
        b"",
        b"hello",
        b"\x00\x01\x02\xff",
        bytes(range(256)),
        "🚀".encode(),
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_bytearray_roundtrip():
    val = bytearray(b"hello world")
    result = _roundtrip_envelope(val)
    assert result == b"hello world"  # Returns bytes, not bytearray


def test_envelope_memoryview_roundtrip():
    val = memoryview(b"memory test")
    result = _roundtrip_envelope(val)
    assert result == b"memory test"  # Returns bytes, not memoryview


# endregion


# region EnvelopeSerDes Container Types
def test_envelope_tuple_roundtrip():
    values = [
        (),
        (1,),
        (1, 2, 3),
        ("a", "b", "c"),
        (1, "mixed", math.pi),
        ((1, 2), (3, 4)),  # Nested tuples
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_list_roundtrip():
    values = [
        [],
        [1],
        [1, 2, 3],
        ["a", "b", "c"],
        [1, "mixed", math.pi],
        [[1, 2], [3, 4]],  # Nested lists
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


def test_envelope_dict_roundtrip():
    values = [
        {},
        {"a": 1},
        {"x": 1, "y": 2, "z": 3},
        {"nested": {"inner": "value"}},
        {"mixed": [1, {"deep": True}]},
    ]
    for val in values:
        assert _roundtrip_envelope(val) == val


# endregion


# region EnvelopeSerDes Complex Structures
def test_envelope_deeply_nested_structure():
    complex_data = {
        "user": {
            "id": uuid.uuid4(),
            "created": datetime.now(UTC),
            "balance": Decimal("1234.56"),
            "metadata": b"binary_data",
            "coordinates": (40.7128, -74.0060),
            "tags": ["premium", "verified"],
            "settings": {
                "notifications": True,
                "theme": "dark",
                "limits": {
                    "daily": Decimal("500.00"),
                    "monthly": Decimal("10000.00"),
                },
            },
        },
        "session": {
            "started": datetime.now(UTC),
            "expires": date.today(),  # noqa: DTZ011
            "token": uuid.uuid4(),
        },
    }
    assert _roundtrip_envelope(complex_data) == complex_data


def test_envelope_mixed_type_collections():
    mixed_list = [
        None,
        True,
        42,
        math.pi,
        "string",
        datetime.now(UTC),
        Decimal("99.99"),
        uuid.uuid4(),
        b"bytes",
        (1, 2, 3),
        [4, 5, 6],
        {"key": "value"},
    ]
    assert _roundtrip_envelope(mixed_list) == mixed_list


def test_envelope_tuple_with_all_types():
    all_types_tuple = (
        None,
        True,
        42,
        math.pi,
        "string",
        datetime(2024, 1, 1, tzinfo=UTC),
        date(2024, 1, 1),
        Decimal("123.45"),
        uuid.uuid4(),
        b"binary",
        [1, 2, 3],
        {"nested": "dict"},
    )
    assert _roundtrip_envelope(all_types_tuple) == all_types_tuple


# endregion


# region EnvelopeSerDes Error Cases
def test_envelope_unsupported_type_error():
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    with pytest.raises(SerDesError, match="Unsupported type: <class 'object'>"):
        serdes.serialize(object(), context)


# endregion


# region EnvelopeSerDes Format Validation
def test_envelope_format_structure():
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    # Dict will use envelope format, primitives use plain JSON
    serialized = serdes.serialize({"test": "value"}, context)
    parsed = json.loads(serialized)

    # Verify envelope structure
    assert "t" in parsed
    assert "v" in parsed
    assert parsed["t"] == "m"  # dict tag
    assert parsed["v"]["test"]["v"] == "value"


def test_envelope_compact_json_output():
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    serialized = serdes.serialize({"key": "value"}, context)
    # Should not contain extra whitespace
    assert " " not in serialized
    assert "\n" not in serialized


def test_envelope_bytes_base64_encoding():
    serdes = ExtendedTypeSerDes()
    context = SerDesContext("test-op", "test-arn")
    test_bytes = b"hello world"
    serialized = serdes.serialize(test_bytes, context)
    parsed = json.loads(serialized)

    # Verify base64 encoding
    encoded_value = parsed["v"]
    assert base64.b64decode(encoded_value) == test_bytes


# endregion


# region EnvelopeSerDes Integration Tests
def test_envelope_with_main_api():
    """Test EnvelopeSerDes works with main serialize/deserialize functions."""
    envelope_serdes = ExtendedTypeSerDes()

    test_data = {
        "id": uuid.uuid4(),
        "timestamp": datetime.now(UTC),
        "amount": Decimal("123.45"),
        "data": b"binary_data",
        "coordinates": (40.7128, -74.0060),
        "tags": ["important", "verified"],
    }

    # Serialize with EnvelopeSerDes
    serialized = serialize(envelope_serdes, test_data, "test-op", "test-arn")

    # Deserialize with EnvelopeSerDes
    deserialized = deserialize(envelope_serdes, serialized, "test-op", "test-arn")

    assert deserialized == test_data


def test_envelope_vs_json_serdes_compatibility():
    """Test that EnvelopeSerDes and JsonSerDes can coexist."""
    json_serdes = JsonSerDes()
    envelope_serdes = ExtendedTypeSerDes()

    # Simple data that both can handle
    simple_data = {"name": "test", "value": 123, "active": True}

    # Both should serialize successfully
    json_result = serialize(json_serdes, simple_data, "test-op", "test-arn")
    envelope_result = serialize(envelope_serdes, simple_data, "test-op", "test-arn")

    # Results should be different (envelope has wrapper)
    assert json_result != envelope_result

    # Both should deserialize to same data
    json_deserialized = deserialize(json_serdes, json_result, "test-op", "test-arn")
    envelope_deserialized = deserialize(
        envelope_serdes, envelope_result, "test-op", "test-arn"
    )

    assert json_deserialized == simple_data
    assert envelope_deserialized == simple_data


def test_envelope_handles_json_incompatible_types():
    """Test that EnvelopeSerDes handles types that JsonSerDes cannot."""
    json_serdes = JsonSerDes()
    envelope_serdes = ExtendedTypeSerDes()

    # Data with types JsonSerDes cannot handle
    complex_data = {
        "uuid": uuid.uuid4(),
        "decimal": Decimal("123.45"),
        "bytes": b"binary",
        "tuple": (1, 2, 3),
    }

    # JsonSerDes should fail
    with pytest.raises(ExecutionError):
        serialize(json_serdes, complex_data, "test-op", "test-arn")

    # EnvelopeSerDes should succeed
    serialized = serialize(envelope_serdes, complex_data, "test-op", "test-arn")
    deserialized = deserialize(envelope_serdes, serialized, "test-op", "test-arn")

    assert deserialized == complex_data


def test_envelope_error_handling_with_main_api():
    """Test error handling when using EnvelopeSerDes with main API."""
    envelope_serdes = ExtendedTypeSerDes()

    # Test serialization error
    with pytest.raises(ExecutionError, match="Serialization failed"):
        serialize(envelope_serdes, object(), "test-op", "test-arn")

    # Test deserialization error
    with pytest.raises(ExecutionError, match="Deserialization failed"):
        deserialize(envelope_serdes, "invalid json", "test-op", "test-arn")


def test_primitive_codec_errors():
    """Test PrimitiveCodec error cases."""
    primitive_codec = PrimitiveCodec()
    with pytest.raises(SerDesError, match="Unsupported primitive type"):
        primitive_codec.encode(object())

    with pytest.raises(SerDesError, match="Unknown primitive tag"):
        primitive_codec.decode(TypeTag.BYTES, "test")


def test_bytes_codec_errors():
    """Test BytesCodec error cases."""
    bytes_codec = BytesCodec()
    with pytest.raises(SerDesError, match="Expected BYTES tag, got"):
        bytes_codec.decode(TypeTag.STR, "test")


def test_uuid_codec_errors():
    """Test UuidCodec error cases."""
    uuid_codec = UuidCodec()
    with pytest.raises(SerDesError, match="Expected UUID tag, got"):
        uuid_codec.decode(TypeTag.STR, "test")


def test_decimal_codec_errors():
    """Test DecimalCodec error cases."""

    decimal_codec = DecimalCodec()
    with pytest.raises(SerDesError, match="Expected DECIMAL tag, got"):
        decimal_codec.decode(TypeTag.STR, "test")


def test_datetime_codec_errors():
    """Test DateTimeCodec error cases."""
    datetime_codec = DateTimeCodec()
    with pytest.raises(SerDesError, match="Unsupported datetime type"):
        datetime_codec.encode("not a datetime")

    with pytest.raises(SerDesError, match="Unknown datetime tag"):
        datetime_codec.decode(TypeTag.BYTES, "test")


def test_datetime_codec_z_suffix():
    """Test DateTimeCodec Z suffix handling."""
    datetime_codec = DateTimeCodec()
    result = datetime_codec.decode(TypeTag.DATETIME, "2024-01-01T00:00:00Z")
    expected = datetime.fromisoformat("2024-01-01T00:00:00+00:00")
    assert result == expected


def test_container_codec_errors():
    """Test ContainerCodec error cases."""
    container_codec = ContainerCodec()
    type_codec = TypeCodec()
    container_codec.set_dispatcher(type_codec)

    with pytest.raises(SerDesError, match="Unsupported container type"):
        container_codec.encode("not a container")

    with pytest.raises(SerDesError, match="Unknown container tag"):
        container_codec.decode(TypeTag.BYTES, "test")

    with pytest.raises(SerDesError, match="Tuple keys not supported"):
        container_codec.encode({(1, 2): "value"})

    # Test without dispatcher
    container_codec_no_dispatcher = ContainerCodec()
    with pytest.raises(
        DurableExecutionsError,
        match="ContainerCodec not linked to a TypeCodec dispatcher",
    ):
        _ = container_codec_no_dispatcher.dispatcher

    # Test decode with wrong value types
    with pytest.raises(SerDesError, match="Expected list, got"):
        container_codec.decode(TypeTag.LIST, "not a list")

    with pytest.raises(SerDesError, match="Expected list, got"):
        container_codec.decode(TypeTag.TUPLE, "not a list")

    with pytest.raises(SerDesError, match="Expected dict, got"):
        container_codec.decode(TypeTag.DICT, "not a dict")

    # Test _unwrap with plain object (case _ branch)
    result = ContainerCodec._unwrap("plain_string", type_codec)  # noqa: SLF001
    assert result == "plain_string"

    # Test _unwrap with EncodedValue (case EncodedValue branch)
    encoded_val = EncodedValue(TypeTag.STR, "test")
    result = ContainerCodec._unwrap(encoded_val, type_codec)  # noqa: SLF001
    assert result == "test"


def test_type_codec_errors():
    """Test TypeCodec error cases."""
    type_codec = TypeCodec()

    with pytest.raises(SerDesError, match="Unsupported type"):
        type_codec.encode(object())

    class MockTag:
        def __str__(self):
            return "unknown"

    with pytest.raises(SerDesError, match="Unknown type tag"):
        type_codec.decode(MockTag(), "test")


def test_extended_serdes_errors():
    """Test ExtendedTypesSerDes error cases."""
    serdes = ExtendedTypeSerDes()

    with pytest.raises(
        SerDesError, match='Malformed envelope: missing "t" or "v" at root'
    ):
        serdes.deserialize('{"invalid": "envelope"}', None)

    with pytest.raises(SerDesError, match='Unknown type tag: "unknown"'):
        serdes.deserialize('{"t": "unknown", "v": "test"}', None)


# endregion


# region EnvelopeSerDes Performance and Edge Cases
def test_envelope_large_data_structure():
    """Test with reasonably large data."""
    large_list = list(range(1000))
    large_dict = {f"key_{i}": f"value_{i}" for i in range(100)}
    large_tuple = tuple(range(500))

    large_structure = {
        "list": large_list,
        "dict": large_dict,
        "tuple": large_tuple,
    }

    result = _roundtrip_envelope(large_structure)
    assert result == large_structure


def test_envelope_empty_containers():
    empty_data = {
        "empty_list": [],
        "empty_dict": {},
        "empty_tuple": (),
        "empty_string": "",
        "empty_bytes": b"",
    }
    assert _roundtrip_envelope(empty_data) == empty_data


def test_envelope_type_preservation_after_roundtrip():
    original = {
        "none": None,
        "bool": True,
        "int": 42,
        "float": math.pi,
        "str": "text",
        "datetime": datetime.now(UTC),
        "date": date.today(),  # noqa: DTZ011
        "decimal": Decimal("123.45"),
        "uuid": uuid.uuid4(),
        "bytes": b"data",
        "tuple": (1, 2, 3),
        "list": [1, 2, 3],
        "dict": {"nested": True},
    }

    result = _roundtrip_envelope(original)

    # Verify types are preserved
    assert type(result["none"]) is type(None)
    assert type(result["bool"]) is bool
    assert type(result["int"]) is int
    assert type(result["float"]) is float
    assert type(result["str"]) is str
    assert type(result["datetime"]) is datetime
    assert type(result["date"]) is date
    assert type(result["decimal"]) is Decimal
    assert type(result["uuid"]) is uuid.UUID
    assert type(result["bytes"]) is bytes
    assert type(result["tuple"]) is tuple
    assert type(result["list"]) is list
    assert type(result["dict"]) is dict


def test_envelope_unicode_and_special_characters():
    unicode_data = {
        "emoji": "🚀🌟💫",
        "chinese": "你好世界",
        "arabic": "مرحبا بالعالم",
        "russian": "Привет мир",
        "special": "\"'\\n\\t\\r",
        "zero_width": "\u200b\u200c\u200d",
    }
    assert _roundtrip_envelope(unicode_data) == unicode_data


def test_primitives():
    primitives = [
        123,
        "hello",
        True,
        False,
        None,
        math.pi,
        Decimal("10.5"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        b"bytes here",
        date(2025, 10, 22),
        datetime(2025, 10, 22, 15, 30, 0),  # noqa: DTZ001
    ]
    serdes = ExtendedTypeSerDes()
    ctx = SerDesContext("test-op", "test-arn")
    for val in primitives:
        serialized = serdes.serialize(val, ctx)
        deserialized = serdes.deserialize(serialized, ctx)
        assert deserialized == val


def test_nested_arrays():
    serdes = ExtendedTypeSerDes()
    ctx = SerDesContext("test-op", "test-arn")
    val = [1, "two", [3, {"four": 4}], True, b"hi"]
    serialized = serdes.serialize(val, ctx)
    deserialized = serdes.deserialize(serialized, ctx)
    assert deserialized == val


def test_nested_dicts():
    val = {
        "a": 1,
        "b": [2, 3, {"c": 4}],
        "d": {"e": "f", "g": [5, 6]},
        "h": b"bytes in dict",
        "i": uuid.UUID("12345678-1234-5678-1234-567812345678"),
    }
    serdes = ExtendedTypeSerDes()
    ctx = SerDesContext("test-op", "test-arn")
    serialized = serdes.serialize(val, ctx)
    deserialized = serdes.deserialize(serialized, ctx)
    assert deserialized == val


def test_user_dict_with_t_v_keys():
    val = {"t": "user t value", "v": "user v value"}
    serdes = ExtendedTypeSerDes()
    ctx = SerDesContext("test-op", "test-arn")
    serialized = serdes.serialize(val, ctx)
    deserialized = serdes.deserialize(serialized, ctx)
    assert deserialized == val


def test_complex_nested_structure():
    val = {
        "list": [1, 2, [3, 4], {"nested_bytes": b"abc"}],
        "tuple": (Decimal("3.14"), True),
        "dict": {
            "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "date": date(2025, 10, 22),
            "datetime": datetime(2025, 10, 22, 15, 30),  # noqa: DTZ001
        },
        "t_v": {"t": "inner t", "v": [1, b"bytes"]},
    }
    serdes = ExtendedTypeSerDes()
    ctx = SerDesContext("test-op", "test-arn")
    serialized = serdes.serialize(val, ctx)
    deserialized = serdes.deserialize(serialized, ctx)
    assert deserialized == val


def test_all_t_v_nested_dicts():
    val = {
        "t": {"t": "s", "v": "outer t"},
        "v": {
            "t": {"t": "s", "v": "inner t"},
            "v": {"t": {"t": "s", "v": "deep t"}, "v": "deep v"},
        },
    }
    serdes = ExtendedTypeSerDes()
    ctx = SerDesContext("test-op", "test-arn")
    serialized = serdes.serialize(val, ctx)
    deserialized = serdes.deserialize(serialized, ctx)
    assert deserialized == val


# endregion


# to_dict() support tests
def test_default_serdes_supports_to_dict_objects():
    """Test that default serdes automatically handles BatchResult serialization/deserialization."""

    result = BatchResult(
        all=[BatchItem(0, BatchItemStatus.SUCCEEDED, result="test")],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    # Default serdes should automatically handle BatchResult
    serialized = serialize(
        serdes=None,
        value=result,
        operation_id="test_op",
        durable_execution_arn="arn:test",
    )

    # Deserialize returns BatchResult (not dict)
    deserialized = deserialize(
        serdes=None,
        data=serialized,
        operation_id="test_op",
        durable_execution_arn="arn:test",
    )

    assert isinstance(deserialized, BatchResult)
    assert deserialized.completion_reason == CompletionReason.ALL_COMPLETED
    assert len(deserialized.all) == 1
    assert deserialized.all[0].result == "test"


def test_to_dict_output_is_serializable():
    """Test that to_dict() output is serializable by default serdes."""

    result = BatchResult(
        all=[
            BatchItem(0, BatchItemStatus.SUCCEEDED, result={"key": "value"}),
            BatchItem(
                1,
                BatchItemStatus.FAILED,
                error=ErrorObject(
                    message="error", type="TestError", data=None, stack_trace=[]
                ),
            ),
        ],
        completion_reason=CompletionReason.ALL_COMPLETED,
    )

    # Convert to dict
    result_dict = result.to_dict()

    # Dict should be serializable
    serialized = serialize(
        serdes=None,
        value=result_dict,
        operation_id="test_op",
        durable_execution_arn="arn:test",
    )

    # Deserialize
    deserialized_dict = deserialize(
        serdes=None,
        data=serialized,
        operation_id="test_op",
        durable_execution_arn="arn:test",
    )

    # Verify structure preserved
    assert deserialized_dict["completionReason"] == "ALL_COMPLETED"
    assert len(deserialized_dict["all"]) == 2
    assert deserialized_dict["all"][0]["result"] == {"key": "value"}
    assert deserialized_dict["all"][1]["error"]["ErrorType"] == "TestError"

    # Can reconstruct BatchResult
    reconstructed = BatchResult.from_dict(deserialized_dict)
    assert len(reconstructed.all) == 2
    assert reconstructed.completion_reason == CompletionReason.ALL_COMPLETED

"""Codec-based serialization and deserialization for Python types.

This module provides comprehensive serialization support using a codec-based
architecture with recursive encoding/decoding for nested structures.

Key Features:
- Plain JSON for primitives and simple lists (performance optimization)
- Envelope format with type tags for complex types
- Modular codec architecture
- Recursive handling of nested structures

Serialization Strategy:
- Primitives (None, str, int, float, bool): Plain JSON
- Simple lists containing only primitives: Plain JSON
- Everything else: Envelope format with type tags

Wire Formats:
    Plain JSON: 42, "hello", [1, 2, 3]
    Envelope: {"t": "<type_tag>", "v": <encoded_value>}
"""

from __future__ import annotations

import base64
import json
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Generic, Protocol, TypeVar

from aws_durable_execution_sdk_python.concurrency.models import BatchResult
from aws_durable_execution_sdk_python.exceptions import (
    DurableExecutionsError,
    ExecutionError,
    SerDesError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

TYPE_TOKEN: str = "t"
VALUE_TOKEN: str = "v"


class TypeTag(StrEnum):
    """Type tags for envelope format."""

    NONE = "n"
    STR = "s"
    INT = "i"
    FLOAT = "f"
    BOOL = "b"
    BYTES = "B"
    UUID = "u"
    DECIMAL = "d"
    DATETIME = "dt"
    DATE = "D"
    TUPLE = "t"
    LIST = "l"
    DICT = "m"
    BATCH_RESULT = "br"


@dataclass(frozen=True)
class EncodedValue:
    """Encoded value with type tag."""

    tag: TypeTag

    value: Any


# region codecs
class Codec(Protocol):
    """Protocol for type-specific codecs."""

    def encode(self, obj: Any) -> EncodedValue: ...

    def decode(self, tag: TypeTag, value: Any) -> Any: ...


class PrimitiveCodec:
    """Codec for primitive types."""

    def encode(self, obj: Any) -> EncodedValue:  # noqa: PLR6301
        match obj:
            case None:
                return EncodedValue(TypeTag.NONE, None)
            case str():
                return EncodedValue(TypeTag.STR, obj)
            case bool():  # Must come before int
                return EncodedValue(TypeTag.BOOL, obj)
            case int():
                return EncodedValue(TypeTag.INT, obj)
            case float():
                return EncodedValue(TypeTag.FLOAT, obj)
            case _:
                msg = f"Unsupported primitive type: {type(obj)!r}"
                raise SerDesError(msg)

    def decode(self, tag: TypeTag, value: Any) -> Any:  # noqa: PLR6301
        match tag:
            case TypeTag.NONE:
                return None
            case TypeTag.STR:
                return str(value)
            case TypeTag.BOOL:
                return bool(value)
            case TypeTag.INT:
                return int(value)
            case TypeTag.FLOAT:
                return float(value)
            case _:
                msg = f"Unknown primitive tag: {tag}"
                raise SerDesError(msg)


class BytesCodec:
    """Codec for bytes, bytearray, and memoryview."""

    def encode(self, obj: Any) -> EncodedValue:  # noqa: PLR6301
        encoded = base64.b64encode(bytes(obj)).decode("utf-8")
        return EncodedValue(TypeTag.BYTES, encoded)

    def decode(self, tag: TypeTag, value: Any) -> Any:  # noqa: PLR6301
        if tag != TypeTag.BYTES:
            msg = f"Expected BYTES tag, got {tag}"
            raise SerDesError(msg)
        return base64.b64decode(value.encode("utf-8"))


class UuidCodec:
    """Codec for UUID objects."""

    def encode(self, obj: Any) -> EncodedValue:  # noqa: PLR6301
        return EncodedValue(TypeTag.UUID, str(obj))

    def decode(self, tag: TypeTag, value: Any) -> Any:  # noqa: PLR6301
        if tag != TypeTag.UUID:
            msg = f"Expected UUID tag, got {tag}"
            raise SerDesError(msg)
        return uuid.UUID(value)


class DecimalCodec:
    """Codec for Decimal objects."""

    def encode(self, obj: Any) -> EncodedValue:  # noqa: PLR6301
        return EncodedValue(TypeTag.DECIMAL, str(obj))

    def decode(self, tag: TypeTag, value: Any) -> Any:  # noqa: PLR6301
        if tag != TypeTag.DECIMAL:
            msg = f"Expected DECIMAL tag, got {tag}"
            raise SerDesError(msg)
        return Decimal(value)


class DateTimeCodec:
    """Codec for datetime and date objects."""

    def encode(self, obj: Any) -> EncodedValue:  # noqa: PLR6301
        match obj:
            case datetime():
                return EncodedValue(TypeTag.DATETIME, obj.isoformat())
            case date():
                return EncodedValue(TypeTag.DATE, obj.isoformat())
            case _:
                msg = f"Unsupported datetime type: {type(obj)!r}"
                raise SerDesError(msg)

    def decode(self, tag: TypeTag, value: Any) -> Any:  # noqa: PLR6301
        match tag:
            case TypeTag.DATETIME:
                # Handle Z suffix for UTC
                s = value
                if isinstance(s, str) and s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                return datetime.fromisoformat(s)
            case TypeTag.DATE:
                return date.fromisoformat(value)
            case _:
                msg = f"Unknown datetime tag: {tag}"
                raise SerDesError(msg)


class ContainerCodec(Codec):
    """Codec for container types with recursive encoding/decoding."""

    def __init__(self) -> None:
        self._dispatcher: TypeCodec | None = None

    def set_dispatcher(self, dispatcher) -> None:
        """Set the main codec dispatcher for recursive encoding."""
        self._dispatcher = dispatcher

    @property
    def dispatcher(self):
        """Get the dispatcher, raising error if not set."""
        if self._dispatcher is None:
            msg = "ContainerCodec not linked to a TypeCodec dispatcher."
            raise DurableExecutionsError(msg)
        return self._dispatcher

    def encode(self, obj: Any) -> EncodedValue:
        """Encode container using dispatcher for recursive elements."""

        match obj:
            case BatchResult():
                # Encode BatchResult as dict with special tag
                return EncodedValue(
                    TypeTag.BATCH_RESULT,
                    self._wrap(obj.to_dict(), self.dispatcher).value,
                )
            case list():
                return EncodedValue(
                    TypeTag.LIST, [self._wrap(v, self.dispatcher) for v in obj]
                )
            case tuple():
                return EncodedValue(
                    TypeTag.TUPLE, [self._wrap(v, self.dispatcher) for v in obj]
                )
            case dict():
                for k in obj:
                    if isinstance(k, tuple):
                        msg = "Tuple keys not supported"
                        raise SerDesError(msg)
                return EncodedValue(
                    TypeTag.DICT,
                    {k: self._wrap(v, self.dispatcher) for k, v in obj.items()},
                )
            case _:
                msg = f"Unsupported container type: {type(obj)!r}"
                raise SerDesError(msg)

    def decode(self, tag: TypeTag, value: Any) -> Any:
        """Decode container using dispatcher for recursive elements."""

        match tag:
            case TypeTag.BATCH_RESULT:
                # Decode BatchResult from dict - value is already the dict structure
                # First decode it as a dict to unwrap all nested EncodedValues
                decoded_dict = self.decode(TypeTag.DICT, value)
                return BatchResult.from_dict(decoded_dict)
            case TypeTag.LIST:
                if not isinstance(value, list):
                    msg = f"Expected list, got {type(value)}"
                    raise SerDesError(msg)
                return [self._unwrap(v, self.dispatcher) for v in value]
            case TypeTag.TUPLE:
                if not isinstance(value, list):
                    msg = f"Expected list, got {type(value)}"
                    raise SerDesError(msg)
                return tuple(self._unwrap(v, self.dispatcher) for v in value)
            case TypeTag.DICT:
                if not isinstance(value, dict):
                    msg = f"Expected dict, got {type(value)}"
                    raise SerDesError(msg)
                return {k: self._unwrap(v, self.dispatcher) for k, v in value.items()}
            case _:
                msg = f"Unknown container tag: {tag}"
                raise SerDesError(msg)

    @staticmethod
    def _wrap(obj: Any, dispatcher) -> EncodedValue:
        """Wrap object using dispatcher."""
        return dispatcher.encode(obj)

    @staticmethod
    def _unwrap(obj: Any, dispatcher) -> Any:
        """Unwrap object using dispatcher."""
        match obj:
            case EncodedValue():
                return dispatcher.decode(obj.tag, obj.value)
            case dict() if TYPE_TOKEN in obj and VALUE_TOKEN in obj:
                tag = TypeTag(obj[TYPE_TOKEN])
                return dispatcher.decode(tag, obj[VALUE_TOKEN])
            case _:
                return obj


class TypeCodec(Codec):
    """Main codec dispatcher."""

    def __init__(self):
        self.primitive_codec = PrimitiveCodec()
        self.bytes_codec = BytesCodec()
        self.uuid_codec = UuidCodec()
        self.decimal_codec = DecimalCodec()
        self.datetime_codec = DateTimeCodec()
        self.container_codec = ContainerCodec()
        self.container_codec.set_dispatcher(self)

    def encode(self, obj: Any) -> EncodedValue:
        match obj:
            case None | str() | bool() | int() | float():
                return self.primitive_codec.encode(obj)
            case bytes() | bytearray() | memoryview():
                return self.bytes_codec.encode(bytes(obj))
            case uuid.UUID():
                return self.uuid_codec.encode(obj)
            case Decimal():
                return self.decimal_codec.encode(obj)
            case datetime() | date():
                return self.datetime_codec.encode(obj)
            case list() | tuple() | dict() | BatchResult():
                return self.container_codec.encode(obj)
            case _:
                msg = f"Unsupported type: {type(obj)}"
                raise SerDesError(msg)

    def decode(self, tag: TypeTag, value: Any) -> Any:
        match tag:
            case (
                TypeTag.NONE
                | TypeTag.STR
                | TypeTag.BOOL
                | TypeTag.INT
                | TypeTag.FLOAT
            ):
                return self.primitive_codec.decode(tag, value)
            case TypeTag.BYTES:
                return self.bytes_codec.decode(tag, value)
            case TypeTag.UUID:
                return self.uuid_codec.decode(tag, value)
            case TypeTag.DECIMAL:
                return self.decimal_codec.decode(tag, value)
            case TypeTag.DATETIME | TypeTag.DATE:
                return self.datetime_codec.decode(tag, value)
            case TypeTag.LIST | TypeTag.TUPLE | TypeTag.DICT | TypeTag.BATCH_RESULT:
                return self.container_codec.decode(tag, value)
            case _:
                msg = f"Unknown type tag: {tag}"
                raise SerDesError(msg)


TYPE_CODEC = TypeCodec()


# endregion


@dataclass(frozen=True)
class SerDesContext:
    """Context for serialization operations."""

    operation_id: str = ""

    durable_execution_arn: str = ""


class SerDes(ABC, Generic[T]):
    @abstractmethod
    def serialize(self, value: T, serdes_context: SerDesContext) -> str:
        pass

    @abstractmethod
    def deserialize(self, data: str, serdes_context: SerDesContext) -> T:
        pass

    @staticmethod
    def is_primitive(obj: Any) -> bool:
        """Check if object contains only JSON-serializable primitives."""
        if obj is None or isinstance(obj, str | int | float | bool):
            return True
        if isinstance(obj, list):
            return all(SerDes.is_primitive(item) for item in obj)
        return False


class PassThroughSerDes(SerDes[T]):
    def serialize(self, value: T, _: SerDesContext) -> str:  # noqa: PLR6301
        return value  # type: ignore

    def deserialize(self, data: str, _: SerDesContext) -> T:  # noqa: PLR6301
        return data  # type: ignore


class JsonSerDes(SerDes[T]):
    def serialize(self, value: T, _: SerDesContext) -> str:  # noqa: PLR6301
        return json.dumps(value)

    def deserialize(self, data: str, _: SerDesContext) -> T:  # noqa: PLR6301
        return json.loads(data)


class ExtendedTypeSerDes(SerDes[T]):
    """Main serializer class."""

    def __init__(self):
        self._codec = TYPE_CODEC

    def serialize(self, value: Any, context: SerDesContext | None = None) -> str:  # noqa: ARG002
        """Serialize value to JSON string."""
        # Fast path for primitives
        if SerDes.is_primitive(value):
            return json.dumps(value, separators=(",", ":"))

        encoded = self._codec.encode(value)
        wrapped = self._to_json_serializable(encoded)
        return json.dumps(wrapped, separators=(",", ":"))

    def deserialize(self, data: str, context: SerDesContext | None = None) -> Any:  # noqa: ARG002
        """Deserialize JSON string to Python object."""
        obj = json.loads(data)

        # Fast path for primitives
        if SerDes.is_primitive(obj):
            return obj

        if not (isinstance(obj, dict) and TYPE_TOKEN in obj and VALUE_TOKEN in obj):
            msg = 'Malformed envelope: missing "t" or "v" at root.'
            raise SerDesError(msg)
        # Python 3.11 compatibility: Using try-except instead of 'in' operator
        # because checking 'str in EnumType' raises TypeError in Python 3.11
        try:
            tag = TypeTag(obj[TYPE_TOKEN])
        except ValueError:
            msg = f'Unknown type tag: "{obj[TYPE_TOKEN]}"'
            raise SerDesError(msg) from None

        return self._codec.decode(tag, obj[VALUE_TOKEN])

    def _to_json_serializable(self, obj: Any) -> Any:
        """Convert EncodedValue objects to JSON-serializable format."""
        match obj:
            case EncodedValue():
                return {
                    TYPE_TOKEN: obj.tag,
                    VALUE_TOKEN: self._to_json_serializable(obj.value),
                }
            case list():
                return [self._to_json_serializable(x) for x in obj]
            case dict():
                return {k: self._to_json_serializable(v) for k, v in obj.items()}
            case _:
                return obj


_DEFAULT_JSON_SERDES: SerDes[Any] = JsonSerDes()
_EXTENDED_TYPES_SERDES: SerDes[Any] = ExtendedTypeSerDes()


def serialize(
    serdes: SerDes[T] | None, value: T, operation_id: str, durable_execution_arn: str
) -> str:
    """Serialize value using provided or default serializer.

    Args:
        serdes: Custom serializer or None for default
        value: Object to serialize
        operation_id: Unique operation identifier
        durable_execution_arn: ARN of durable execution

    Returns:
        Serialized string representation

    Raises:
        FatalError: If serialization fails
    """
    serdes_context: SerDesContext = SerDesContext(operation_id, durable_execution_arn)
    active_serdes: SerDes[T] = serdes or _EXTENDED_TYPES_SERDES
    try:
        return active_serdes.serialize(value, serdes_context)
    except Exception as e:
        logger.exception(
            "⚠️ Serialization failed for id: %s",
            operation_id,
        )
        msg = f"Serialization failed for id: {operation_id}, error: {e}."
        raise ExecutionError(msg) from e


def deserialize(
    serdes: SerDes[T] | None, data: str, operation_id: str, durable_execution_arn: str
) -> T:
    """Deserialize data using provided or default serializer.

    Args:
        serdes: Custom serializer or None for default
        data: Serialized string data
        operation_id: Unique operation identifier
        durable_execution_arn: ARN of durable execution

    Returns:
        Deserialized Python object

    Raises:
        FatalError: If deserialization fails
    """
    serdes_context: SerDesContext = SerDesContext(operation_id, durable_execution_arn)
    active_serdes: SerDes[T] = serdes or _EXTENDED_TYPES_SERDES
    try:
        return active_serdes.deserialize(data, serdes_context)
    except Exception as e:
        logger.exception("⚠️ Deserialization failed for id: %s", operation_id)
        msg = f"Deserialization failed for id: {operation_id}"
        raise ExecutionError(msg) from e

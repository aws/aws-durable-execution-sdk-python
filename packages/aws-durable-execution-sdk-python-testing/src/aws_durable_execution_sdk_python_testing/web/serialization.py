"""Serialization interfaces and AWS boto integration for HTTP request/response models.

This module provides Protocol interfaces for serialization and deserialization,
along with AWS-compatible implementations using boto's rest-json serializers.
"""

from __future__ import annotations

import json
import os
from typing import Any, Protocol
from datetime import datetime

import aws_durable_execution_sdk_python
import botocore.loaders  # type: ignore
from botocore.model import ServiceModel  # type: ignore
from botocore.parsers import create_parser  # type: ignore
from botocore.serialize import create_serializer  # type: ignore

from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


class Serializer(Protocol):
    """Interface for serializing data to bytes."""

    def to_bytes(self, data: Any) -> bytes:
        """Serialize data to bytes.

        Args:
            data: The data to serialize

        Returns:
            bytes: The serialized data

        Raises:
            InvalidParameterValueException: If serialization fails
        """
        ...  # pragma: no cover


class Deserializer(Protocol):
    """Interface for deserializing bytes to data."""

    def from_bytes(self, data: bytes) -> dict[str, Any]:
        """Deserialize bytes to dictionary.

        Args:
            data: The bytes to deserialize

        Returns:
            dict: The deserialized data

        Raises:
            InvalidParameterValueException: If deserialization fails
        """
        ...  # pragma: no cover


class JSONSerializer:
    """JSON serializer with datetime support."""

    def to_bytes(self, data: Any) -> bytes:
        """Serialize data to JSON bytes."""
        try:
            json_string = json.dumps(
                data, separators=(",", ":"), default=self._default_handler
            )
            return json_string.encode("utf-8")
        except (TypeError, ValueError) as e:
            raise InvalidParameterValueException(
                f"Failed to serialize data to JSON: {str(e)}"
            )

    def _default_handler(self, obj: Any) -> float:
        """Handle non-permitive objects."""
        if isinstance(obj, datetime):
            return obj.timestamp()
        # Raise TypeError for unsupported types
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class AwsRestJsonSerializer:
    """AWS rest-json serializer using boto."""

    def __init__(self, operation_name: str, serializer: Any, operation_model: Any):
        """Initialize the AWS rest-json serializer.

        Args:
            operation_name: Name of the AWS operation
            serializer: Boto serializer instance
            operation_model: Boto operation model
        """
        self._operation_name = operation_name
        self._serializer = serializer
        self._operation_model = operation_model

    @classmethod
    def create(cls, operation_name: str) -> AwsRestJsonSerializer:
        """Create serializer with boto components.

        Args:
            operation_name: Name of the AWS operation

        Returns:
            AwsRestJsonSerializer: Configured serializer instance

        Raises:
            InvalidParameterValueException: If serializer creation fails
        """
        try:
            # Load service model
            loader = botocore.loaders.Loader()

            raw_model = loader.load_service_model("lambda", "service-2")
            service_model = ServiceModel(raw_model)

            # Create serializer (rest-json protocol)
            serializer = create_serializer("rest-json", include_validation=True)
            operation_model = service_model.operation_model(operation_name)

            return cls(operation_name, serializer, operation_model)
        except Exception as e:
            msg = f"Failed to create serializer for {operation_name}: {e}"
            raise InvalidParameterValueException(msg) from e

    def to_bytes(self, data: dict[str, Any]) -> bytes:
        """Serialize data using boto rest-json serializer.

        Args:
            data: Dictionary data to serialize

        Returns:
            bytes: Serialized data

        Raises:
            InvalidParameterValueException: If serialization fails
        """
        if not self._serializer or not self._operation_model:
            msg = f"Serializer not initialized for {self._operation_name}"
            raise InvalidParameterValueException(msg)

        try:
            serialized = self._serializer.serialize_to_request(
                data, self._operation_model
            )
            body = serialized.get("body", b"")

            if isinstance(body, str):
                return body.encode("utf-8")

            return body  # noqa: TRY300
        except Exception as e:
            msg = f"Failed to serialize data for {self._operation_name}: {e}"
            raise InvalidParameterValueException(msg) from e


class AwsRestJsonDeserializer:
    """AWS rest-json deserializer using boto."""

    def __init__(self, operation_name: str, parser: Any, operation_model: Any):
        """Initialize the AWS rest-json deserializer.

        Args:
            operation_name: Name of the AWS operation
            parser: Boto parser instance
            operation_model: Boto operation model
        """
        self._operation_name = operation_name
        self._parser = parser
        self._operation_model = operation_model

    @classmethod
    def create(cls, operation_name: str) -> AwsRestJsonDeserializer:
        """Create deserializer with boto components.

        Args:
            operation_name: Name of the AWS operation

        Returns:
            AwsRestJsonDeserializer: Configured deserializer instance

        Raises:
            InvalidParameterValueException: If deserializer creation fails
        """
        try:
            # Load service model
            loader = botocore.loaders.Loader()

            raw_model = loader.load_service_model("lambda", "service-2")
            service_model = ServiceModel(raw_model)

            # Create parser (rest-json protocol)
            parser = create_parser("rest-json")
            operation_model = service_model.operation_model(operation_name)

            return cls(operation_name, parser, operation_model)
        except Exception as e:
            msg = f"Failed to create deserializer for {operation_name}: {e}"
            raise InvalidParameterValueException(msg) from e

    def from_bytes(self, data: bytes) -> dict[str, Any]:
        """Deserialize bytes using boto rest-json parser.

        Args:
            data: Bytes to deserialize

        Returns:
            dict: Deserialized data

        Raises:
            InvalidParameterValueException: If deserialization fails
        """
        if not self._parser or not self._operation_model:
            msg = f"Parser not initialized for {self._operation_name}"
            raise InvalidParameterValueException(msg)

        try:
            if self._operation_model.output_shape:
                # Create response dict for boto parser
                response_dict = {
                    "body": data,
                    "headers": {"content-type": "application/json"},
                    "status_code": 200,
                }
                return self._parser.parse(
                    response_dict, self._operation_model.output_shape
                )

            # If no output shape, just parse as JSON
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            msg = f"Failed to deserialize data for {self._operation_name}: {e}"
            raise InvalidParameterValueException(msg) from e

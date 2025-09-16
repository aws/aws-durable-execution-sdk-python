"""Serialization and deserialization"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

from aws_durable_functions_sdk_python.exceptions import FatalError

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class SerDesContext:
    operation_id: str
    durable_execution_arn: str


class SerDes(ABC, Generic[T]):
    @abstractmethod
    def serialize(self, value: T, serdes_context: SerDesContext) -> str:
        pass

    @abstractmethod
    def deserialize(self, data: str, serdes_context: SerDesContext) -> T:
        pass


class JsonSerDes(SerDes[T]):
    def serialize(self, value: T, _: SerDesContext) -> str:
        return json.dumps(value)

    def deserialize(self, data: str, _: SerDesContext) -> T:
        return json.loads(data)


_DEFAULT_JSON_SERDES: SerDes = JsonSerDes()


def serialize(
    serdes: SerDes[T] | None, value: T, operation_id: str, durable_execution_arn: str
) -> str:
    serdes_context: SerDesContext = SerDesContext(operation_id, durable_execution_arn)
    if serdes is None:
        serdes = _DEFAULT_JSON_SERDES
    try:
        return serdes.serialize(value, serdes_context)
    except Exception as e:
        logger.exception(
            "⚠️ Serialization failed for id: %s",
            operation_id,
        )
        msg = f"Serialization failed for id: {operation_id}, error: {e}."
        raise FatalError(msg) from e


def deserialize(
    serdes: SerDes[T] | None, data: str, operation_id: str, durable_execution_arn: str
) -> T:
    serdes_context: SerDesContext = SerDesContext(operation_id, durable_execution_arn)
    if serdes is None:
        serdes = _DEFAULT_JSON_SERDES
    try:
        return serdes.deserialize(data, serdes_context)
    except Exception as e:
        logger.exception(
            "⚠️ Deserialization failed for id: %s",
            operation_id,
        )
        msg = f"Deserialization failed for id: {operation_id}"
        raise FatalError(msg) from e

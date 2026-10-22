"""Reconstruct exceptions from serialized error metadata for span events.

The SDK checkpoints an ``ErrorObject`` (message/type/data/stack_trace) for a
failed operation or attempt. OpenTelemetry derives ``exception.type`` from the
runtime class passed to ``Span.record_exception``, so constructing a generic
``Exception`` collapses every failure to ``Exception`` and loses the original
type. These helpers rebuild an exception that carries the recorded type and
keep the original message and checkpointed stack trace.
"""

from __future__ import annotations

import builtins
import importlib
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import ErrorObject


_DEFAULT_MESSAGE = "Unknown error"


def _resolve_exception_class(type_name: str) -> type[BaseException] | None:
    """Resolve a wire ErrorType to a real exception class, if available."""
    candidate: Any = getattr(builtins, type_name, None)
    if isinstance(candidate, type) and issubclass(candidate, BaseException):
        return candidate
    if "." in type_name:
        module_name, _, qualname = type_name.rpartition(".")
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            return None
        candidate = module
        for part in qualname.split("."):
            candidate = getattr(candidate, part, None)
            if candidate is None:
                return None
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            return candidate
    return None


def _synthesize_exception_class(type_name: str) -> type[BaseException]:
    """Build an exception class whose qualified name matches ``type_name``.

    Used when the original type is not importable. ``record_exception`` reads
    ``__module__``/``__qualname__`` from the class, so a class carrying the
    original name reproduces ``exception.type`` without rerunning user code.
    """
    if "." in type_name:
        module_name, _, qualname = type_name.rpartition(".")
    else:
        module_name, qualname = "builtins", type_name
    return type(qualname, (Exception,), {"__module__": module_name})


def exception_from_error_object(error: ErrorObject) -> BaseException:
    """Rebuild an exception that preserves the SDK-recorded error type.

    Prefers a resolvable real class, then a synthesized class named after the
    wire type, falling back to ``Exception`` only when no type is available.
    """
    message: str = error.message if error.message is not None else ""
    type_name: str | None = error.type
    if not type_name:
        return Exception(message or _DEFAULT_MESSAGE)
    resolved: type[BaseException] | None = _resolve_exception_class(type_name)
    cls: type[BaseException] = resolved or _synthesize_exception_class(type_name)
    try:
        return cls(message)
    except Exception:  # noqa: BLE001 - a class may reject positional args
        exc: BaseException = cls.__new__(cls)
        BaseException.__init__(exc, message)
        return exc


def error_object_exception_attributes(error: ErrorObject) -> dict[str, Any]:
    """Return explicit event attributes for a checkpointed error.

    OTel derives ``exception.message`` from ``str(exception)`` and auto-fills
    ``exception.stacktrace`` from a live traceback; a rebuilt exception has
    neither the exact message formatting (e.g. ``KeyError`` quotes its
    argument) nor a traceback, so the recorded values are supplied explicitly.
    """
    attributes: dict[str, Any] = {}
    if error.type is not None:
        attributes["exception.type"] = error.type
    if error.message is not None:
        attributes["exception.message"] = error.message
    if error.stack_trace:
        attributes["exception.stacktrace"] = "\n".join(error.stack_trace)
    return attributes


def record_error_object_exception(span: Any, error: ErrorObject | None) -> None:
    """Record a span exception that preserves the SDK's original error type.

    ``error`` may be ``None`` for a failed outcome that carried no metadata; a
    generic exception is recorded in that case.
    """
    if error is None:
        span.record_exception(Exception(_DEFAULT_MESSAGE))
        return
    span.record_exception(
        exception_from_error_object(error),
        attributes=error_object_exception_attributes(error),
    )

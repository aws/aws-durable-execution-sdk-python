"""Exceptions for the Durable Executions Testing Library.

This module provides AWS-compliant exceptions that serialize to the exact JSON format
expected by boto3 and AWS services. All exceptions follow Smithy model definitions
for field names and structure.

## AWS-Compliant Error Format

All AWS API exceptions inherit from `AwsApiException` and implement the `to_dict()` method
to serialize to AWS-compliant JSON format. The format varies by exception type based on
their Smithy model definitions:

### Standard Format (most exceptions):
```json
{
  "Type": "ExceptionName",
  "message": "Error message"  // or "Message" depending on Smithy definition
}
```

### Special Cases:
- `ExecutionAlreadyStartedException`: No "Type" field, includes "DurableExecutionArn"
```json
{
  "message": "Error message",
  "DurableExecutionArn": "arn:aws:states:..."
}
```

## Field Name Conventions

Field names follow the exact Smithy model definitions:
- `InvalidParameterValueException`: uses lowercase "message"
- `CallbackTimeoutException`: uses lowercase "message"
- `ResourceNotFoundException`: uses capital "Message"
- `ServiceException`: uses capital "Message"
- `ExecutionAlreadyStartedException`: uses lowercase "message" + "DurableExecutionArn"

## HTTP Status Codes

Each exception maps to appropriate HTTP status codes:
- 400: InvalidParameterValueException (Bad Request)
- 404: ResourceNotFoundException (Not Found)
- 408: CallbackTimeoutException (Request Timeout)
- 409: ExecutionAlreadyStartedException (Conflict)
- 500: ServiceException (Internal Server Error)

## Usage Examples

```python
# Create and serialize an exception
exception = InvalidParameterValueException("Invalid parameter value")
json_dict = exception.to_dict()
# Result: {"Type": "InvalidParameterValueException", "message": "Invalid parameter value"}

# HTTP response creation
from aws_durable_execution_sdk_python_testing.web.models import HTTPResponse

response = HTTPResponse.create_error_from_exception(exception)
# Creates HTTP 400 response with AWS-compliant JSON body
```

## Boto3 Compatibility

All exceptions are designed to be compatible with boto3's error handling:
- JSON structure matches boto3 expectations
- Field names match Smithy model definitions
- Type field values match exception class names
- Can be deserialized by boto3's error factory

Avoid any non-stdlib references in this module, it is at the bottom of the dependency chain.
"""

from __future__ import annotations

from typing import Any


# region Local Runner
class DurableFunctionsLocalRunnerError(Exception):
    """Base class for Durable Executions exceptions"""


class UnknownRouteError(DurableFunctionsLocalRunnerError):
    """No route matches the requested path pattern."""

    def __init__(self, method: str, path: str) -> None:
        """Initialize UnknownRouteError with method and path.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: Request path that couldn't be matched
        """
        self.method = method
        self.path = path
        message = f"Unknown path pattern: {method} {path}"
        super().__init__(message)


# endregion Local Runner


class SerializationError(DurableFunctionsLocalRunnerError):
    """Exception for serialization errors."""


# region Testing
class DurableFunctionsTestError(Exception):
    """Base class for testing errors."""


# endregion Testing


# region AWS API Exceptions
class AwsApiException(DurableFunctionsLocalRunnerError):  # noqa: N818
    """Base class for AWS API-style exceptions that can be serialized to AWS format."""

    http_status_code: int = 500  # Default to server error

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        raise NotImplementedError


# Smithy-Mapped Exceptions (defined in Smithy models)
class InvalidParameterValueException(AwsApiException):
    """Exception for invalid parameter values."""

    http_status_code = 400

    def __init__(self, message: str) -> None:
        """Initialize with message field (lowercase per Smithy definition)."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        return {"Type": "InvalidParameterValueException", "message": self.message}


class ResourceNotFoundException(AwsApiException):
    """Exception for resource not found errors."""

    http_status_code = 404

    def __init__(
        self,
        Message: str,  # noqa: N803
    ) -> None:  # Capital M per Smithy definition
        """Initialize with Message field (capital M per Smithy definition)."""
        self.Message = Message
        super().__init__(Message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        return {"Type": "ResourceNotFoundException", "Message": self.Message}


class ServiceException(AwsApiException):
    """Exception for general service errors."""

    http_status_code = 500

    def __init__(
        self,
        Message: str,  # noqa: N803
    ) -> None:  # Capital M per Smithy definition
        """Initialize with Message field (capital M per Smithy definition)."""
        self.Message = Message
        super().__init__(Message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        return {"Type": "ServiceException", "Message": self.Message}


class ExecutionAlreadyStartedException(AwsApiException):
    """Exception for execution already started errors."""

    http_status_code = 409

    def __init__(self, message: str, DurableExecutionArn: str) -> None:  # noqa: N803
        """Initialize with message and DurableExecutionArn fields."""
        self.message = message
        self.DurableExecutionArn = DurableExecutionArn
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure (no Type field per Smithy definition)."""
        return {
            "message": self.message,
            "DurableExecutionArn": self.DurableExecutionArn,
        }


class ExecutionConflictException(AwsApiException):
    """Exception for execution conflict errors."""

    http_status_code = 409

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        return {"Type": "ExecutionConflictException", "message": self.message}


class CallbackTimeoutException(AwsApiException):
    """Exception for callback timeout errors."""

    http_status_code = 408

    def __init__(self, message: str) -> None:
        """Initialize with message field (lowercase per Smithy definition)."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        return {"Type": "CallbackTimeoutException", "message": self.message}


class TooManyRequestsException(AwsApiException):
    """Exception for too many requests errors."""

    http_status_code = 429

    def __init__(self, message: str) -> None:
        """Initialize with message field (lowercase per Smithy definition)."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure."""
        return {"Type": "TooManyRequestsException", "message": self.message}


# Unmapped Exceptions (thrown by services but not in Smithy)
class IllegalStateException(AwsApiException):
    """IllegalStateException."""

    http_status_code = 500

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure (maps to ServiceException)."""
        return {"Type": "ServiceException", "Message": self.message}


class RuntimeException(AwsApiException):
    """RuntimeException."""

    http_status_code = 500

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure (maps to ServiceException)."""
        return {"Type": "ServiceException", "Message": self.message}


class IllegalArgumentException(AwsApiException):
    """IllegalArgumentException."""

    http_status_code = 400

    def __init__(self, message: str) -> None:
        """Initialize with message field."""
        self.message = message
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to AWS-compliant JSON structure (maps to InvalidParameterValueException)."""
        return {"Type": "InvalidParameterValueException", "message": self.message}


# endregion AWS API Exceptions

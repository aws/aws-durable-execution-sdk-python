# AWS-Compliant Error Response Documentation

This document describes the AWS-compliant error response format used by the Durable Executions Testing Library.

## Overview

The testing library implements AWS-compliant error responses that match the exact format expected by boto3 and AWS services. All error responses follow Smithy model definitions for structure and field naming.

## Error Response Format

### HTTP Response Structure

All error responses use the following HTTP structure:

```
HTTP/1.1 <status_code>
Content-Type: application/json

<JSON_BODY>
```

### JSON Body Format

The JSON body format varies by exception type based on Smithy model definitions:

#### Standard Format (Most Exceptions)

```json
{
  "Type": "ExceptionName",
  "message": "Detailed error message"
}
```

**Used by:**
- `InvalidParameterValueException`
- `CallbackTimeoutException`

#### Capital Message Format

```json
{
  "Type": "ExceptionName", 
  "Message": "Detailed error message"
}
```

**Used by:**
- `ResourceNotFoundException`
- `ServiceException`

#### Special Format (ExecutionAlreadyStartedException)

```json
{
  "message": "Detailed error message",
  "DurableExecutionArn": "arn:aws:states:region:account:execution:name"
}
```

**Note:** This exception has no "Type" field per AWS Smithy definition.

## Exception Types and Examples

### InvalidParameterValueException (HTTP 400)

**When:** Invalid parameter values are provided to API operations.

**Example Response:**
```json
{
  "Type": "InvalidParameterValueException",
  "message": "The parameter 'executionName' cannot be empty"
}
```

**Common Causes:**
- Empty or null required parameters
- Invalid parameter formats
- Parameter values outside allowed ranges

### ResourceNotFoundException (HTTP 404)

**When:** Requested resource does not exist.

**Example Response:**
```json
{
  "Type": "ResourceNotFoundException",
  "Message": "Execution with ID 'exec-123' not found"
}
```

**Common Causes:**
- Non-existent execution IDs
- Deleted or expired resources
- Incorrect resource identifiers

### ServiceException (HTTP 500)

**When:** Internal service errors occur.

**Example Response:**
```json
{
  "Type": "ServiceException", 
  "Message": "An internal error occurred while processing the request"
}
```

**Common Causes:**
- Unexpected internal errors
- System unavailability
- Configuration issues

### CallbackTimeoutException (HTTP 408)

**When:** Callback operations timeout.

**Example Response:**
```json
{
  "Type": "CallbackTimeoutException",
  "message": "Callback operation timed out after 30 seconds"
}
```

**Common Causes:**
- Callback not received within timeout period
- Network connectivity issues
- Client-side delays

### ExecutionAlreadyStartedException (HTTP 409)

**When:** Attempting to start an execution that is already running.

**Example Response:**
```json
{
  "message": "Execution is already started",
  "DurableExecutionArn": "arn:aws:states:us-east-1:123456789012:execution:MyExecution:abc123"
}
```

**Common Causes:**
- Duplicate start execution requests
- Race conditions in execution management
- Client retry logic issues

## HTTP Status Code Mapping

| Exception | HTTP Status | Description |
|-----------|-------------|-------------|
| InvalidParameterValueException | 400 | Bad Request - Invalid input parameters |
| ResourceNotFoundException | 404 | Not Found - Resource does not exist |
| CallbackTimeoutException | 408 | Request Timeout - Operation timed out |
| ExecutionAlreadyStartedException | 409 | Conflict - Resource already exists |
| ServiceException | 500 | Internal Server Error - System error |

## Field Name Conventions

Field names strictly follow Smithy model definitions:

- **lowercase "message"**: InvalidParameterValueException, CallbackTimeoutException, ExecutionAlreadyStartedException
- **capital "Message"**: ResourceNotFoundException, ServiceException
- **"Type"**: Present in all exceptions except ExecutionAlreadyStartedException
- **"DurableExecutionArn"**: Only in ExecutionAlreadyStartedException

## Boto3 Compatibility

All error responses are designed for boto3 compatibility:

### Client Error Handling

```python
import boto3
from botocore.exceptions import ClientError

try:
    # API call that might fail
    response = client.some_operation()
except ClientError as e:
    error_code = e.response['Error']['Code']
    error_message = e.response['Error']['Message']
    
    if error_code == 'InvalidParameterValueException':
        # Handle invalid parameter
        pass
    elif error_code == 'ResourceNotFoundException':
        # Handle not found
        pass
```

### Error Response Structure

The testing library's error responses match the structure boto3 expects:

```python
# What boto3 receives
{
    'Error': {
        'Code': 'InvalidParameterValueException',
        'Message': 'The parameter cannot be empty'
    },
    'ResponseMetadata': {
        'HTTPStatusCode': 400,
        'HTTPHeaders': {...}
    }
}
```

## Migration from Legacy Format

### Old Format (Deprecated)
```json
{
  "error": {
    "type": "InvalidParameterError",
    "message": "Error message",
    "code": "INVALID_PARAMETER",
    "requestId": "req-123"
  }
}
```

### New AWS-Compliant Format
```json
{
  "Type": "InvalidParameterValueException",
  "message": "Error message"
}
```

### Key Changes
1. **No wrapper object**: Direct JSON structure, no "error" wrapper
2. **AWS exception names**: Use official AWS exception names
3. **Smithy field names**: Follow exact Smithy model field naming
4. **Simplified structure**: Only essential fields per AWS standards
5. **Consistent HTTP codes**: Match AWS service status codes

## Testing Error Responses

### Unit Testing

```python
from aws_durable_execution_sdk_python_testing.exceptions import InvalidParameterValueException
from aws_durable_execution_sdk_python_testing.web.models import HTTPResponse

def test_error_response():
    exception = InvalidParameterValueException("Test error")
    response = HTTPResponse.create_error_from_exception(exception)
    
    assert response.status_code == 400
    assert response.body == {
        "Type": "InvalidParameterValueException",
        "message": "Test error"
    }
```

### Integration Testing

```python
import requests

def test_api_error_response():
    response = requests.post('http://localhost:8080/invalid-endpoint')
    
    assert response.status_code == 404
    error_data = response.json()
    assert error_data['Type'] == 'ResourceNotFoundException'
    assert 'Message' in error_data
```

## Best Practices

### Error Message Guidelines

1. **Be specific**: Include relevant details about what went wrong
2. **Be actionable**: Suggest how to fix the issue when possible
3. **Be consistent**: Use consistent terminology across similar errors
4. **Avoid sensitive data**: Don't include passwords, tokens, or PII

### Exception Selection

1. **InvalidParameterValueException**: For all input validation errors
2. **ResourceNotFoundException**: When requested resources don't exist
3. **ServiceException**: For unexpected internal errors only
4. **CallbackTimeoutException**: Specifically for callback timeouts
5. **ExecutionAlreadyStartedException**: Only for duplicate execution starts

### HTTP Status Codes

1. **Use standard codes**: Follow HTTP and AWS conventions
2. **Be consistent**: Same error types should use same status codes
3. **Client vs Server**: 4xx for client errors, 5xx for server errors

## Troubleshooting

### Common Issues

1. **Wrong field names**: Ensure "message" vs "Message" matches exception type
2. **Missing Type field**: All exceptions except ExecutionAlreadyStartedException need "Type"
3. **Wrong status codes**: Verify HTTP status matches exception type
4. **JSON serialization**: Ensure all fields are JSON-serializable

### Debugging Tips

1. **Check exception type**: Verify you're using the correct AWS exception
2. **Validate JSON structure**: Use `to_dict()` to see exact output
3. **Test with boto3**: Verify compatibility with actual boto3 client
4. **Compare with AWS**: Match format with real AWS service responses
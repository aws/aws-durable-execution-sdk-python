# Serialization

Learn how the SDK serializes and deserializes data for durable execution checkpoints.

## Table of Contents

- [Terminology](#terminology)
- [What is serialization?](#what-is-serialization)
- [Key features](#key-features)
- [Default serialization behavior](#default-serialization-behavior)
- [Supported types](#supported-types)
- [Converting non-serializable types](#converting-non-serializable-types)
- [Custom serialization](#custom-serialization)
- [Serialization in configurations](#serialization-in-configurations)
- [Best practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)

[← Back to main index](../index.md)

## Terminology

**Serialization** - Converting Python objects to strings for storage in checkpoints.

**Deserialization** - Converting checkpoint strings back to Python objects.

**SerDes** - Short for Serializer/Deserializer, a custom class that handles both serialization and deserialization.

**Checkpoint** - A saved state of execution that includes serialized operation results.

**Extended types** - Types beyond basic JSON (datetime, Decimal, UUID, bytes) that the SDK serializes automatically.

**Envelope format** - The SDK's internal format that wraps complex types with type tags for accurate deserialization.

[↑ Back to top](#table-of-contents)

## What is serialization?

Serialization converts Python objects into strings that can be stored in checkpoints. When your durable function resumes, deserialization converts those strings back into Python objects. The SDK handles this automatically for most types.

[↑ Back to top](#table-of-contents)

## Key features

- Automatic serialization for common Python types
- Extended type support (datetime, Decimal, UUID, bytes)
- Custom serialization for complex objects
- Type preservation during round-trip serialization
- Efficient plain JSON for primitives

[↑ Back to top](#table-of-contents)

## Default serialization behavior

The SDK handles most Python types automatically:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # All these types serialize automatically
    result = context.step(
        process_order,
        order_id=uuid4(),
        amount=Decimal("99.99"),
        timestamp=datetime.now()
    )
    return result
```

The SDK serializes data automatically when:
- Checkpointing step results
- Storing callback payloads
- Passing data to child contexts
- Returning results from your handler

[↑ Back to top](#table-of-contents)

## Supported types

### Primitive types

These types serialize as plain JSON for performance:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Primitives - plain JSON
    none_value = None
    text = "hello"
    number = 42
    decimal_num = 3.14
    flag = True
    
    # Simple lists of primitives - plain JSON
    numbers = [1, 2, 3, 4, 5]
    
    return {
        "none": none_value,
        "text": text,
        "number": number,
        "decimal": decimal_num,
        "flag": flag,
        "numbers": numbers
    }
```

**Supported primitive types:**
- `None`
- `str`
- `int`
- `float`
- `bool`
- Lists containing only primitives

[↑ Back to top](#table-of-contents)

### Extended types

The SDK automatically handles these types using envelope format:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID, uuid4

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Extended types - automatic serialization
    order_data = {
        "order_id": uuid4(),                    # UUID
        "amount": Decimal("99.99"),             # Decimal
        "created_at": datetime.now(),           # datetime
        "delivery_date": date.today(),          # date
        "signature": b"binary_signature_data",  # bytes
        "coordinates": (40.7128, -74.0060),     # tuple
    }
    
    result = context.step(process_order, order_data)
    return result
```

**Supported extended types:**
- `datetime` - ISO format with timezone
- `date` - ISO date format
- `Decimal` - Precise decimal numbers
- `UUID` - Universally unique identifiers
- `bytes`, `bytearray`, `memoryview` - Binary data (base64 encoded)
- `tuple` - Immutable sequences
- `list` - Mutable sequences (including nested)
- `dict` - Dictionaries (including nested)

[↑ Back to top](#table-of-contents)

### Container types

Containers can hold any supported type, including nested containers:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Nested structures serialize automatically
    complex_data = {
        "user": {
            "id": uuid4(),
            "created": datetime.now(),
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
        }
    }
    
    result = context.step(process_user, complex_data)
    return result
```

[↑ Back to top](#table-of-contents)

## Converting non-serializable types

Some Python types aren't serializable by default. Convert them before passing to durable operations.

### Dataclasses

Convert dataclasses to dictionaries:

```python
from dataclasses import dataclass, asdict
from aws_durable_execution_sdk_python import DurableContext, durable_execution

@dataclass
class Order:
    order_id: str
    amount: float
    customer: str

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    order = Order(
        order_id="ORD-123",
        amount=99.99,
        customer="Jane Doe"
    )
    
    # Convert to dict before passing to step
    result = context.step(process_order, asdict(order))
    return result
```

### Pydantic models

Use Pydantic's built-in serialization:

```python
from pydantic import BaseModel
from aws_durable_execution_sdk_python import DurableContext, durable_execution

class Order(BaseModel):
    order_id: str
    amount: float
    customer: str

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    order = Order(
        order_id="ORD-123",
        amount=99.99,
        customer="Jane Doe"
    )
    
    # Use model_dump() to convert to dict
    result = context.step(process_order, order.model_dump())
    return result
```

### Custom objects

Implement `to_dict()` and `from_dict()` methods:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution

class Order:
    def __init__(self, order_id: str, amount: float, customer: str):
        self.order_id = order_id
        self.amount = amount
        self.customer = customer
    
    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "amount": self.amount,
            "customer": self.customer
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Order":
        return cls(
            order_id=data["order_id"],
            amount=data["amount"],
            customer=data["customer"]
        )

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    order = Order("ORD-123", 99.99, "Jane Doe")
    
    # Convert to dict before passing to step
    result = context.step(process_order, order.to_dict())
    return result
```

[↑ Back to top](#table-of-contents)

## Custom serialization

Implement custom serialization for specialized needs like encryption or compression.

### Creating a custom SerDes

Extend the `SerDes` base class:

```python
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
import json

class UpperCaseSerDes(SerDes[str]):
    """Example: Convert strings to uppercase during serialization."""
    
    def serialize(self, value: str, serdes_context: SerDesContext) -> str:
        return value.upper()
    
    def deserialize(self, data: str, serdes_context: SerDesContext) -> str:
        return data.lower()
```

### Using custom SerDes with steps

Pass your custom SerDes in `StepConfig`:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution, durable_step, StepContext
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
import json

class CompressedSerDes(SerDes[dict]):
    """Example: Compress large dictionaries."""
    
    def serialize(self, value: dict, serdes_context: SerDesContext) -> str:
        # In production, use actual compression like gzip
        return json.dumps(value, separators=(',', ':'))
    
    def deserialize(self, data: str, serdes_context: SerDesContext) -> dict:
        return json.loads(data)

@durable_step
def process_large_data(step_context: StepContext, data: dict) -> dict:
    # Process the data
    return {"processed": True, "items": len(data)}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    large_data = {"items": [f"item_{i}" for i in range(1000)]}
    
    # Use custom SerDes for this step
    config = StepConfig(serdes=CompressedSerDes())
    result = context.step(process_large_data(large_data), config=config)
    
    return result
```

### Encryption example

Encrypt sensitive data in checkpoints:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution, durable_step, StepContext
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext
import json
import base64

class EncryptedSerDes(SerDes[dict]):
    """Example: Encrypt sensitive data (simplified for demonstration)."""
    
    def __init__(self, encryption_key: str):
        self.encryption_key = encryption_key
    
    def serialize(self, value: dict, serdes_context: SerDesContext) -> str:
        json_str = json.dumps(value)
        # In production, use proper encryption like AWS KMS
        encrypted = base64.b64encode(json_str.encode()).decode()
        return encrypted
    
    def deserialize(self, data: str, serdes_context: SerDesContext) -> dict:
        # In production, use proper decryption
        decrypted = base64.b64decode(data.encode()).decode()
        return json.loads(decrypted)

@durable_step
def process_sensitive_data(step_context: StepContext, data: dict) -> dict:
    return {"processed": True}

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    sensitive_data = {
        "ssn": "123-45-6789",
        "credit_card": "4111-1111-1111-1111"
    }
    
    # Encrypt data in checkpoints
    config = StepConfig(serdes=EncryptedSerDes("my-key"))
    result = context.step(process_sensitive_data(sensitive_data), config=config)
    
    return result
```

[↑ Back to top](#table-of-contents)

## Serialization in configurations

Different operations support custom serialization through their configuration objects.

### StepConfig

Control serialization for step results:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import StepConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    config = StepConfig(serdes=CustomSerDes())
    result = context.step(my_function(), config=config)
    return result
```

### CallbackConfig

Control serialization for callback payloads:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import CallbackConfig, Duration

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    config = CallbackConfig(
        timeout=Duration.from_hours(2),
        serdes=CustomSerDes()
    )
    callback = context.create_callback(config=config)
    
    # Send callback.callback_id to external system
    return {"callback_id": callback.callback_id}
```

### MapConfig and ParallelConfig

Control serialization for batch results:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import MapConfig

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    items = [1, 2, 3, 4, 5]
    
    # Custom serialization for BatchResult
    config = MapConfig(
        serdes=CustomSerDes(),        # For the entire BatchResult
        item_serdes=ItemSerDes()      # For individual item results
    )
    
    result = context.map(process_item, items, config=config)
    return {"processed": len(result.succeeded)}
```

**Note:** When both `serdes` and `item_serdes` are provided:
- `item_serdes` serializes individual item results in child contexts
- `serdes` serializes the entire `BatchResult` at the handler level

For backward compatibility, if only `serdes` is provided, it's used for both individual items and the `BatchResult`.

[↑ Back to top](#table-of-contents)

## Best practices

### Use default serialization when possible

The SDK handles most cases efficiently without custom serialization:

```python
# Good - uses default serialization
from datetime import datetime
from decimal import Decimal

result = context.step(
    process_order,
    order_id="ORD-123",
    amount=Decimal("99.99"),
    timestamp=datetime.now()
)
```

### Convert complex objects to dicts

Convert custom objects to dictionaries before passing to durable operations:

```python
# Good - convert to dict first
order_dict = order.to_dict()
result = context.step(process_order, order_dict)

# Avoid - custom objects aren't serializable
result = context.step(process_order, order)  # Will fail
```

### Keep serialized data small

Large checkpoints might slow down execution. Keep data compact:

```python
# Good - only checkpoint what you need
result = context.step(
    process_data,
    {"id": order.id, "amount": order.amount}
)

# Avoid - large objects in checkpoints
result = context.step(
    process_data,
    entire_database_dump  # Too large
)
```

### Use appropriate types

Choose types that serialize efficiently:

```python
# Good - Decimal for precise amounts
amount = Decimal("99.99")

# Avoid - float for money (precision issues)
amount = 99.99
```

### Test serialization round-trips

Verify your data survives serialization:

```python
from aws_durable_execution_sdk_python.serdes import serialize, deserialize

def test_serialization():
    original = {"amount": Decimal("99.99")}
    serialized = serialize(None, original, "test-op", "test-arn")
    deserialized = deserialize(None, serialized, "test-op", "test-arn")
    
    assert deserialized == original
```

### Handle serialization errors gracefully

Catch and handle serialization errors:

```python
from aws_durable_execution_sdk_python.exceptions import ExecutionError

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    try:
        result = context.step(process_data, complex_object)
    except ExecutionError as e:
        if "Serialization failed" in str(e):
            # Convert to serializable format
            simple_data = convert_to_dict(complex_object)
            result = context.step(process_data, simple_data)
        else:
            raise
    
    return result
```

[↑ Back to top](#table-of-contents)

## Troubleshooting

### Unsupported type error

**Problem:** `SerDesError: Unsupported type: <class 'MyClass'>`

**Solution:** Convert custom objects to supported types:

```python
# Before - fails
result = context.step(process_order, order_object)

# After - works
result = context.step(process_order, order_object.to_dict())
```

### Serialization failed error

**Problem:** `ExecutionError: Serialization failed for id: step-123`

**Cause:** The data contains types that can't be serialized.

**Solution:** Check for circular references or unsupported types:

```python
# Circular reference - fails
data = {"self": None}
data["self"] = data

# Fix - remove circular reference
data = {"id": 123, "name": "test"}
```

### Type not preserved after deserialization

**Problem:** `tuple` becomes `list` or `Decimal` becomes `float`

**Cause:** Using a custom SerDes that doesn't preserve types.

**Solution:** Use default serialization which preserves types:

```python
# Default serialization preserves tuple
result = context.step(process_data, (1, 2, 3))  # Stays as tuple

# If using custom SerDes, ensure it preserves types
class TypePreservingSerDes(SerDes[Any]):
    def serialize(self, value: Any, context: SerDesContext) -> str:
        # Implement type preservation logic
        pass
```

### Large payload errors

**Problem:** Checkpoint size exceeds limits

**Solution:** Reduce data size or use summary generators:

```python
# Option 1: Reduce data
small_data = {"id": order.id, "status": order.status}
result = context.step(process_order, small_data)

# Option 2: Use summary generator (for map/parallel)
def generate_summary(result):
    return json.dumps({"count": len(result.all)})

config = MapConfig(summary_generator=generate_summary)
result = context.map(process_item, items, config=config)
```

### Datetime timezone issues

**Problem:** Datetime loses timezone information

**Solution:** Always use timezone-aware datetime objects:

```python
from datetime import datetime, UTC

# Good - timezone aware
timestamp = datetime.now(UTC)

# Avoid - naive datetime
timestamp = datetime.now()  # No timezone
```

[↑ Back to top](#table-of-contents)

## FAQ

### What types can I serialize?

The SDK supports:
- Primitives: `None`, `str`, `int`, `float`, `bool`
- Extended: `datetime`, `date`, `Decimal`, `UUID`, `bytes`, `tuple`
- Containers: `list`, `dict` (including nested)

For other types, convert to dictionaries first.

### Do I need custom serialization?

Most applications don't need custom serialization. Use it for:
- Encryption of sensitive data
- Compression of large payloads
- Special encoding requirements
- Legacy format compatibility

### How does serialization affect performance?

The SDK optimizes for performance:
- Primitives use plain JSON (fast)
- Extended types use envelope format (slightly slower but preserves types)
- Custom SerDes adds overhead based on your implementation

### Can I serialize Pydantic models?

Yes, convert them to dictionaries:

```python
order = Order(order_id="ORD-123", amount=99.99)
result = context.step(process_order, order.model_dump())
```

### What's the difference between serdes and item_serdes?

In `MapConfig` and `ParallelConfig`:
- `item_serdes`: Serializes individual item results in child contexts
- `serdes`: Serializes the entire `BatchResult` at handler level

If only `serdes` is provided, it's used for both (backward compatibility).

### How do I handle binary data?

Use `bytes` type - it's automatically base64 encoded:

```python
binary_data = b"binary content"
result = context.step(process_binary, binary_data)
```

### Can I use JSON strings directly?

Yes, use `PassThroughSerDes` or `JsonSerDes`:

```python
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.config import StepConfig

config = StepConfig(serdes=JsonSerDes())
result = context.step(process_json, json_string, config=config)
```

### What happens if serialization fails?

The SDK raises `ExecutionError` with details. Handle it in your code:

```python
from aws_durable_execution_sdk_python.exceptions import ExecutionError

try:
    result = context.step(process_data, data)
except ExecutionError as e:
    context.logger.error(f"Serialization failed: {e}")
    # Handle error or convert data
```

### How do I debug serialization issues?

Test serialization independently:

```python
from aws_durable_execution_sdk_python.serdes import serialize, deserialize

try:
    serialized = serialize(None, my_data, "test-op", "test-arn")
    deserialized = deserialize(None, serialized, "test-op", "test-arn")
    print("Serialization successful")
except Exception as e:
    print(f"Serialization failed: {e}")
```

### Are there size limits for serialized data?

Yes, checkpoints have size limits (typically 256KB). Keep data compact:
- Only checkpoint necessary data
- Use summary generators for large results
- Store large data externally (S3) and checkpoint references

[↑ Back to top](#table-of-contents)

## See also

- [Steps](../core/steps.md) - Using steps with custom serialization
- [Callbacks](../core/callbacks.md) - Serializing callback payloads
- [Map Operations](../core/map.md) - Serialization in map operations
- [Error Handling](error-handling.md) - Handling serialization errors
- [Best Practices](../best-practices.md) - General best practices

[↑ Back to index](#table-of-contents)

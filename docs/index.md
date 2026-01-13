# AWS Durable Execution SDK for Python

> **Using JavaScript or TypeScript?** Check out the [AWS Durable Execution SDK for JavaScript](https://github.com/aws/aws-durable-execution-sdk-js) instead.

## Table of Contents

- [What is the Durable Execution SDK?](#what-is-the-durable-execution-sdk)
- [Key features](#key-features)
- [Installation](#installation)
- [Quick example](#quick-example)
- [Core concepts](#core-concepts)
- [Architecture](#architecture)
- [Use cases](#use-cases)
- [Getting help](#getting-help)
- [License](#license)

## What is the Durable Execution SDK?

The AWS Durable Execution SDK for Python lets you build reliable, long-running workflows in AWS Lambda. Your functions can pause execution, wait for external events, retry failed operations, and resume exactly where they left off—even if Lambda recycles your execution environment.

The SDK provides a `DurableContext` that gives you operations like steps, waits, callbacks, and parallel execution. Each operation is checkpointed automatically, so your workflow state is preserved across interruptions.

[↑ Back to top](#table-of-contents)

## Key features

- **Automatic checkpointing** - Your workflow state is saved automatically after each operation
- **Durable steps** - Execute code with configurable retry strategies and at-most-once or at-least-once semantics
- **Wait operations** - Pause execution for seconds, minutes, or hours without blocking Lambda resources
- **Callbacks** - Wait for external systems to respond with results or approvals
- **Parallel execution** - Run multiple operations concurrently with configurable completion criteria
- **Map operations** - Process collections in parallel with batching and failure tolerance
- **Child contexts** - Isolate nested workflows for better organization and error handling
- **Structured logging** - Integrate with your logger to track execution flow and debug issues

[↑ Back to top](#table-of-contents)

## Installation

Install the SDK using pip:

```console
pip install aws-durable-execution-sdk-python
```

[↑ Back to top](#table-of-contents)

## Quick example

Here's a simple durable function that processes an order:

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_step,
)

@durable_step
def validate_order(order_id: str) -> dict:
    # Validation logic here
    return {"order_id": order_id, "valid": True}

@durable_step
def charge_payment(order_id: str, amount: float) -> dict:
    # Payment processing logic here
    return {"transaction_id": "txn_123", "status": "completed"}

@durable_step
def fulfill_order(order_id: str) -> dict:
    # Fulfillment logic here
    return {"tracking_number": "TRK123456"}

@durable_execution
def process_order(event: dict, context: DurableContext) -> dict:
    order_id = event["order_id"]
    amount = event["amount"]
    
    # Step 1: Validate the order
    validation = context.step(validate_order(order_id))
    
    if not validation["valid"]:
        return {"status": "failed", "reason": "invalid_order"}
    
    # Step 2: Charge payment
    payment = context.step(charge_payment(order_id, amount))
    
    # Step 3: Wait for payment confirmation (simulated)
    context.wait(seconds=5)
    
    # Step 4: Fulfill the order
    fulfillment = context.step(fulfill_order(order_id))
    
    return {
        "status": "completed",
        "order_id": order_id,
        "transaction_id": payment["transaction_id"],
        "tracking_number": fulfillment["tracking_number"]
    }
```

Each `context.step()` call is checkpointed automatically. If Lambda recycles your execution environment, the function resumes from the last completed step.

[↑ Back to top](#table-of-contents)

## Core concepts

### Durable functions

A durable function is a Lambda function decorated with `@durable_execution` that can be checkpointed and resumed. The function receives a `DurableContext` that provides methods for durable operations.

### Operations

Operations are units of work in a durable execution. Each operation type serves a specific purpose:

- **Steps** - Execute code and checkpoint the result with retry support
- **Waits** - Pause execution for a specified duration without blocking Lambda
- **Callbacks** - Wait for external systems to respond with results
- **Invoke** - Call other durable functions to compose complex workflows
- **Child contexts** - Isolate nested workflows for better organization
- **Parallel** - Execute multiple operations concurrently with completion criteria
- **Map** - Process collections in parallel with batching and failure tolerance

### Checkpoints

Checkpoints are saved states of execution that allow resumption. When your function calls `context.step()` or other operations, the SDK creates a checkpoint and sends it to AWS. If Lambda recycles your environment or your function waits for an external event, execution can resume from the last checkpoint.

### Replay

When your function resumes, completed operations don't re-execute. Instead, they return their checkpointed results instantly. This means your function code runs multiple times, but side effects only happen once per operation.

### Decorators

The SDK provides decorators to mark functions as durable:

- `@durable_execution` - Marks your Lambda handler as a durable function
- `@durable_step` - Marks a function that can be used with `context.step()`
- `@durable_with_child_context` - Marks a function that receives a child context

[↑ Back to top](#table-of-contents)

## Architecture

The SDK integrates with AWS Lambda's durable execution service to provide reliable, long-running workflows. Here's how it works:

1. **Execution starts** - Lambda invokes your function with a `DurableContext`
2. **Operations checkpoint** - Each `context.step()`, `context.wait()`, or other operation creates a checkpoint
3. **State is saved** - Checkpoints are sent to the durable execution service and persisted
4. **Execution may pause** - Lambda can recycle your environment or wait for external events
5. **Execution resumes** - When ready, Lambda invokes your function again with the saved state
6. **Operations replay** - Completed operations return their saved results instantly
7. **New operations execute** - Your function continues from where it left off

### Key components

- **DurableContext** - Main interface for durable operations, provided by Lambda
- **ExecutionState** - Manages checkpoints and tracks operation results
- **Operation handlers** - Execute steps, waits, callbacks, and other operations
- **Checkpoint batching** - Groups multiple checkpoints into efficient API calls
- **SerDes system** - Serializes and deserializes operation inputs and results

### Checkpointing

The SDK uses a background thread to batch checkpoints for efficiency. Critical operations (like step starts with at-most-once semantics) block until the checkpoint is confirmed. Non-critical operations (like observability checkpoints) are asynchronous for better performance

[**See architecture diagrams**](architecture.md) for class diagrams and concurrency flows.

[↑ Back to top](#table-of-contents)

## Use cases

The SDK helps you build:

**Order processing workflows** - Validate orders, charge payments, and fulfill shipments with automatic retry on failures.

**Approval workflows** - Wait for human approvals or external system responses using callbacks.

**Data processing pipelines** - Process large datasets in parallel with map operations and failure tolerance.

**Multi-step integrations** - Coordinate calls to multiple services with proper error handling and state management.

**Long-running tasks** - Execute workflows that take minutes or hours without blocking Lambda resources.

**Saga patterns** - Implement distributed transactions with compensation logic for failures.

[↑ Back to top](#table-of-contents)

## Getting help

**Documentation** - You're reading it! Use the navigation above to find specific topics.

**Examples** - Check the `examples/` directory in the repository for working code samples.

**Issues** - Report bugs or request features on the [GitHub repository](https://github.com/awslabs/aws-durable-execution-sdk-python).

**Contributing** - See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines on contributing to the project.

[↑ Back to top](#table-of-contents)

## License

See the [LICENSE](../LICENSE) file for our project's licensing.

[↑ Back to top](#table-of-contents)

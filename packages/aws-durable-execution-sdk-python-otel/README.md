# aws-durable-execution-sdk-python-otel

OpenTelemetry instrumentation for the [AWS Durable Execution SDK for Python](https://github.com/aws/aws-durable-execution-sdk-python).

## Overview

This package provides automatic OpenTelemetry tracing for durable execution workflows, giving you visibility into step execution, waits, retries, and overall workflow performance.

## Installation

```bash
pip install aws-durable-execution-sdk-python-otel
```

## Quick Start

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python_otel import instrument_durable_execution

# Instrument the SDK (call once at module load)
instrument_durable_execution()

@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # Steps, waits, and invokes are automatically traced
    result = context.step(lambda _: do_work(), name="my-step")
    return {"result": result}
```

## Features

- Automatic span creation for steps, waits, invokes, and child contexts
- Replay-aware tracing (distinguishes fresh executions from replays)
- Error recording with proper OTel status codes
- Configurable span attributes and naming

## Requirements

- Python >= 3.11
- `aws-durable-execution-sdk-python` >= 1.5.0
- `opentelemetry-api` >= 1.20.0
- `opentelemetry-sdk` >= 1.20.0

## License

Apache-2.0

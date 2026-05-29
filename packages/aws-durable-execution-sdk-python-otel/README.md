# aws-durable-execution-sdk-python-otel

OpenTelemetry instrumentation for the [AWS Durable Execution SDK for Python](https://github.com/aws/aws-durable-execution-sdk-python).

> **Note:** v0.1.0 reserves the package name. Instrumentation lands in v0.2.0.

## Overview

This package will provide automatic OpenTelemetry tracing for durable execution workflows, giving you visibility into step execution, waits, retries, and overall workflow performance.

## Installation

```bash
pip install aws-durable-execution-sdk-python-otel
```

## Quick Start

```python
from aws_durable_execution_sdk_python_otel import __version__

print(__version__)
```

## Planned Features (v0.2.0)

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

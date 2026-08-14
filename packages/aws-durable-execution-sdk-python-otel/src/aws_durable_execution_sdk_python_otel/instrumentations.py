"""Shared instrumentation registration for the durable-execution OTel plugins.

Mirrors the JS ``registerStandaloneInstrumentations``:

* A custom (explicit) provider skips ALL instrumentation registration.
* When the global provider is in use (``ProviderSource.GLOBAL``), only the
  AWS SDK instrumentation is registered (not HTTP).

The JS SDK uses ``AwsInstrumentation`` (AWS SDK v3). The Python equivalent is
``BotocoreInstrumentor`` because boto3/botocore is the AWS SDK for Python. The
instrumentation package is an optional import: when it is not installed,
registration is skipped with a warning rather than raising, so the module stays
import-safe.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python_otel.otel_plugin_config import ProviderSource


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_otel.provider import ProviderResult


logger = logging.getLogger(__name__)


def _register_aws_instrumentation(tracer_provider: object | None) -> None:
    """Register AWS SDK (botocore) instrumentation, if the package is available."""
    try:
        from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    except ImportError:
        logger.warning(
            "opentelemetry-instrumentation-botocore is not installed; "
            "AWS SDK calls will not be traced. Install it to enable AWS "
            "instrumentation."
        )
        return
    instrumentor = BotocoreInstrumentor()
    if not instrumentor.is_instrumented_by_opentelemetry:
        kwargs = {}
        if tracer_provider is not None:
            kwargs["tracer_provider"] = tracer_provider
        instrumentor.instrument(**kwargs)


def register_standalone_instrumentations(result: ProviderResult) -> None:
    """Register AWS SDK instrumentation per the resolved source.

    Args:
        result: The resolved provider and its :class:`ProviderSource`.
    """
    if result.source is ProviderSource.EXPLICIT:
        # Caller manages their own instrumentation: skip everything.
        return

    if result.source is ProviderSource.GLOBAL:
        # Global provider: register AWS instrumentation only.
        _register_aws_instrumentation(None)
        return

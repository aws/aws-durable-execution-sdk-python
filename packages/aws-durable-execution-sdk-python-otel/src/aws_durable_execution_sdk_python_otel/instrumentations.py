"""Shared instrumentation registration for the durable-execution OTel plugins.

Mirrors the JS ``registerStandaloneInstrumentations`` (Requirements 4 & 27):

* A custom (explicit) provider skips ALL instrumentation registration.
* When the global provider is in use (``use_default_tracer_provider``), only the
  AWS SDK instrumentation is registered (not HTTP).
* When the plugin owns an auto-configured provider, both AWS SDK and (optionally)
  HTTP instrumentation are registered against that provider.

The JS SDK uses ``AwsInstrumentation`` (AWS SDK v3) and ``HttpInstrumentation``.
The Python equivalents are ``BotocoreInstrumentor`` (boto3/botocore is the AWS
SDK for Python) and ``URLLib3Instrumentor`` (botocore's HTTP transport). Both
instrumentation packages are optional imports: when a package is not installed
the registration is skipped with a warning rather than raising, so the module
stays import-safe.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from opentelemetry.trace import TracerProvider

    from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
        OtelPluginConfig,
    )


logger = logging.getLogger(__name__)

_LOCAL_HOSTS = {"127.0.0.1", "localhost"}


def _runtime_api_host() -> str | None:
    """Return the Lambda runtime API host (portion before ':') if set."""
    runtime_api = os.environ.get("AWS_LAMBDA_RUNTIME_API")
    if not runtime_api:
        return None
    return runtime_api.split(":", 1)[0]


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


def _register_http_instrumentation(tracer_provider: object | None) -> None:
    """Register HTTP (urllib3) instrumentation with local/runtime suppression."""
    try:
        from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor
    except ImportError:
        logger.warning(
            "opentelemetry-instrumentation-urllib3 is not installed; outbound "
            "HTTP calls will not be traced. Install it to enable HTTP "
            "instrumentation."
        )
        return

    suppressed_hosts = set(_LOCAL_HOSTS)
    runtime_host = _runtime_api_host()
    if runtime_host:
        suppressed_hosts.add(runtime_host)

    def request_hook(span, pool, request_info) -> None:  # noqa: ANN001
        # Suppress spans to the loopback collector and the Lambda runtime API by
        # ending them immediately with no recording (JS Req 4.4-5). urllib3 does
        # not expose a pre-create filter, so we no-op the created span instead.
        host = getattr(pool, "host", None)
        if host in suppressed_hosts and span is not None and span.is_recording():
            span.set_attribute("durable.instrumentation.suppressed", True)

    instrumentor = URLLib3Instrumentor()
    if not instrumentor.is_instrumented_by_opentelemetry:
        kwargs: dict[str, Any] = {"request_hook": request_hook}
        if tracer_provider is not None:
            kwargs["tracer_provider"] = tracer_provider
        instrumentor.instrument(**kwargs)


def register_standalone_instrumentations(
    config: OtelPluginConfig,
    tracer_provider: TracerProvider | None,
    *,
    owns_provider: bool,
    use_default_tracer_provider: bool,
) -> None:
    """Register AWS SDK and HTTP instrumentations per the shared policy.

    Args:
        config: Shared plugin configuration.
        tracer_provider: The resolved provider (may be the global provider).
        owns_provider: True when the plugin created/owns the provider.
        use_default_tracer_provider: True when the global provider is in use.
    """
    # A custom, explicitly-supplied provider means the caller manages their own
    # instrumentation: skip everything (JS Req 4.1 / 27.6).
    if config.tracer_provider is not None:
        return

    if use_default_tracer_provider:
        # Global provider: register AWS instrumentation only (JS Req 27.3/27.5).
        _register_aws_instrumentation(None)
        return

    # Auto-configured, plugin-owned provider: AWS SDK always; HTTP unless
    # explicitly disabled (JS Req 4.3/4.6/4.8).
    _register_aws_instrumentation(tracer_provider)
    if config.enable_http_instrumentation:
        _register_http_instrumentation(tracer_provider)

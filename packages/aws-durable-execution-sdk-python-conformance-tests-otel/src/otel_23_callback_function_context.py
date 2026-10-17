# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Observe callback contexts at SDK-owned user-function lifecycle boundaries."""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import (
    ChildConfig,
    Duration,
    JitterStrategy,
    StepConfig,
)
from aws_durable_execution_sdk_python.exceptions import ChildContextError
from aws_durable_execution_sdk_python.retries import (
    RetryDecision,
    RetryStrategyConfig,
    WithRetryConfig,
    create_retry_strategy,
    with_retry,
)
from aws_durable_execution_sdk_python.types import (
    WaitForCallbackContext,
    WaitForConditionCheckContext,
)
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from common import otel_plugin, require_scenario
from opentelemetry import trace


HELPER_FAILURE = "intentional-helper-failure"


def probe(label: str) -> None:
    if not trace.get_current_span().get_span_context().is_valid:
        raise RuntimeError(f"No active span for conformance.{label}")
    span = trace.get_tracer("aws-durable-execution-conformance").start_span(
        f"conformance.{label}", attributes={"conformance.callback": label}
    )
    span.end()


def retry_step(context: StepContext) -> str:
    probe(f"retry-attempt-{context.attempt}")
    if context.attempt == 1:
        raise RuntimeError("intentional-step-retry")
    return "retried"


def check_condition(state: int, _context: WaitForConditionCheckContext) -> int:
    next_state = state + 1
    probe(f"condition-check-{next_state}")
    return next_state


def wait_strategy(state: int, _attempt: int) -> WaitForConditionDecision:
    if state >= 2:
        return WaitForConditionDecision.stop_polling()
    return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))


def submit_callback(_callback_id: str, _context: WaitForCallbackContext) -> None:
    probe("callback-submitter")


def helper_body(_context: DurableContext, _attempt: int) -> str:
    probe("with-retry-body")
    raise RuntimeError(HELPER_FAILURE)


def helper_retry_strategy(_error: Exception, _attempt: int) -> RetryDecision:
    probe("with-retry-strategy")
    return RetryDecision.no_retry()


def virtual_child(_context: DurableContext) -> str:
    probe("virtual-child")
    return "virtual"


@durable_execution(plugins=[otel_plugin()])
def handler(event: dict[str, Any], context: DurableContext) -> str:
    require_scenario(event, "callback-function-context")
    context.step(
        retry_step,
        name="otel-context-retry-step",
        config=StepConfig(
            retry_strategy=create_retry_strategy(
                RetryStrategyConfig(
                    max_attempts=2,
                    initial_delay=Duration.from_seconds(1),
                    max_delay=Duration.from_seconds(1),
                    backoff_rate=1.0,
                    jitter_strategy=JitterStrategy.NONE,
                    retryable_error_types=[RuntimeError],
                )
            )
        ),
    )
    context.wait_for_condition(
        check_condition,
        name="otel-context-condition",
        config=WaitForConditionConfig(initial_state=0, wait_strategy=wait_strategy),
    )
    context.wait_for_callback(submit_callback, name="otel-context-callback")

    # These callbacks run after the last asynchronous wait, so normal replay
    # does not repeat the helper body or the checkpointless virtual child.
    try:
        with_retry(
            context,
            helper_body,
            WithRetryConfig(retry_strategy=helper_retry_strategy),
            name="otel-context-with-retry",
        )
    except ChildContextError as error:
        if error.message != HELPER_FAILURE:
            raise
    else:
        raise RuntimeError("Expected the intentional helper failure")
    context.run_in_child_context(
        virtual_child,
        name="otel-context-virtual",
        config=ChildConfig(is_virtual=True),
    )
    return "callback-context-complete"

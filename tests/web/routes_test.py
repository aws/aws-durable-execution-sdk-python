"""Tests for the strongly-typed route parsing system."""

from __future__ import annotations

import threading
import time
from urllib.parse import quote

import pytest

from aws_durable_execution_sdk_python_testing.exceptions import (
    UnknownRouteError,
)
from aws_durable_execution_sdk_python_testing.web.routes import (
    CallbackFailureRoute,
    CallbackHeartbeatRoute,
    CallbackSuccessRoute,
    CheckpointDurableExecutionRoute,
    GetDurableExecutionHistoryRoute,
    GetDurableExecutionRoute,
    GetDurableExecutionStateRoute,
    HealthRoute,
    ListDurableExecutionsByFunctionRoute,
    ListDurableExecutionsRoute,
    MetricsRoute,
    Route,
    Router,
    StartExecutionRoute,
    StopDurableExecutionRoute,
)


def test_route_from_string_basic():
    """Test basic route creation from string."""
    route = Route.from_string("/test/path")
    assert route.raw_path == "/test/path"
    assert route.segments == ["test", "path"]


def test_route_from_string_with_leading_trailing_slashes():
    """Test route creation handles leading and trailing slashes."""
    route = Route.from_string("///test/path///")
    assert route.raw_path == "///test/path///"
    assert route.segments == ["test", "path"]


def test_route_from_string_empty_segments():
    """Test route creation filters out empty segments."""
    route = Route.from_string("/test//path/")
    assert route.raw_path == "/test//path/"
    assert route.segments == ["test", "path"]


def test_route_from_string_root():
    """Test route creation for root path."""
    route = Route.from_string("/")
    assert route.raw_path == "/"
    assert route.segments == []


def test_route_matches_pattern_exact():
    """Test pattern matching with exact segments."""
    route = Route.from_string("/test/path")
    assert route.matches_pattern(["test", "path"]) is True
    assert route.matches_pattern(["test", "other"]) is False


def test_route_matches_pattern_wildcard():
    """Test pattern matching with wildcards."""
    route = Route.from_string("/test/123/path")
    assert route.matches_pattern(["test", "*", "path"]) is True
    assert route.matches_pattern(["test", "*", "other"]) is False


def test_route_matches_pattern_length_mismatch():
    """Test pattern matching fails with different lengths."""
    route = Route.from_string("/test/path")
    assert route.matches_pattern(["test"]) is False
    assert route.matches_pattern(["test", "path", "extra"]) is False


def test_start_execution_route_is_match():
    """Test StartExecutionRoute pattern matching."""
    route = Route.from_string("/start-durable-execution")
    assert StartExecutionRoute.is_match(route, "POST") is True
    assert StartExecutionRoute.is_match(route, "GET") is False

    route = Route.from_string("/start-execution")
    assert StartExecutionRoute.is_match(route, "POST") is False


def test_start_execution_route_from_route():
    """Test StartExecutionRoute creation from base route."""
    base_route = Route.from_string("/start-durable-execution")
    start_route = StartExecutionRoute.from_route(base_route)

    assert start_route.raw_path == "/start-durable-execution"
    assert start_route.segments == ["start-durable-execution"]


# Removed test_start_execution_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_get_durable_execution_route_is_match():
    """Test GetDurableExecutionRoute pattern matching."""
    route = Route.from_string(
        "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:123456789012:function:my-function"
    )
    assert GetDurableExecutionRoute.is_match(route, "GET") is True
    assert GetDurableExecutionRoute.is_match(route, "POST") is False

    route = Route.from_string("/2025-12-01/executions/some-arn")
    assert GetDurableExecutionRoute.is_match(route, "GET") is False

    route = Route.from_string("/2025-12-01/durable-executions")
    assert GetDurableExecutionRoute.is_match(route, "GET") is False


def test_get_durable_execution_route_from_route():
    """Test GetDurableExecutionRoute creation from base route."""
    arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
    base_route = Route.from_string(f"/2025-12-01/durable-executions/{arn}")
    get_route = GetDurableExecutionRoute.from_route(base_route)

    assert get_route.raw_path == f"/2025-12-01/durable-executions/{arn}"
    assert get_route.segments == ["2025-12-01", "durable-executions", arn]
    assert get_route.arn == arn


# Removed test_get_durable_execution_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_checkpoint_durable_execution_route_is_match():
    """Test CheckpointDurableExecutionRoute pattern matching."""
    route = Route.from_string("/2025-12-01/durable-executions/some-arn/checkpoint")
    assert CheckpointDurableExecutionRoute.is_match(route, "POST") is True
    assert CheckpointDurableExecutionRoute.is_match(route, "GET") is False

    route = Route.from_string("/2025-12-01/durable-executions/some-arn/stop")
    assert CheckpointDurableExecutionRoute.is_match(route, "POST") is False


def test_checkpoint_durable_execution_route_from_route():
    """Test CheckpointDurableExecutionRoute creation from base route."""
    arn = "test-arn"
    base_route = Route.from_string(f"/2025-12-01/durable-executions/{arn}/checkpoint")
    checkpoint_route = CheckpointDurableExecutionRoute.from_route(base_route)

    assert (
        checkpoint_route.raw_path == f"/2025-12-01/durable-executions/{arn}/checkpoint"
    )
    assert checkpoint_route.segments == [
        "2025-12-01",
        "durable-executions",
        arn,
        "checkpoint",
    ]
    assert checkpoint_route.arn == arn


# Removed test_checkpoint_durable_execution_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_stop_durable_execution_route_is_match():
    """Test StopDurableExecutionRoute pattern matching."""
    route = Route.from_string("/2025-12-01/durable-executions/some-arn/stop")
    assert StopDurableExecutionRoute.is_match(route, "POST") is True
    assert StopDurableExecutionRoute.is_match(route, "GET") is False

    route = Route.from_string("/2025-12-01/durable-executions/some-arn/checkpoint")
    assert StopDurableExecutionRoute.is_match(route, "POST") is False


def test_stop_durable_execution_route_from_route():
    """Test StopDurableExecutionRoute creation from base route."""
    arn = "test-arn"
    base_route = Route.from_string(f"/2025-12-01/durable-executions/{arn}/stop")
    stop_route = StopDurableExecutionRoute.from_route(base_route)

    assert stop_route.raw_path == f"/2025-12-01/durable-executions/{arn}/stop"
    assert stop_route.segments == ["2025-12-01", "durable-executions", arn, "stop"]
    assert stop_route.arn == arn


# Removed test_stop_durable_execution_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_get_durable_execution_state_route_is_match():
    """Test GetDurableExecutionStateRoute pattern matching."""
    route = Route.from_string("/2025-12-01/durable-executions/some-arn/state")
    assert GetDurableExecutionStateRoute.is_match(route, "GET") is True
    assert GetDurableExecutionStateRoute.is_match(route, "POST") is False

    route = Route.from_string("/2025-12-01/durable-executions/some-arn/history")
    assert GetDurableExecutionStateRoute.is_match(route, "GET") is False


def test_get_durable_execution_state_route_from_route():
    """Test GetDurableExecutionStateRoute creation from base route."""
    arn = "test-arn"
    base_route = Route.from_string(f"/2025-12-01/durable-executions/{arn}/state")
    state_route = GetDurableExecutionStateRoute.from_route(base_route)

    assert state_route.raw_path == f"/2025-12-01/durable-executions/{arn}/state"
    assert state_route.segments == ["2025-12-01", "durable-executions", arn, "state"]
    assert state_route.arn == arn


# Removed test_get_durable_execution_state_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_get_durable_execution_history_route_is_match():
    """Test GetDurableExecutionHistoryRoute pattern matching."""
    route = Route.from_string("/2025-12-01/durable-executions/some-arn/history")
    assert GetDurableExecutionHistoryRoute.is_match(route, "GET") is True
    assert GetDurableExecutionHistoryRoute.is_match(route, "POST") is False

    route = Route.from_string("/2025-12-01/durable-executions/some-arn/state")
    assert GetDurableExecutionHistoryRoute.is_match(route, "GET") is False


def test_get_durable_execution_history_route_from_route():
    """Test GetDurableExecutionHistoryRoute creation from base route."""
    arn = "test-arn"
    base_route = Route.from_string(f"/2025-12-01/durable-executions/{arn}/history")
    history_route = GetDurableExecutionHistoryRoute.from_route(base_route)

    assert history_route.raw_path == f"/2025-12-01/durable-executions/{arn}/history"
    assert history_route.segments == [
        "2025-12-01",
        "durable-executions",
        arn,
        "history",
    ]
    assert history_route.arn == arn


# Removed test_get_durable_execution_history_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_list_durable_executions_route_is_match():
    """Test ListDurableExecutionsRoute pattern matching."""
    route = Route.from_string("/2025-12-01/durable-executions")
    assert ListDurableExecutionsRoute.is_match(route, "GET") is True
    assert ListDurableExecutionsRoute.is_match(route, "POST") is False

    route = Route.from_string("/2025-12-01/durable-executions/some-arn")
    assert ListDurableExecutionsRoute.is_match(route, "GET") is False


def test_list_durable_executions_route_from_route():
    """Test ListDurableExecutionsRoute creation from base route."""
    base_route = Route.from_string("/2025-12-01/durable-executions")
    list_route = ListDurableExecutionsRoute.from_route(base_route)

    assert list_route.raw_path == "/2025-12-01/durable-executions"
    assert list_route.segments == ["2025-12-01", "durable-executions"]


# Removed test_list_durable_executions_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_list_durable_executions_by_function_route_is_match():
    """Test ListDurableExecutionsByFunctionRoute pattern matching."""
    route = Route.from_string("/2025-12-01/functions/my-function/durable-executions")
    assert ListDurableExecutionsByFunctionRoute.is_match(route, "GET") is True
    assert ListDurableExecutionsByFunctionRoute.is_match(route, "POST") is False

    route = Route.from_string("/2025-12-01/functions/my-function")
    assert ListDurableExecutionsByFunctionRoute.is_match(route, "GET") is False


def test_list_durable_executions_by_function_route_from_route():
    """Test ListDurableExecutionsByFunctionRoute creation from base route."""
    function_name = "my-function"
    base_route = Route.from_string(
        f"/2025-12-01/functions/{function_name}/durable-executions"
    )
    list_route = ListDurableExecutionsByFunctionRoute.from_route(base_route)

    assert (
        list_route.raw_path
        == f"/2025-12-01/functions/{function_name}/durable-executions"
    )
    assert list_route.segments == [
        "2025-12-01",
        "functions",
        function_name,
        "durable-executions",
    ]
    assert list_route.function_name == function_name


# Removed test_list_durable_executions_by_function_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_callback_success_route_is_match():
    """Test CallbackSuccessRoute pattern matching."""
    route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/callback-123/succeed"
    )
    assert CallbackSuccessRoute.is_match(route, "POST") is True
    assert CallbackSuccessRoute.is_match(route, "GET") is False

    route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/callback-123/fail"
    )
    assert CallbackSuccessRoute.is_match(route, "POST") is False


def test_callback_success_route_from_route():
    """Test CallbackSuccessRoute creation from base route."""
    callback_id = "callback-123"
    base_route = Route.from_string(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/succeed"
    )
    callback_route = CallbackSuccessRoute.from_route(base_route)

    assert (
        callback_route.raw_path
        == f"/2025-12-01/durable-execution-callbacks/{callback_id}/succeed"
    )
    assert callback_route.segments == [
        "2025-12-01",
        "durable-execution-callbacks",
        callback_id,
        "succeed",
    ]
    assert callback_route.callback_id == callback_id


# Removed test_callback_success_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_callback_failure_route_is_match():
    """Test CallbackFailureRoute pattern matching."""
    route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/callback-123/fail"
    )
    assert CallbackFailureRoute.is_match(route, "POST") is True
    assert CallbackFailureRoute.is_match(route, "GET") is False

    route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/callback-123/succeed"
    )
    assert CallbackFailureRoute.is_match(route, "POST") is False


def test_callback_failure_route_from_route():
    """Test CallbackFailureRoute creation from base route."""
    callback_id = "callback-123"
    base_route = Route.from_string(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/fail"
    )
    callback_route = CallbackFailureRoute.from_route(base_route)

    assert (
        callback_route.raw_path
        == f"/2025-12-01/durable-execution-callbacks/{callback_id}/fail"
    )
    assert callback_route.segments == [
        "2025-12-01",
        "durable-execution-callbacks",
        callback_id,
        "fail",
    ]
    assert callback_route.callback_id == callback_id


# Removed test_callback_failure_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_callback_heartbeat_route_is_match():
    """Test CallbackHeartbeatRoute pattern matching."""
    route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/callback-123/heartbeat"
    )
    assert CallbackHeartbeatRoute.is_match(route, "POST") is True
    assert CallbackHeartbeatRoute.is_match(route, "GET") is False

    route = Route.from_string(
        "/2025-12-01/durable-execution-callbacks/callback-123/succeed"
    )
    assert CallbackHeartbeatRoute.is_match(route, "POST") is False


def test_callback_heartbeat_route_from_route():
    """Test CallbackHeartbeatRoute creation from base route."""
    callback_id = "callback-123"
    base_route = Route.from_string(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/heartbeat"
    )
    callback_route = CallbackHeartbeatRoute.from_route(base_route)

    assert (
        callback_route.raw_path
        == f"/2025-12-01/durable-execution-callbacks/{callback_id}/heartbeat"
    )
    assert callback_route.segments == [
        "2025-12-01",
        "durable-execution-callbacks",
        callback_id,
        "heartbeat",
    ]
    assert callback_route.callback_id == callback_id


# Removed test_callback_heartbeat_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_health_route_is_match():
    """Test HealthRoute pattern matching."""
    route = Route.from_string("/health")
    assert HealthRoute.is_match(route, "GET") is True
    assert HealthRoute.is_match(route, "POST") is False

    route = Route.from_string("/metrics")
    assert HealthRoute.is_match(route, "GET") is False


def test_health_route_from_route():
    """Test HealthRoute creation from base route."""
    base_route = Route.from_string("/health")
    health_route = HealthRoute.from_route(base_route)

    assert health_route.raw_path == "/health"
    assert health_route.segments == ["health"]


# Removed test_health_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_metrics_route_is_match():
    """Test MetricsRoute pattern matching."""
    route = Route.from_string("/metrics")
    assert MetricsRoute.is_match(route, "GET") is True
    assert MetricsRoute.is_match(route, "POST") is False

    route = Route.from_string("/health")
    assert MetricsRoute.is_match(route, "GET") is False


def test_metrics_route_from_route():
    """Test MetricsRoute creation from base route."""
    base_route = Route.from_string("/metrics")
    metrics_route = MetricsRoute.from_route(base_route)

    assert metrics_route.raw_path == "/metrics"
    assert metrics_route.segments == ["metrics"]


# Removed test_metrics_route_from_route_invalid - from_route() no longer validates
# Call is_match() first to ensure route is valid


def test_route_immutability():
    """Test that route objects are immutable (frozen dataclasses)."""
    route = StartExecutionRoute.from_route(
        Route.from_string("/start-durable-execution")
    )

    # Should not be able to modify frozen dataclass
    with pytest.raises(AttributeError):
        route.raw_path = "/modified"  # type: ignore[misc]

    with pytest.raises(AttributeError):
        route.segments = ["modified"]  # type: ignore[misc]


def test_route_with_special_characters():
    """Test route parsing with special characters in ARNs and IDs.

    URL-decoding happens once in ``Route.from_string`` so every captured
    path segment (``segments[N]`` and any named field that mirrors it,
    such as ``arn`` or ``callback_id``) carries the literal value the
    caller passed to boto. ``raw_path`` keeps the original wire string.
    """
    # ARN with %20-encoded spaces should round-trip back to a literal space.
    encoded_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:my-function%20with%20spaces"
    )
    decoded_arn = (
        "arn:aws:lambda:us-east-1:123456789012:function:my-function with spaces"
    )
    raw_path = f"/2025-12-01/durable-executions/{encoded_arn}"
    router = Router()
    route = router.find_route(raw_path, "GET")
    assert isinstance(route, GetDurableExecutionRoute)
    assert route.arn == decoded_arn
    assert route.segments[2] == decoded_arn
    # raw_path is preserved as the original wire form for logging/debugging.
    assert route.raw_path == raw_path

    # Test with callback ID containing special characters
    callback_id = "callback-123-abc_def"
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)
    assert route.callback_id == callback_id


def test_route_edge_cases():
    """Test route parsing edge cases."""
    router = Router()

    # Empty path
    with pytest.raises(UnknownRouteError, match="Unknown path pattern"):
        router.find_route("", "GET")

    # Root path
    with pytest.raises(UnknownRouteError, match="Unknown path pattern"):
        router.find_route("/", "GET")

    # Path with only slashes
    with pytest.raises(UnknownRouteError, match="Unknown path pattern"):
        router.find_route("///", "GET")


def test_route_case_sensitivity():
    """Test that route matching is case-sensitive."""
    router = Router()

    # Should not match due to case difference
    with pytest.raises(UnknownRouteError, match="Unknown path pattern"):
        router.find_route("/START-DURABLE-EXECUTION", "POST")

    with pytest.raises(UnknownRouteError, match="Unknown path pattern"):
        router.find_route("/Health", "GET")


def test_router_find_route_method_validation():
    """Test that Router.find_route validates HTTP methods correctly."""
    router = Router()

    # Valid method combinations
    route = router.find_route("/start-durable-execution", "POST")
    assert isinstance(route, StartExecutionRoute)

    route = router.find_route("/2025-12-01/durable-executions/test-arn", "GET")
    assert isinstance(route, GetDurableExecutionRoute)

    # Invalid method combinations
    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: GET /start-durable-execution",
    ):
        router.find_route("/start-durable-execution", "GET")

    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: POST /2025-12-01/durable-executions/test-arn",
    ):
        router.find_route("/2025-12-01/durable-executions/test-arn", "POST")

    with pytest.raises(UnknownRouteError, match="Unknown path pattern: DELETE /health"):
        router.find_route("/health", "DELETE")


def test_router_find_route_method_case_sensitivity():
    """Test that HTTP method matching is case-sensitive."""
    router = Router()

    # Should work with uppercase methods
    route = router.find_route("/start-durable-execution", "POST")
    assert isinstance(route, StartExecutionRoute)

    # Should not work with lowercase methods
    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: post /start-durable-execution",
    ):
        router.find_route("/start-durable-execution", "post")

    with pytest.raises(UnknownRouteError, match="Unknown path pattern: get /health"):
        router.find_route("/health", "get")


def test_router_initialization_default():
    """Test Router initialization with default route types."""
    router = Router()

    # Should work with default route types
    route = router.find_route("/start-durable-execution", "POST")
    assert isinstance(route, StartExecutionRoute)

    route = router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)


def test_router_initialization_custom_route_types():
    """Test Router initialization with custom route types."""
    # Create router with only health and metrics routes
    custom_route_types = [HealthRoute, MetricsRoute]
    router = Router(route_types=custom_route_types)

    # Should work with included route types
    route = router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)

    route = router.find_route("/metrics", "GET")
    assert isinstance(route, MetricsRoute)

    # Should not work with excluded route types
    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: POST /start-durable-execution",
    ):
        router.find_route("/start-durable-execution", "POST")


def test_router_initialization_empty_route_types():
    """Test Router initialization with empty route types list."""
    router = Router(route_types=[])

    # Should not match any routes
    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: GET /health",
    ):
        router.find_route("/health", "GET")


def test_router_find_route_basic():
    """Test Router.find_route with basic routes."""
    router = Router()

    # Test various route types
    route = router.find_route("/start-durable-execution", "POST")
    assert isinstance(route, StartExecutionRoute)

    arn = "test-arn"
    route = router.find_route(f"/2025-12-01/durable-executions/{arn}", "GET")
    assert isinstance(route, GetDurableExecutionRoute)
    assert route.arn == arn

    route = router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)


def test_router_find_route_with_parameters():
    """Test Router.find_route extracts route parameters correctly."""
    router = Router()

    # Test ARN extraction
    arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
    route = router.find_route(
        f"/2025-12-01/durable-executions/{arn}/checkpoint", "POST"
    )
    assert isinstance(route, CheckpointDurableExecutionRoute)
    assert route.arn == arn

    # Test function name extraction
    function_name = "my-test-function"
    route = router.find_route(
        f"/2025-12-01/functions/{function_name}/durable-executions", "GET"
    )
    assert isinstance(route, ListDurableExecutionsByFunctionRoute)
    assert route.function_name == function_name

    # Test callback ID extraction
    callback_id = "callback-123-abc"
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)
    assert route.callback_id == callback_id


def test_router_find_route_unknown_route():
    """Test Router.find_route with unknown route patterns."""
    router = Router()

    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: GET /unknown/path",
    ):
        router.find_route("/unknown/path", "GET")

    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: DELETE /health",
    ):
        router.find_route("/health", "DELETE")


def test_unknown_route_error_attributes():
    """Test UnknownRouteError provides structured access to method and path."""
    router = Router()
    with pytest.raises(UnknownRouteError) as exc_info:
        router.find_route("/unknown/path", "POST")

    e = exc_info.value
    assert e.method == "POST"
    assert e.path == "/unknown/path"
    assert str(e) == "Unknown path pattern: POST /unknown/path"


def test_router_find_route_priority_order():
    """Test Router.find_route respects priority order for overlapping patterns."""
    router = Router()

    # Test that more specific patterns are matched before general ones
    # This tests the order in DEFAULT_ROUTE_TYPES registry

    # Should match GetDurableExecutionRoute, not ListDurableExecutionsRoute
    route = router.find_route("/2025-12-01/durable-executions/some-arn", "GET")
    assert isinstance(route, GetDurableExecutionRoute)
    assert route.arn == "some-arn"

    # Should match ListDurableExecutionsRoute
    route = router.find_route("/2025-12-01/durable-executions", "GET")
    assert isinstance(route, ListDurableExecutionsRoute)


def test_router_multiple_instances():
    """Test that multiple Router instances work independently."""
    # Create two routers with different route types
    health_router = Router(route_types=[HealthRoute])
    full_router = Router()  # Uses default route types

    # Health router should only handle health routes
    route = health_router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)

    with pytest.raises(UnknownRouteError):
        health_router.find_route("/metrics", "GET")

    # Full router should handle all routes
    route = full_router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)

    route = full_router.find_route("/metrics", "GET")
    assert isinstance(route, MetricsRoute)

    route = full_router.find_route("/start-durable-execution", "POST")
    assert isinstance(route, StartExecutionRoute)


def test_router_find_route_start_execution():
    """Test Router.find_route with start execution route."""
    router = Router()
    route = router.find_route("/start-durable-execution", "POST")
    assert isinstance(route, StartExecutionRoute)
    assert route.raw_path == "/start-durable-execution"


def test_router_find_route_get_durable_execution():
    """Test Router.find_route with get durable execution route."""
    router = Router()
    arn = "test-arn"
    route = router.find_route(f"/2025-12-01/durable-executions/{arn}", "GET")
    assert isinstance(route, GetDurableExecutionRoute)
    assert route.arn == arn


def test_router_find_route_checkpoint_durable_execution():
    """Test Router.find_route with checkpoint durable execution route."""
    router = Router()
    arn = "test-arn"
    route = router.find_route(
        f"/2025-12-01/durable-executions/{arn}/checkpoint", "POST"
    )
    assert isinstance(route, CheckpointDurableExecutionRoute)
    assert route.arn == arn


def test_router_find_route_stop_durable_execution():
    """Test Router.find_route with stop durable execution route."""
    router = Router()
    arn = "test-arn"
    route = router.find_route(f"/2025-12-01/durable-executions/{arn}/stop", "POST")
    assert isinstance(route, StopDurableExecutionRoute)
    assert route.arn == arn


def test_router_find_route_get_durable_execution_state():
    """Test Router.find_route with get durable execution state route."""
    router = Router()
    arn = "test-arn"
    route = router.find_route(f"/2025-12-01/durable-executions/{arn}/state", "GET")
    assert isinstance(route, GetDurableExecutionStateRoute)
    assert route.arn == arn


def test_router_find_route_get_durable_execution_history():
    """Test Router.find_route with get durable execution history route."""
    router = Router()
    arn = "test-arn"
    route = router.find_route(f"/2025-12-01/durable-executions/{arn}/history", "GET")
    assert isinstance(route, GetDurableExecutionHistoryRoute)
    assert route.arn == arn


def test_router_find_route_list_durable_executions():
    """Test Router.find_route with list durable executions route."""
    router = Router()
    route = router.find_route("/2025-12-01/durable-executions", "GET")
    assert isinstance(route, ListDurableExecutionsRoute)


def test_router_find_route_list_durable_executions_by_function():
    """Test Router.find_route with list durable executions by function route."""
    router = Router()
    function_name = "my-function"
    route = router.find_route(
        f"/2025-12-01/functions/{function_name}/durable-executions", "GET"
    )
    assert isinstance(route, ListDurableExecutionsByFunctionRoute)
    assert route.function_name == function_name


def test_router_find_route_callback_success():
    """Test Router.find_route with callback success route."""
    router = Router()
    callback_id = "callback-123"
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)
    assert route.callback_id == callback_id


def test_router_find_route_callback_failure():
    """Test Router.find_route with callback failure route."""
    router = Router()
    callback_id = "callback-123"
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/fail", "POST"
    )
    assert isinstance(route, CallbackFailureRoute)
    assert route.callback_id == callback_id


def test_router_find_route_callback_heartbeat():
    """Test Router.find_route with callback heartbeat route."""
    router = Router()
    callback_id = "callback-123"
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{callback_id}/heartbeat", "POST"
    )
    assert isinstance(route, CallbackHeartbeatRoute)
    assert route.callback_id == callback_id


def test_router_find_route_health():
    """Test Router.find_route with health route."""
    router = Router()
    route = router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)


def test_router_find_route_metrics():
    """Test Router.find_route with metrics route."""
    router = Router()
    route = router.find_route("/metrics", "GET")
    assert isinstance(route, MetricsRoute)


def test_router_find_route_unknown():
    """Test Router.find_route with unknown route pattern."""
    router = Router()
    with pytest.raises(
        UnknownRouteError,
        match="Unknown path pattern: GET /unknown/path",
    ):
        router.find_route("/unknown/path", "GET")


def test_router_constructor_with_all_default_route_types():
    """Test Router constructor includes all expected default route types."""
    router = Router()

    # Test that all route types are included by trying to match each one
    test_cases = [
        ("/start-durable-execution", "POST", StartExecutionRoute),
        ("/2025-12-01/durable-executions/test-arn", "GET", GetDurableExecutionRoute),
        (
            "/2025-12-01/durable-executions/test-arn/checkpoint",
            "POST",
            CheckpointDurableExecutionRoute,
        ),
        (
            "/2025-12-01/durable-executions/test-arn/stop",
            "POST",
            StopDurableExecutionRoute,
        ),
        (
            "/2025-12-01/durable-executions/test-arn/state",
            "GET",
            GetDurableExecutionStateRoute,
        ),
        (
            "/2025-12-01/durable-executions/test-arn/history",
            "GET",
            GetDurableExecutionHistoryRoute,
        ),
        ("/2025-12-01/durable-executions", "GET", ListDurableExecutionsRoute),
        (
            "/2025-12-01/functions/test-func/durable-executions",
            "GET",
            ListDurableExecutionsByFunctionRoute,
        ),
        (
            "/2025-12-01/durable-execution-callbacks/test-id/succeed",
            "POST",
            CallbackSuccessRoute,
        ),
        (
            "/2025-12-01/durable-execution-callbacks/test-id/fail",
            "POST",
            CallbackFailureRoute,
        ),
        (
            "/2025-12-01/durable-execution-callbacks/test-id/heartbeat",
            "POST",
            CallbackHeartbeatRoute,
        ),
        ("/health", "GET", HealthRoute),
        ("/metrics", "GET", MetricsRoute),
    ]

    for path, method, expected_type in test_cases:
        route = router.find_route(path, method)
        assert isinstance(route, expected_type), (
            f"Expected {expected_type.__name__} for {method} {path}"
        )


def test_router_constructor_with_subset_of_route_types():
    """Test Router constructor with a subset of route types."""
    # Create router with only callback routes
    callback_route_types = [
        CallbackSuccessRoute,
        CallbackFailureRoute,
        CallbackHeartbeatRoute,
    ]
    router = Router(route_types=callback_route_types)

    # Should work with callback routes
    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-id/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)

    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-id/fail", "POST"
    )
    assert isinstance(route, CallbackFailureRoute)

    route = router.find_route(
        "/2025-12-01/durable-execution-callbacks/test-id/heartbeat", "POST"
    )
    assert isinstance(route, CallbackHeartbeatRoute)

    # Should not work with other route types
    with pytest.raises(UnknownRouteError):
        router.find_route("/health", "GET")

    with pytest.raises(UnknownRouteError):
        router.find_route("/start-durable-execution", "POST")


def test_router_constructor_with_single_route_type():
    """Test Router constructor with a single route type."""
    router = Router(route_types=[HealthRoute])

    # Should work with the single route type
    route = router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)

    # Should not work with any other route types
    with pytest.raises(UnknownRouteError):
        router.find_route("/metrics", "GET")

    with pytest.raises(UnknownRouteError):
        router.find_route("/start-durable-execution", "POST")


def test_router_constructor_with_duplicate_route_types():
    """Test Router constructor handles duplicate route types gracefully."""
    # Include HealthRoute twice
    duplicate_route_types = [HealthRoute, MetricsRoute, HealthRoute]
    router = Router(route_types=duplicate_route_types)

    # Should still work correctly (first match wins)
    route = router.find_route("/health", "GET")
    assert isinstance(route, HealthRoute)

    route = router.find_route("/metrics", "GET")
    assert isinstance(route, MetricsRoute)


def test_router_find_route_error_handling_comprehensive():
    """Test Router.find_route error handling with various invalid inputs."""
    router = Router()

    # Test various invalid path/method combinations
    invalid_cases = [
        ("", "GET", "Unknown path pattern: GET "),
        ("/", "GET", "Unknown path pattern: GET /"),
        ("///", "GET", "Unknown path pattern: GET ///"),
        ("/unknown", "GET", "Unknown path pattern: GET /unknown"),
        (
            "/start-durable-execution",
            "GET",
            "Unknown path pattern: GET /start-durable-execution",
        ),
        ("/health", "POST", "Unknown path pattern: POST /health"),
        ("/metrics", "DELETE", "Unknown path pattern: DELETE /metrics"),
        (
            "/2025-12-01/durable-executions/test-arn",
            "POST",
            "Unknown path pattern: POST /2025-12-01/durable-executions/test-arn",
        ),
        (
            "/2025-12-01/durable-executions/test-arn/checkpoint",
            "GET",
            "Unknown path pattern: GET /2025-12-01/durable-executions/test-arn/checkpoint",
        ),
    ]

    for path, method, expected_message in invalid_cases:
        with pytest.raises(UnknownRouteError, match=expected_message):
            router.find_route(path, method)


def test_router_find_route_with_complex_parameters():
    """Test Router.find_route with complex parameter extraction."""
    router = Router()

    # Test with complex ARN
    complex_arn = "arn:aws:lambda:us-west-2:123456789012:function:my-complex-function-name_with_underscores-and-dashes"
    route = router.find_route(f"/2025-12-01/durable-executions/{complex_arn}", "GET")
    assert isinstance(route, GetDurableExecutionRoute)
    assert route.arn == complex_arn

    # Test with complex function name
    complex_function_name = "my_complex-function.name123"
    route = router.find_route(
        f"/2025-12-01/functions/{complex_function_name}/durable-executions", "GET"
    )
    assert isinstance(route, ListDurableExecutionsByFunctionRoute)
    assert route.function_name == complex_function_name

    # Test with complex callback ID
    complex_callback_id = "callback-123_abc-def.456"
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{complex_callback_id}/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)
    assert route.callback_id == complex_callback_id


def test_router_find_route_order_dependency():
    """Test that Router.find_route respects route type ordering for disambiguation."""
    router = Router()

    # These paths could potentially match multiple patterns if ordering is wrong
    # The more specific patterns should match first

    # Should match GetDurableExecutionRoute, not ListDurableExecutionsRoute
    route = router.find_route("/2025-12-01/durable-executions/specific-arn", "GET")
    assert isinstance(route, GetDurableExecutionRoute)
    assert route.arn == "specific-arn"

    # Should match ListDurableExecutionsRoute
    route = router.find_route("/2025-12-01/durable-executions", "GET")
    assert isinstance(route, ListDurableExecutionsRoute)

    # Should match CheckpointDurableExecutionRoute, not GetDurableExecutionRoute
    route = router.find_route(
        "/2025-12-01/durable-executions/test-arn/checkpoint", "POST"
    )
    assert isinstance(route, CheckpointDurableExecutionRoute)
    assert route.arn == "test-arn"


def test_router_thread_safety():
    """Test that Router instances are thread-safe for concurrent access."""

    router = Router()
    results = []
    errors = []

    def worker(worker_id: int):
        try:
            for i in range(10):
                # Test different route types to ensure no interference
                route = router.find_route(
                    f"/2025-12-01/durable-executions/arn-{worker_id}-{i}", "GET"
                )
                assert isinstance(route, GetDurableExecutionRoute)
                assert route.arn == f"arn-{worker_id}-{i}"

                route = router.find_route("/health", "GET")
                assert isinstance(route, HealthRoute)

                time.sleep(0.001)  # Small delay to increase chance of race conditions

            results.append(f"Worker {worker_id} completed successfully")
        except (UnknownRouteError, AssertionError) as e:
            errors.append(f"Worker {worker_id} failed: {e}")

    # Create multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Check results
    assert len(errors) == 0, f"Thread safety test failed with errors: {errors}"
    assert len(results) == 5, f"Expected 5 successful workers, got {len(results)}"


def test_callback_routes_url_decoding():
    """Test that callback routes properly URL-decode callback IDs."""
    # Test callback ID with special characters that need URL encoding
    callback_id = "eyJhcm4iOiJhcm4iLCJvcCI6ImVhNjZjMDZjMWUxYzA1ZmEifQ=="
    encoded_callback_id = quote(callback_id, safe="")

    # Test CallbackSuccessRoute
    base_route = Route.from_string(
        f"/2025-12-01/durable-execution-callbacks/{encoded_callback_id}/succeed"
    )
    success_route = CallbackSuccessRoute.from_route(base_route)
    assert success_route.callback_id == callback_id  # Should be decoded

    # Test CallbackFailureRoute
    base_route = Route.from_string(
        f"/2025-12-01/durable-execution-callbacks/{encoded_callback_id}/fail"
    )
    failure_route = CallbackFailureRoute.from_route(base_route)
    assert failure_route.callback_id == callback_id  # Should be decoded

    # Test CallbackHeartbeatRoute
    base_route = Route.from_string(
        f"/2025-12-01/durable-execution-callbacks/{encoded_callback_id}/heartbeat"
    )
    heartbeat_route = CallbackHeartbeatRoute.from_route(base_route)
    assert heartbeat_route.callback_id == callback_id  # Should be decoded


def test_router_callback_routes_url_decoding():
    """Test Router properly handles URL-encoded callback IDs."""
    router = Router()
    callback_id = "eyJhcm4iOiJhcm4iLCJvcCI6ImVhNjZjMDZjMWUxYzA1ZmEifQ=="
    encoded_callback_id = quote(callback_id, safe="")

    # Test success route
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{encoded_callback_id}/succeed", "POST"
    )
    assert isinstance(route, CallbackSuccessRoute)
    assert route.callback_id == callback_id  # Should be decoded

    # Test failure route
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{encoded_callback_id}/fail", "POST"
    )
    assert isinstance(route, CallbackFailureRoute)
    assert route.callback_id == callback_id  # Should be decoded

    # Test heartbeat route
    route = router.find_route(
        f"/2025-12-01/durable-execution-callbacks/{encoded_callback_id}/heartbeat",
        "POST",
    )
    assert isinstance(route, CallbackHeartbeatRoute)
    assert route.callback_id == callback_id  # Should be decoded

"""Strongly-typed route parsing system for HTTP request routing."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote

from aws_durable_execution_sdk_python_testing.exceptions import (
    UnknownRouteError,
)


@dataclass(frozen=True)
class Route:
    """Base route with segments and pattern matching capabilities."""

    raw_path: str
    segments: list[str]

    @classmethod
    def from_route(cls, _route: Route) -> Route:
        """Create a typed route from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            Typed route instance

        Raises:
            NotImplementedError: This is an abstract method that must be implemented by subclasses
        """
        msg = "Subclasses must implement from_route()"
        raise NotImplementedError(msg)

    @classmethod
    def from_string(cls, path: str) -> Route:
        """Create a Route from a string.

        Each segment is URL-decoded; ``raw_path`` is preserved as the
        original wire path. Splitting on ``/`` happens before decoding so
        that an encoded ``%2F`` inside a captured value (e.g. an ARN that
        contains ``/``) stays inside its segment instead of being treated
        as a path separator.

        Args:
            path: The raw path string

        Returns:
            Route instance with parsed, URL-decoded segments
        """
        # Remove leading/trailing slashes, split on '/', then URL-decode each
        # segment. Order matters: split on the literal '/' first so '%2F'-
        # encoded slashes inside values don't act as separators.
        segments = [unquote(s) for s in path.strip("/").split("/") if s]
        return cls(raw_path=path, segments=segments)

    def matches_pattern(self, pattern: list[str]) -> bool:
        """Check if route matches the given pattern.

        Args:
            pattern: List of pattern segments. Use '*' for wildcards.

        Returns:
            True if the route matches the pattern
        """
        if len(self.segments) != len(pattern):
            return False

        for segment, pattern_part in zip(self.segments, pattern, strict=False):
            if pattern_part not in ("*", segment):
                return False
        return True

    @classmethod
    def is_match(cls, _route: Route, _method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            _route: Route to check
            _method: HTTP method to check

        Returns:
            True if the route and method match

        Raises:
            NotImplementedError: This is an abstract method that must be implemented by subclasses
        """
        msg = "Subclasses must implement is_match()"
        raise NotImplementedError(msg)


@dataclass(frozen=True)
class StartExecutionRoute(Route):
    """Route: POST /start-durable-execution"""

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return route.raw_path == "/start-durable-execution" and method == "POST"

    @classmethod
    def from_route(cls, route: Route) -> StartExecutionRoute:
        """Create a StartExecutionRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            StartExecutionRoute instance
        """
        return cls(raw_path=route.raw_path, segments=route.segments)


@dataclass(frozen=True)
class GetDurableExecutionRoute(Route):
    """Route: GET /2025-12-01/durable-executions/{arn}"""

    arn: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(["2025-12-01", "durable-executions", "*"])
            and method == "GET"
        )

    @classmethod
    def from_route(cls, route: Route) -> GetDurableExecutionRoute:
        """Create a GetDurableExecutionRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            GetDurableExecutionRoute instance with extracted ARN
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            arn=route.segments[2],
        )


@dataclass(frozen=True)
class CheckpointDurableExecutionRoute(Route):
    """Route: POST /2025-12-01/durable-executions/{arn}/checkpoint"""

    arn: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(
                ["2025-12-01", "durable-executions", "*", "checkpoint"]
            )
            and method == "POST"
        )

    @classmethod
    def from_route(cls, route: Route) -> CheckpointDurableExecutionRoute:
        """Create a CheckpointDurableExecutionRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            CheckpointDurableExecutionRoute instance with extracted ARN
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            arn=route.segments[2],
        )


@dataclass(frozen=True)
class StopDurableExecutionRoute(Route):
    """Route: POST /2025-12-01/durable-executions/{arn}/stop"""

    arn: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(["2025-12-01", "durable-executions", "*", "stop"])
            and method == "POST"
        )

    @classmethod
    def from_route(cls, route: Route) -> StopDurableExecutionRoute:
        """Create a StopDurableExecutionRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            StopDurableExecutionRoute instance with extracted ARN
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            arn=route.segments[2],
        )


@dataclass(frozen=True)
class GetDurableExecutionStateRoute(Route):
    """Route: GET /2025-12-01/durable-executions/{arn}/state"""

    arn: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(["2025-12-01", "durable-executions", "*", "state"])
            and method == "GET"
        )

    @classmethod
    def from_route(cls, route: Route) -> GetDurableExecutionStateRoute:
        """Create a GetDurableExecutionStateRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            GetDurableExecutionStateRoute instance with extracted ARN
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            arn=route.segments[2],
        )


@dataclass(frozen=True)
class GetDurableExecutionHistoryRoute(Route):
    """Route: GET /2025-12-01/durable-executions/{arn}/history"""

    arn: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(["2025-12-01", "durable-executions", "*", "history"])
            and method == "GET"
        )

    @classmethod
    def from_route(cls, route: Route) -> GetDurableExecutionHistoryRoute:
        """Create a GetDurableExecutionHistoryRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            GetDurableExecutionHistoryRoute instance with extracted ARN
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            arn=route.segments[2],
        )


@dataclass(frozen=True)
class ListDurableExecutionsRoute(Route):
    """Route: GET /2025-12-01/durable-executions"""

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(["2025-12-01", "durable-executions"])
            and method == "GET"
        )

    @classmethod
    def from_route(cls, route: Route) -> ListDurableExecutionsRoute:
        """Create a ListDurableExecutionsRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            ListDurableExecutionsRoute instance
        """
        return cls(raw_path=route.raw_path, segments=route.segments)


@dataclass(frozen=True)
class ListDurableExecutionsByFunctionRoute(Route):
    """Route: GET /2025-12-01/functions/{function_name}/durable-executions"""

    function_name: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(
                ["2025-12-01", "functions", "*", "durable-executions"]
            )
            and method == "GET"
        )

    @classmethod
    def from_route(cls, route: Route) -> ListDurableExecutionsByFunctionRoute:
        """Create a ListDurableExecutionsByFunctionRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            ListDurableExecutionsByFunctionRoute instance with extracted function name
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            function_name=route.segments[2],
        )


@dataclass(frozen=True)
class BytesPayloadRoute(Route):
    """Base class for routes that handle raw bytes payloads instead of JSON."""


@dataclass(frozen=True)
class CallbackSuccessRoute(BytesPayloadRoute):
    """Route: POST /2025-12-01/durable-execution-callbacks/{callback_id}/succeed"""

    callback_id: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(
                ["2025-12-01", "durable-execution-callbacks", "*", "succeed"]
            )
            and method == "POST"
        )

    @classmethod
    def from_route(cls, route: Route) -> CallbackSuccessRoute:
        """Create a CallbackSuccessRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            CallbackSuccessRoute instance with extracted callback ID
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            callback_id=route.segments[2],
        )


@dataclass(frozen=True)
class CallbackFailureRoute(BytesPayloadRoute):
    """Route: POST /2025-12-01/durable-execution-callbacks/{callback_id}/fail"""

    callback_id: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(
                ["2025-12-01", "durable-execution-callbacks", "*", "fail"]
            )
            and method == "POST"
        )

    @classmethod
    def from_route(cls, route: Route) -> CallbackFailureRoute:
        """Create a CallbackFailureRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            CallbackFailureRoute instance with extracted callback ID
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            callback_id=route.segments[2],
        )


@dataclass(frozen=True)
class CallbackHeartbeatRoute(Route):
    """Route: POST /2025-12-01/durable-execution-callbacks/{callback_id}/heartbeat"""

    callback_id: str

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return (
            route.matches_pattern(
                ["2025-12-01", "durable-execution-callbacks", "*", "heartbeat"]
            )
            and method == "POST"
        )

    @classmethod
    def from_route(cls, route: Route) -> CallbackHeartbeatRoute:
        """Create a CallbackHeartbeatRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            CallbackHeartbeatRoute instance with extracted callback ID
        """
        return cls(
            raw_path=route.raw_path,
            segments=route.segments,
            callback_id=route.segments[2],
        )


@dataclass(frozen=True)
class HealthRoute(Route):
    """Route: GET /health"""

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return route.raw_path == "/health" and method == "GET"

    @classmethod
    def from_route(cls, route: Route) -> HealthRoute:
        """Create a HealthRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            HealthRoute instance
        """
        return cls(raw_path=route.raw_path, segments=route.segments)


@dataclass(frozen=True)
class UpdateLambdaEndpointRoute(Route):
    """Route: PUT /lambda-endpoint"""

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return route.raw_path == "/lambda-endpoint" and method == "PUT"

    @classmethod
    def from_route(cls, route: Route) -> UpdateLambdaEndpointRoute:
        """Create UpdateLambdaEndpointRoute from base route.

        Args:
            route: Base route to convert

        Returns:
            UpdateLambdaEndpointRoute instance
        """
        return cls(raw_path=route.raw_path, segments=route.segments)


@dataclass(frozen=True)
class MetricsRoute(Route):
    """Route: GET /metrics"""

    @classmethod
    def is_match(cls, route: Route, method: str) -> bool:
        """Check if the route and HTTP method match this route type.

        Args:
            route: Route to check
            method: HTTP method to check

        Returns:
            True if the route and method match
        """
        return route.raw_path == "/metrics" and method == "GET"

    @classmethod
    def from_route(cls, route: Route) -> MetricsRoute:
        """Create a MetricsRoute from a base Route.

        Note: Call is_match(route, method) first to ensure the route is valid for this type.

        Args:
            route: Base route to convert

        Returns:
            MetricsRoute instance
        """
        return cls(raw_path=route.raw_path, segments=route.segments)


# Default registry of all route types for matching
DEFAULT_ROUTE_TYPES: list[type[Route]] = [
    StartExecutionRoute,
    GetDurableExecutionRoute,
    CheckpointDurableExecutionRoute,
    StopDurableExecutionRoute,
    GetDurableExecutionStateRoute,
    GetDurableExecutionHistoryRoute,
    ListDurableExecutionsRoute,
    ListDurableExecutionsByFunctionRoute,
    CallbackSuccessRoute,
    CallbackFailureRoute,
    CallbackHeartbeatRoute,
    HealthRoute,
    UpdateLambdaEndpointRoute,
    MetricsRoute,
]


class Router:
    """HTTP request router that matches routes to strongly-typed route objects."""

    def __init__(self, route_types: list[type[Route]] | None = None) -> None:
        """Initialize the router with route types.

        Args:
            route_types: List of route type classes to use for matching.
                        If None, uses the default route types.
        """
        self._route_types = (
            route_types if route_types is not None else DEFAULT_ROUTE_TYPES
        )

    def find_route(self, path: str, method: str) -> Route:
        """Find a matching route for the given path and HTTP method.

        Args:
            path: The raw path string to parse
            method: The HTTP method (GET, POST, etc.)

        Returns:
            Strongly-typed Route instance

        Raises:
            UnknownRouteError: If the path and method don't match any known pattern
        """
        base_route = Route.from_string(path)

        for route_type in self._route_types:
            if route_type.is_match(base_route, method):
                return route_type.from_route(base_route)

        raise UnknownRouteError(method, path)

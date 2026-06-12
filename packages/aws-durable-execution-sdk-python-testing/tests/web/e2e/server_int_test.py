"""Integration tests for web server routing and handler integration."""

from __future__ import annotations

from unittest.mock import Mock

from aws_durable_execution_sdk_python_testing.web.models import (
    HTTPRequest,
    HTTPResponse,
)
from aws_durable_execution_sdk_python_testing.web.routes import (
    HealthRoute,
    StartExecutionRoute,
)
from aws_durable_execution_sdk_python_testing.web.server import (
    WebServer,
    WebServiceConfig,
)


def test_web_server_router_integration():
    """Test that router can find routes and handlers can handle them."""
    executor = Mock()
    config = WebServiceConfig(port=0)  # Use port 0 to get any available port

    server = WebServer(config, executor)

    try:
        # Test router can find a route
        route = server.router.find_route("/health", "GET")
        assert isinstance(route, HealthRoute)

        # Test handler exists for the route
        handler = server.endpoint_handlers.get(type(route))
        assert handler is not None

        # Test handler can handle the route
        request = HTTPRequest(
            method="GET", path=route, headers={}, query_params={}, body={}
        )

        response = handler.handle(route, request)
        assert isinstance(response, HTTPResponse)
        assert response.status_code == 200
        assert response.body == {"status": "healthy"}
    finally:
        server.server_close()


def test_web_server_start_execution_route_integration():
    """Test that start execution route is properly integrated."""
    executor = Mock()
    config = WebServiceConfig(port=0)  # Use port 0 to get any available port

    server = WebServer(config, executor)

    try:
        # Test router can find start execution route
        route = server.router.find_route("/start-durable-execution", "POST")
        assert isinstance(route, StartExecutionRoute)

        # Test handler exists for the route
        handler = server.endpoint_handlers.get(type(route))
        assert handler is not None

        # Test handler returns 400 for invalid input (now implemented)
        request = HTTPRequest(
            method="POST",
            path=route,
            headers={},
            query_params={},
            body={"test": "data"},  # Invalid input - missing required fields
        )

        response = handler.handle(route, request)
        assert isinstance(response, HTTPResponse)
        assert response.status_code == 400  # Bad request for invalid input
    finally:
        server.server_close()


def test_web_server_context_manager_with_integration():
    """Test that WebServer context manager works with integrated components."""
    executor = Mock()
    config = WebServiceConfig(port=0)  # Use port 0 to get any available port

    with WebServer(config, executor) as server:
        # Verify server is properly initialized
        assert server.router is not None
        assert server.endpoint_handlers is not None

        # Test a simple route resolution
        route = server.router.find_route("/health", "GET")
        handler = server.endpoint_handlers[type(route)]

        request = HTTPRequest(
            method="GET", path=route, headers={}, query_params={}, body={}
        )

        response = handler.handle(route, request)
        assert response.status_code == 200

"""Local testing web server for AWS Lambda Durable Functions that mimics the actual Lambda backend services."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Self
from urllib.parse import parse_qs, urlparse

from aws_durable_execution_sdk_python_testing.exceptions import (
    AwsApiException,
    ServiceException,
    UnknownRouteError,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.executor import Executor


# Removed deprecated imports from web.errors
from aws_durable_execution_sdk_python_testing.web.handlers import (
    CheckpointDurableExecutionHandler,
    EndpointHandler,
    GetDurableExecutionHandler,
    GetDurableExecutionHistoryHandler,
    GetDurableExecutionStateHandler,
    HealthHandler,
    ListDurableExecutionsByFunctionHandler,
    ListDurableExecutionsHandler,
    MetricsHandler,
    SendDurableExecutionCallbackFailureHandler,
    SendDurableExecutionCallbackHeartbeatHandler,
    SendDurableExecutionCallbackSuccessHandler,
    StartExecutionHandler,
    StopDurableExecutionHandler,
    UpdateLambdaEndpointHandler,
)
from aws_durable_execution_sdk_python_testing.web.models import (
    HTTPRequest,
    HTTPResponse,
)
from aws_durable_execution_sdk_python_testing.web.routes import (
    BytesPayloadRoute,
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
    UpdateLambdaEndpointRoute,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WebServiceConfig:
    """Configuration for the web service."""

    host: str = "localhost"
    port: int = 5000
    log_level: int = logging.INFO
    max_request_size: int = 10 * 1024 * 1024  # 10MB


class RequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for durable execution operations."""

    def __init__(self, request, client_address, server) -> None:
        self.executor: Executor = server.executor
        self.router: Router = server.router  # Access shared router
        self.endpoint_handlers: dict[type[Route], EndpointHandler] = (
            server.endpoint_handlers
        )  # Access shared handlers
        super().__init__(request, client_address, server)

    def do_GET(self) -> None:  # noqa: N802
        """Handle GET requests."""
        self._handle_request("GET")

    def do_POST(self) -> None:  # noqa: N802
        """Handle POST requests."""
        self._handle_request("POST")

    def do_PUT(self) -> None:  # noqa: N802
        """Handle PUT requests."""
        self._handle_request("PUT")

    def _handle_request(self, method: str) -> None:
        """Handle HTTP request with strongly-typed routing."""
        try:
            # Parse URL path and method into strongly-typed Route object using shared router
            url_path: str = self.path.split("?")[0]
            parsed_route: Route = self.router.find_route(url_path, method)

            # Find handler for this route type
            handler: EndpointHandler | None = self.endpoint_handlers.get(
                type(parsed_route)
            )

            if not handler:
                raise UnknownRouteError(method, url_path)  # noqa: TRY301

            # Parse query parameters and request body
            parsed_url = urlparse(self.path)
            query_params: dict[str, list[str]] = parse_qs(parsed_url.query)
            content_length: int = int(self.headers.get("Content-Length", 0))
            body_bytes: bytes = (
                self.rfile.read(content_length) if content_length > 0 else b""
            )

            # For callback operations, use raw bytes directly
            if isinstance(parsed_route, BytesPayloadRoute):
                request = HTTPRequest.from_raw_bytes(
                    body_bytes=body_bytes,
                    method=method,
                    path=parsed_route,
                    headers=dict(self.headers),
                    query_params=query_params,
                )
            else:
                # Create strongly-typed HTTP request object with pre-parsed body
                request = HTTPRequest.from_bytes(
                    body_bytes=body_bytes,
                    operation_name=None,
                    method=method,
                    path=parsed_route,
                    headers=dict(self.headers),
                    query_params=query_params,
                )

            # Handle request with appropriate handler
            response: HTTPResponse = handler.handle(parsed_route, request)

            # Send HTTP response
            self._send_response(response)

        except Exception as e:
            logger.exception("Request handling failed")

            aws_exception: AwsApiException = (
                e if isinstance(e, AwsApiException) else ServiceException(str(e))
            )

            http_response = HTTPResponse.create_error_from_exception(aws_exception)
            self._send_response(http_response)

    def _send_response(self, response: HTTPResponse) -> None:
        """Send HTTP response to client."""
        self.send_response(response.status_code)
        for header_name, header_value in response.headers.items():
            self.send_header(header_name, header_value)
        self.end_headers()

        # Convert response body to bytes for transmission
        if response.body:
            self.wfile.write(response.body_to_bytes())

    def log_message(self, format_string: str, *args) -> None:
        """Override to use Python logging instead of stderr."""
        logger.info("%s - %s", self.address_string(), format_string % args)


class WebServer(ThreadingHTTPServer):
    """Multi-threaded HTTP server for durable execution operations."""

    def __init__(self, config: WebServiceConfig, executor: Executor) -> None:
        """Initialize the web server.

        Args:
            config: Server configuration
            executor: Executor instance for handling operations
        """
        self.config = config
        self.executor = executor

        # Configure logging
        logging.basicConfig(level=config.log_level)
        logging.getLogger("botocore").setLevel(logging.WARNING)

        # Create shared router and endpoint handlers
        self.router = Router()  # Shared across all request handlers
        self.endpoint_handlers = (
            self._create_endpoint_handlers()
        )  # Shared handler registry

        # Initialize the HTTP server
        super().__init__((config.host, config.port), RequestHandler)

        logger.info("Web server initialized on %s:%s", config.host, config.port)

    def _create_endpoint_handlers(self) -> dict[type[Route], EndpointHandler]:
        """Create endpoint handlers registry - called once during server initialization."""
        return {
            StartExecutionRoute: StartExecutionHandler(self.executor),
            GetDurableExecutionRoute: GetDurableExecutionHandler(self.executor),
            CheckpointDurableExecutionRoute: CheckpointDurableExecutionHandler(
                self.executor
            ),
            StopDurableExecutionRoute: StopDurableExecutionHandler(self.executor),
            GetDurableExecutionStateRoute: GetDurableExecutionStateHandler(
                self.executor
            ),
            GetDurableExecutionHistoryRoute: GetDurableExecutionHistoryHandler(
                self.executor
            ),
            ListDurableExecutionsRoute: ListDurableExecutionsHandler(self.executor),
            ListDurableExecutionsByFunctionRoute: ListDurableExecutionsByFunctionHandler(
                self.executor
            ),
            CallbackSuccessRoute: SendDurableExecutionCallbackSuccessHandler(
                self.executor
            ),
            CallbackFailureRoute: SendDurableExecutionCallbackFailureHandler(
                self.executor
            ),
            CallbackHeartbeatRoute: SendDurableExecutionCallbackHeartbeatHandler(
                self.executor
            ),
            HealthRoute: HealthHandler(self.executor),
            UpdateLambdaEndpointRoute: UpdateLambdaEndpointHandler(self.executor),
            MetricsRoute: MetricsHandler(self.executor),
        }

    def __enter__(self) -> Self:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup server resources."""
        self.server_close()

"""Command-line interface for the AWS Durable Functions Local Runner.

This module provides the dex-local-runner CLI with commands for:
- start-server: Start the local web server
- invoke: Invoke a durable execution
- get-durable-execution: Get execution details
- get-durable-execution-history: Get execution history
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import aws_durable_execution_sdk_python
import boto3  # type: ignore
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from botocore.exceptions import ConnectionError  # type: ignore

from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsLocalRunnerError,
    DurableFunctionsTestError,
)
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
)
from aws_durable_execution_sdk_python_testing.runner import WebRunner, WebRunnerConfig
from aws_durable_execution_sdk_python_testing.stores.base import StoreType
from aws_durable_execution_sdk_python_testing.web.server import WebServiceConfig


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CliConfig:
    """Configuration for the CLI application with environment variable support."""

    # Server configuration
    host: str = "0.0.0.0"  # noqa:S104
    port: int = 5000
    log_level: int = logging.INFO
    lambda_endpoint: str = "http://127.0.0.1:3001"
    local_runner_endpoint: str = "http://0.0.0.0:5000"
    local_runner_region: str = "us-west-2"
    local_runner_mode: str = "local"

    # Store configuration
    store_type: StoreType = StoreType.MEMORY
    store_path: str | None = None

    @classmethod
    def from_environment(cls) -> CliConfig:
        """Create configuration from environment variables with defaults."""
        # Convert log level string to integer if provided
        log_level_str = os.getenv("AWS_DEX_LOG_LEVEL", "INFO")
        log_level = logging.getLevelNamesMapping().get(log_level_str, logging.INFO)

        return cls(
            host=os.getenv("AWS_DEX_HOST", "0.0.0.0"),  # noqa:S104
            port=int(os.getenv("AWS_DEX_PORT", "5000")),
            log_level=log_level,
            lambda_endpoint=os.getenv(
                "AWS_DEX_LAMBDA_ENDPOINT", "http://127.0.0.1:3001"
            ),
            local_runner_endpoint=os.getenv(
                "AWS_DEX_LOCAL_RUNNER_ENDPOINT", "http://0.0.0.0:5000"
            ),
            local_runner_region=os.getenv("AWS_DEX_LOCAL_RUNNER_REGION", "us-west-2"),
            local_runner_mode=os.getenv("AWS_DEX_LOCAL_RUNNER_MODE", "local"),
            store_type=StoreType(os.getenv("AWS_DEX_STORE_TYPE", "memory")),
            store_path=os.getenv("AWS_DEX_STORE_PATH"),
        )


class CliApp:
    """Main CLI application for dex-local-runner."""

    def __init__(self) -> None:
        """Initialize the CLI application."""
        self.config = CliConfig.from_environment()

    def run(self, args: list[str] | None = None) -> int:
        """Run the CLI application with the given arguments.

        Args:
            args: Command line arguments. If None, uses sys.argv[1:]

        Returns:
            Exit code (0 for success, non-zero for error)
        """
        try:
            parser = self._create_parsers()
            parsed_args = parser.parse_args(args)

            # Configure logging based on log level
            if hasattr(parsed_args, "log_level") and isinstance(
                parsed_args.log_level, str
            ):
                level = logging.getLevelNamesMapping().get(
                    parsed_args.log_level, logging.INFO
                )
            else:
                # config.log_level is always an integer
                level = self.config.log_level

            logging.basicConfig(
                level=level,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            )
            logging.getLogger("botocore").setLevel(logging.WARNING)

            # Execute the appropriate command
            return parsed_args.func(parsed_args)

        except SystemExit as e:
            # argparse calls sys.exit() for help, errors, etc.
            return int(e.code) if e.code is not None else 1
        except KeyboardInterrupt:
            print("\nOperation cancelled by user", file=sys.stderr)  # noqa: T201
            return 130  # Standard exit code for SIGINT
        except DurableFunctionsTestError:
            logger.exception("Error")
            return 1
        except Exception:
            logger.exception("Unexpected error.")
            return 1

    def _create_parsers(self) -> argparse.ArgumentParser:
        """Create the argument parsers for all commands."""
        parser = argparse.ArgumentParser(
            prog="dex-local-runner",
            description="AWS Durable Functions Local Runner CLI",
        )

        subparsers = parser.add_subparsers(
            dest="command", help="Available commands", required=True
        )

        # Create individual parsers
        self._create_start_server_parser(subparsers)
        self._create_invoke_parser(subparsers)
        self._create_get_durable_execution_parser(subparsers)
        self._create_get_durable_execution_history_parser(subparsers)

        return parser

    # region parsers

    def _create_start_server_parser(self, subparsers) -> None:
        """Create the start-server command parser."""
        start_server_parser = subparsers.add_parser(
            "start-server", help="Start the local Durable Functions Server"
        )
        start_server_parser.add_argument(
            "--host",
            default=self.config.host,
            help=f"Server bind address (default: {self.config.host}, env: AWS_DEX_HOST)",
        )
        start_server_parser.add_argument(
            "--port",
            type=int,
            default=self.config.port,
            help=f"Server port (default: {self.config.port}, env: AWS_DEX_PORT)",
        )
        start_server_parser.add_argument(
            "--log-level",
            type=str,
            choices=list(logging.getLevelNamesMapping().keys()),
            default=logging.getLevelName(self.config.log_level),
            help=f"Logging level (default: {logging.getLevelName(self.config.log_level)}, env: AWS_DEX_LOG_LEVEL)",
        )
        start_server_parser.add_argument(
            "--lambda-endpoint",
            default=self.config.lambda_endpoint,
            help=f"Lambda Service endpoint (default: {self.config.lambda_endpoint}, env: AWS_DEX_LAMBDA_ENDPOINT)",
        )
        start_server_parser.add_argument(
            "--local-runner-endpoint",
            default=self.config.local_runner_endpoint,
            help=f"Local Runner endpoint (default: {self.config.local_runner_endpoint}, env: AWS_DEX_LOCAL_RUNNER_ENDPOINT)",
        )
        start_server_parser.add_argument(
            "--local-runner-region",
            default=self.config.local_runner_region,
            help=f"Local Runner region (default: {self.config.local_runner_region}, env: AWS_DEX_LOCAL_RUNNER_REGION)",
        )
        start_server_parser.add_argument(
            "--local-runner-mode",
            default=self.config.local_runner_mode,
            help=f"Local Runner mode (default: {self.config.local_runner_mode}, env: AWS_DEX_LOCAL_RUNNER_MODE)",
        )
        start_server_parser.add_argument(
            "--store-type",
            choices=[store_type.value for store_type in StoreType],
            default=self.config.store_type.value,
            help=f"Store type for execution persistence (default: {self.config.store_type.value}, env: AWS_DEX_STORE_TYPE)",
        )
        start_server_parser.add_argument(
            "--store-path",
            default=self.config.store_path,
            help=f"Path for filesystem store (default: {self.config.store_path or '.durable_executions'}, env: AWS_DEX_STORE_PATH)",
        )
        start_server_parser.set_defaults(func=self.start_server_command)

    def _create_invoke_parser(self, subparsers) -> None:
        """Create the invoke command parser."""
        invoke_parser = subparsers.add_parser(
            "invoke", help="Invoke a Durable Execution"
        )
        invoke_parser.add_argument(
            "--function-name", required=True, help="Function name (required)"
        )
        invoke_parser.add_argument(
            "--input", default="{}", help="Input data (default: {})"
        )
        invoke_parser.add_argument(
            "--durable-execution-name", help="Durable execution name (optional)"
        )
        invoke_parser.set_defaults(func=self.invoke_command)

    def _create_get_durable_execution_parser(self, subparsers) -> None:
        """Create the get-durable-execution command parser."""
        get_execution_parser = subparsers.add_parser(
            "get-durable-execution", help="Get execution details"
        )
        get_execution_parser.add_argument(
            "--durable-execution-arn",
            required=True,
            help="Durable execution ARN (required)",
        )
        get_execution_parser.set_defaults(func=self.get_durable_execution_command)

    def _create_get_durable_execution_history_parser(self, subparsers) -> None:
        """Create the get-durable-execution-history command parser."""
        get_history_parser = subparsers.add_parser(
            "get-durable-execution-history", help="Get execution history"
        )
        get_history_parser.add_argument(
            "--durable-execution-arn",
            required=True,
            help="Durable execution ARN (required)",
        )
        get_history_parser.set_defaults(func=self.get_durable_execution_history_command)

    # endregion parsers

    # region commands

    def start_server_command(self, args: argparse.Namespace) -> int:
        """Execute the start-server command.

        Args:
            args: Parsed command line arguments

        Returns:
            Exit code (0 for success, non-zero for error)
        """
        try:
            # Create web service configuration from CLI arguments
            web_config = WebServiceConfig(
                host=args.host,
                port=args.port,
                log_level=args.log_level,
            )

            # Create web runner configuration with composition
            runner_config = WebRunnerConfig(
                web_service=web_config,
                lambda_endpoint=args.lambda_endpoint,
                local_runner_endpoint=args.local_runner_endpoint,
                local_runner_region=args.local_runner_region,
                local_runner_mode=args.local_runner_mode,
                store_type=StoreType(args.store_type),
                store_path=args.store_path,
            )

            logger.info(
                "Starting Durable Functions Local Runner on %s:%s",
                args.host,
                args.port,
            )
            logger.info("Configuration:")
            logger.info("  Host: %s", args.host)
            logger.info("  Port: %s", args.port)
            logger.info("  Log Level: %s", args.log_level)
            logger.info("  Lambda Endpoint: %s", args.lambda_endpoint)
            logger.info("  Local Runner Endpoint: %s", args.local_runner_endpoint)
            logger.info("  Local Runner Region: %s", args.local_runner_region)
            logger.info("  Local Runner Mode: %s", args.local_runner_mode)
            logger.info("  Store Type: %s", args.store_type)
            if StoreType(args.store_type) == StoreType.FILESYSTEM:
                store_path = args.store_path or ".durable_executions"
                logger.info("  Store Path: %s", store_path)

            # Use runner as context manager for proper lifecycle
            with WebRunner(runner_config) as runner:
                logger.info("Server started successfully. Press Ctrl+C to stop.")
                runner.serve_forever()

            return 0  # noqa: TRY300

        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping server...")
            return 130  # Standard exit code for SIGINT
        except Exception:
            logger.exception("Failed to start server")
            return 1

    def invoke_command(self, args: argparse.Namespace) -> int:
        """Execute the invoke command.

        Args:
            args: Parsed command line arguments

        Returns:
            Exit code (0 for success, non-zero for error)
        """
        # Validate input JSON
        try:
            json.loads(args.input)  # Just validate, don't store
        except json.JSONDecodeError:
            logger.exception("JSON decode error")
            return 1

        try:
            # Create StartDurableExecutionInput
            start_input = StartDurableExecutionInput(
                account_id="123456789012",  # Default account ID for local testing
                function_name=args.function_name,
                function_qualifier="$LATEST",  # Default qualifier
                execution_name=args.durable_execution_name
                or f"{args.function_name}-execution",
                execution_timeout_seconds=300,  # 5 minutes default
                execution_retention_period_days=7,  # 1 week default
                invocation_id=str(uuid.uuid4()),  # Generate unique invocation ID
                input=args.input,
            )

            # Make HTTP request to start-durable-execution endpoint
            endpoint_url = self.config.local_runner_endpoint
            url = urljoin(endpoint_url, "/start-durable-execution")

            payload = start_input.to_dict()
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            try:
                with urlopen(req, timeout=10) as response:  # noqa: S310
                    result = json.loads(response.read().decode("utf-8"))
                    print(json.dumps(result, indent=2))  # noqa: T201
                    return 0
            except HTTPError as e:
                try:
                    error_data = json.loads(e.read().decode("utf-8"))
                    logger.exception("HTTP error response")
                    print(  # noqa: T201
                        f"Error: {error_data.get('ErrorMessage', 'Unknown error')}",
                        file=sys.stderr,
                    )
                except json.JSONDecodeError:
                    logger.exception("Non-JSON error response")
                return 1

        except URLError:
            logger.exception(
                "Error: Could not connect to the local runner server. Is it running?"
            )
            return 1
        except Exception:
            logger.exception("Unexpected error in invoke command")
            return 1

    def get_durable_execution_command(self, args: argparse.Namespace) -> int:
        """Execute the get-durable-execution command.

        Args:
            args: Parsed command line arguments

        Returns:
            Exit code (0 for success, non-zero for error)
        """
        try:
            # Set up boto3 client with local endpoint
            client = self._create_boto3_client()

            # Call get_durable_execution
            response = client.get_durable_execution(
                DurableExecutionArn=args.durable_execution_arn
            )

            # Print formatted response
            print(json.dumps(response, indent=2, default=str))  # noqa: T201
            return 0  # noqa: TRY300

        except client.exceptions.InvalidParameterValueException as e:
            print(f"Error: Invalid parameter - {e}", file=sys.stderr)  # noqa: T201
            return 1
        except client.exceptions.ResourceNotFoundException as e:
            print(f"Error: Execution not found - {e}", file=sys.stderr)  # noqa: T201
            return 1
        except client.exceptions.TooManyRequestsException as e:
            print(f"Error: Too many requests - {e}", file=sys.stderr)  # noqa: T201
            return 1
        except client.exceptions.ServiceException as e:
            print(f"Error: Service error - {e}", file=sys.stderr)  # noqa: T201
            return 1
        except ConnectionError:
            logger.exception(
                "Error: Could not connect to the local runner server. Is it running?"
            )
            return 1
        except Exception:
            logger.exception("Unexpected error in get-durable-execution command")
            return 1

    def get_durable_execution_history_command(self, args: argparse.Namespace) -> int:
        """Execute the get-durable-execution-history command.

        TODO: implement - this is incomplete

        Args:
            args: Parsed command line arguments

        Returns:
            Exit code (0 for success, non-zero for error)
        """
        try:
            # Set up boto3 client with local endpoint
            client = self._create_boto3_client()

            # Call get_durable_execution_history
            response = client.get_durable_execution_history(
                DurableExecutionArn=args.durable_execution_arn
            )

            print(json.dumps(response, indent=2, default=str))  # noqa: T201
            return 0  # noqa: TRY300

        except Exception:
            logger.exception("General error")
            return 1

    # endregion commands

    def _create_boto3_client(
        self, endpoint_url: str | None = None, region_name: str | None = None
    ) -> Any:
        """Create boto3 client for Lambda service.

        Args:
            endpoint_url: Optional endpoint URL override
            region_name: Optional region name override

        Returns:
            Configured boto3 client for local runner

        Raises:
            Exception: If client creation fails
        """
        try:
            # Use provided values or fall back to config
            final_endpoint = endpoint_url or self.config.local_runner_endpoint
            final_region = region_name or self.config.local_runner_region

            # Create client with local endpoint - no AWS access keys required
            return boto3.client(
                "lambda",
                endpoint_url=final_endpoint,
                region_name=final_region,
            )
        except Exception as e:
            msg = f"Failed to create boto3 client: {e}"
            raise DurableFunctionsLocalRunnerError(msg) from e


def main() -> int:
    """Main entry point for the dex-local-runner CLI."""
    app = CliApp()
    return app.run()


if __name__ == "__main__":
    sys.exit(main())

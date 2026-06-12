"""Tests for the CLI module."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from http.client import HTTPMessage
from io import StringIO, BytesIO
from unittest.mock import Mock, patch

import pytest
from urllib.error import HTTPError, URLError

from botocore.exceptions import ConnectionError  # type: ignore

from aws_durable_execution_sdk_python_testing.cli import CliApp, CliConfig, main
from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsLocalRunnerError,
    InvalidParameterValueException,
    ResourceNotFoundException,
    ServiceException,
    TooManyRequestsException,
)


def test_cli_config_has_correct_default_values() -> None:
    """Test that CliConfig has correct default values."""
    config = CliConfig()

    assert config.host == "0.0.0.0"  # noqa: S104
    assert config.port == 5000
    assert config.log_level == logging.INFO
    assert config.lambda_endpoint == "http://127.0.0.1:3001"
    assert config.local_runner_endpoint == "http://0.0.0.0:5000"
    assert config.local_runner_region == "us-west-2"
    assert config.local_runner_mode == "local"


def test_cli_config_from_environment_uses_defaults_when_no_env_vars() -> None:
    """Test from_environment with no environment variables set."""
    with patch.dict(os.environ, {}, clear=True):
        config = CliConfig.from_environment()

        assert config.host == "0.0.0.0"  # noqa: S104
        assert config.port == 5000
        assert config.log_level == logging.INFO
        assert config.lambda_endpoint == "http://127.0.0.1:3001"
        assert config.local_runner_endpoint == "http://0.0.0.0:5000"
        assert config.local_runner_region == "us-west-2"
        assert config.local_runner_mode == "local"


def test_cli_config_from_environment_uses_all_env_vars_when_set() -> None:
    """Test from_environment with all environment variables set."""
    env_vars = {
        "AWS_DEX_HOST": "127.0.0.1",
        "AWS_DEX_PORT": "8080",
        "AWS_DEX_LOG_LEVEL": "DEBUG",
        "AWS_DEX_LAMBDA_ENDPOINT": "http://localhost:4000",
        "AWS_DEX_LOCAL_RUNNER_ENDPOINT": "http://localhost:8080",
        "AWS_DEX_LOCAL_RUNNER_REGION": "us-east-1",
        "AWS_DEX_LOCAL_RUNNER_MODE": "remote",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        config = CliConfig.from_environment()

        assert config.host == "127.0.0.1"
        assert config.port == 8080
        assert config.log_level == logging.DEBUG
        assert config.lambda_endpoint == "http://localhost:4000"
        assert config.local_runner_endpoint == "http://localhost:8080"
        assert config.local_runner_region == "us-east-1"
        assert config.local_runner_mode == "remote"


def test_cli_config_from_environment_uses_partial_env_vars_with_defaults() -> None:
    """Test from_environment with some environment variables set."""
    env_vars = {
        "AWS_DEX_HOST": "192.168.1.1",
        "AWS_DEX_PORT": "9000",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        config = CliConfig.from_environment()

        assert config.host == "192.168.1.1"
        assert config.port == 9000
        # Other values should be defaults
        assert config.log_level == logging.INFO
        assert config.lambda_endpoint == "http://127.0.0.1:3001"


def test_cli_app_loads_config_from_environment_on_init() -> None:
    """Test that CliApp loads configuration from environment on init."""
    env_vars = {"AWS_DEX_HOST": "test-host", "AWS_DEX_PORT": "7777"}

    with patch.dict(os.environ, env_vars, clear=True):
        app = CliApp()

        assert app.config.host == "test-host"
        assert app.config.port == 7777


def test_cli_app_shows_help_and_returns_error_when_no_command() -> None:
    """Test that running with no command shows help and returns error code."""
    app = CliApp()

    with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
        exit_code = app.run([])

        assert exit_code == 2  # argparse error code
        assert "required" in mock_stderr.getvalue().lower()


def test_cli_app_shows_usage_information_with_help_flag() -> None:
    """Test that --help shows usage information."""
    app = CliApp()

    with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
        exit_code = app.run(["--help"])

        assert exit_code == 0
        output = mock_stdout.getvalue()
        assert "dex-local-runner" in output
        assert "start-server" in output
        assert "invoke" in output
        assert "get-durable-execution" in output
        assert "get-durable-execution-history" in output


def test_cli_app_handles_keyboard_interrupt_gracefully() -> None:
    """Test that KeyboardInterrupt is handled gracefully."""
    app = CliApp()

    with patch.object(app, "_create_parsers") as mock_setup:
        mock_setup.side_effect = KeyboardInterrupt()

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = app.run(["start-server"])

            assert exit_code == 130
            assert "cancelled by user" in mock_stderr.getvalue()


def test_start_server_command_parses_arguments_correctly() -> None:
    """Test that start-server command parses arguments correctly."""
    app = CliApp()

    # Test with default values
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.side_effect = KeyboardInterrupt()

        exit_code = app.run(["start-server"])
        assert exit_code == 130  # KeyboardInterrupt exit code

    # Test with custom values
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.side_effect = KeyboardInterrupt()

        exit_code = app.run(
            [
                "start-server",
                "--host",
                "127.0.0.1",
                "--port",
                "8080",
                "--log-level",
                "DEBUG",
                "--lambda-endpoint",
                "http://localhost:4000",
                "--local-runner-endpoint",
                "http://localhost:8080",
                "--local-runner-region",
                "us-east-1",
                "--local-runner-mode",
                "remote",
            ]
        )
        assert exit_code == 130  # KeyboardInterrupt exit code


def test_invoke_command_parses_arguments_correctly() -> None:
    """Test that invoke command parses arguments correctly."""
    app = CliApp()

    # Test with required function-name
    with patch("sys.stdout", new_callable=StringIO):
        exit_code = app.run(["invoke", "--function-name", "test-function"])
        assert exit_code == 1  # Not implemented yet

    # Test with all parameters
    with patch("sys.stdout", new_callable=StringIO):
        exit_code = app.run(
            [
                "invoke",
                "--function-name",
                "test-function",
                "--input",
                '{"key": "value"}',
                "--durable-execution-name",
                "test-execution",
            ]
        )
        assert exit_code == 1  # Not implemented yet


def test_invoke_command_requires_function_name_parameter() -> None:
    """Test that invoke command requires function-name parameter."""
    app = CliApp()

    with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
        exit_code = app.run(["invoke"])

        assert exit_code == 2  # argparse error code
        assert "required" in mock_stderr.getvalue().lower()


def test_invoke_command_validates_json_input_format() -> None:
    """Test that invoke command validates JSON input."""
    app = CliApp()

    exit_code = app.run(
        [
            "invoke",
            "--function-name",
            "test-function",
            "--input",
            "invalid-json",
        ]
    )

    assert exit_code == 1


def test_get_durable_execution_command_parses_arguments_correctly() -> None:
    """Test that get-durable-execution command parses arguments correctly."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = Mock()
        mock_client.get_durable_execution.side_effect = Exception("Connection refused")
        mock_create_client.return_value = mock_client

        with patch("sys.stderr", new_callable=StringIO):
            exit_code = app.run(
                ["get-durable-execution", "--durable-execution-arn", "test-arn"]
            )
            assert exit_code == 1  # Connection error


def test_get_durable_execution_command_requires_arn_parameter() -> None:
    """Test that get-durable-execution command requires ARN parameter."""
    app = CliApp()

    with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
        exit_code = app.run(["get-durable-execution"])

        assert exit_code == 2  # argparse error code
        assert "required" in mock_stderr.getvalue().lower()


def test_get_durable_execution_history_command_parses_arguments_correctly() -> None:
    """Test that get-durable-execution-history command parses arguments correctly."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = Mock()
        mock_client.get_durable_execution_history.side_effect = Exception(
            "Connection refused"
        )
        mock_create_client.return_value = mock_client

        with patch("sys.stderr", new_callable=StringIO):
            exit_code = app.run(
                ["get-durable-execution-history", "--durable-execution-arn", "test-arn"]
            )
            assert exit_code == 1  # Connection error


def test_get_durable_execution_history_command_requires_arn_parameter() -> None:
    """Test that get-durable-execution-history command requires ARN parameter."""
    app = CliApp()

    with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
        exit_code = app.run(["get-durable-execution-history"])

        assert exit_code == 2  # argparse error code
        assert "required" in mock_stderr.getvalue().lower()


def test_logging_configuration_uses_specified_log_level() -> None:
    """Test that logging is configured based on log level."""
    app = CliApp()

    with patch("logging.basicConfig") as mock_basic_config:
        with patch("sys.stdout", new_callable=StringIO):
            with patch.object(app, "start_server_command", return_value=0):
                app.run(["start-server", "--log-level", "DEBUG"])

                mock_basic_config.assert_called_once()
                call_args = mock_basic_config.call_args
                assert call_args[1]["level"] == 10


def test_parser_creation_includes_all_subcommands() -> None:
    """Test that parser creation includes all expected subcommands."""
    app = CliApp()

    # Test that all subcommands are available
    with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
        exit_code = app.run(["--help"])
        assert exit_code == 0
        output = mock_stdout.getvalue()
        assert "start-server" in output
        assert "invoke" in output
        assert "get-durable-execution" in output
        assert "get-durable-execution-history" in output


def test_start_server_command_works_with_mocked_dependencies() -> None:
    """Test start-server command with mocked WebRunner."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None

        # Mock serve_forever to avoid blocking
        mock_runner_instance.serve_forever.side_effect = KeyboardInterrupt()

        exit_code = app.run(
            [
                "start-server",
                "--host",
                "127.0.0.1",
                "--port",
                "8080",
                "--log-level",
                "DEBUG",
            ]
        )

        assert exit_code == 130  # KeyboardInterrupt exit code
        mock_web_runner.assert_called_once()

        # Verify WebRunnerConfig was created with correct values
        call_args = mock_web_runner.call_args[0][0]  # First positional argument
        assert call_args.web_service.host == "127.0.0.1"
        assert call_args.web_service.port == 8080
        assert call_args.web_service.log_level == "DEBUG"


def test_start_server_command_handles_server_startup_errors() -> None:
    """Test start-server command handles server startup errors."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Make WebRunner constructor raise an exception
        mock_web_runner.side_effect = Exception("Server startup failed")

        exit_code = app.run(["start-server"])

        assert exit_code == 1


def test_start_server_command_creates_correct_web_runner_config() -> None:
    """Test that start-server command creates WebRunnerConfig with all CLI arguments."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.side_effect = KeyboardInterrupt()

        exit_code = app.run(
            [
                "start-server",
                "--host",
                "192.168.1.100",
                "--port",
                "9000",
                "--log-level",
                "WARNING",
                "--lambda-endpoint",
                "http://custom-lambda:4000",
                "--local-runner-endpoint",
                "http://custom-runner:9000",
                "--local-runner-region",
                "eu-west-1",
                "--local-runner-mode",
                "remote",
            ]
        )

        assert exit_code == 130  # KeyboardInterrupt exit code
        mock_web_runner.assert_called_once()

        # Verify WebRunnerConfig was created with all custom values
        config = mock_web_runner.call_args[0][0]  # First positional argument

        # Verify web service configuration
        assert config.web_service.host == "192.168.1.100"
        assert config.web_service.port == 9000
        assert config.web_service.log_level == "WARNING"

        # Verify Lambda service configuration
        assert config.lambda_endpoint == "http://custom-lambda:4000"
        assert config.local_runner_endpoint == "http://custom-runner:9000"
        assert config.local_runner_region == "eu-west-1"
        assert config.local_runner_mode == "remote"


def test_start_server_command_uses_context_manager_properly() -> None:
    """Test that start-server command uses WebRunner as context manager."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.return_value = None

        exit_code = app.run(["start-server"])

        assert exit_code == 0
        mock_web_runner.assert_called_once()

        # Verify context manager methods were called
        mock_runner_instance.__enter__.assert_called_once()
        mock_runner_instance.__exit__.assert_called_once()
        mock_runner_instance.serve_forever.assert_called_once()


def test_start_server_command_handles_runtime_error_from_web_runner() -> None:
    """Test that start-server command handles DurableFunctionsLocalRunnerError from WebRunner."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager that raises DurableFunctionsLocalRunnerError
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.side_effect = DurableFunctionsLocalRunnerError(
            "Server already running"
        )

        exit_code = app.run(["start-server"])

        assert exit_code == 1


def test_start_server_command_logs_configuration_details() -> None:
    """Test that start-server command logs configuration details."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.side_effect = KeyboardInterrupt()

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            exit_code = app.run(
                [
                    "start-server",
                    "--host",
                    "test-host",
                    "--port",
                    "8888",
                ]
            )

            assert exit_code == 130

            # Verify configuration logging
            mock_logger.info.assert_any_call(
                "Starting Durable Functions Local Runner on %s:%s",
                "test-host",
                8888,
            )
            mock_logger.info.assert_any_call("Configuration:")
            mock_logger.info.assert_any_call("  Host: %s", "test-host")
            mock_logger.info.assert_any_call("  Port: %s", 8888)


def test_start_server_command_maintains_backward_compatible_logging() -> None:
    """Test that start-server command maintains backward compatible logging messages."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.side_effect = KeyboardInterrupt()

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            exit_code = app.run(["start-server"])

            assert exit_code == 130

            # Verify backward compatible logging messages
            mock_logger.info.assert_any_call(
                "Server started successfully. Press Ctrl+C to stop."
            )
            mock_logger.info.assert_any_call(
                "Received shutdown signal, stopping server..."
            )


def test_start_server_command_handles_serve_forever_exception() -> None:
    """Test that start-server command handles exceptions from serve_forever."""
    app = CliApp()

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner:
        # Mock runner context manager
        mock_runner_instance = mock_web_runner.return_value
        mock_runner_instance.__enter__.return_value = mock_runner_instance
        mock_runner_instance.__exit__.return_value = None
        mock_runner_instance.serve_forever.side_effect = (
            DurableFunctionsLocalRunnerError("Server error during operation")
        )

        exit_code = app.run(["start-server"])

        assert exit_code == 1


def test_main_function_creates_cli_app_and_runs() -> None:
    """Test the main function entry point."""
    with patch("aws_durable_execution_sdk_python_testing.cli.CliApp") as mock_cli_app:
        mock_app_instance = mock_cli_app.return_value
        mock_app_instance.run.return_value = 42

        exit_code = main()

        mock_cli_app.assert_called_once()
        mock_app_instance.run.assert_called_once()
        assert exit_code == 42


def test_main_function_works_when_called_as_script() -> None:
    """Test that main function works when called as script."""
    original_argv = sys.argv[:]
    try:
        sys.argv = ["dex-local-runner", "--help"]

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.CliApp"
        ) as mock_cli_app:
            mock_app_instance = mock_cli_app.return_value
            mock_app_instance.run.return_value = 0

            exit_code = main()

            assert exit_code == 0
            mock_app_instance.run.assert_called_once()
    finally:
        sys.argv = original_argv


# Tests for client operation CLI commands


def test_invoke_command_makes_http_request_to_start_execution_endpoint() -> None:
    """Test that invoke command makes HTTP request to start-durable-execution endpoint."""
    app = CliApp()

    response_body = json.dumps(
        {
            "ExecutionArn": "arn:aws:lambda:us-west-2:123456789012:function:test-function:execution:test-execution"
        }
    ).encode("utf-8")

    mock_response = Mock()
    mock_response.read.return_value = response_body
    mock_response.__enter__ = Mock(return_value=mock_response)
    mock_response.__exit__ = Mock(return_value=False)

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.urlopen",
        return_value=mock_response,
    ) as mock_urlopen:
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            exit_code = app.invoke_command(
                argparse.Namespace(
                    function_name="test-function",
                    input='{"key": "value"}',
                    durable_execution_name="test-execution",
                )
            )

            assert exit_code == 0
            mock_urlopen.assert_called_once()

            # Verify the request details
            call_args = mock_urlopen.call_args
            req = call_args[0][0]
            assert req.full_url.endswith("/start-durable-execution")
            assert req.get_header("Content-type") == "application/json"
            assert call_args[1]["timeout"] == 10

            # Verify payload structure
            payload = json.loads(req.data.decode("utf-8"))
            assert payload["FunctionName"] == "test-function"
            assert payload["Input"] == '{"key": "value"}'
            assert payload["ExecutionName"] == "test-execution"

            # Verify output
            output = mock_stdout.getvalue()
            assert "ExecutionArn" in output


def test_invoke_command_uses_default_execution_name_when_not_provided() -> None:
    """Test that invoke command generates default execution name when not provided."""
    app = CliApp()

    response_body = json.dumps({"ExecutionArn": "test-arn"}).encode("utf-8")
    mock_response = Mock()
    mock_response.read.return_value = response_body
    mock_response.__enter__ = Mock(return_value=mock_response)
    mock_response.__exit__ = Mock(return_value=False)

    with patch(
        "aws_durable_execution_sdk_python_testing.cli.urlopen",
        return_value=mock_response,
    ) as mock_urlopen:
        app.invoke_command(
            argparse.Namespace(
                function_name="my-function",
                input="{}",
                durable_execution_name=None,
            )
        )

        # Verify default execution name is generated
        req = mock_urlopen.call_args[0][0]
        payload = json.loads(req.data.decode("utf-8"))
        assert payload["ExecutionName"] == "my-function-execution"


def test_invoke_command_handles_connection_error() -> None:
    """Test that invoke command handles connection errors gracefully."""
    app = CliApp()

    with patch("aws_durable_execution_sdk_python_testing.cli.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = URLError("Connection refused")

        exit_code = app.invoke_command(
            argparse.Namespace(
                function_name="test-function",
                input="{}",
                durable_execution_name=None,
            )
        )

        assert exit_code == 1


def test_invoke_command_handles_http_error_response() -> None:
    """Test that invoke command handles HTTP error responses."""
    app = CliApp()

    error_body = json.dumps(
        {
            "ErrorMessage": "Invalid parameter value",
            "ErrorType": "InvalidParameterValueException",
        }
    ).encode("utf-8")

    with patch("aws_durable_execution_sdk_python_testing.cli.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = HTTPError(
            url="http://0.0.0.0:5000/start-durable-execution",
            code=400,
            msg="Bad Request",
            hdrs=HTTPMessage(),
            fp=BytesIO(error_body),
        )

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = app.invoke_command(
                argparse.Namespace(
                    function_name="test-function",
                    input="{}",
                    durable_execution_name=None,
                )
            )

            assert exit_code == 1
            assert "Invalid parameter value" in mock_stderr.getvalue()


def test_invoke_command_handles_non_json_error_response() -> None:
    """Test that invoke command handles non-JSON error responses."""
    app = CliApp()

    with patch("aws_durable_execution_sdk_python_testing.cli.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = HTTPError(
            url="http://0.0.0.0:5000/start-durable-execution",
            code=500,
            msg="Internal Server Error",
            hdrs=HTTPMessage(),
            fp=BytesIO(b"Internal Server Error"),
        )

        exit_code = app.invoke_command(
            argparse.Namespace(
                function_name="test-function",
                input="{}",
                durable_execution_name=None,
            )
        )

        assert exit_code == 1


def test_get_durable_execution_command_uses_boto3_client() -> None:
    """Test that get-durable-execution command uses boto3 client."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value
        mock_client.get_durable_execution.return_value = {
            "DurableExecutionArn": "test-arn",
            "Status": "SUCCEEDED",
            "Result": {"output": "success"},
        }

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="test-arn")
            )

            assert exit_code == 0
            mock_create_client.assert_called_once()
            mock_client.get_durable_execution.assert_called_once_with(
                DurableExecutionArn="test-arn"
            )

            # Verify JSON output
            output = mock_stdout.getvalue()
            assert "test-arn" in output
            assert "SUCCEEDED" in output


def test_get_durable_execution_command_handles_resource_not_found() -> None:
    """Test that get-durable-execution command handles ResourceNotFoundException."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value

        mock_client.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.exceptions.InvalidParameterValueException = (
            InvalidParameterValueException
        )
        mock_client.exceptions.TooManyRequestsException = TooManyRequestsException
        mock_client.exceptions.ServiceException = ServiceException

        mock_client.get_durable_execution.side_effect = ResourceNotFoundException(
            "Resource not found"
        )

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="nonexistent-arn")
            )

            assert exit_code == 1
            assert "Error: Execution not found" in mock_stderr.getvalue()


def test_get_durable_execution_command_handles_invalid_parameter() -> None:
    """Test that get-durable-execution command handles InvalidParameterValueException."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value

        mock_client.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.exceptions.InvalidParameterValueException = (
            InvalidParameterValueException
        )
        mock_client.exceptions.TooManyRequestsException = TooManyRequestsException
        mock_client.exceptions.ServiceException = ServiceException

        mock_client.get_durable_execution.side_effect = InvalidParameterValueException(
            "Invalid parameters"
        )

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="invalid-arn")
            )

            assert exit_code == 1
            assert "Error: Invalid parameter" in mock_stderr.getvalue()


def test_get_durable_execution_command_handles_too_many_requests() -> None:
    """Test that get-durable-execution command handles InvalidParameterValueException."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value

        mock_client.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.exceptions.InvalidParameterValueException = (
            InvalidParameterValueException
        )
        mock_client.exceptions.TooManyRequestsException = TooManyRequestsException
        mock_client.exceptions.ServiceException = ServiceException

        mock_client.get_durable_execution.side_effect = TooManyRequestsException(
            "Too many requests"
        )

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="my-arn")
            )

            assert exit_code == 1
            assert "Error: Too many requests" in mock_stderr.getvalue()


def test_get_durable_execution_command_handles_service_exception() -> None:
    """Test that get-durable-execution command handles InvalidParameterValueException."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value

        mock_client.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.exceptions.InvalidParameterValueException = (
            InvalidParameterValueException
        )
        mock_client.exceptions.TooManyRequestsException = TooManyRequestsException
        mock_client.exceptions.ServiceException = ServiceException

        mock_client.get_durable_execution.side_effect = ServiceException(
            "Service exception"
        )

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="my-arn")
            )

            assert exit_code == 1
            assert "Error: Service error" in mock_stderr.getvalue()


def test_get_durable_execution_command_handles_connection_error() -> None:
    """Test that get-durable-execution command handles connection errors."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value

        mock_client.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.exceptions.InvalidParameterValueException = (
            InvalidParameterValueException
        )
        mock_client.exceptions.TooManyRequestsException = TooManyRequestsException
        mock_client.exceptions.ServiceException = ServiceException

        mock_client.get_durable_execution.side_effect = ConnectionError(
            error="Mocked connection error"
        )

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="my-arn")
            )

            assert exit_code == 1
            mock_logger.exception.assert_called_once_with(
                "Error: Could not connect to the local runner server. Is it running?"
            )


def test_get_durable_execution_history_command_uses_boto3_client() -> None:
    """Test that get-durable-execution-history command uses boto3 client."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value
        mock_client.get_durable_execution_history.return_value = {
            "Events": [
                {
                    "EventType": "ExecutionStarted",
                    "EventTimestamp": "2024-01-01T00:00:00Z",
                },
                {
                    "EventType": "ExecutionSucceeded",
                    "EventTimestamp": "2024-01-01T00:01:00Z",
                },
            ]
        }

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            exit_code = app.get_durable_execution_history_command(
                argparse.Namespace(durable_execution_arn="test-arn")
            )

            assert exit_code == 0
            mock_create_client.assert_called_once()
            mock_client.get_durable_execution_history.assert_called_once_with(
                DurableExecutionArn="test-arn"
            )

            # Verify JSON output
            output = mock_stdout.getvalue()
            assert "ExecutionStarted" in output
            assert "ExecutionSucceeded" in output


def test_get_durable_execution_history_command_handles_resource_not_found() -> None:
    """Test that get-durable-execution-history command handles ResourceNotFoundException."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value
        mock_client.get_durable_execution_history.side_effect = Exception(
            "ResourceNotFoundException: Execution not found"
        )

        exit_code = app.get_durable_execution_history_command(
            argparse.Namespace(durable_execution_arn="nonexistent-arn")
        )

        assert exit_code == 1


def test_get_durable_execution_history_command_handles_connection_error() -> None:
    """Test that get-durable-execution-history command handles connection errors."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value
        mock_client.get_durable_execution_history.side_effect = Exception(
            "Connection refused"
        )

        exit_code = app.get_durable_execution_history_command(
            argparse.Namespace(durable_execution_arn="test-arn")
        )

        assert exit_code == 1


def test_create_boto3_client_creates_client_correctly() -> None:
    """Test that _create_boto3_client creates boto3 client correctly."""
    app = CliApp()

    with patch("boto3.client") as mock_boto3_client:
        app._create_boto3_client()  # noqa: SLF001

        # Verify boto3 client is created with correct parameters
        mock_boto3_client.assert_called_once_with(
            "lambda",
            endpoint_url=app.config.local_runner_endpoint,
            region_name=app.config.local_runner_region,
        )


def test_create_boto3_client_handles_creation_failure() -> None:
    """Test that _create_boto3_client handles client creation failures."""
    app = CliApp()

    with patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = Exception("Client creation failed")

        with pytest.raises(DurableFunctionsLocalRunnerError) as exc_info:
            app._create_boto3_client()  # noqa: SLF001

        assert "Failed to create boto3 client" in str(exc_info.value)
        assert "Client creation failed" in str(exc_info.value)


def test_cli_app_handles_durable_functions_test_error() -> None:
    """Test that DurableFunctionsTestError is handled gracefully."""
    app = CliApp()

    with patch.object(app, "_create_parsers") as mock_setup:
        from aws_durable_execution_sdk_python_testing.exceptions import (
            DurableFunctionsTestError,
        )

        mock_setup.side_effect = DurableFunctionsTestError("Test error")

        exit_code = app.run(["start-server"])

        assert exit_code == 1


def test_cli_app_handles_unexpected_exception() -> None:
    """Test that unexpected exceptions are handled gracefully."""
    app = CliApp()

    with patch.object(app, "_create_parsers") as mock_setup:
        mock_setup.side_effect = RuntimeError("Unexpected error")

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            exit_code = app.run(["start-server"])

            assert exit_code == 1
            mock_logger.exception.assert_called_once_with("Unexpected error.")


def test_invoke_command_handles_general_exception() -> None:
    """Test that invoke command handles general exceptions."""
    app = CliApp()

    with patch("aws_durable_execution_sdk_python_testing.cli.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = ValueError("Some unexpected error")

        exit_code = app.invoke_command(
            argparse.Namespace(
                function_name="test-function",
                input="{}",
                durable_execution_name=None,
            )
        )

        assert exit_code == 1


def test_get_durable_execution_command_handles_general_exception() -> None:
    """Test that get-durable-execution command handles general exceptions."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value
        mock_client.exceptions.ResourceNotFoundException = ResourceNotFoundException
        mock_client.exceptions.InvalidParameterValueException = (
            InvalidParameterValueException
        )
        mock_client.exceptions.TooManyRequestsException = TooManyRequestsException
        mock_client.exceptions.ServiceException = ServiceException
        mock_client.get_durable_execution.side_effect = ValueError(
            "Some unexpected error"
        )

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            exit_code = app.get_durable_execution_command(
                argparse.Namespace(durable_execution_arn="my-arn")
            )

            assert exit_code == 1
            mock_logger.exception.assert_called_once_with(
                "Unexpected error in get-durable-execution command"
            )


def test_get_durable_execution_history_command_handles_general_exception() -> None:
    """Test that get-durable-execution-history command handles general exceptions."""
    app = CliApp()

    with patch.object(app, "_create_boto3_client") as mock_create_client:
        mock_client = mock_create_client.return_value
        mock_client.get_durable_execution_history.side_effect = ValueError(
            "Some unexpected error"
        )

        exit_code = app.get_durable_execution_history_command(
            argparse.Namespace(durable_execution_arn="test-arn")
        )

        assert exit_code == 1

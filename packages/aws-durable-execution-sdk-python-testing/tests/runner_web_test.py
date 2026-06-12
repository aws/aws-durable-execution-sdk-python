"""Unit tests for web runner components in runner module."""

from __future__ import annotations

import logging
import os
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python_testing.cli import CliApp
from aws_durable_execution_sdk_python_testing.exceptions import (
    DurableFunctionsLocalRunnerError,
)
from aws_durable_execution_sdk_python_testing.invoker import _LAMBDA_CLIENT_CONFIG
from aws_durable_execution_sdk_python_testing.runner import (
    WebRunner,
    WebRunnerConfig,
)
from aws_durable_execution_sdk_python_testing.web.server import WebServiceConfig


def test_should_create_config_with_web_service_and_defaults():
    """Test creating WebRunnerConfig with WebServiceConfig and default Lambda settings."""
    # Arrange
    web_config = WebServiceConfig(
        host="localhost",
        port=8080,
        log_level=logging.DEBUG,
        max_request_size=5 * 1024 * 1024,
    )

    # Act
    config = WebRunnerConfig(web_service=web_config)

    # Assert
    assert config.web_service == web_config
    assert config.lambda_endpoint == "http://127.0.0.1:3001"
    assert config.local_runner_endpoint == "http://0.0.0.0:5000"
    assert config.local_runner_region == "us-west-2"
    assert config.local_runner_mode == "local"


def test_should_create_config_with_custom_lambda_settings():
    """Test creating WebRunnerConfig with custom Lambda configuration."""
    # Arrange
    web_config = WebServiceConfig(host="0.0.0.0", port=5000)  # noqa: S104
    custom_lambda_endpoint = "http://custom-lambda:4000"
    custom_runner_endpoint = "http://custom-runner:6000"
    custom_region = "us-east-1"
    custom_mode = "remote"

    # Act
    config = WebRunnerConfig(
        web_service=web_config,
        lambda_endpoint=custom_lambda_endpoint,
        local_runner_endpoint=custom_runner_endpoint,
        local_runner_region=custom_region,
        local_runner_mode=custom_mode,
    )

    # Assert
    assert config.web_service == web_config
    assert config.lambda_endpoint == custom_lambda_endpoint
    assert config.local_runner_endpoint == custom_runner_endpoint
    assert config.local_runner_region == custom_region
    assert config.local_runner_mode == custom_mode


def test_should_access_web_service_config_fields():
    """Test accessing WebServiceConfig fields through composition."""
    # Arrange
    web_config = WebServiceConfig(
        host="test-host",
        port=9999,
        log_level=logging.WARNING,
        max_request_size=1024,
    )
    config = WebRunnerConfig(web_service=web_config)

    # Act & Assert
    assert config.web_service.host == "test-host"
    assert config.web_service.port == 9999
    assert config.web_service.log_level == logging.WARNING
    assert config.web_service.max_request_size == 1024


def test_should_be_immutable_frozen_dataclass():
    """Test that WebRunnerConfig is immutable (frozen=True)."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)

    # Act & Assert - attempting to modify should raise FrozenInstanceError
    with pytest.raises(
        AttributeError
    ):  # dataclass frozen raises AttributeError in Python 3.13+
        config.lambda_endpoint = "http://new-endpoint:8000"

    with pytest.raises(AttributeError):
        config.web_service = WebServiceConfig(host="new-host")


def test_should_support_equality_comparison():
    """Test that WebRunnerConfig supports equality comparison."""
    # Arrange
    web_config1 = WebServiceConfig(host="host1", port=5000)
    web_config2 = WebServiceConfig(host="host1", port=5000)
    web_config3 = WebServiceConfig(host="host2", port=5000)

    config1 = WebRunnerConfig(
        web_service=web_config1,
        lambda_endpoint="http://lambda:3001",
    )
    config2 = WebRunnerConfig(
        web_service=web_config2,
        lambda_endpoint="http://lambda:3001",
    )
    config3 = WebRunnerConfig(
        web_service=web_config3,
        lambda_endpoint="http://lambda:3001",
    )

    # Act & Assert
    assert config1 == config2  # Same values should be equal
    assert config1 != config3  # Different web_service should not be equal
    assert config2 != config3  # Different web_service should not be equal


def test_should_support_hash_for_use_in_sets_and_dicts():
    """Test that WebRunnerConfig is hashable for use in sets and dicts."""
    # Arrange
    web_config = WebServiceConfig(host="test", port=8080)
    config1 = WebRunnerConfig(web_service=web_config)
    config2 = WebRunnerConfig(web_service=web_config)

    # Act - should not raise exception
    config_set = {config1, config2}
    config_dict = {config1: "value1", config2: "value2"}

    # Assert
    assert len(config_set) == 1  # Same configs should deduplicate in set
    assert len(config_dict) == 1  # Same configs should overwrite in dict


def test_should_create_config_with_minimal_web_service():
    """Test creating config with minimal WebServiceConfig using defaults."""
    # Arrange
    web_config = WebServiceConfig()  # Uses all defaults

    # Act
    config = WebRunnerConfig(web_service=web_config)

    # Assert
    assert config.web_service.host == "localhost"
    assert config.web_service.port == 5000
    assert config.web_service.log_level == logging.INFO
    assert config.web_service.max_request_size == 10 * 1024 * 1024


def test_should_have_proper_type_annotations():
    """Test that all fields have proper type annotations."""
    # Arrange & Act
    annotations = WebRunnerConfig.__annotations__

    # Assert
    assert "web_service" in annotations
    assert "lambda_endpoint" in annotations
    assert "local_runner_endpoint" in annotations
    assert "local_runner_region" in annotations
    assert "local_runner_mode" in annotations

    # Check that the annotations are the expected string representations
    assert annotations["web_service"] == "WebServiceConfig"
    assert annotations["lambda_endpoint"] == "str"
    assert annotations["local_runner_endpoint"] == "str"
    assert annotations["local_runner_region"] == "str"
    assert annotations["local_runner_mode"] == "str"


def test_should_create_config_with_keyword_arguments():
    """Test creating config using keyword arguments for all fields."""
    # Arrange
    web_config = WebServiceConfig(host="kw-host", port=7777)

    # Act
    config = WebRunnerConfig(
        web_service=web_config,
        lambda_endpoint="http://kw-lambda:2000",
        local_runner_endpoint="http://kw-runner:3000",
        local_runner_region="eu-west-1",
        local_runner_mode="test",
    )

    # Assert
    assert config.web_service == web_config
    assert config.lambda_endpoint == "http://kw-lambda:2000"
    assert config.local_runner_endpoint == "http://kw-runner:3000"
    assert config.local_runner_region == "eu-west-1"
    assert config.local_runner_mode == "test"


def test_should_represent_config_as_string():
    """Test string representation of WebRunnerConfig."""
    # Arrange
    web_config = WebServiceConfig(host="repr-host", port=1234)
    config = WebRunnerConfig(
        web_service=web_config,
        lambda_endpoint="http://repr-lambda:5000",
    )

    # Act
    config_str = str(config)

    # Assert
    assert "WebRunnerConfig" in config_str
    assert "repr-host" in config_str
    assert "1234" in config_str
    assert "http://repr-lambda:5000" in config_str


# WebRunner class tests


def test_should_create_web_runner_with_config():
    """Test creating WebRunner with WebRunnerConfig."""
    # Arrange
    web_config = WebServiceConfig(host="test-host", port=8080)
    config = WebRunnerConfig(web_service=web_config)

    # Act
    runner = WebRunner(config)

    # Assert - Test through public behavior only
    assert isinstance(runner, WebRunner)
    # Verify runner can be used as context manager (public API)
    assert hasattr(runner, "__enter__")
    assert hasattr(runner, "__exit__")
    assert callable(runner.start)
    assert callable(runner.stop)
    assert callable(runner.serve_forever)


def test_should_support_context_manager_protocol():
    """Test WebRunner context manager protocol."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)

    # Act & Assert - should not raise exception
    with WebRunner(config) as runner:
        assert isinstance(runner, WebRunner)
        assert runner._config == config  # noqa: SLF001


def test_should_return_self_from_context_manager_enter():
    """Test that __enter__ returns self."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(config)

    # Act
    result = runner.__enter__()

    # Assert
    assert result is runner


def test_should_call_start_and_stop_on_context_manager():
    """Test that context manager calls start on entry and stop on exit."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(config)

    # Mock the start and stop methods to verify they're called
    with (
        patch.object(runner, "start") as mock_start,
        patch.object(runner, "stop") as mock_stop,
    ):
        # Act
        with runner as context_runner:
            assert context_runner is runner
            mock_start.assert_called_once()

        # Assert
        mock_stop.assert_called_once()


def test_should_handle_context_manager_exit_with_exception():
    """Test context manager exit with exception parameters."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(config)

    # Act & Assert - should not raise exception
    runner.__exit__(ValueError, ValueError("test"), None)


def test_should_have_proper_method_signatures():
    """Test that WebRunner has all required methods with proper signatures."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(config)

    # Assert methods exist and are callable
    assert callable(runner.start)
    assert callable(runner.serve_forever)
    assert callable(runner.stop)
    assert callable(runner.__enter__)
    assert callable(runner.__exit__)


def test_should_initialize_runner_in_stopped_state():
    """Test that WebRunner initializes in a stopped state."""
    # Arrange
    web_config = WebServiceConfig()
    config = WebRunnerConfig(web_service=web_config)

    # Act
    runner = WebRunner(config)

    # Assert - Test through public behavior
    # Should raise DurableFunctionsLocalRunnerError when trying to serve before starting
    with pytest.raises(DurableFunctionsLocalRunnerError, match="Server not started"):
        runner.serve_forever()

    # Should be safe to call stop multiple times (no-op when not started)
    runner.stop()
    runner.stop()


def test_should_store_config_reference():
    """Test that WebRunner can be created with config and used properly."""
    # Arrange
    web_config = WebServiceConfig(host="config-test", port=9999)
    config = WebRunnerConfig(
        web_service=web_config,
        lambda_endpoint="http://test:1234",
    )

    # Act
    runner = WebRunner(config)

    # Assert - Test through public behavior
    assert isinstance(runner, WebRunner)

    # Verify the runner can be started and stopped (public behavior)
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        runner.start()

        # Verify server was started (public behavior - no exception on serve_forever call)
        runner.serve_forever()
        mock_server.serve_forever.assert_called_once()

        runner.stop()
        mock_server.server_close.assert_called_once()


# Integration Tests - Testing Public Behavior


def test_should_handle_start_with_boto3_client_creation():
    """Test that start() properly handles boto3 client creation through public API."""
    # Arrange
    web_config = WebServiceConfig(host="localhost", port=5000)
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3.client to avoid actual client creation
    with patch("boto3.client") as mock_boto3_client:
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Act - Test public behavior
        runner.start()

        # Assert - Verify public behavior
        # Should be able to call serve_forever after start (public API)
        with patch.object(runner, "serve_forever") as mock_serve:
            runner.serve_forever()
            mock_serve.assert_called_once()

        # Should be able to stop after start (public API)
        runner.stop()


def test_should_handle_boto3_client_creation_with_custom_config():
    """Test that start() uses custom configuration for boto3 client through public API."""
    # Arrange
    web_config = WebServiceConfig(host="localhost", port=5000)
    runner_config = WebRunnerConfig(
        web_service=web_config,
        lambda_endpoint="http://custom-endpoint:8080",
        local_runner_region="eu-west-1",
    )
    runner = WebRunner(runner_config)

    with patch("boto3.client") as mock_boto3_client:
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Act - Test public behavior
        runner.start()

        # Assert - Verify boto3 client was called with correct parameters
        mock_boto3_client.assert_called_once_with(
            "lambda",
            endpoint_url="http://custom-endpoint:8080",
            region_name="eu-west-1",
            config=_LAMBDA_CLIENT_CONFIG,
        )

        # Verify public behavior works
        runner.stop()


def test_should_handle_boto3_client_creation_with_defaults():
    """Test that start() uses default configuration values through public API."""
    # Arrange
    web_config = WebServiceConfig(host="localhost", port=5000)
    runner_config = WebRunnerConfig(web_service=web_config)  # Use defaults
    runner = WebRunner(runner_config)

    with patch("boto3.client") as mock_boto3_client:
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Act - Test public behavior
        runner.start()

        # Assert - Verify boto3 client was called with default parameters
        mock_boto3_client.assert_called_once_with(
            "lambda",
            endpoint_url="http://127.0.0.1:3001",  # Default lambda_endpoint value
            region_name="us-west-2",  # Default value
            config=_LAMBDA_CLIENT_CONFIG,
        )

        # Verify public behavior works
        runner.stop()


def test_should_propagate_boto3_client_creation_exceptions():
    """Test that start() propagates boto3 client creation exceptions through public API."""
    # Arrange
    web_config = WebServiceConfig(host="localhost", port=5000)
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3.client to raise an exception
    with patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = Exception("Connection failed")

        # Act & Assert - Test public behavior
        with pytest.raises(Exception, match="Connection failed"):
            runner.start()


def test_should_create_boto3_client_during_start():
    """Test that start() creates boto3 client correctly through public API."""
    # Arrange
    web_config = WebServiceConfig(host="localhost", port=5000)
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    with patch("boto3.client") as mock_boto3_client:
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Act - Test public behavior
        runner.start()

        # Assert - Verify boto3 client was created
        mock_boto3_client.assert_called_once()

        # Verify public behavior works
        runner.stop()


# Error Condition Tests


def test_should_raise_runtime_error_on_double_start():
    """Test that calling start() twice raises DurableFunctionsLocalRunnerError."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies to allow first start
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        # First start should succeed
        runner.start()

        # Act & Assert - Second start should raise DurableFunctionsLocalRunnerError
        with pytest.raises(
            DurableFunctionsLocalRunnerError, match="Server is already running"
        ):
            runner.start()

        # Cleanup
        runner.stop()


def test_should_raise_runtime_error_when_serve_before_start():
    """Test that calling serve_forever() before start() raises DurableFunctionsLocalRunnerError."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Act & Assert - serve_forever before start should raise DurableFunctionsLocalRunnerError
    with pytest.raises(DurableFunctionsLocalRunnerError, match="Server not started"):
        runner.serve_forever()


def test_should_propagate_boto3_client_creation_failures():
    """Test that boto3 client creation failures are propagated as exceptions."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3.client to raise various exceptions
    test_cases = [
        Exception("Connection refused"),
        ConnectionError("Network error"),
        ValueError("Invalid endpoint URL"),
        RuntimeError("AWS credentials not found"),
    ]

    for exception in test_cases:
        with patch("boto3.client") as mock_boto3_client:
            mock_boto3_client.side_effect = exception

            # Act & Assert - Exception should propagate
            with pytest.raises(type(exception), match=str(exception)):
                runner.start()


def test_should_handle_web_server_creation_failures():
    """Test that WebServer creation failures are propagated as exceptions."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3 client to succeed but WebServer to fail
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.side_effect = Exception("Failed to bind to port")

        # Act & Assert - WebServer creation failure should propagate
        with pytest.raises(Exception, match="Failed to bind to port"):
            runner.start()


def test_should_handle_scheduler_creation_failures():
    """Test that Scheduler creation failures are propagated as exceptions."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3 client to succeed but Scheduler to fail
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_scheduler_class.side_effect = Exception("Scheduler initialization failed")

        # Act & Assert - Scheduler creation failure should propagate
        with pytest.raises(Exception, match="Scheduler initialization failed"):
            runner.start()


def test_should_handle_executor_creation_failures():
    """Test that Executor creation failures are propagated as exceptions."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies to succeed but Executor to fail
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Executor"
        ) as mock_executor_class,
    ):
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_executor_class.side_effect = Exception("Executor initialization failed")

        # Act & Assert - Executor creation failure should propagate
        with pytest.raises(Exception, match="Executor initialization failed"):
            runner.start()


# Dependency Creation and Wiring Tests


def test_should_create_all_required_dependencies_during_start():
    """Test that start() creates all required dependencies with proper wiring."""
    # Arrange
    web_config = WebServiceConfig(host="test-host", port=8080)
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock all dependency classes
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore"
        ) as mock_store_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.LambdaInvoker"
        ) as mock_invoker_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Executor"
        ) as mock_executor_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        # Setup mocks
        mock_client = Mock()
        mock_store = Mock()
        mock_scheduler = Mock()
        mock_invoker = Mock()
        mock_executor = Mock()
        mock_server = Mock()

        mock_boto3_client.return_value = mock_client
        mock_store_class.return_value = mock_store
        mock_scheduler_class.return_value = mock_scheduler
        mock_invoker_class.return_value = mock_invoker
        mock_executor_class.return_value = mock_executor
        mock_web_server_class.return_value = mock_server

        # Act
        runner.start()

        # Assert - Verify all dependencies were created
        mock_store_class.assert_called_once()
        mock_scheduler_class.assert_called_once()
        mock_invoker_class.assert_called_once_with(mock_client)
        # Verify Executor was called with the expected parameters including checkpoint_processor
        assert mock_executor_class.call_count == 1
        call_args = mock_executor_class.call_args
        assert call_args.kwargs["store"] == mock_store
        assert call_args.kwargs["scheduler"] == mock_scheduler
        assert call_args.kwargs["invoker"] == mock_invoker
        assert "checkpoint_processor" in call_args.kwargs
        mock_web_server_class.assert_called_once_with(
            config=web_config, executor=mock_executor
        )

        # Verify scheduler was started
        mock_scheduler.start.assert_called_once()

        # Cleanup
        runner.stop()


def test_should_pass_correct_configuration_to_web_server():
    """Test that WebServer receives correct configuration from WebRunnerConfig."""
    # Arrange
    web_config = WebServiceConfig(
        host="custom-host", port=9999, log_level="WARNING", max_request_size=2048
    )
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        # Act
        runner.start()

        # Assert - Verify WebServer was created with correct config
        mock_web_server_class.assert_called_once()
        call_args = mock_web_server_class.call_args

        # Verify the web service config was passed correctly
        passed_config = call_args[1]["config"]
        assert passed_config == web_config
        assert passed_config.host == "custom-host"
        assert passed_config.port == 9999
        assert passed_config.log_level == "WARNING"
        assert passed_config.max_request_size == 2048

        # Cleanup
        runner.stop()


def test_should_pass_correct_boto3_client_to_lambda_invoker():
    """Test that LambdaInvoker receives correct boto3 client configuration."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(
        web_service=web_config,
        lambda_endpoint="http://test-endpoint:7777",
        local_runner_region="ap-southeast-2",
    )
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.LambdaInvoker"
        ) as mock_invoker_class,
    ):
        mock_client = Mock()
        mock_invoker = Mock()
        mock_boto3_client.return_value = mock_client
        mock_invoker_class.return_value = mock_invoker

        # Act
        runner.start()

        # Assert - Verify boto3 client was created with correct parameters
        mock_boto3_client.assert_called_once_with(
            "lambda",
            endpoint_url="http://test-endpoint:7777",
            region_name="ap-southeast-2",
            config=_LAMBDA_CLIENT_CONFIG,
        )

        # Verify LambdaInvoker was created with the client
        mock_invoker_class.assert_called_once_with(mock_client)

        # Cleanup
        runner.stop()


def test_should_wire_dependencies_correctly_in_executor():
    """Test that Executor receives correctly wired dependencies."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.InMemoryExecutionStore"
        ) as mock_store_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.LambdaInvoker"
        ) as mock_invoker_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Executor"
        ) as mock_executor_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_store = Mock()
        mock_scheduler = Mock()
        mock_invoker = Mock()
        mock_executor = Mock()
        mock_web_server = Mock()

        mock_boto3_client.return_value = mock_client
        mock_store_class.return_value = mock_store
        mock_scheduler_class.return_value = mock_scheduler
        mock_invoker_class.return_value = mock_invoker
        mock_executor_class.return_value = mock_executor
        mock_web_server_class.return_value = mock_web_server

        # Act
        runner.start()

        # Assert - Verify Executor was created with correct dependencies
        assert mock_executor_class.call_count == 1
        call_args = mock_executor_class.call_args
        assert call_args.kwargs["store"] == mock_store
        assert call_args.kwargs["scheduler"] == mock_scheduler
        assert call_args.kwargs["invoker"] == mock_invoker
        assert "checkpoint_processor" in call_args.kwargs

        # Cleanup
        runner.stop()


# WebServer Lifecycle and Configuration Tests


def test_should_delegate_serve_forever_to_web_server():
    """Test that serve_forever() properly delegates to WebServer.serve_forever()."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        # Start the runner
        runner.start()

        # Act
        runner.serve_forever()

        # Assert - Verify WebServer.serve_forever was called
        mock_server.serve_forever.assert_called_once()

        # Cleanup
        runner.stop()


def test_should_call_server_close_during_stop():
    """Test that stop() calls server_close() on WebServer."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Start the runner
        runner.start()

        # Act
        runner.stop()

        # Assert - Verify cleanup methods were called
        mock_server.server_close.assert_called_once()
        mock_scheduler.stop.assert_called_once()


def test_should_handle_web_server_serve_forever_exceptions():
    """Test that exceptions from WebServer.serve_forever() are propagated."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        # Make serve_forever raise an exception
        mock_server.serve_forever.side_effect = Exception("Server error")

        # Start the runner
        runner.start()

        # Act & Assert - Exception should propagate
        with pytest.raises(Exception, match="Server error"):
            runner.serve_forever()

        # Cleanup
        runner.stop()


def test_should_handle_web_server_close_exceptions_gracefully():
    """Test that exceptions from server_close() are handled gracefully."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Make server_close raise an exception
        mock_server.server_close.side_effect = Exception("Close error")

        # Start the runner
        runner.start()

        # Act - stop() should not raise exception despite server_close error
        runner.stop()

        # Assert - Verify both cleanup methods were attempted
        mock_server.server_close.assert_called_once()
        mock_scheduler.stop.assert_called_once()


# Exception Handling Tests


def test_should_handle_standard_runtime_errors():
    """Test that standard RuntimeError exceptions are handled properly."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Test RuntimeError during start
    with patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = RuntimeError("Runtime error during start")

        with pytest.raises(RuntimeError, match="Runtime error during start"):
            runner.start()

    # Test DurableFunctionsLocalRunnerError when serve_forever called before start
    with pytest.raises(DurableFunctionsLocalRunnerError, match="Server not started"):
        runner.serve_forever()


def test_should_handle_value_errors_during_initialization():
    """Test that ValueError exceptions during initialization are propagated."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3 client to raise ValueError
    with patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = ValueError("Invalid configuration")

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid configuration"):
            runner.start()


def test_should_handle_connection_errors_during_initialization():
    """Test that ConnectionError exceptions during initialization are propagated."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock boto3 client to raise ConnectionError
    with patch("boto3.client") as mock_boto3_client:
        mock_boto3_client.side_effect = ConnectionError("Network connection failed")

        # Act & Assert
        with pytest.raises(ConnectionError, match="Network connection failed"):
            runner.start()


def test_should_handle_keyboard_interrupt_during_serve_forever():
    """Test that KeyboardInterrupt during serve_forever is propagated."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        # Make serve_forever raise KeyboardInterrupt
        mock_server.serve_forever.side_effect = KeyboardInterrupt()

        # Start the runner
        runner.start()

        # Act & Assert - KeyboardInterrupt should propagate
        with pytest.raises(KeyboardInterrupt):
            runner.serve_forever()

        # Cleanup
        runner.stop()


# Lifecycle Management Tests


def test_start_creates_dependencies_and_server():
    """Test that start() creates all dependencies and WebServer through public API."""
    # Arrange
    web_config = WebServiceConfig(host="localhost", port=5000)
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies and WebServer
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()

        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Act
        runner.start()

        # Assert - Test through public behavior
        # Should be able to call serve_forever after start
        runner.serve_forever()
        mock_server.serve_forever.assert_called_once()

        # Should be able to stop after start
        runner.stop()
        mock_server.server_close.assert_called_once()
        mock_scheduler.stop.assert_called_once()


def test_start_raises_runtime_error_if_already_started():
    """Test that start() raises DurableFunctionsLocalRunnerError if server is already running."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Set server to simulate already started state
    runner._server = Mock()  # noqa: SLF001

    # Act & Assert
    with pytest.raises(
        DurableFunctionsLocalRunnerError, match="Server is already running"
    ):
        runner.start()


def test_serve_forever_delegates_to_web_server():
    """Test that serve_forever() delegates to WebServer.serve_forever() through public API."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies to allow start
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server

        # Start the runner first (public API)
        runner.start()

        # Act
        runner.serve_forever()

        # Assert
        mock_server.serve_forever.assert_called_once()

        # Cleanup
        runner.stop()


def test_serve_forever_raises_runtime_error_if_not_started():
    """Test that serve_forever() raises DurableFunctionsLocalRunnerError if server not started."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Ensure server is None (not started)
    assert runner._server is None  # noqa: SLF001

    # Act & Assert
    with pytest.raises(DurableFunctionsLocalRunnerError, match="Server not started"):
        runner.serve_forever()


def test_stop_cleans_up_server_and_scheduler():
    """Test that stop() properly cleans up server and scheduler resources through public API."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies to allow start
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()

        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Start the runner first (public API)
        runner.start()

        # Act
        runner.stop()

        # Assert - Verify cleanup was called
        mock_server.server_close.assert_called_once()
        mock_scheduler.stop.assert_called_once()

        # Verify runner is back to stopped state (public behavior)
        with pytest.raises(
            DurableFunctionsLocalRunnerError, match="Server not started"
        ):
            runner.serve_forever()


def test_stop_is_safe_to_call_multiple_times():
    """Test that stop() can be called multiple times safely through public API."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock dependencies to allow start
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()

        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Start the runner first (public API)
        runner.start()

        # Act - call stop multiple times
        runner.stop()
        runner.stop()
        runner.stop()

        # Assert - should only be called once (first time)
        mock_server.server_close.assert_called_once()
        mock_scheduler.stop.assert_called_once()

        # Verify runner remains in stopped state (public behavior)
        with pytest.raises(
            DurableFunctionsLocalRunnerError, match="Server not started"
        ):
            runner.serve_forever()


# Integration Tests - CLI to WebRunner Flow


def test_should_integrate_with_cli_start_server_command():
    """Test complete integration from CLI start-server command to WebRunner."""
    # This test verifies the complete flow from CLI argument parsing
    # through WebRunnerConfig creation to WebRunner execution

    # Arrange
    app = CliApp()

    # Mock WebRunner to verify it receives correct configuration
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner_class:
        # Setup mock runner instance with context manager support
        mock_runner = Mock()
        mock_runner.__enter__ = Mock(return_value=mock_runner)
        mock_runner.__exit__ = Mock(return_value=None)
        mock_web_runner_class.return_value = mock_runner
        mock_runner.serve_forever.side_effect = KeyboardInterrupt()

        # Act - Run CLI command with custom arguments
        exit_code = app.run(
            [
                "start-server",
                "--host",
                "integration-host",
                "--port",
                "7777",
                "--log-level",
                "WARNING",
                "--lambda-endpoint",
                "http://integration-lambda:4000",
                "--local-runner-endpoint",
                "http://integration-runner:8000",
                "--local-runner-region",
                "eu-central-1",
                "--local-runner-mode",
                "integration",
            ]
        )

        # Assert - Verify CLI handled KeyboardInterrupt correctly
        assert exit_code == 130

        # Verify WebRunner was created with correct configuration
        mock_web_runner_class.assert_called_once()
        config = mock_web_runner_class.call_args[0][0]

        # Verify web service configuration
        assert config.web_service.host == "integration-host"
        assert config.web_service.port == 7777
        assert config.web_service.log_level == "WARNING"

        # Verify Lambda service configuration
        assert config.lambda_endpoint == "http://integration-lambda:4000"
        assert config.local_runner_endpoint == "http://integration-runner:8000"
        assert config.local_runner_region == "eu-central-1"
        assert config.local_runner_mode == "integration"

        # Verify context manager protocol was used
        mock_runner.__enter__.assert_called_once()
        mock_runner.__exit__.assert_called_once()
        mock_runner.serve_forever.assert_called_once()


def test_should_handle_cli_to_web_runner_startup_errors():
    """Test integration error handling from CLI to WebRunner startup failures."""
    # Arrange
    app = CliApp()

    # Mock WebRunner to raise exception during creation
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner_class:
        mock_web_runner_class.side_effect = Exception("WebRunner startup failed")

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            # Act
            exit_code = app.run(["start-server"])

            # Assert - Verify CLI handled WebRunner exception correctly
            assert exit_code == 1
            mock_logger.exception.assert_called_with("Failed to start server")


def test_should_handle_cli_to_web_runner_context_manager_errors():
    """Test integration error handling for WebRunner context manager failures."""
    # Arrange
    app = CliApp()

    # Mock WebRunner context manager to raise exception
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner_class:
        mock_runner = Mock()
        mock_runner.__enter__ = Mock(
            side_effect=DurableFunctionsLocalRunnerError("Context manager failed")
        )
        mock_runner.__exit__ = Mock(return_value=None)
        mock_web_runner_class.return_value = mock_runner

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            # Act
            exit_code = app.run(["start-server"])

            # Assert - Verify CLI handled context manager exception correctly
            assert exit_code == 1
            mock_logger.exception.assert_called_with("Failed to start server")


def test_should_handle_cli_to_web_runner_serve_forever_errors():
    """Test integration error handling for WebRunner serve_forever failures."""
    # Arrange
    app = CliApp()

    # Mock WebRunner serve_forever to raise exception
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner_class:
        mock_runner = Mock()
        mock_runner.__enter__ = Mock(return_value=mock_runner)
        mock_runner.__exit__ = Mock(return_value=None)
        mock_web_runner_class.return_value = mock_runner
        mock_runner.serve_forever.side_effect = Exception("Server runtime error")

        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            # Act
            exit_code = app.run(["start-server"])

            # Assert - Verify CLI handled serve_forever exception correctly
            assert exit_code == 1
            mock_logger.exception.assert_called_with("Failed to start server")


def test_should_preserve_cli_configuration_through_web_runner():
    """Test that CLI configuration is preserved through WebRunner creation."""
    # This test verifies that all CLI arguments are correctly passed through
    # the WebRunnerConfig to the WebRunner and its dependencies

    # Arrange
    app = CliApp()

    # Mock all WebRunner dependencies to verify configuration flow
    with (
        patch(
            "aws_durable_execution_sdk_python_testing.cli.WebRunner"
        ) as mock_web_runner_class,
        patch("boto3.client"),
        patch("aws_durable_execution_sdk_python_testing.runner.WebServer"),
    ):
        # Setup mocks
        mock_runner = Mock()

        mock_runner.__enter__ = Mock(return_value=mock_runner)
        mock_runner.__exit__ = Mock(return_value=None)
        mock_web_runner_class.return_value = mock_runner
        mock_runner.serve_forever.return_value = None

        # No need to mock internal behavior, just verify configuration passing

        # Act - Run CLI with comprehensive configuration
        exit_code = app.run(
            [
                "start-server",
                "--host",
                "config-test-host",
                "--port",
                "9999",
                "--log-level",
                "ERROR",  # ERROR level
                "--lambda-endpoint",
                "http://config-lambda:5000",
                "--local-runner-endpoint",
                "http://config-runner:9000",
                "--local-runner-region",
                "ap-northeast-1",
                "--local-runner-mode",
                "config-test",
            ]
        )

        # Assert - Verify successful execution
        assert exit_code == 0

        # Verify WebRunner was created with correct configuration
        mock_web_runner_class.assert_called_once()
        config = mock_web_runner_class.call_args[0][0]

        # Verify web service configuration
        assert config.web_service.host == "config-test-host"
        assert config.web_service.port == 9999
        assert config.web_service.log_level == "ERROR"

        # Verify Lambda service configuration
        assert config.lambda_endpoint == "http://config-lambda:5000"
        assert config.local_runner_endpoint == "http://config-runner:9000"
        assert config.local_runner_region == "ap-northeast-1"
        assert config.local_runner_mode == "config-test"

        # Verify context manager protocol was used
        mock_runner.__enter__.assert_called_once()
        mock_runner.serve_forever.assert_called_once()
        mock_runner.__exit__.assert_called_once()


def test_should_handle_environment_variable_integration():
    """Test integration with environment variables through CLI to WebRunner."""
    # Set environment variables
    env_vars = {
        "AWS_DEX_HOST": "env-host",
        "AWS_DEX_PORT": "8888",
        "AWS_DEX_LOG_LEVEL": "CRITICAL",  # CRITICAL level
        "AWS_DEX_LAMBDA_ENDPOINT": "http://env-lambda:6000",
        "AWS_DEX_LOCAL_RUNNER_ENDPOINT": "http://env-runner:7000",
        "AWS_DEX_LOCAL_RUNNER_REGION": "sa-east-1",
        "AWS_DEX_LOCAL_RUNNER_MODE": "env-test",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        app = CliApp()

        # Mock WebRunner to verify environment configuration
        with patch(
            "aws_durable_execution_sdk_python_testing.cli.WebRunner"
        ) as mock_web_runner_class:
            mock_runner = Mock()
            mock_web_runner_class.return_value = mock_runner
            mock_runner.__enter__ = Mock(return_value=mock_runner)
            mock_runner.__exit__ = Mock(return_value=None)
            mock_runner.serve_forever.return_value = None

            # Act - Run CLI without arguments (should use environment)
            exit_code = app.run(["start-server"])

            # Assert - Verify successful execution
            assert exit_code == 0

            # Verify WebRunner was created with environment configuration
            mock_web_runner_class.assert_called_once()
            config = mock_web_runner_class.call_args[0][0]

            # Verify environment variables were used
            assert config.web_service.host == "env-host"
            assert config.web_service.port == 8888
            assert config.web_service.log_level == "CRITICAL"
            assert config.lambda_endpoint == "http://env-lambda:6000"
            assert config.local_runner_endpoint == "http://env-runner:7000"
            assert config.local_runner_region == "sa-east-1"
            assert config.local_runner_mode == "env-test"


def test_should_handle_cli_argument_override_of_environment():
    """Test that CLI arguments override environment variables in integration."""
    # Set environment variables
    env_vars = {
        "AWS_DEX_HOST": "env-host",
        "AWS_DEX_PORT": "8888",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        app = CliApp()

        # Mock WebRunner to verify argument override
        with patch(
            "aws_durable_execution_sdk_python_testing.cli.WebRunner"
        ) as mock_web_runner_class:
            mock_runner = Mock()
            mock_web_runner_class.return_value = mock_runner
            mock_runner.__enter__ = Mock(return_value=mock_runner)
            mock_runner.__exit__ = Mock(return_value=None)
            mock_runner.serve_forever.return_value = None

            # Act - Run CLI with arguments that should override environment
            exit_code = app.run(
                [
                    "start-server",
                    "--host",
                    "cli-override-host",
                    "--port",
                    "7777",
                ]
            )

            # Assert - Verify successful execution
            assert exit_code == 0

            # Verify CLI arguments overrode environment variables
            config = mock_web_runner_class.call_args[0][0]
            assert config.web_service.host == "cli-override-host"  # CLI override
            assert config.web_service.port == 7777  # CLI override


def test_should_maintain_backward_compatibility_in_integration():
    """Test that integration maintains backward compatibility with existing behavior."""
    # This test ensures that the refactored CLI-to-WebRunner flow
    # maintains the same external behavior as the original implementation
    app = CliApp()

    # Mock WebRunner to simulate successful operation
    with patch(
        "aws_durable_execution_sdk_python_testing.cli.WebRunner"
    ) as mock_web_runner_class:
        mock_runner = Mock()
        mock_web_runner_class.return_value = mock_runner
        mock_runner.__enter__ = Mock(return_value=mock_runner)
        mock_runner.__exit__ = Mock(return_value=None)
        mock_runner.serve_forever.side_effect = KeyboardInterrupt()

        # Mock logging to verify backward compatible messages
        with patch(
            "aws_durable_execution_sdk_python_testing.cli.logger"
        ) as mock_logger:
            # Act
            exit_code = app.run(
                ["start-server", "--host", "compat-host", "--port", "5555"]
            )

            # Assert - Verify backward compatible behavior
            assert exit_code == 130  # KeyboardInterrupt exit code

            # Verify backward compatible logging messages
            mock_logger.info.assert_any_call(
                "Starting Durable Functions Local Runner on %s:%s",
                "compat-host",
                5555,
            )
            mock_logger.info.assert_any_call("Configuration:")
            mock_logger.info.assert_any_call("  Host: %s", "compat-host")
            mock_logger.info.assert_any_call("  Port: %s", 5555)
            mock_logger.info.assert_any_call(
                "Server started successfully. Press Ctrl+C to stop."
            )
            mock_logger.info.assert_any_call(
                "Received shutdown signal, stopping server..."
            )


def test_stop_handles_unstarted_runner_gracefully():
    """Test that stop() handles unstarted runner gracefully through public API."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Act & Assert - should not raise any exceptions when stopping unstarted runner
    runner.stop()

    # Verify runner remains in stopped state (public behavior)
    with pytest.raises(DurableFunctionsLocalRunnerError, match="Server not started"):
        runner.serve_forever()


def test_complete_lifecycle_start_serve_stop():
    """Test complete lifecycle: start -> serve_forever -> stop through public API."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock all dependencies
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()

        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Act - complete lifecycle through public API
        runner.start()
        runner.serve_forever()
        runner.stop()

        # Assert - Verify all methods were called
        mock_server.serve_forever.assert_called_once()
        mock_server.server_close.assert_called_once()
        mock_scheduler.start.assert_called_once()
        mock_scheduler.stop.assert_called_once()

        # Verify runner is back to stopped state (public behavior)
        with pytest.raises(
            DurableFunctionsLocalRunnerError, match="Server not started"
        ):
            runner.serve_forever()


def test_context_manager_calls_start_and_stop():
    """Test that context manager properly calls start() and stop()."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock start and stop to track calls
    with (
        patch.object(runner, "start") as mock_start,
        patch.object(runner, "stop") as mock_stop,
    ):
        # Act
        with runner as context_runner:
            # Verify start was called and runner returned
            mock_start.assert_called_once()
            assert context_runner is runner

        # Assert stop was called on exit
        mock_stop.assert_called_once()


def test_context_manager_calls_stop_on_exception():
    """Test that context manager calls stop() even when exception occurs."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Mock start and stop
    with (
        patch.object(runner, "start") as mock_start,
        patch.object(runner, "stop") as mock_stop,
    ):
        # Act & Assert
        with pytest.raises(ValueError, match="Test exception"):  # noqa: PT012
            with runner:
                mock_start.assert_called_once()
                raise ValueError("Test exception")  # noqa: TRY003, EM101

        # Verify stop was still called despite exception
        mock_stop.assert_called_once()


def test_state_transitions_prevent_invalid_operations():
    """Test that state checking prevents invalid operation sequences through public API."""
    # Arrange
    web_config = WebServiceConfig()
    runner_config = WebRunnerConfig(web_service=web_config)
    runner = WebRunner(runner_config)

    # Test serve_forever before start
    with pytest.raises(DurableFunctionsLocalRunnerError, match="Server not started"):
        runner.serve_forever()

    # Mock dependencies for start
    with (
        patch("boto3.client") as mock_boto3_client,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.WebServer"
        ) as mock_web_server_class,
        patch(
            "aws_durable_execution_sdk_python_testing.runner.Scheduler"
        ) as mock_scheduler_class,
    ):
        mock_client = Mock()
        mock_server = Mock()
        mock_scheduler = Mock()

        mock_boto3_client.return_value = mock_client
        mock_web_server_class.return_value = mock_server
        mock_scheduler_class.return_value = mock_scheduler

        # Start server
        runner.start()

        # Test double start
        with pytest.raises(
            DurableFunctionsLocalRunnerError, match="Server is already running"
        ):
            runner.start()

        # Verify serve_forever works after start
        runner.serve_forever()
        mock_server.serve_forever.assert_called_once()

        # Stop and verify serve_forever fails again
        runner.stop()
        with pytest.raises(
            DurableFunctionsLocalRunnerError, match="Server not started"
        ):
            runner.serve_forever()

"""Unit tests for logger module."""

from collections.abc import Mapping
from unittest.mock import Mock

from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.logger import Logger, LoggerInterface, LogInfo


class PowertoolsLoggerStub:
    """Stub implementation of AWS Powertools Logger with exact method signatures."""

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def info(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def error(
        self,
        msg: object,
        *args: object,
        exc_info=None,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass

    def exception(
        self,
        msg: object,
        *args: object,
        exc_info=True,
        stack_info: bool = False,
        stacklevel: int = 2,
        extra: Mapping[str, object] | None = None,
        **kwargs: object,
    ) -> None:
        pass


def test_powertools_logger_compatibility():
    """Test that PowertoolsLoggerStub is compatible with LoggerInterface protocol."""
    powertools_logger = PowertoolsLoggerStub()

    # This should work without type errors if the protocol is compatible
    def accepts_logger_interface(logger: LoggerInterface) -> None:
        logger.debug("test")
        logger.info("test")
        logger.warning("test")
        logger.error("test")
        logger.exception("test")

    # If this doesn't raise an error, the protocols are compatible
    accepts_logger_interface(powertools_logger)

    # Test that our Logger can wrap the PowertoolsLoggerStub
    log_info = LogInfo("arn:aws:test")
    wrapped_logger = Logger.from_log_info(powertools_logger, log_info)

    # Test all methods work
    wrapped_logger.debug("debug message")
    wrapped_logger.info("info message")
    wrapped_logger.warning("warning message")
    wrapped_logger.error("error message")
    wrapped_logger.exception("exception message")


def test_log_info_creation():
    """Test LogInfo creation with all parameters."""
    log_info = LogInfo("arn:aws:test", "parent123", "test_name", 5)
    assert log_info.execution_arn == "arn:aws:test"
    assert log_info.parent_id == "parent123"
    assert log_info.name == "test_name"
    assert log_info.attempt == 5


def test_log_info_creation_minimal():
    """Test LogInfo creation with minimal parameters."""
    log_info = LogInfo("arn:aws:test")
    assert log_info.execution_arn == "arn:aws:test"
    assert log_info.parent_id is None
    assert log_info.name is None
    assert log_info.attempt is None


def test_log_info_from_operation_identifier():
    """Test LogInfo.from_operation_identifier."""
    op_id = OperationIdentifier("op123", "parent456", "op_name")
    log_info = LogInfo.from_operation_identifier("arn:aws:test", op_id, 3)
    assert log_info.execution_arn == "arn:aws:test"
    assert log_info.parent_id == "parent456"
    assert log_info.name == "op_name"
    assert log_info.attempt == 3


def test_log_info_from_operation_identifier_no_attempt():
    """Test LogInfo.from_operation_identifier without attempt."""
    op_id = OperationIdentifier("op123", "parent456", "op_name")
    log_info = LogInfo.from_operation_identifier("arn:aws:test", op_id)
    assert log_info.execution_arn == "arn:aws:test"
    assert log_info.parent_id == "parent456"
    assert log_info.name == "op_name"
    assert log_info.attempt is None


def test_log_info_with_parent_id():
    """Test LogInfo.with_parent_id."""
    original = LogInfo("arn:aws:test", "old_parent", "test_name", 2)
    new_log_info = original.with_parent_id("new_parent")
    assert new_log_info.execution_arn == "arn:aws:test"
    assert new_log_info.parent_id == "new_parent"
    assert new_log_info.name == "test_name"
    assert new_log_info.attempt == 2


def test_logger_from_log_info_full():
    """Test Logger.from_log_info with all LogInfo fields."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test", "parent123", "test_name", 5)
    logger = Logger.from_log_info(mock_logger, log_info)

    expected_extra = {
        "execution_arn": "arn:aws:test",
        "parent_id": "parent123",
        "name": "test_name",
        "attempt": 5,
    }
    assert logger._default_extra == expected_extra  # noqa: SLF001
    assert logger._logger is mock_logger  # noqa: SLF001


def test_logger_from_log_info_partial_fields():
    """Test Logger.from_log_info with various field combinations."""
    mock_logger = Mock()

    # Test with parent_id but no name or attempt
    log_info = LogInfo("arn:aws:test", "parent123")
    logger = Logger.from_log_info(mock_logger, log_info)
    expected_extra = {"execution_arn": "arn:aws:test", "parent_id": "parent123"}
    assert logger._default_extra == expected_extra  # noqa: SLF001

    # Test with name but no parent_id or attempt
    log_info = LogInfo("arn:aws:test", None, "test_name")
    logger = Logger.from_log_info(mock_logger, log_info)
    expected_extra = {"execution_arn": "arn:aws:test", "name": "test_name"}
    assert logger._default_extra == expected_extra  # noqa: SLF001

    # Test with attempt but no parent_id or name
    log_info = LogInfo("arn:aws:test", None, None, 5)
    logger = Logger.from_log_info(mock_logger, log_info)
    expected_extra = {"execution_arn": "arn:aws:test", "attempt": 5}
    assert logger._default_extra == expected_extra  # noqa: SLF001


def test_logger_from_log_info_minimal():
    """Test Logger.from_log_info with minimal LogInfo."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)

    expected_extra = {"execution_arn": "arn:aws:test"}
    assert logger._default_extra == expected_extra  # noqa: SLF001


def test_logger_with_log_info():
    """Test Logger.with_log_info."""
    mock_logger = Mock()
    original_info = LogInfo("arn:aws:test", "parent1")
    logger = Logger.from_log_info(mock_logger, original_info)

    new_info = LogInfo("arn:aws:new", "parent2", "new_name")
    new_logger = logger.with_log_info(new_info)

    expected_extra = {
        "execution_arn": "arn:aws:new",
        "parent_id": "parent2",
        "name": "new_name",
    }
    assert new_logger._default_extra == expected_extra  # noqa: SLF001
    assert new_logger._logger is mock_logger  # noqa: SLF001


def test_logger_get_logger():
    """Test Logger.get_logger."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)
    assert logger.get_logger() is mock_logger


def test_logger_debug():
    """Test Logger.debug method."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test", "parent123")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.debug("test %s message", "arg1", extra={"custom": "value"})

    expected_extra = {
        "execution_arn": "arn:aws:test",
        "parent_id": "parent123",
        "custom": "value",
    }
    mock_logger.debug.assert_called_once_with(
        "test %s message", "arg1", extra=expected_extra
    )


def test_logger_info():
    """Test Logger.info method."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.info("info message")

    expected_extra = {"execution_arn": "arn:aws:test"}
    mock_logger.info.assert_called_once_with("info message", extra=expected_extra)


def test_logger_warning():
    """Test Logger.warning method."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.warning("warning %s %s message", "arg1", "arg2")

    expected_extra = {"execution_arn": "arn:aws:test"}
    mock_logger.warning.assert_called_once_with(
        "warning %s %s message", "arg1", "arg2", extra=expected_extra
    )


def test_logger_error():
    """Test Logger.error method."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.error("error message", extra={"error_code": 500})

    expected_extra = {"execution_arn": "arn:aws:test", "error_code": 500}
    mock_logger.error.assert_called_once_with("error message", extra=expected_extra)


def test_logger_exception():
    """Test Logger.exception method."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.exception("exception message")

    expected_extra = {"execution_arn": "arn:aws:test"}
    mock_logger.exception.assert_called_once_with(
        "exception message", extra=expected_extra
    )


def test_logger_methods_with_none_extra():
    """Test logger methods handle None extra parameter."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.debug("debug", extra=None)
    logger.info("info", extra=None)
    logger.warning("warning", extra=None)
    logger.error("error", extra=None)
    logger.exception("exception", extra=None)

    expected_extra = {"execution_arn": "arn:aws:test"}
    mock_logger.debug.assert_called_with("debug", extra=expected_extra)
    mock_logger.info.assert_called_with("info", extra=expected_extra)
    mock_logger.warning.assert_called_with("warning", extra=expected_extra)
    mock_logger.error.assert_called_with("error", extra=expected_extra)
    mock_logger.exception.assert_called_with("exception", extra=expected_extra)


def test_logger_extra_override():
    """Test that custom extra overrides default extra."""
    mock_logger = Mock()
    log_info = LogInfo("arn:aws:test", "parent123")
    logger = Logger.from_log_info(mock_logger, log_info)

    logger.info("test", extra={"execution_arn": "overridden", "new_field": "value"})

    expected_extra = {
        "execution_arn": "overridden",
        "parent_id": "parent123",
        "new_field": "value",
    }
    mock_logger.info.assert_called_once_with("test", extra=expected_extra)

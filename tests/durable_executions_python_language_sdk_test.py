"""Tests for DurableExecutionsPythonLanguageSDK module."""


def test_aws_durable_execution_sdk_python_importable():
    """Test aws_durable_execution_sdk_python is importable."""
    import aws_durable_execution_sdk_python  # noqa: PLC0415, F401


def test_version_is_accessible():
    """Test __version__ is accessible from package root."""
    import aws_durable_execution_sdk_python  # noqa: PLC0415

    assert hasattr(aws_durable_execution_sdk_python, "__version__")
    assert isinstance(aws_durable_execution_sdk_python.__version__, str)
    assert len(aws_durable_execution_sdk_python.__version__) > 0

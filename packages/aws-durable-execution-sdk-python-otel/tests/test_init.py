"""Tests for aws_durable_execution_sdk_python_otel package."""

from aws_durable_execution_sdk_python_otel import __version__


def test_version_is_set():
    """Verify the package version is defined."""
    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


def test_version_format():
    """Verify the package version follows semver format."""
    parts = __version__.split(".")
    assert len(parts) == 3
    for part in parts:
        assert part.isdigit()

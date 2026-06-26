"""Integration tests for filesystem serdes examples.

These tests use a temporary directory as the filesystem mount path to verify
that the filesystem serdes correctly writes and reads files during durable
execution. They bypass the standard durable_runner fixture because they need
to control the FILESYSTEM_MOUNT_PATH environment variable before handler import.
"""

import importlib
import json
import os

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.serdes import EXTENDED_TYPES_SERDES
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from test.conftest import deserialize_operation_payload


@pytest.mark.example
def test_filesystem_serdes_basic(tmp_path, monkeypatch):
    """Test basic filesystem serdes in ALWAYS mode.

    Verifies that:
    - The handler completes successfully
    - Step results are written as JSON files to the mounted filesystem
    - Deserialized results are correct
    """
    monkeypatch.setenv("FILESYSTEM_MOUNT_PATH", str(tmp_path))

    from src.filesystem_serdes import filesystem_serdes_basic

    importlib.reload(filesystem_serdes_basic)

    runner = DurableFunctionTestRunner(handler=filesystem_serdes_basic.handler)

    with runner:
        result = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify the workflow completed successfully
    assert result_data["success"] is True
    assert result_data["processed_count"] == 10
    assert len(result_data["user_ids"]) == 10
    assert result_data["user_ids"] == list(range(10))

    # Verify files were actually written to the temp directory
    json_files = []
    for root, dirs, files in os.walk(str(tmp_path)):
        for f in files:
            if f.endswith(".json"):
                json_files.append(os.path.join(root, f))

    # At least 2 files (one per step)
    assert len(json_files) >= 2

    # Verify file contents are valid JSON
    for file_path in json_files:
        with open(file_path) as f:
            data = json.load(f)
            assert data is not None


@pytest.mark.example
def test_filesystem_serdes_overflow(tmp_path, monkeypatch):
    """Test filesystem serdes in OVERFLOW mode.

    Verifies that:
    - Small payloads stay inline (no file written for small step)
    - Large payloads overflow to file
    - The handler returns correct results
    """
    monkeypatch.setenv("FILESYSTEM_MOUNT_PATH", str(tmp_path))

    from src.filesystem_serdes import filesystem_serdes_overflow

    importlib.reload(filesystem_serdes_overflow)

    runner = DurableFunctionTestRunner(handler=filesystem_serdes_overflow.handler)

    with runner:
        result = runner.run(input="test", timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    assert result_data["small_status"] == "ok"
    assert result_data["large_record_count"] == 300

    # In overflow mode, at least the large step should have written a file
    json_files = []
    for root, dirs, files in os.walk(str(tmp_path)):
        for f in files:
            if f.endswith(".json"):
                json_files.append(os.path.join(root, f))

    assert len(json_files) >= 1


@pytest.mark.example
def test_filesystem_serdes_preview(tmp_path, monkeypatch):
    """Test filesystem serdes with preview configuration.

    Verifies that:
    - The handler completes successfully
    - Files are written containing the FULL data (not just preview)
    - The handler returns correct results
    """
    monkeypatch.setenv("FILESYSTEM_MOUNT_PATH", str(tmp_path))

    from src.filesystem_serdes import filesystem_serdes_preview

    importlib.reload(filesystem_serdes_preview)

    runner = DurableFunctionTestRunner(handler=filesystem_serdes_preview.handler)

    with runner:
        result = runner.run(input="test", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    result_data = deserialize_operation_payload(result.result)

    # Verify the order was processed
    assert result_data["order_id"] == "ORD-12345"
    assert result_data["status"] == "completed"
    assert result_data["item_count"] == 2

    # Verify a file was written for the order step
    json_files = []
    for root, dirs, files in os.walk(str(tmp_path)):
        for f in files:
            if f.endswith(".json"):
                json_files.append(os.path.join(root, f))

    assert len(json_files) >= 1

    # Verify the written file contains the full order data (not just preview)
    with open(json_files[0]) as f:
        stored_data = EXTENDED_TYPES_SERDES.deserialize(f.read())
    assert "order_id" in stored_data
    assert "items" in stored_data  # Full data, not truncated preview
    assert "shipping_address" in stored_data

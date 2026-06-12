"""Unit tests for token models."""

import base64
import json

import pytest

from aws_durable_execution_sdk_python_testing.token import (
    CallbackToken,
    CheckpointToken,
)


def test_checkpoint_token_init():
    """Test CheckpointToken initialization."""
    token = CheckpointToken("arn:aws:states:us-east-1:123456789012:execution:test", 42)

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.token_sequence == 42


def test_checkpoint_token_to_str():
    """Test CheckpointToken serialization to string."""
    token = CheckpointToken("arn:aws:states:us-east-1:123456789012:execution:test", 42)

    result = token.to_str()

    # Decode and verify the structure
    decoded = base64.b64decode(result).decode()
    data = json.loads(decoded)
    assert data["arn"] == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert data["seq"] == 42


def test_checkpoint_token_from_str():
    """Test CheckpointToken deserialization from string."""
    data = {"arn": "arn:aws:states:us-east-1:123456789012:execution:test", "seq": 42}
    json_str = json.dumps(data, separators=(",", ":"))
    token_str = base64.b64encode(json_str.encode()).decode()

    token = CheckpointToken.from_str(token_str)

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.token_sequence == 42


def test_checkpoint_token_round_trip():
    """Test CheckpointToken serialization and deserialization round trip."""
    original = CheckpointToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", 123
    )

    token_str = original.to_str()
    restored = CheckpointToken.from_str(token_str)

    assert restored == original


def test_checkpoint_token_frozen_dataclass():
    """Test that CheckpointToken is immutable."""
    token = CheckpointToken("arn:aws:states:us-east-1:123456789012:execution:test", 42)

    with pytest.raises(AttributeError):
        token.execution_arn = "new-arn"

    with pytest.raises(AttributeError):
        token.token_sequence = 999


def test_callback_token_init():
    """Test CallbackToken initialization."""
    token = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "op-123"
    )

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.operation_id == "op-123"


def test_callback_token_to_str():
    """Test CallbackToken serialization to string."""
    token = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "op-123"
    )

    result = token.to_str()

    # Decode and verify the structure
    decoded = base64.b64decode(result).decode()
    data = json.loads(decoded)
    assert data["arn"] == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert data["op"] == "op-123"


def test_callback_token_from_str():
    """Test CallbackToken deserialization from string."""
    data = {
        "arn": "arn:aws:states:us-east-1:123456789012:execution:test",
        "op": "op-123",
    }
    json_str = json.dumps(data, separators=(",", ":"))
    token_str = base64.b64encode(json_str.encode()).decode()

    token = CallbackToken.from_str(token_str)

    assert token.execution_arn == "arn:aws:states:us-east-1:123456789012:execution:test"
    assert token.operation_id == "op-123"


def test_callback_token_round_trip():
    """Test CallbackToken serialization and deserialization round trip."""
    original = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "callback-op"
    )

    token_str = original.to_str()
    restored = CallbackToken.from_str(token_str)

    assert restored == original


def test_callback_token_frozen_dataclass():
    """Test that CallbackToken is immutable."""
    token = CallbackToken(
        "arn:aws:states:us-east-1:123456789012:execution:test", "op-123"
    )

    with pytest.raises(AttributeError):
        token.execution_arn = "new-arn"

    with pytest.raises(AttributeError):
        token.operation_id = "new-op"

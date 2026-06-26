"""Unit tests for filesystem serdes module."""

import hashlib
import json
import os

import pytest

from aws_durable_execution_sdk_python.filesystem_serdes import (
    FileSystemPathEncoding,
    FileSystemSerdesConfig,
    FileSystemSerdesMode,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    _OVERFLOW_THRESHOLD_BYTES,
    _encode_segment,
    _parse_durable_execution_arn,
    _resolve_execution_dir,
    _write_to_file,
    build_preview,
    create_filesystem_serdes,
)
from aws_durable_execution_sdk_python.serdes import SerDesContext

BASE_PATH = "/mnt/s3"
TEST_ARN = "arn:aws:lambda:us-east-1:123456789012:function:test-function:1/durable-execution/test-exec-id/test-invocation-id"
TEST_OPERATION_ID = "step-1"

# A non-durable-execution ARN (fallback path)
NON_DURABLE_ARN = "arn:aws:lambda:us-east-1:123456789012:function:my-func"

MOCK_CONTEXT = SerDesContext(
    operation_id=TEST_OPERATION_ID,
    durable_execution_arn=TEST_ARN,
)

# --- ARN Parsing Tests ---


def test_parse_durable_execution_arn_valid():
    """Test parsing a valid durable execution ARN."""
    result = _parse_durable_execution_arn(TEST_ARN)
    assert result is not None
    function_name, execution_name, invocation_id = result
    assert function_name == "test-function"
    assert execution_name == "test-exec-id"
    assert invocation_id == "test-invocation-id"


def test_parse_durable_execution_arn_with_different_partition():
    """Test parsing ARN with different partition (e.g., aws-cn)."""
    arn = "arn:aws-cn:lambda:cn-north-1:123456789012:function:my-func:3/durable-execution/exec-123/inv-456"
    result = _parse_durable_execution_arn(arn)
    assert result is not None
    assert result == ("my-func", "exec-123", "inv-456")


def test_parse_durable_execution_arn_non_durable():
    """Test parsing a non-durable-execution ARN returns None."""
    result = _parse_durable_execution_arn(NON_DURABLE_ARN)
    assert result is None


def test_parse_durable_execution_arn_empty_string():
    """Test parsing an empty string returns None."""
    result = _parse_durable_execution_arn("")
    assert result is None


def test_parse_durable_execution_arn_random_string():
    """Test parsing a random string returns None."""
    result = _parse_durable_execution_arn("not-an-arn-at-all")
    assert result is None


def test_parse_durable_execution_arn_missing_version():
    """Test parsing an ARN without version qualifier returns None."""
    arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function/durable-execution/exec/inv"
    result = _parse_durable_execution_arn(arn)
    assert result is None


# --- Path Encoding Tests ---


def test_encode_segment_uri_simple():
    """Test URI encoding of a simple string."""
    result = _encode_segment("step-1", FileSystemPathEncoding.URI)
    assert result == "step-1"


def test_encode_segment_uri_with_special_chars():
    """Test URI encoding of a string with special characters."""
    result = _encode_segment("../invoices/2026", FileSystemPathEncoding.URI)
    # Should percent-encode slashes (path separators)
    assert "/" not in result
    # Dots are safe in file names, only slashes cause traversal
    assert "%2F" in result


def test_encode_segment_uri_with_spaces():
    """Test URI encoding of a string with spaces."""
    result = _encode_segment("my step name", FileSystemPathEncoding.URI)
    assert " " not in result
    assert "%20" in result


def test_encode_segment_hash():
    """Test HASH encoding produces SHA-256 hex digest."""
    result = _encode_segment("step-1", FileSystemPathEncoding.HASH)
    expected = hashlib.sha256("step-1".encode()).hexdigest()
    assert result == expected
    assert len(result) == 64


def test_encode_segment_hash_fixed_length():
    """Test HASH encoding produces fixed-length output regardless of input."""
    short_result = _encode_segment("x", FileSystemPathEncoding.HASH)
    long_result = _encode_segment("x" * 10000, FileSystemPathEncoding.HASH)
    assert len(short_result) == 64
    assert len(long_result) == 64


# --- Resolve Execution Dir Tests ---


def test_resolve_execution_dir_uri_with_durable_arn():
    """Test URI mode derives compact directory from ARN parts."""
    result = _resolve_execution_dir(BASE_PATH, TEST_ARN, FileSystemPathEncoding.URI)
    expected = os.path.join(
        BASE_PATH, "test-function", "test-exec-id", "test-invocation-id"
    )
    assert result == expected


def test_resolve_execution_dir_uri_with_non_durable_arn():
    """Test URI mode falls back to encoding the whole ARN for non-durable ARN."""
    result = _resolve_execution_dir(
        BASE_PATH, NON_DURABLE_ARN, FileSystemPathEncoding.URI
    )
    from urllib.parse import quote

    expected = os.path.join(BASE_PATH, quote(NON_DURABLE_ARN, safe=""))
    assert result == expected


def test_resolve_execution_dir_hash():
    """Test HASH mode hashes the whole ARN into a single segment."""
    result = _resolve_execution_dir(BASE_PATH, TEST_ARN, FileSystemPathEncoding.HASH)
    expected = os.path.join(BASE_PATH, hashlib.sha256(TEST_ARN.encode()).hexdigest())
    assert result == expected


# --- Write to File Tests ---


def test_write_to_file_creates_directory_and_writes(tmp_path):
    """Test that _write_to_file creates directory structure and writes JSON."""
    context = SerDesContext(
        operation_id="my-step",
        durable_execution_arn=TEST_ARN,
    )
    value = {"id": 1, "name": "Alice"}

    file_path = _write_to_file(
        str(tmp_path), value, context, FileSystemPathEncoding.URI
    )

    assert os.path.exists(file_path)
    with open(file_path) as f:
        written_data = json.load(f)
    assert written_data == value


def test_write_to_file_with_hash_encoding(tmp_path):
    """Test _write_to_file with HASH encoding."""
    context = SerDesContext(
        operation_id="my-step",
        durable_execution_arn=TEST_ARN,
    )
    value = {"test": "data"}

    file_path = _write_to_file(
        str(tmp_path), value, context, FileSystemPathEncoding.HASH
    )

    assert os.path.exists(file_path)
    # File name should be the hash of operation_id + .json
    file_name = os.path.basename(file_path)
    expected_name = f"{hashlib.sha256('my-step'.encode()).hexdigest()}.json"
    assert file_name == expected_name


def test_write_to_file_unsafe_operation_id(tmp_path):
    """Test _write_to_file with an unsafe operation ID containing path traversal."""
    context = SerDesContext(
        operation_id="../invoices/2026",
        durable_execution_arn=TEST_ARN,
    )
    value = {"id": 1}

    file_path = _write_to_file(
        str(tmp_path), value, context, FileSystemPathEncoding.URI
    )

    assert os.path.exists(file_path)
    # The file should be in the expected directory, not escaped
    dir_path = os.path.dirname(file_path)
    expected_dir = _resolve_execution_dir(
        str(tmp_path), TEST_ARN, FileSystemPathEncoding.URI
    )
    assert dir_path == expected_dir
    # No path traversal
    assert "/../" not in file_path


class TestAlwaysMode:
    """Tests for FileSystemSerdesMode.ALWAYS."""

    def test_serialize_writes_to_file_and_returns_envelope(self, tmp_path):
        """Serialize should write to file and return file pointer envelope."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {"id": 1, "name": "Alice"}

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "file" in envelope
        assert "data" not in envelope
        assert os.path.exists(envelope["file"])

    def test_deserialize_reads_from_file(self, tmp_path):
        """Deserialize should read value from file pointer envelope."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {"id": 1, "name": "Alice"}

        serialized = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(serialized, MOCK_CONTEXT)

        assert deserialized == value

    def test_roundtrip_complex_data(self, tmp_path):
        """Test round-trip with complex nested data."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {
            "users": [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
            ],
            "metadata": {"count": 2, "page": 1},
            "tags": ["admin", "verified"],
        }

        serialized = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(serialized, MOCK_CONTEXT)

        assert deserialized == value

    def test_serialize_none_value(self, tmp_path):
        """Test serialization of None value."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result = serdes.serialize(None, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "file" in envelope
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)
        assert deserialized is None

    def test_serialize_string_value(self, tmp_path):
        """Test serialization of a plain string value."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result = serdes.serialize("hello world", MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == "hello world"

    def test_serialize_numeric_value(self, tmp_path):
        """Test serialization of numeric values."""
        serdes = create_filesystem_serdes(str(tmp_path))

        for value in [42, 3.14, -100, 0]:
            result = serdes.serialize(value, MOCK_CONTEXT)
            deserialized = serdes.deserialize(result, MOCK_CONTEXT)
            assert deserialized == value

    def test_serialize_list_value(self, tmp_path):
        """Test serialization of list values."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = [1, 2, 3, "hello", None, True]

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_multiple_operations_different_files(self, tmp_path):
        """Different operations should produce different files."""
        serdes = create_filesystem_serdes(str(tmp_path))
        ctx1 = SerDesContext(operation_id="step-1", durable_execution_arn=TEST_ARN)
        ctx2 = SerDesContext(operation_id="step-2", durable_execution_arn=TEST_ARN)

        result1 = serdes.serialize({"a": 1}, ctx1)
        result2 = serdes.serialize({"b": 2}, ctx2)

        envelope1 = json.loads(result1)
        envelope2 = json.loads(result2)

        assert envelope1["file"] != envelope2["file"]
        assert os.path.exists(envelope1["file"])
        assert os.path.exists(envelope2["file"])


class TestOverflowMode:
    """Tests for FileSystemSerdesMode.OVERFLOW."""

    def _create_overflow_serdes(self, tmp_path):
        return create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(storage_mode=FileSystemSerdesMode.OVERFLOW),
        )

    def test_small_value_stored_inline(self, tmp_path):
        """Small values should be stored inline in the envelope."""
        serdes = self._create_overflow_serdes(tmp_path)
        value = {"id": 1}

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "data" in envelope
        assert "file" not in envelope
        assert json.loads(envelope["data"]) == value

    def test_small_value_roundtrip(self, tmp_path):
        """Small values should round-trip through inline storage."""
        serdes = self._create_overflow_serdes(tmp_path)
        value = {"name": "test", "value": 123}

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_large_value_overflows_to_file(self, tmp_path):
        """Values exceeding threshold should overflow to file."""
        serdes = self._create_overflow_serdes(tmp_path)
        # Create a value that exceeds the ~255KB threshold
        value = {"data": "x" * (256 * 1024)}

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "file" in envelope
        assert "data" not in envelope
        assert os.path.exists(envelope["file"])

    def test_large_value_roundtrip(self, tmp_path):
        """Large values should round-trip through file storage."""
        serdes = self._create_overflow_serdes(tmp_path)
        value = {"data": "x" * (256 * 1024)}

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_deserialize_file_pointer_envelope(self, tmp_path):
        """Deserialize should handle file pointer envelope from ALWAYS mode."""
        always_serdes = create_filesystem_serdes(str(tmp_path))
        overflow_serdes = self._create_overflow_serdes(tmp_path)
        value = {"id": 1, "name": "Alice"}

        # Serialize with ALWAYS (creates file), deserialize with OVERFLOW
        serialized = always_serdes.serialize(value, MOCK_CONTEXT)
        deserialized = overflow_serdes.deserialize(serialized, MOCK_CONTEXT)

        assert deserialized == value

    def test_deserialize_inline_data_envelope(self, tmp_path):
        """Deserialize should handle inline data envelope."""
        serdes = self._create_overflow_serdes(tmp_path)
        value = {"id": 1}

        envelope = json.dumps({"data": json.dumps(value)})
        deserialized = serdes.deserialize(envelope, MOCK_CONTEXT)

        assert deserialized == value


class TestPathEncodingIntegration:
    """Tests for path encoding modes with full serialize/deserialize cycle."""

    def test_uri_encoding_with_durable_arn(self, tmp_path):
        """URI encoding should derive compact directory from ARN."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result = serdes.serialize({"id": 1}, MOCK_CONTEXT)
        envelope = json.loads(result)

        file_path = envelope["file"]
        # Should contain the function name and execution details in the path
        assert "test-function" in file_path
        assert "test-exec-id" in file_path
        assert "test-invocation-id" in file_path

    def test_uri_encoding_with_non_durable_arn(self, tmp_path):
        """URI encoding should fallback for non-durable ARNs."""
        serdes = create_filesystem_serdes(str(tmp_path))
        context = SerDesContext(
            operation_id="step-1",
            durable_execution_arn=NON_DURABLE_ARN,
        )

        result = serdes.serialize({"id": 1}, context)
        envelope = json.loads(result)

        file_path = envelope["file"]
        # Should NOT contain raw colons (they get encoded)
        dir_name = os.path.basename(os.path.dirname(file_path))
        assert ":" not in dir_name

    def test_hash_encoding(self, tmp_path):
        """HASH encoding should produce fixed-length segment names."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(path_encoding=FileSystemPathEncoding.HASH),
        )

        result = serdes.serialize({"id": 1}, MOCK_CONTEXT)
        envelope = json.loads(result)

        file_path = envelope["file"]
        file_name = os.path.basename(file_path)
        # Hash (64 chars) + ".json" (5 chars)
        assert len(file_name) == 69
        # Directory should also be a hash
        dir_name = os.path.basename(os.path.dirname(file_path))
        assert len(dir_name) == 64

    def test_hash_encoding_roundtrip(self, tmp_path):
        """HASH encoding should support full round-trip."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(path_encoding=FileSystemPathEncoding.HASH),
        )
        value = {"complex": {"nested": [1, 2, 3]}}

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_uri_encoding_unsafe_entity_id(self, tmp_path):
        """URI encoding should safely encode path-traversal entity IDs."""
        serdes = create_filesystem_serdes(str(tmp_path))
        unsafe_context = SerDesContext(
            operation_id="../invoices/2026",
            durable_execution_arn=TEST_ARN,
        )

        result = serdes.serialize({"id": 1}, unsafe_context)
        envelope = json.loads(result)

        file_path = envelope["file"]
        # The file should be under the expected directory
        assert str(tmp_path) in file_path
        # No path traversal
        assert "/../" not in file_path

    def test_uri_encoding_safe_ids_unchanged(self, tmp_path):
        """URI encoding should not alter safe IDs."""
        serdes = create_filesystem_serdes(str(tmp_path))
        context = SerDesContext(
            operation_id="simple-step-id",
            durable_execution_arn=TEST_ARN,
        )

        result = serdes.serialize({"id": 1}, context)
        envelope = json.loads(result)

        file_path = envelope["file"]
        assert "simple-step-id.json" in file_path


class TestPreviewWithSerdes:
    """Tests for preview feature integrated with filesystem serdes."""

    def test_always_mode_with_preview(self, tmp_path):
        """ALWAYS mode should include preview in envelope alongside file pointer."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(
                generate_preview=lambda value: build_preview(
                    value,
                    PreviewConfig(
                        mode=PreviewMode.EXCLUDE_ALL,
                        include=[PreviewField(name="id")],
                        mask=[PreviewField(name="secret")],
                    ),
                ),
            ),
        )
        value = {"id": "abc", "secret": "s3cr3t", "other": "ignored"}

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "file" in envelope
        assert "preview" in envelope
        assert envelope["preview"] == {"id": "abc", "secret": "***"}

    def test_deserialize_ignores_preview(self, tmp_path):
        """Deserialize should ignore preview field and read from file."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(
                generate_preview=lambda value: build_preview(
                    value,
                    PreviewConfig(
                        mode=PreviewMode.INCLUDE_ALL,
                    ),
                ),
            ),
        )
        value = {"id": "abc", "full_data": "complete"}

        serialized = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(serialized, MOCK_CONTEXT)

        # Full data is returned, not just the preview
        assert deserialized == value

    def test_overflow_mode_no_preview_for_inline(self, tmp_path):
        """OVERFLOW mode should not include preview for inline payloads."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(
                storage_mode=FileSystemSerdesMode.OVERFLOW,
                generate_preview=lambda value: build_preview(
                    value,
                    PreviewConfig(mode=PreviewMode.INCLUDE_ALL),
                ),
            ),
        )
        value = {"id": "abc"}  # small — stays inline

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "data" in envelope
        assert "preview" not in envelope

    def test_overflow_mode_includes_preview_when_overflows(self, tmp_path):
        """OVERFLOW mode should include preview when payload overflows to file."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(
                storage_mode=FileSystemSerdesMode.OVERFLOW,
                generate_preview=lambda value: build_preview(
                    value,
                    PreviewConfig(
                        mode=PreviewMode.EXCLUDE_ALL,
                        include=[PreviewField(name="id")],
                    ),
                ),
            ),
        )
        value = {"id": "abc", "data": "x" * (256 * 1024)}

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "file" in envelope
        assert "preview" in envelope
        assert envelope["preview"] == {"id": "abc"}

    def test_no_preview_when_generate_returns_none(self, tmp_path):
        """No preview field in envelope when generate_preview returns None."""
        serdes = create_filesystem_serdes(
            str(tmp_path),
            FileSystemSerdesConfig(
                generate_preview=lambda value: None,
            ),
        )
        value = {"id": "abc"}

        result = serdes.serialize(value, MOCK_CONTEXT)
        envelope = json.loads(result)

        assert "file" in envelope
        assert "preview" not in envelope


class TestSerdesApiIntegration:
    """Tests for filesystem serdes with the top-level serialize/deserialize functions."""

    def test_works_with_top_level_serialize_function(self, tmp_path):
        """FileSystem serdes should work with the top-level serialize function."""
        from aws_durable_execution_sdk_python.serdes import deserialize, serialize

        fs_serdes = create_filesystem_serdes(str(tmp_path))
        value = {"id": 1, "name": "test"}

        serialized = serialize(fs_serdes, value, TEST_OPERATION_ID, TEST_ARN)
        deserialized = deserialize(fs_serdes, serialized, TEST_OPERATION_ID, TEST_ARN)

        assert deserialized == value

    def test_serialization_error_handling(self, tmp_path):
        """Serialize should raise ExecutionError on failure via top-level API."""
        from aws_durable_execution_sdk_python.exceptions import ExecutionError
        from aws_durable_execution_sdk_python.serdes import serialize

        fs_serdes = create_filesystem_serdes("/nonexistent/readonly/path")

        with pytest.raises(ExecutionError, match="Serialization failed"):
            serialize(fs_serdes, {"data": "test"}, TEST_OPERATION_ID, TEST_ARN)

    def test_deserialization_error_handling(self, tmp_path):
        """Deserialize should raise ExecutionError on failure via top-level API."""
        from aws_durable_execution_sdk_python.exceptions import ExecutionError
        from aws_durable_execution_sdk_python.serdes import deserialize

        fs_serdes = create_filesystem_serdes(str(tmp_path))
        # Invalid envelope pointing to nonexistent file
        invalid_data = json.dumps({"file": "/nonexistent/file.json"})

        with pytest.raises(ExecutionError, match="Deserialization failed"):
            deserialize(fs_serdes, invalid_data, TEST_OPERATION_ID, TEST_ARN)


class TestEdgeCases:
    """Tests for edge cases and error scenarios."""

    def test_empty_dict(self, tmp_path):
        """Empty dict should serialize and deserialize correctly."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result = serdes.serialize({}, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == {}

    def test_empty_list(self, tmp_path):
        """Empty list should serialize and deserialize correctly."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result = serdes.serialize([], MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == []

    def test_deeply_nested_structure(self, tmp_path):
        """Deeply nested structures should serialize correctly."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {"a": {"b": {"c": {"d": {"e": "deep"}}}}}

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_unicode_content(self, tmp_path):
        """Unicode content should be handled correctly."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {"emoji": "🚀", "japanese": "日本語", "arabic": "العربية"}

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_boolean_values(self, tmp_path):
        """Boolean values should serialize correctly."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {"active": True, "deleted": False}

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_large_number_values(self, tmp_path):
        """Large numbers should serialize correctly."""
        serdes = create_filesystem_serdes(str(tmp_path))
        value = {
            "big_int": 2**53 - 1,
            "negative": -(2**53),
            "float": 1.7976931348623157e308,
        }

        result = serdes.serialize(value, MOCK_CONTEXT)
        deserialized = serdes.deserialize(result, MOCK_CONTEXT)

        assert deserialized == value

    def test_same_context_overwrites_file(self, tmp_path):
        """Serializing with same context should overwrite the file."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result1 = serdes.serialize({"version": 1}, MOCK_CONTEXT)
        result2 = serdes.serialize({"version": 2}, MOCK_CONTEXT)

        # Both should point to the same file path
        envelope1 = json.loads(result1)
        envelope2 = json.loads(result2)
        assert envelope1["file"] == envelope2["file"]

        # File should contain the latest value
        deserialized = serdes.deserialize(result2, MOCK_CONTEXT)
        assert deserialized == {"version": 2}

    def test_default_config(self, tmp_path):
        """Default config should use ALWAYS mode and URI encoding."""
        serdes = create_filesystem_serdes(str(tmp_path))

        result = serdes.serialize({"id": 1}, MOCK_CONTEXT)
        envelope = json.loads(result)

        # ALWAYS mode: file pointer, no inline data
        assert "file" in envelope
        assert "data" not in envelope
        # URI encoding: human-readable path
        assert "test-function" in envelope["file"]

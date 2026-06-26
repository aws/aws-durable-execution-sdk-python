"""Unit tests for the preview module."""

from aws_durable_execution_sdk_python.preview import (
    FieldMatchMode,
    PreviewConfig,
    PreviewField,
    PreviewMode,
    build_preview,
)


class TestBuildPreview:
    """Tests for the build_preview function."""

    def test_include_all_mode(self):
        """INCLUDE_ALL should include all fields by default."""
        value = {"id": "123", "email": "alice@example.com", "ssn": "000-00-0000"}
        result = build_preview(value, PreviewConfig(mode=PreviewMode.INCLUDE_ALL))

        assert result is not None
        assert result["id"] == "123"
        assert result["email"] == "alice@example.com"
        assert result["ssn"] == "000-00-0000"

    def test_include_all_with_exclude(self):
        """INCLUDE_ALL + exclude should omit excluded fields."""
        value = {"id": "123", "email": "alice@example.com", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                exclude=[PreviewField(name="ssn")],
            ),
        )

        assert result is not None
        assert "ssn" not in result
        assert result["id"] == "123"

    def test_exclude_all_with_include(self):
        """EXCLUDE_ALL + include should only include specified fields."""
        value = {"id": "123", "email": "alice@example.com", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.EXCLUDE_ALL,
                include=[PreviewField(name="id"), PreviewField(name="email")],
            ),
        )

        assert result is not None
        assert result["id"] == "123"
        assert result["email"] == "alice@example.com"
        assert "ssn" not in result

    def test_mask_replaces_value(self):
        """Mask should replace visible field value with mask_string."""
        value = {"id": "123", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                mask=[PreviewField(name="ssn")],
            ),
        )

        assert result is not None
        assert result["ssn"] == "***"
        assert result["id"] == "123"

    def test_mask_custom_string(self):
        """Mask should use custom mask_string."""
        value = {"id": "123", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                mask=[PreviewField(name="ssn")],
                mask_string="[REDACTED]",
            ),
        )

        assert result is not None
        assert result["ssn"] == "[REDACTED]"

    def test_mask_implies_visibility_in_exclude_all(self):
        """Mask implies visibility in EXCLUDE_ALL — masked field shown even without include."""
        value = {"id": "123", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.EXCLUDE_ALL,
                mask=[PreviewField(name="ssn")],
            ),
        )

        assert result is not None
        assert result["ssn"] == "***"
        assert "id" not in result

    def test_exclude_wins_over_mask(self):
        """Exclude always wins — excluded field is not shown even if in mask."""
        value = {"id": "123", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                exclude=[PreviewField(name="ssn")],
                mask=[PreviewField(name="ssn")],
            ),
        )

        assert result is not None
        assert "ssn" not in result

    def test_path_match_mode(self):
        """PATH match should only match exact path."""
        value = {"email": "root@example.com", "user": {"email": "nested@example.com"}}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.EXCLUDE_ALL,
                include=[PreviewField(name="email", match=FieldMatchMode.PATH)],
            ),
        )

        assert result is not None
        assert result["email"] == "root@example.com"
        assert "user" not in result

    def test_anywhere_match_mode(self):
        """ANYWHERE match should match field at any depth."""
        value = {"email": "root@example.com", "user": {"email": "nested@example.com"}}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.EXCLUDE_ALL,
                include=[PreviewField(name="email")],
            ),
        )

        assert result is not None
        assert result["email"] == "root@example.com"
        assert result["user"]["email"] == "nested@example.com"

    def test_nested_objects(self):
        """Preview should handle nested objects."""
        value = {"user": {"name": "Alice", "role": "admin"}}
        result = build_preview(value, PreviewConfig(mode=PreviewMode.INCLUDE_ALL))

        assert result is not None
        assert result["user"]["name"] == "Alice"
        assert result["user"]["role"] == "admin"

    def test_arrays_merged(self):
        """Array structure is not preserved — fields merged into a plain object."""
        value = {"items": [{"secret": "xyz"}, {"secret": "abc"}]}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                mask=[PreviewField(name="secret")],
            ),
        )

        assert result is not None
        assert result["items"]["secret"] == "***"

    def test_max_preview_bytes_budget(self):
        """Preview should respect maxPreviewBytes budget."""
        value = {"id": "123", "email": "alice@example.com", "ssn": "000-00-0000"}
        result = build_preview(
            value,
            PreviewConfig(
                mode=PreviewMode.INCLUDE_ALL,
                max_preview_bytes=20,
            ),
        )

        assert result is not None
        assert len(result) < len(value)

    def test_returns_none_for_non_dict(self):
        """Preview should return None for non-dict values."""
        assert (
            build_preview("string", PreviewConfig(mode=PreviewMode.INCLUDE_ALL)) is None
        )
        assert build_preview(42, PreviewConfig(mode=PreviewMode.INCLUDE_ALL)) is None
        assert build_preview(None, PreviewConfig(mode=PreviewMode.INCLUDE_ALL)) is None
        assert (
            build_preview([1, 2, 3], PreviewConfig(mode=PreviewMode.INCLUDE_ALL))
            is None
        )

    def test_returns_none_when_no_fields_visible(self):
        """Preview should return None when no fields match."""
        value = {"id": "123", "name": "Alice"}
        result = build_preview(
            value,
            PreviewConfig(mode=PreviewMode.EXCLUDE_ALL),
        )

        assert result is None

    def test_skips_keys_with_dots(self):
        """Preview should skip keys containing dots."""
        value = {"id": "123", "some.dotted.key": "value"}
        result = build_preview(value, PreviewConfig(mode=PreviewMode.INCLUDE_ALL))

        assert result is not None
        assert result["id"] == "123"
        assert "some.dotted.key" not in result

#!/usr/bin/env python3

from ops.lintcommit import validate_message, validate_subject


# region validate_subject: valid subjects


def test_valid_feat() -> None:
    assert validate_subject("feat: add new feature") is None


def test_valid_fix() -> None:
    assert validate_subject("fix: resolve issue") is None


def test_valid_fix_with_scope() -> None:
    assert validate_subject("fix(sdk): resolve issue") is None


def test_valid_build() -> None:
    assert validate_subject("build: update build process") is None


def test_valid_chore() -> None:
    assert validate_subject("chore: update dependencies") is None


def test_valid_ci() -> None:
    assert validate_subject("ci: configure ci/cd") is None


def test_valid_deps() -> None:
    assert validate_subject("deps: bump aws-sdk group with 5 updates") is None


def test_valid_docs() -> None:
    assert validate_subject("docs: update documentation") is None


def test_valid_feat_with_scope() -> None:
    assert validate_subject("feat(sdk): add new feature") is None


def test_valid_feat_scope_bar() -> None:
    assert validate_subject("feat(sdk): bar") is None


def test_valid_feat_foo() -> None:
    assert validate_subject("feat: foo") is None


def test_valid_fix_foo() -> None:
    assert validate_subject("fix: foo") is None


# region validate_subject: invalid subjects


def test_invalid_type() -> None:
    assert validate_subject("config: foo") == 'invalid type "config"'


def test_missing_colon() -> None:
    assert validate_subject("invalid title") == "missing colon (:) char"


def test_period_at_end() -> None:
    assert validate_subject("feat: add thing.") == "subject must not end with a period"


def test_empty_subject() -> None:
    assert validate_subject("feat: ") == "empty subject"


def test_subject_too_long() -> None:
    long_subject: str = "feat: " + "a" * 51
    result = validate_subject(long_subject)
    assert result is not None
    assert "invalid subject" in result


def test_type_with_whitespace() -> None:
    assert validate_subject("fe at: foo") == 'type contains whitespace: "fe at"'


def test_scope_not_closed() -> None:
    assert validate_subject("feat(sdk: foo") == "must be formatted like type(scope):"


def test_scope_too_long() -> None:
    long_scope: str = "a" * 31
    result = validate_subject(f"feat({long_scope}): foo")
    assert result is not None
    assert "invalid scope" in result


def test_scope_uppercase() -> None:
    result = validate_subject("feat(SDK): foo")
    assert result is not None
    assert "invalid scope" in result


def test_subject_uppercase() -> None:
    assert validate_subject("feat: Add new feature") == "subject must be lowercase"


def test_subject_uppercase_acronym_rejected() -> None:
    assert validate_subject("ci: configure CI/CD") == "subject must be lowercase"


# region validate_message


def test_valid_subject_only() -> None:
    error, warnings = validate_message("feat: add thing")
    assert error is None
    assert warnings == []


def test_valid_with_body() -> None:
    error, warnings = validate_message("feat: add thing\n\nThis is the body.")
    assert error is None
    assert warnings == []


def test_missing_blank_line() -> None:
    _, warnings = validate_message("feat: add thing\nNo blank line.")
    assert "missing blank line between subject and body" in warnings


def test_missing_blank_line_body_still_checked() -> None:
    _, warnings = validate_message("feat: add thing\n" + "x" * 80)
    assert "missing blank line between subject and body" in warnings
    assert any("exceeds 72 chars" in w for w in warnings), (
        "body line length should be checked even without blank line"
    )


def test_long_body_line() -> None:
    _, warnings = validate_message("feat: add thing\n\n" + "x" * 80)
    assert len(warnings) == 1
    assert "exceeds 72 chars" in warnings[0]


def test_empty_message() -> None:
    error, _ = validate_message("")
    assert error == "empty commit message"


def test_invalid_subject_in_message() -> None:
    error, _ = validate_message("invalid title")
    assert error == "missing colon (:) char"

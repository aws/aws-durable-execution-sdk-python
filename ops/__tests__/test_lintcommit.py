#!/usr/bin/env python3

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from lintcommit import validate_message, validate_subject


class TestValidateSubject(unittest.TestCase):
    # Valid subjects
    def test_valid_feat(self) -> None:
        self.assertIsNone(validate_subject("feat: add new feature"))

    def test_valid_fix(self) -> None:
        self.assertIsNone(validate_subject("fix: resolve issue"))

    def test_valid_fix_with_scope(self) -> None:
        self.assertIsNone(validate_subject("fix(sdk): resolve issue"))

    def test_valid_build(self) -> None:
        self.assertIsNone(validate_subject("build: update build process"))

    def test_valid_chore(self) -> None:
        self.assertIsNone(validate_subject("chore: update dependencies"))

    def test_valid_ci(self) -> None:
        self.assertIsNone(validate_subject("ci: configure CI/CD"))

    def test_valid_deps(self) -> None:
        self.assertIsNone(validate_subject("deps: bump aws-sdk group with 5 updates"))

    def test_valid_docs(self) -> None:
        self.assertIsNone(validate_subject("docs: update documentation"))

    def test_valid_feat_with_scope(self) -> None:
        self.assertIsNone(validate_subject("feat(sdk): add new feature"))

    def test_valid_feat_scope_bar(self) -> None:
        self.assertIsNone(validate_subject("feat(sdk): bar"))

    def test_valid_feat_foo(self) -> None:
        self.assertIsNone(validate_subject("feat: foo"))

    def test_valid_fix_foo(self) -> None:
        self.assertIsNone(validate_subject("fix: foo"))

    # Invalid subjects
    def test_invalid_type(self) -> None:
        self.assertEqual(validate_subject("config: foo"), 'invalid type "config"')

    def test_missing_colon(self) -> None:
        self.assertEqual(validate_subject("invalid title"), "missing colon (:) char")

    def test_period_at_end(self) -> None:
        self.assertEqual(
            validate_subject("feat: add thing."),
            "subject must not end with a period",
        )

    def test_empty_subject(self) -> None:
        self.assertEqual(validate_subject("feat: "), "empty subject")

    def test_subject_too_long(self) -> None:
        long_subject: str = "feat: " + "a" * 51
        result: str | None = validate_subject(long_subject)
        self.assertIsNotNone(result)
        self.assertIn("invalid subject", result)  # type: ignore[arg-type]

    def test_type_with_whitespace(self) -> None:
        self.assertEqual(
            validate_subject("fe at: foo"), 'type contains whitespace: "fe at"'
        )

    def test_scope_not_closed(self) -> None:
        self.assertEqual(
            validate_subject("feat(sdk: foo"), "must be formatted like type(scope):"
        )

    def test_scope_too_long(self) -> None:
        long_scope: str = "a" * 31
        result: str | None = validate_subject(f"feat({long_scope}): foo")
        self.assertIsNotNone(result)
        self.assertIn("invalid scope", result)  # type: ignore[arg-type]

    def test_scope_uppercase(self) -> None:
        result: str | None = validate_subject("feat(SDK): foo")
        self.assertIsNotNone(result)
        self.assertIn("invalid scope", result)  # type: ignore[arg-type]


class TestValidateMessage(unittest.TestCase):
    def test_valid_subject_only(self) -> None:
        error, warnings = validate_message("feat: add thing")
        self.assertIsNone(error)
        self.assertEqual(warnings, [])

    def test_valid_with_body(self) -> None:
        error, warnings = validate_message("feat: add thing\n\nThis is the body.")
        self.assertIsNone(error)
        self.assertEqual(warnings, [])

    def test_missing_blank_line(self) -> None:
        _, warnings = validate_message("feat: add thing\nNo blank line.")
        self.assertIn("missing blank line between subject and body", warnings)

    def test_long_body_line(self) -> None:
        _, warnings = validate_message("feat: add thing\n\n" + "x" * 80)
        self.assertEqual(len(warnings), 1)
        self.assertIn("exceeds 72 chars", warnings[0])

    def test_empty_message(self) -> None:
        error, _ = validate_message("")
        self.assertEqual(error, "empty commit message")

    def test_invalid_subject_in_message(self) -> None:
        error, _ = validate_message("invalid title")
        self.assertEqual(error, "missing colon (:) char")

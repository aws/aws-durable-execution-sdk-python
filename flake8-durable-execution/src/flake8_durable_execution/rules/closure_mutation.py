"""DAR008: Detect closure mutations that get lost on replay."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error


class ClosureMutationRule(BaseRule):
    code = "DAR008"
    message = "Avoid mutating closure variables in steps - mutations are lost on replay"

    def __init__(self) -> None:
        self._outer_vars: set[str] = set()
        self._in_step_function = False

    def check(self, node: ast.AST) -> Iterator[Error]:
        # Look for augmented assignments (+=, -=, etc.) to outer scope vars
        if isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                # This is a heuristic - we flag any augmented assignment
                # that looks like it could be mutating an outer variable
                yield self._error(node)

        # Look for attribute mutations on outer variables
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    # list[0] = x or dict["key"] = x
                    yield self._error(node)

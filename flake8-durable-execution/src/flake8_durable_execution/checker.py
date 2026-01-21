"""Flake8 checker for AWS Durable Execution SDK best practices."""

import ast
from typing import Iterator

from flake8_durable_execution.rules import ALL_RULES
from flake8_durable_execution.rules.nested_durable_op import NestedDurableOpRule

DURABLE_EXECUTION_DECORATORS = {"durable_execution"}
DURABLE_STEP_DECORATORS = {"durable_step"}
DURABLE_OPS = {"step", "invoke", "parallel", "map"}


class DurableExecutionChecker:
    name = "flake8-durable-execution"
    version = "0.1.0"

    def __init__(self, tree: ast.AST) -> None:
        self.tree = tree
        self.rules = [rule() for rule in ALL_RULES if rule != NestedDurableOpRule]
        self.nested_rule = NestedDurableOpRule()

    def run(self) -> Iterator[tuple[int, int, str, type]]:
        for node in ast.walk(self.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                # Check @durable_execution functions for non-determinism
                if self._has_decorator(node, DURABLE_EXECUTION_DECORATORS):
                    yield from self._check_durable_function(node)

                # Check @durable_step functions for nested durable ops
                if self._has_decorator(node, DURABLE_STEP_DECORATORS):
                    yield from self.nested_rule.check_function(node)

    def _has_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef, decorators: set[str]) -> bool:
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name) and decorator.id in decorators:
                return True
            if isinstance(decorator, ast.Attribute) and decorator.attr in decorators:
                return True
        return False

    def _check_durable_function(self, func: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterator[tuple[int, int, str, type]]:
        # Collect all nodes that are inside ctx.step() calls - these are safe
        safe_nodes: set[int] = set()
        self._collect_safe_nodes(func, safe_nodes)

        for node in ast.walk(func):
            if id(node) in safe_nodes:
                continue
            for rule in self.rules:
                yield from rule.check(node)

    def _collect_safe_nodes(self, node: ast.AST, safe_nodes: set[int]) -> None:
        """Collect all nodes inside ctx.step(lambda: ...) calls."""
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue

            # Check if this is a ctx.step(...) call
            if isinstance(child.func, ast.Attribute) and child.func.attr in DURABLE_OPS:
                # Mark all nodes inside the step arguments as safe
                for arg in child.args:
                    for inner in ast.walk(arg):
                        safe_nodes.add(id(inner))
                for kw in child.keywords:
                    for inner in ast.walk(kw.value):
                        safe_nodes.add(id(inner))

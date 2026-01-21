"""Base class for all rules."""

import ast
from abc import ABC, abstractmethod
from typing import Iterator

Error = tuple[int, int, str, type]


class BaseRule(ABC):
    """Base class for lint rules."""

    code: str
    message: str

    @abstractmethod
    def check(self, node: ast.AST) -> Iterator[Error]:
        """Check a node and yield errors."""
        pass

    def _error(self, node: ast.AST) -> Error:
        """Create an error tuple for the given node."""
        return (node.lineno, node.col_offset, f"{self.code} {self.message}", type(self))

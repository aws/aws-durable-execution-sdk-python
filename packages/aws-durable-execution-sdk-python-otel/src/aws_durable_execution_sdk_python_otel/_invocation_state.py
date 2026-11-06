"""Visible completion metadata owned by one instrumentation invocation."""

from collections.abc import Mapping
from graphlib import TopologicalSorter

from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    OperationInfo,
    OperationStatus,
    OperationType,
)


_TERMINAL_STATUSES = frozenset(
    {
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
        OperationStatus.CANCELLED,
    }
)


class CompletionState:
    """Pending deliveries for one invocation, guarded by its span-registry lock.

    Callers hold that lock for every access, including parent registration and
    dequeue. Span rendering runs outside the lock, between claim and release.
    Only complete records successful delivery; a failed attempt stays pending.
    """

    def __init__(self) -> None:
        self.pending: dict[str, OperationInfo] = {}
        # An ordered ID index; completion payloads live only in pending.
        self.children_by_parent: dict[str, dict[str, None]] = {}
        self.in_flight: set[str] = set()
        self.delivered: set[str] = set()

    def remember(self, updates: Mapping[str, OperationInfo]) -> None:
        """Keep explicit terminal updates that have not already been delivered."""
        for operation_id, operation in updates.items():
            if (
                operation.status in _TERMINAL_STATUSES
                and not operation.is_replayed
                and operation_id not in self.delivered
            ):
                self.pending[operation_id] = operation
                self._index_child(operation)

    def _index_child(self, operation: OperationInfo) -> None:
        if operation.parent_id is not None:
            self.children_by_parent.setdefault(operation.parent_id, {})[
                operation.operation_id
            ] = None

    def take_children(self, parent_id: str) -> list[OperationInfo]:
        """Dequeue children in observation order, retaining undelivered payloads."""
        return [
            self.pending[operation_id]
            for operation_id in self.children_by_parent.pop(parent_id, {})
        ]

    def claim(
        self, operation: OperationInfo, *, parent_available: bool, repeat: bool = False
    ) -> bool:
        """Claim delivery, or defer it until its parent is available.

        A newly opened context segment may repeat a delivered operation, but
        it must still respect another caller's in-flight claim.
        """
        operation_id = operation.operation_id
        if operation_id in self.in_flight or (
            operation_id in self.delivered and not repeat
        ):
            return False
        self.pending[operation_id] = operation
        if not parent_available:
            self._index_child(operation)
            return False
        self.in_flight.add(operation_id)
        return True

    def complete(self, operation: OperationInfo) -> None:
        """Record success and remove the payload and any remaining index entry."""
        operation_id = operation.operation_id
        self.delivered.add(operation_id)
        self.pending.pop(operation_id, None)
        if operation.parent_id is not None:
            siblings = self.children_by_parent.get(operation.parent_id)
            if siblings is not None:
                siblings.pop(operation_id, None)
                if not siblings:
                    del self.children_by_parent[operation.parent_id]

    def release(self, operation_id: str) -> None:
        """Release a claim after either successful or failed rendering."""
        self.in_flight.discard(operation_id)

    def clear(self) -> None:
        """Release all invocation-owned delivery metadata."""
        self.pending.clear()
        self.children_by_parent.clear()
        self.in_flight.clear()
        self.delivered.clear()


def completed_input_parent_ids(info: InvocationStartInfo) -> set[str]:
    """Visible completed contexts may parent input or later checkpoint updates."""
    return {
        operation.operation_id
        for operation in info.operations.values()
        if operation.operation_type is OperationType.CONTEXT
        and operation.status in _TERMINAL_STATUSES
    }


def parent_first(
    operations: Mapping[str, OperationInfo],
) -> list[OperationInfo]:
    """Materialize pending ancestors before referencing them for descendants.

    Durable operations form a parent tree. Missing ancestors remain unresolved;
    their view-specific fallback is chosen by the plugin, without inventing an
    operation or treating its non-recording reference as an exported completion.
    """
    order = TopologicalSorter(
        {
            operation_id: (
                (operation.parent_id,)
                if operation.parent_id is not None and operation.parent_id in operations
                else ()
            )
            for operation_id, operation in operations.items()
        }
    )
    return [operations[operation_id] for operation_id in order.static_order()]

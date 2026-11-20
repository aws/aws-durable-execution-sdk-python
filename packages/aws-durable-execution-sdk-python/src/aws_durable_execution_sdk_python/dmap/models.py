"""Result models returned by ``ctx.distributed_map``."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Self, TypedDict

from aws_durable_execution_sdk_python.exceptions import (
    DistributedMapError,
    ExecutionError,
    ValidationError,
)
from aws_durable_execution_sdk_python.config import (
    DistributedMapItemStatus,
    DistributedMapStatus,
    ItemOutput,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject, OperationStatus
from aws_durable_execution_sdk_python.serdes import SerDesContext
from aws_durable_execution_sdk_python.types import DurableExecutionArn

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import Operation

_STATUS_FOR_OPERATION_STATUS = {
    OperationStatus.SUCCEEDED: DistributedMapStatus.SUCCEEDED,
    OperationStatus.FAILED: DistributedMapStatus.FAILED,
    OperationStatus.STOPPED: DistributedMapStatus.STOPPED,
    OperationStatus.TIMED_OUT: DistributedMapStatus.TIMED_OUT,
}


def _to_distributed_map_status(operation: Operation | None) -> DistributedMapStatus:
    """Derive the map run status from the operation's terminal status."""
    op_status = operation.status if operation else None
    resolved = (
        _STATUS_FOR_OPERATION_STATUS.get(op_status) if op_status is not None else None
    )
    if resolved is None:
        msg = (
            "Cannot derive distributed map status from operation status "
            f"{op_status.value if op_status else 'UNKNOWN'}"
        )
        raise ExecutionError(msg)
    return resolved


class _SummaryFields(TypedDict):
    """The summary fields both factories pass to the constructor."""

    status: DistributedMapStatus
    success_count: int
    failure_count: int
    read_count: int
    distributed_map_run_arn: str | None
    source_exhausted: bool | None
    error: ErrorObject | None
    total_count: int | None


def _require_count(value: int | None, field_name: str) -> int:
    """Return a count the service must send at terminal, or raise when absent."""
    if value is None:
        msg = f"missing distributed map {field_name} from backend"
        raise ExecutionError(msg)
    return value


def _summary_fields(operation: Operation | None) -> _SummaryFields:
    """Read the summary fields a terminal operation must carry."""
    details = operation.distributed_map_details if operation else None
    if details is None:
        # The raw operation status, not _to_distributed_map_status, which raises
        # on a non-terminal status and would replace this error with its own.
        op_status = (
            operation.status.value if operation and operation.status else "UNKNOWN"
        )
        msg = (
            f"DISTRIBUTED_MAP operation ended {op_status} but carried no "
            f"DistributedMapDetails"
        )
        raise ExecutionError(msg)
    status = _to_distributed_map_status(operation)
    # SourceExhausted is required at terminal once a run exists, and is absent
    # only when the operation failed before one was created.
    if details.distributed_map_run_arn is not None and details.source_exhausted is None:
        msg = (
            f"DISTRIBUTED_MAP operation ended {status.value} but carried no "
            f"SourceExhausted"
        )
        raise ExecutionError(msg)
    # These three counts are required at every terminal state. An absent one
    # must not read as zero, since an absent FailureCount would then report a
    # run with failed items as a run with none.
    success_count = _require_count(details.success_count, "SuccessCount")
    failure_count = _require_count(details.failure_count, "FailureCount")
    read_count = _require_count(details.read_count, "ReadCount")
    # TotalCount is required only once the run succeeded, because a run that
    # failed, timed out or was stopped may never have counted its source.
    if status is DistributedMapStatus.SUCCEEDED and details.total_count is None:
        msg = "missing distributed map TotalCount from backend"
        raise ExecutionError(msg)
    return {
        "status": status,
        "success_count": success_count,
        "failure_count": failure_count,
        "read_count": read_count,
        "distributed_map_run_arn": details.distributed_map_run_arn,
        "source_exhausted": details.source_exhausted,
        "error": details.error,
        "total_count": details.total_count,
    }


_RUN_ARN_SEPARATOR = "/distributed-map-run/"


@dataclass(frozen=True)
class DistributedMapRunArn:
    """Parsed components of a distributed map run ARN.

    Attributes:
        durable_execution_arn: The parent durable execution's ARN.
        run_id: The map run id, unique per account.
    """

    durable_execution_arn: str
    run_id: str

    @classmethod
    def from_arn(cls, arn: str) -> DistributedMapRunArn | None:
        """Parse a map run ARN, returning ``None`` when it does not match the format."""
        # A run id contains no "/", so the separator is always its last
        # occurrence. The parent's execution name or invocation id can itself be
        # "distributed-map-run", which partition would split on instead.
        parent, _, run_id = arn.rpartition(_RUN_ARN_SEPARATOR)
        if not run_id or "/" in run_id or DurableExecutionArn.from_arn(parent) is None:
            return None
        return cls(durable_execution_arn=parent, run_id=run_id)

    def to_arn(self) -> str:
        """Reassemble the ARN this was parsed from.

        ``from_arn`` splits on the last ``/distributed-map-run/`` and refuses a
        run id containing ``/``, so joining the two parts back restores the
        original string exactly.
        """
        return f"{self.durable_execution_arn}{_RUN_ARN_SEPARATOR}{self.run_id}"

    def serdes_context(self, role: str, *parts: str | int | None) -> SerDesContext:
        """Address one piece of this run's data so writer and reader agree on it.

        A serdes that stores data outside the payload uses the context it is
        handed as the address. FileSystemSerDes writes the file
        durable_execution_arn/operation_id. One run hands a serdes every item,
        every item's output and every reader state, so each of those needs a
        distinct address. The processor writes an item's output and the parent
        reads it back, so both must compute the same address. Both call this.

        ``role`` is "body", "output", "item", "state" or "next-state". ``parts``
        tells two of the same role apart, such as an item id.
        """
        key = json.dumps([self.run_id, role, *parts])
        return SerDesContext(
            operation_id=hashlib.blake2b(key.encode()).hexdigest()[:64],
            durable_execution_arn=self.durable_execution_arn,
        )


@dataclass(frozen=True)
class DistributedMapSummary:
    """Outcome of a map run without per-item results.

    Resolved by ``ctx.distributed_map`` for every terminal state. A non-``SUCCEEDED``
    run resolves rather than raising. Use :meth:`throw_if_error` to opt into raising.
    """

    status: DistributedMapStatus
    success_count: int
    failure_count: int
    read_count: int
    distributed_map_run_arn: str | None = None
    source_exhausted: bool | None = None
    error: ErrorObject | None = None
    total_count: int | None = None

    @classmethod
    def from_operation(cls, operation: Operation | None) -> Self:
        """Rebuild the summary from a terminal DISTRIBUTED_MAP operation."""
        return cls(**_summary_fields(operation))

    @property
    def distributed_map_id(self) -> str | None:
        """Run id from :attr:`distributed_map_run_arn`, ``None`` if absent or unparseable."""
        if self.distributed_map_run_arn is None:
            return None
        parsed = DistributedMapRunArn.from_arn(self.distributed_map_run_arn)
        return parsed.run_id if parsed else None

    @property
    def has_failure(self) -> bool:
        """``True`` when any item permanently failed."""
        return self.failure_count > 0

    def throw_if_error(self) -> None:
        """Raise :class:`DistributedMapError` on any non-success outcome."""
        if self.status is not DistributedMapStatus.SUCCEEDED:
            error = self.error
            msg = f"Map run ended {self.status.value}"
            if error is not None and error.message:
                msg = f"{msg}: {error.message}"
            # A wholly empty error carries nothing to rebuild, so it gets no cause.
            cause = (
                error.to_durable_operation_error()
                if error is not None and error.to_dict()
                else None
            )
            raise DistributedMapError(
                message=msg,
                error_type=error.type if error else None,
                data=error.data if error else None,
                stack_trace=error.stack_trace if error else None,
            ) from cause
        if self.failure_count > 0:
            msg = (
                f"Map run succeeded but {self.failure_count} item(s) permanently failed"
            )
            raise DistributedMapError(msg)


@dataclass(frozen=True)
class DistributedMapResultItem(Generic[ItemOutput]):
    """Outcome of a single map run item."""

    item_id: str
    status: DistributedMapItemStatus
    output: ItemOutput | None = None
    error: ErrorObject | None = None


@dataclass(frozen=True)
class DistributedMapResult(DistributedMapSummary, Generic[ItemOutput]):
    """Outcome of a map run with per-item results.

    Resolved by ``ctx.distributed_map`` when passed a
    :class:`DistributedMapResultConfig`. Extends :class:`DistributedMapSummary`
    with the retained per-item results.
    """

    all: list[DistributedMapResultItem[ItemOutput]] = field(default_factory=list)

    @classmethod
    def from_operation(cls, operation: Operation | None) -> Self:
        """Rebuild a result from a terminal operation that retained no items.

        The inherited summary factory cannot deserialize items, so it leaves
        ``all`` empty. That is the right answer for a run that retained
        nothing. For a run that did retain results it would return counts that
        contradict an empty ``all``, so that call is refused here rather than
        answered wrongly. Use :meth:`from_operation_and_items` for those.

        Raises:
            ValidationError: if the operation carries retained results.
        """
        details = operation.distributed_map_details if operation else None
        if details is not None and details.results:
            msg = (
                "DistributedMapResult.from_operation cannot carry retained "
                "results. Use from_operation_and_items with the deserialized "
                "items instead."
            )
            raise ValidationError(msg)
        return super().from_operation(operation)

    @classmethod
    def from_operation_and_items(
        cls,
        operation: Operation | None,
        items: list[DistributedMapResultItem[ItemOutput]],
    ) -> Self:
        """Rebuild the result from a terminal operation and its deserialized items."""
        return cls(**_summary_fields(operation), all=items)

    def succeeded(self) -> list[DistributedMapResultItem[ItemOutput]]:
        """Return the items that succeeded."""
        return [
            item
            for item in self.all
            if item.status is DistributedMapItemStatus.SUCCEEDED
        ]

    def failed(self) -> list[DistributedMapResultItem[ItemOutput]]:
        """Return the items that permanently failed."""
        return [
            item for item in self.all if item.status is DistributedMapItemStatus.FAILED
        ]

    def get_results(self) -> list[ItemOutput | None]:
        """Return one output per succeeded item, None where none was recorded."""
        return [
            item.output
            for item in self.all
            if item.status is DistributedMapItemStatus.SUCCEEDED
        ]

    def get_errors(self) -> list[ErrorObject | None]:
        """Return one error per failed item, None where none was recorded."""
        return [
            item.error
            for item in self.all
            if item.status is DistributedMapItemStatus.FAILED
        ]

    def throw_if_error(self) -> None:
        """Raise the first failed item's error, otherwise defer to the summary rule."""
        if self.status is DistributedMapStatus.SUCCEEDED and self.failure_count > 0:
            failed = self.failed()
            if failed:
                first = failed[0]
                error = first.error
                if error is not None and error.to_dict():
                    error.raise_as_operation_error(DistributedMapError)
                msg = f"item {first.item_id} failed"
                raise DistributedMapError(msg)
        super().throw_if_error()

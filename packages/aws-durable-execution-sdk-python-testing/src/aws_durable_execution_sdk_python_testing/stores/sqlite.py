"""SQLite-based execution store implementation."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from aws_durable_execution_sdk_python_testing.exceptions import (
    ResourceNotFoundException,
    InvalidParameterValueException,
    RuntimeException,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.stores.base import (
    ExecutionStore,
)


class SQLiteExecutionStore(ExecutionStore):
    """SQLite-based execution store for efficient querying."""

    def __init__(self, db_path: Path) -> None:
        self.db_path: Path = db_path

    @classmethod
    def create_and_initialize(
        cls, db_path: Path | str | None = None
    ) -> SQLiteExecutionStore:
        """Create SQLite store with default path."""
        path: Path = Path(db_path) if db_path else Path("durable-executions.db")
        path.parent.mkdir(exist_ok=True)
        store: SQLiteExecutionStore = cls(path)
        store._init_db()
        return store

    def _get_connection(self) -> sqlite3.Connection:
        """Get SQLite connection with optimizations."""
        conn: sqlite3.Connection = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        """Initialize database schema."""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS executions (
                        durable_execution_arn TEXT PRIMARY KEY,
                        function_name TEXT NOT NULL,
                        execution_name TEXT,
                        status TEXT NOT NULL,
                        start_timestamp REAL,
                        end_timestamp REAL,
                        data TEXT NOT NULL
                    )
                """)
                # Create indexes for better query performance
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_function_name ON executions(function_name)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_status ON executions(status)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_start_timestamp ON executions(start_timestamp)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_composite ON executions(function_name, status, start_timestamp)"
                )
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to initialize database: {e}") from e

    def save(self, execution: Execution) -> None:
        """Save execution to SQLite."""
        try:
            execution_op = execution.get_operation_execution_started()
            status: str = execution.current_status().value

            with self._get_connection() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO executions
                    (durable_execution_arn, function_name, execution_name, status, start_timestamp, end_timestamp, data)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution.durable_execution_arn,
                        execution.start_input.function_name,
                        execution.start_input.execution_name,
                        status,
                        execution_op.start_timestamp.timestamp()
                        if execution_op.start_timestamp
                        else None,
                        execution_op.end_timestamp.timestamp()
                        if execution_op.end_timestamp
                        else None,
                        json.dumps(execution.to_json_dict()),
                    ),
                )
        except sqlite3.Error as e:
            raise RuntimeError(
                f"Failed to save execution {execution.durable_execution_arn}: {e}"
            ) from e
        except (AttributeError, TypeError) as e:
            raise ValueError(f"Invalid execution data: {e}") from e

    def load(self, execution_arn: str) -> Execution:
        """Load execution from SQLite."""
        try:
            with self._get_connection() as conn:
                cursor: sqlite3.Cursor = conn.execute(
                    "SELECT data FROM executions WHERE durable_execution_arn = ?",
                    (execution_arn,),
                )
                row: tuple[str] | None = cursor.fetchone()

            if not row:
                raise ResourceNotFoundException(f"Execution {execution_arn} not found")

            return Execution.from_json_dict(json.loads(row[0]))
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to load execution {execution_arn}: {e}") from e
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Corrupted execution data for {execution_arn}: {e}"
            ) from e

    def update(self, execution: Execution) -> None:
        """Update execution (same as save)."""
        self.save(execution)

    def query(
        self,
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,
    ) -> tuple[list[Execution], str | None]:
        """Query executions with efficient SQL filtering."""
        try:
            # Build query safely with parameterized conditions
            conditions: list[str] = []
            params: list[str | float | int] = []

            if function_name:
                conditions.append("function_name = ?")
                params.append(function_name)

            if execution_name:
                conditions.append("execution_name = ?")
                params.append(execution_name)

            if status_filter:
                conditions.append("status = ?")
                params.append(status_filter)

            if started_after:
                started_after_float: float = datetime.fromisoformat(
                    started_after
                ).timestamp()
                conditions.append("start_timestamp >= ?")
                params.append(started_after_float)

            if started_before:
                started_before_float: float = datetime.fromisoformat(
                    started_before
                ).timestamp()
                conditions.append("start_timestamp <= ?")
                params.append(started_before_float)

            # Build WHERE clause safely
            where_clause: str = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            # Build ORDER BY clause
            order_direction: str = "DESC" if reverse_order else "ASC"
            order_clause: str = f"ORDER BY start_timestamp {order_direction}"

            # For better performance, only get metadata for counting and pagination
            base_query: str = f"FROM executions {where_clause}"
            count_query: str = f"SELECT COUNT(*) {base_query}"

            limit_exists: bool = limit is not None and limit > 0

            # Only fetch data we need
            if limit_exists:
                data_query: str = f"SELECT durable_execution_arn, data {base_query} {order_clause} LIMIT ? OFFSET ?"
                params_with_limit: list[str | float | int] = params + [
                    cast(int, limit),
                    offset,
                ]
            else:
                data_query = (
                    f"SELECT durable_execution_arn, data {base_query} {order_clause}"
                )
                params_with_limit = params

            with self._get_connection() as conn:
                # Get total count for pagination
                total_count: int = int(conn.execute(count_query, params).fetchone()[0])

                # Get actual data
                cursor: sqlite3.Cursor = conn.execute(data_query, params_with_limit)
                rows: list[tuple[str, str]] = cursor.fetchall()

            # Only deserialize the executions we actually need
            executions: list[Execution] = []
            for durable_execution_arn, data in rows:
                try:
                    executions.append(Execution.from_json_dict(json.loads(data)))
                except (json.JSONDecodeError, ValueError) as e:
                    # Log corrupted data but continue with other records
                    print(
                        f"Warning: Skipping corrupted execution {durable_execution_arn}: {e}"
                    )
                    continue

            # Calculate pagination
            has_more: bool = limit_exists and (offset + len(executions) < total_count)
            next_marker: str | None = (
                str(offset + len(executions)) if has_more else None
            )

            return executions, next_marker

        except sqlite3.Error as e:
            raise RuntimeException(f"Query failed: {e}") from e
        except ValueError as e:
            raise InvalidParameterValueException(
                f"Invalid query parameters: {e}"
            ) from e

    def list_all(self) -> list[Execution]:
        """List all executions (for backward compatibility)."""
        executions, _ = self.query()
        return executions

    def get_execution_metadata(self, execution_arn: str) -> dict[str, Any] | None:
        """Get just the metadata without full deserialization for performance."""
        try:
            with self._get_connection() as conn:
                cursor: sqlite3.Cursor = conn.execute(
                    "SELECT function_name, execution_name, status, start_timestamp, end_timestamp FROM executions WHERE durable_execution_arn = ?",
                    (execution_arn,),
                )
                row: tuple[str, str | None, str, float | None, float | None] | None = (
                    cursor.fetchone()
                )

            if not row:
                return None

            return {
                "durable_execution_arn": execution_arn,
                "function_name": row[0],
                "execution_name": row[1],
                "status": row[2],
                "start_timestamp": row[3],
                "end_timestamp": row[4],
            }
        except sqlite3.Error as e:
            raise RuntimeError(
                f"Failed to get metadata for {execution_arn}: {e}"
            ) from e

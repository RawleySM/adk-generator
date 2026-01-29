#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb>=0.9.0",
#   "pyarrow>=14.0.0",
#   "pandas>=2.0.0",
#   "databricks-sdk>=0.20.0",
# ]
# ///
"""Sync local DuckDB state to Unity Catalog Delta tables.

This utility provides optional post-development synchronization of local ADK state
(sessions, events, telemetry, artifacts) to UC Delta tables. It enables developers
to work locally with DuckDB and then push their data to Databricks when needed.

Features:
- Export local DuckDB tables to Parquet format
- MERGE/INSERT into UC Delta tables via SQL Warehouse API
- Namespace strategy with app_name prefix to avoid collisions

Tables synced:
- sessions
- events
- app_states
- user_states
- adk_telemetry
- artifact_registry

Usage:
    # Sync with defaults (from env vars)
    uv run databricks_rlm_agent/sync_to_uc.py

    # Sync with explicit parameters
    uv run databricks_rlm_agent/sync_to_uc.py \\
        --db-path .adk_local/adk.duckdb \\
        --catalog silo_dev_rs \\
        --schema adk \\
        --profile rstanhope \\
        --app-name-prefix databricks_rlm_agent_local

Environment Variables:
    ADK_LOCAL_DB_PATH: Path to local DuckDB database (default: .adk_local/adk.duckdb)
    ADK_DELTA_CATALOG: Target UC catalog (default: silo_dev_rs)
    ADK_DELTA_SCHEMA: Target UC schema (default: adk)
    DATABRICKS_PROFILE: Databricks CLI profile (default: rstanhope)
    ADK_SQL_WAREHOUSE_ID: SQL warehouse ID (auto-discovered if not set)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default configuration from environment
DEFAULT_DB_PATH = os.environ.get("ADK_LOCAL_DB_PATH", ".adk_local/adk.duckdb")
DEFAULT_CATALOG = os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs")
DEFAULT_SCHEMA = os.environ.get("ADK_DELTA_SCHEMA", "adk")
DEFAULT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
DEFAULT_APP_NAME_PREFIX = "databricks_rlm_agent_local"


# Table definitions for synchronization
# Maps local table name -> (UC table name, primary key columns, has_app_name_column)
TABLE_CONFIGS: dict[str, tuple[str, list[str], bool]] = {
    "sessions": ("sessions", ["app_name", "user_id", "session_id"], True),
    "events": ("events", ["app_name", "user_id", "session_id", "event_id"], True),
    "app_states": ("app_states", ["app_name"], True),
    "user_states": ("user_states", ["app_name", "user_id"], True),
    "adk_telemetry": ("adk_telemetry", ["telemetry_id"], True),
    "artifact_registry": ("artifact_registry", ["artifact_id"], False),
}


@dataclass
class SyncResult:
    """Result from a table sync operation."""

    table_name: str
    rows_exported: int = 0
    rows_merged: int = 0
    rows_inserted: int = 0
    success: bool = True
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


@dataclass
class SyncReport:
    """Complete sync report across all tables."""

    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    tables: list[SyncResult] = field(default_factory=list)
    total_rows_synced: int = 0
    success: bool = True
    error_message: Optional[str] = None

    def add_result(self, result: SyncResult) -> None:
        """Add a table result to the report."""
        self.tables.append(result)
        if result.success:
            self.total_rows_synced += result.rows_merged + result.rows_inserted
        else:
            self.success = False

    def finalize(self) -> None:
        """Mark the sync as complete."""
        self.completed_at = datetime.now(timezone.utc)

    def summary(self) -> str:
        """Generate a human-readable summary."""
        lines = [
            "=" * 60,
            "SYNC REPORT",
            "=" * 60,
            f"Started:   {self.started_at.isoformat()}",
            f"Completed: {self.completed_at.isoformat() if self.completed_at else 'In Progress'}",
            f"Status:    {'SUCCESS' if self.success else 'FAILED'}",
            f"Total Rows Synced: {self.total_rows_synced}",
            "-" * 60,
            "TABLE DETAILS:",
        ]

        for result in self.tables:
            status = "OK" if result.success else "FAILED"
            lines.append(
                f"  {result.table_name:20s} | {status:6s} | "
                f"Exported: {result.rows_exported:5d} | "
                f"Merged: {result.rows_merged:5d} | "
                f"Duration: {result.duration_seconds:.2f}s"
            )
            if result.error_message:
                lines.append(f"    ERROR: {result.error_message}")

        lines.append("=" * 60)

        if self.error_message:
            lines.append(f"OVERALL ERROR: {self.error_message}")

        return "\n".join(lines)


class LocalToUCSyncer:
    """Synchronizes local DuckDB state to Unity Catalog Delta tables.

    This class handles the export of local DuckDB tables to Parquet format
    and MERGE operations into UC Delta tables via SQL Warehouse API.

    Example:
        >>> syncer = LocalToUCSyncer(
        ...     db_path=".adk_local/adk.duckdb",
        ...     catalog="silo_dev_rs",
        ...     schema="adk",
        ...     profile="rstanhope",
        ... )
        >>> report = syncer.sync_all()
        >>> print(report.summary())
    """

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        profile: str = DEFAULT_PROFILE,
        app_name_prefix: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        staging_volume: Optional[str] = None,
    ):
        """Initialize the syncer.

        Args:
            db_path: Path to the local DuckDB database file.
            catalog: Target Unity Catalog name.
            schema: Target schema within the catalog.
            profile: Databricks CLI profile for authentication.
            app_name_prefix: Optional prefix to apply to app_name column values
                            to namespace local data (e.g., 'databricks_rlm_agent_local').
            warehouse_id: Optional SQL warehouse ID (auto-discovered if not set).
            staging_volume: Optional UC volume path for staging Parquet files.
                           If not set, uses SQL MERGE with inline values.
        """
        self._check_dependencies()

        self._db_path = Path(db_path)
        self._catalog = catalog
        self._schema = schema
        self._profile = profile
        self._app_name_prefix = app_name_prefix
        self._warehouse_id = warehouse_id or os.environ.get("ADK_SQL_WAREHOUSE_ID")
        self._staging_volume = staging_volume

        # Validate database exists
        if not self._db_path.exists():
            raise FileNotFoundError(f"DuckDB database not found: {self._db_path}")

        # Initialize DuckDB connection
        self._conn = duckdb.connect(str(self._db_path), read_only=True)

        # Lazy-initialized Databricks client
        self._client = None

        logger.info(
            f"LocalToUCSyncer initialized: db={db_path}, "
            f"target={catalog}.{schema}, profile={profile}"
        )

    def _check_dependencies(self) -> None:
        """Verify required dependencies are installed."""
        missing = []
        if duckdb is None:
            missing.append("duckdb")
        if pd is None:
            missing.append("pandas")
        if pa is None:
            missing.append("pyarrow")

        if missing:
            raise ImportError(
                f"Missing required dependencies: {', '.join(missing)}. "
                f"Install with: uv pip install {' '.join(missing)}"
            )

    def _get_databricks_client(self):
        """Get or create Databricks WorkspaceClient."""
        if self._client is not None:
            return self._client

        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as e:
            raise ImportError(
                "databricks-sdk is required for UC sync. "
                "Install with: uv pip install databricks-sdk"
            ) from e

        # Try profile-based auth first
        try:
            client = WorkspaceClient(profile=self._profile)
            client.current_user.me()  # Verify auth
            logger.info(f"Authenticated using profile: {self._profile}")
            self._client = client
            return client
        except Exception as e:
            logger.debug(f"Profile auth failed: {e}")

        # Fall back to default auth
        try:
            client = WorkspaceClient()
            client.current_user.me()
            logger.info("Using default Databricks authentication")
            self._client = client
            return client
        except Exception as e:
            raise RuntimeError(
                f"Could not authenticate to Databricks. Tried profile '{self._profile}' "
                "and default credentials. Run 'databricks auth login --profile <profile>'"
            ) from e

    def _get_warehouse_id(self) -> str:
        """Get SQL warehouse ID for statement execution."""
        if self._warehouse_id:
            return self._warehouse_id

        client = self._get_databricks_client()
        warehouses = list(client.warehouses.list())

        # Prefer running warehouses
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                logger.info(f"Using SQL warehouse: {wh.name} (id: {wh.id})")
                self._warehouse_id = wh.id
                return wh.id

        # Try starting a stopped warehouse
        for wh in warehouses:
            if wh.state and wh.state.value == "STOPPED":
                logger.info(f"Starting SQL warehouse: {wh.name}")
                client.warehouses.start(wh.id)
                for _ in range(30):
                    status = client.warehouses.get(wh.id)
                    if status.state and status.state.value == "RUNNING":
                        logger.info(f"SQL warehouse started: {wh.name}")
                        self._warehouse_id = wh.id
                        return wh.id
                    time.sleep(10)

        raise RuntimeError(
            "No SQL warehouse available. Please set ADK_SQL_WAREHOUSE_ID "
            "or ensure a warehouse is running."
        )

    def _execute_sql(self, sql: str, wait_timeout: str = "300s") -> Any:
        """Execute SQL via Databricks SQL Warehouse.

        Args:
            sql: SQL statement to execute.
            wait_timeout: Timeout for statement execution.

        Returns:
            Statement execution response.

        Raises:
            RuntimeError: If SQL execution fails.
        """
        from databricks.sdk.service.sql import StatementState

        client = self._get_databricks_client()
        warehouse_id = self._get_warehouse_id()

        logger.debug(f"Executing SQL: {sql[:200]}..." if len(sql) > 200 else f"Executing SQL: {sql}")

        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            catalog=self._catalog,
            schema=self._schema,
            wait_timeout=wait_timeout,
        )

        # Poll for completion
        while response.status and response.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            time.sleep(1)
            response = client.statement_execution.get_statement(response.statement_id)

        # Check for errors
        if response.status and response.status.state == StatementState.FAILED:
            error_msg = "SQL execution failed"
            if response.status.error:
                error_msg = f"{error_msg}: {response.status.error.message}"
            raise RuntimeError(error_msg)

        if response.status and response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(
                f"SQL execution did not succeed. State: {response.status.state}"
            )

        return response

    def _get_local_table_df(self, table_name: str) -> "pd.DataFrame":
        """Export a local DuckDB table to a pandas DataFrame.

        Args:
            table_name: Name of the table in DuckDB.

        Returns:
            DataFrame containing all rows from the table.
        """
        # Check if table exists using DuckDB's information_schema
        tables = self._conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
            [table_name]
        ).fetchall()

        if not tables:
            logger.warning(f"Table '{table_name}' does not exist in local database")
            return pd.DataFrame()

        df = self._conn.execute(f"SELECT * FROM {table_name}").df()
        logger.info(f"Exported {len(df)} rows from local table '{table_name}'")
        return df

    def _apply_app_name_prefix(
        self,
        df: "pd.DataFrame",
        has_app_name: bool,
    ) -> "pd.DataFrame":
        """Apply app_name prefix to namespace local data.

        Args:
            df: DataFrame to modify.
            has_app_name: Whether the table has an app_name column.

        Returns:
            Modified DataFrame with prefixed app_name values.
        """
        if not self._app_name_prefix or not has_app_name:
            return df

        if "app_name" not in df.columns:
            return df

        # Apply prefix to app_name values
        df = df.copy()
        df["app_name"] = df["app_name"].apply(
            lambda x: f"{self._app_name_prefix}_{x}" if x and not x.startswith(self._app_name_prefix) else x
        )
        logger.info(f"Applied app_name prefix: {self._app_name_prefix}")
        return df

    def _escape_sql_value(self, value: Any) -> str:
        """Escape a value for safe SQL insertion.

        Args:
            value: Value to escape.

        Returns:
            SQL-safe string representation of the value.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            return f"TIMESTAMP '{value.isoformat()}'"
        if isinstance(value, pd.Timestamp):
            return f"TIMESTAMP '{value.isoformat()}'"

        # String values - escape single quotes
        str_val = str(value).replace("'", "''")
        return f"'{str_val}'"

    def _build_merge_sql(
        self,
        uc_table: str,
        df: "pd.DataFrame",
        primary_keys: list[str],
    ) -> str:
        """Build MERGE SQL statement for upserting data.

        Uses inline VALUES for small datasets, which works well via SQL Warehouse.

        Args:
            uc_table: Fully qualified UC table name.
            df: DataFrame with data to merge.
            primary_keys: List of primary key column names.

        Returns:
            SQL MERGE statement.
        """
        if df.empty:
            return ""

        columns = df.columns.tolist()

        # Build VALUES clause
        values_rows = []
        for _, row in df.iterrows():
            row_values = [self._escape_sql_value(row[col]) for col in columns]
            values_rows.append(f"({', '.join(row_values)})")

        # Build column aliases for source
        source_columns = ", ".join([f"col{i} AS {col}" for i, col in enumerate(columns)])

        # Build ON clause from primary keys
        on_conditions = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

        # Build UPDATE SET clause (all non-PK columns)
        non_pk_columns = [c for c in columns if c not in primary_keys]
        update_set = ", ".join([f"{col} = source.{col}" for col in non_pk_columns])

        # Build INSERT columns and values
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        sql = f"""
MERGE INTO {uc_table} AS target
USING (
    SELECT {source_columns}
    FROM VALUES
    {', '.join(values_rows)}
) AS source
ON {on_conditions}
WHEN MATCHED THEN
    UPDATE SET {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns})
    VALUES ({insert_values})
"""
        return sql.strip()

    def _sync_table(
        self,
        local_table: str,
        uc_table_name: str,
        primary_keys: list[str],
        has_app_name: bool,
    ) -> SyncResult:
        """Sync a single table from local DuckDB to UC Delta.

        Args:
            local_table: Local DuckDB table name.
            uc_table_name: UC Delta table name (without catalog.schema prefix).
            primary_keys: Primary key columns for MERGE.
            has_app_name: Whether table has app_name column for prefix.

        Returns:
            SyncResult with operation details.
        """
        result = SyncResult(table_name=local_table)
        start_time = time.time()

        try:
            # Export local table
            df = self._get_local_table_df(local_table)
            result.rows_exported = len(df)

            if df.empty:
                logger.info(f"Skipping empty table: {local_table}")
                result.duration_seconds = time.time() - start_time
                return result

            # Apply app_name prefix if configured
            df = self._apply_app_name_prefix(df, has_app_name)

            # Build fully qualified table name
            full_table_name = f"{self._catalog}.{self._schema}.{uc_table_name}"

            # Batch large datasets (SQL Warehouse has limits on statement size)
            batch_size = 100  # Conservative batch size for inline VALUES
            total_merged = 0

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]

                # Build and execute MERGE
                merge_sql = self._build_merge_sql(full_table_name, batch_df, primary_keys)
                if merge_sql:
                    self._execute_sql(merge_sql)
                    total_merged += len(batch_df)
                    logger.debug(
                        f"Merged batch {i // batch_size + 1}: "
                        f"{len(batch_df)} rows into {full_table_name}"
                    )

            result.rows_merged = total_merged
            result.success = True
            logger.info(
                f"Successfully synced {total_merged} rows to {full_table_name}"
            )

        except Exception as e:
            result.success = False
            result.error_message = str(e)
            logger.error(f"Failed to sync table {local_table}: {e}")

        result.duration_seconds = time.time() - start_time
        return result

    def sync_table(self, table_name: str) -> SyncResult:
        """Sync a specific table to UC.

        Args:
            table_name: Name of the table to sync.

        Returns:
            SyncResult with operation details.

        Raises:
            ValueError: If table name is not in the supported list.
        """
        if table_name not in TABLE_CONFIGS:
            raise ValueError(
                f"Unknown table: {table_name}. "
                f"Supported tables: {list(TABLE_CONFIGS.keys())}"
            )

        uc_table, primary_keys, has_app_name = TABLE_CONFIGS[table_name]
        return self._sync_table(table_name, uc_table, primary_keys, has_app_name)

    def sync_all(self, tables: Optional[list[str]] = None) -> SyncReport:
        """Sync all tables (or specified subset) to UC.

        Args:
            tables: Optional list of specific tables to sync.
                   If None, syncs all supported tables.

        Returns:
            SyncReport with complete operation details.
        """
        report = SyncReport()

        tables_to_sync = tables or list(TABLE_CONFIGS.keys())

        logger.info(f"Starting sync of {len(tables_to_sync)} tables to UC")

        for table_name in tables_to_sync:
            if table_name not in TABLE_CONFIGS:
                logger.warning(f"Skipping unknown table: {table_name}")
                continue

            logger.info(f"Syncing table: {table_name}")
            result = self.sync_table(table_name)
            report.add_result(result)

        report.finalize()
        return report

    def export_to_parquet(
        self,
        output_dir: str,
        tables: Optional[list[str]] = None,
    ) -> dict[str, Path]:
        """Export local tables to Parquet files.

        This is useful for manual upload or inspection of local data.

        Args:
            output_dir: Directory to write Parquet files.
            tables: Optional list of specific tables to export.
                   If None, exports all supported tables.

        Returns:
            Dictionary mapping table names to output file paths.
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        tables_to_export = tables or list(TABLE_CONFIGS.keys())
        result_paths: dict[str, Path] = {}

        for table_name in tables_to_export:
            if table_name not in TABLE_CONFIGS:
                logger.warning(f"Skipping unknown table: {table_name}")
                continue

            _, _, has_app_name = TABLE_CONFIGS[table_name]

            df = self._get_local_table_df(table_name)
            if df.empty:
                logger.info(f"Skipping empty table: {table_name}")
                continue

            # Apply app_name prefix
            df = self._apply_app_name_prefix(df, has_app_name)

            # Write Parquet
            file_path = output_path / f"{table_name}.parquet"
            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)

            result_paths[table_name] = file_path
            logger.info(f"Exported {len(df)} rows to {file_path}")

        return result_paths

    def close(self) -> None:
        """Close database connections."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Closed DuckDB connection")


def main() -> int:
    """Main entry point for CLI usage.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    parser = argparse.ArgumentParser(
        description="Sync local DuckDB state to Unity Catalog Delta tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help=f"Path to local DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help=f"Target UC catalog (default: {DEFAULT_CATALOG})",
    )
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help=f"Target UC schema (default: {DEFAULT_SCHEMA})",
    )
    parser.add_argument(
        "--profile",
        default=DEFAULT_PROFILE,
        help=f"Databricks CLI profile (default: {DEFAULT_PROFILE})",
    )
    parser.add_argument(
        "--app-name-prefix",
        default=DEFAULT_APP_NAME_PREFIX,
        help=f"Prefix for app_name values to namespace local data (default: {DEFAULT_APP_NAME_PREFIX})",
    )
    parser.add_argument(
        "--warehouse-id",
        default=None,
        help="SQL warehouse ID (auto-discovered if not set)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(TABLE_CONFIGS.keys()),
        help="Specific tables to sync (default: all)",
    )
    parser.add_argument(
        "--export-only",
        metavar="DIR",
        help="Export tables to Parquet files in the specified directory (no UC sync)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without making changes",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        syncer = LocalToUCSyncer(
            db_path=args.db_path,
            catalog=args.catalog,
            schema=args.schema,
            profile=args.profile,
            app_name_prefix=args.app_name_prefix,
            warehouse_id=args.warehouse_id,
        )

        if args.export_only:
            # Export to Parquet only
            paths = syncer.export_to_parquet(args.export_only, args.tables)
            print(f"\nExported {len(paths)} tables to {args.export_only}")
            for table, path in paths.items():
                print(f"  {table}: {path}")
            return 0

        if args.dry_run:
            # Dry run - just show what would be synced
            print("DRY RUN - Would sync the following tables:")
            tables = args.tables or list(TABLE_CONFIGS.keys())
            for table in tables:
                df = syncer._get_local_table_df(table)
                print(f"  {table}: {len(df)} rows")
            return 0

        # Perform actual sync
        report = syncer.sync_all(args.tables)
        print(report.summary())

        syncer.close()

        return 0 if report.success else 1

    except FileNotFoundError as e:
        logger.error(str(e))
        print(f"ERROR: {e}")
        return 1
    except ImportError as e:
        logger.error(str(e))
        print(f"ERROR: {e}")
        return 1
    except Exception as e:
        logger.exception("Sync failed with unexpected error")
        print(f"ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

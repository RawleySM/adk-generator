#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
# ]
# ///
"""Enable Change Data Feed (CDF) on the JIRA trigger table.

This is a one-time setup script that enables CDF on the trigger table
so that the ingestor can poll for new tasks efficiently using table_changes().

Usage:
    uv run scripts/enable_cdf_jira_table.py
    uv run scripts/enable_cdf_jira_table.py --dry-run
    uv run scripts/enable_cdf_jira_table.py --table silo_dev_rs.task.jira_raw_data

Note: CDF can only be enabled on Delta tables. Once enabled, CDF tracks
all changes (inserts, updates, deletes) with metadata columns:
    - _change_type: insert, update_preimage, update_postimage, delete
    - _commit_version: The Delta version of the change
    - _commit_timestamp: When the change was committed
"""

import argparse
import logging
import sys
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Default table to enable CDF on
DEFAULT_TABLE = "silo_dev_rs.task.jira_raw_data"
DEFAULT_PROFILE = "rstanhope"


def check_cdf_status(
    table_name: str,
    profile: str = DEFAULT_PROFILE,
) -> bool:
    """Check if CDF is already enabled on the table.

    Args:
        table_name: Fully qualified table name.
        profile: Databricks CLI profile.

    Returns:
        True if CDF is enabled, False otherwise.
    """
    from databricks.sdk import WorkspaceClient

    logger.info(f"Checking CDF status for {table_name}...")

    client = WorkspaceClient(profile=profile)

    # Parse catalog.schema.table
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Table name must be fully qualified (catalog.schema.table): {table_name}")

    catalog, schema, table = parts

    # Get table properties using SQL
    try:
        # Use the statement execution API to run SQL
        response = client.statement_execution.execute_statement(
            warehouse_id=get_sql_warehouse_id(client),
            statement=f"SHOW TBLPROPERTIES {table_name}",
            wait_timeout="30s",
        )

        if response.result and response.result.data_array:
            for row in response.result.data_array:
                if len(row) >= 2:
                    key, value = row[0], row[1]
                    if key == "delta.enableChangeDataFeed" and value.lower() == "true":
                        logger.info(f"CDF is already enabled on {table_name}")
                        return True

        logger.info(f"CDF is NOT enabled on {table_name}")
        return False

    except Exception as e:
        logger.warning(f"Could not check CDF status via SQL: {e}")
        # Fall back to assuming it's not enabled
        return False


def get_sql_warehouse_id(client) -> str:
    """Get a SQL warehouse ID to execute statements.

    Args:
        client: WorkspaceClient instance.

    Returns:
        The ID of a running SQL warehouse.

    Raises:
        RuntimeError: If no SQL warehouse is available.
    """
    warehouses = list(client.warehouses.list())

    # Prefer running warehouses
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            return wh.id

    # Try starting a stopped warehouse
    for wh in warehouses:
        if wh.state and wh.state.value == "STOPPED":
            logger.info(f"Starting SQL warehouse: {wh.name}")
            client.warehouses.start(wh.id)
            # Wait for it to start (simplified - in production use proper polling)
            import time
            for _ in range(30):
                status = client.warehouses.get(wh.id)
                if status.state and status.state.value == "RUNNING":
                    return wh.id
                time.sleep(10)

    raise RuntimeError("No SQL warehouse available. Please create or start one.")


def enable_cdf(
    table_name: str,
    profile: str = DEFAULT_PROFILE,
    dry_run: bool = False,
) -> bool:
    """Enable CDF on the specified table.

    Args:
        table_name: Fully qualified table name.
        profile: Databricks CLI profile.
        dry_run: If True, only show what would be done.

    Returns:
        True if CDF was enabled (or would be enabled in dry run), False otherwise.
    """
    from databricks.sdk import WorkspaceClient

    # Check if already enabled
    if check_cdf_status(table_name, profile):
        logger.info("CDF is already enabled - no action needed")
        return True

    sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"

    if dry_run:
        logger.info(f"[DRY RUN] Would execute: {sql}")
        return True

    logger.info(f"Enabling CDF on {table_name}...")
    logger.info(f"Executing: {sql}")

    client = WorkspaceClient(profile=profile)

    try:
        response = client.statement_execution.execute_statement(
            warehouse_id=get_sql_warehouse_id(client),
            statement=sql,
            wait_timeout="50s",
        )

        if response.status and response.status.state.value == "SUCCEEDED":
            logger.info(f"CDF enabled successfully on {table_name}")
            return True
        else:
            error = response.status.error if response.status else "Unknown error"
            logger.error(f"Failed to enable CDF: {error}")
            return False

    except Exception as e:
        logger.error(f"Error enabling CDF: {e}")
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Enable Change Data Feed (CDF) on a Delta table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Fully qualified table name (default: {DEFAULT_TABLE})",
    )
    parser.add_argument(
        "--profile",
        default=DEFAULT_PROFILE,
        help=f"Databricks CLI profile (default: {DEFAULT_PROFILE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only check if CDF is enabled, don't enable it",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Enable CDF on JIRA Trigger Table")
    logger.info("=" * 60)
    logger.info(f"Table: {args.table}")
    logger.info(f"Profile: {args.profile}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info("=" * 60)

    if args.check_only:
        is_enabled = check_cdf_status(args.table, args.profile)
        return 0 if is_enabled else 1

    success = enable_cdf(
        table_name=args.table,
        profile=args.profile,
        dry_run=args.dry_run,
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

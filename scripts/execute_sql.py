#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
#   "click>=8.1.0",
# ]
# ///
"""Execute arbitrary SQL against a Databricks Unity Catalog table.

This script uses the Databricks SDK with a configured profile to execute SQL
statements via the SQL Statement Execution API.

Examples:
    uv run scripts/execute_sql.py --sql "SELECT * FROM my_table LIMIT 10"
    uv run scripts/execute_sql.py --table catalog.schema.table --sql "DESCRIBE HISTORY {table} LIMIT 1"
    uv run scripts/execute_sql.py --sql-file queries.sql
    uv run scripts/execute_sql.py history silo_dev_rs.task.jira_raw_data
    uv run scripts/execute_sql.py select silo_dev_rs.adk.ingestor_state --where "table_name LIKE '%jira%'"

The {table} placeholder in SQL will be replaced with the --table argument value.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_PROFILE = "rstanhope"


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
            logger.info(f"Using SQL warehouse: {wh.name} (id: {wh.id})")
            return wh.id

    # Try starting a stopped warehouse
    for wh in warehouses:
        if wh.state and wh.state.value == "STOPPED":
            logger.info(f"Starting SQL warehouse: {wh.name}")
            client.warehouses.start(wh.id)
            for _ in range(30):
                status = client.warehouses.get(wh.id)
                if status.state and status.state.value == "RUNNING":
                    return wh.id
                time.sleep(10)

    raise RuntimeError("No SQL warehouse available. Please create or start one.")


def execute_sql(
    sql: str,
    profile: str = DEFAULT_PROFILE,
    dry_run: bool = False,
) -> tuple[bool, Optional[list]]:
    """Execute SQL statement.

    Args:
        sql: SQL statement to execute.
        profile: Databricks CLI profile.
        dry_run: If True, only show what would be done.

    Returns:
        Tuple of (success, results).
    """
    from databricks.sdk import WorkspaceClient

    if dry_run:
        click.secho(f"[DRY RUN] Would execute:", fg="yellow", bold=True)
        click.echo(sql)
        return True, None

    click.secho("Executing SQL:", fg="cyan", bold=True)
    click.echo(sql)
    click.echo("-" * 60)

    client = WorkspaceClient(profile=profile)

    try:
        warehouse_id = get_sql_warehouse_id(client)
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s",
        )

        if response.status and response.status.state.value == "SUCCEEDED":
            click.secho("✓ SQL executed successfully!", fg="green", bold=True)

            # Print results if any
            if response.manifest and response.manifest.schema:
                columns = [col.name for col in response.manifest.schema.columns]
                click.secho(f"Columns: {columns}", fg="blue")

            if response.result and response.result.data_array:
                click.secho(f"Results ({len(response.result.data_array)} rows):", fg="blue")
                for i, row in enumerate(response.result.data_array):
                    click.echo(f"  Row {i}: {row}")
                return True, response.result.data_array

            return True, []
        else:
            error = response.status.error if response.status else "Unknown error"
            click.secho(f"✗ SQL execution failed: {error}", fg="red", bold=True)
            return False, None

    except Exception as e:
        click.secho(f"✗ Error executing SQL: {e}", fg="red", bold=True)
        return False, None


HELP_EPILOG = """
\b
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUICK REFERENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

\b
Direct SQL:
  execute_sql.py --sql "SELECT * FROM catalog.schema.table LIMIT 10"
  execute_sql.py -t my.table --sql "DESCRIBE HISTORY {table}"
  execute_sql.py --sql-file query.sql

\b
Common Subcommands:
  execute_sql.py version  TABLE          → Get current Delta version
  execute_sql.py history  TABLE [-l N]   → Show version history
  execute_sql.py select   TABLE [-w X]   → Query with WHERE clause
  execute_sql.py describe TABLE          → Show schema/metadata
  execute_sql.py properties TABLE        → Show table properties (CDF status)
  execute_sql.py cdf TABLE VERSION       → Read Change Data Feed

\b
Examples:
  execute_sql.py version silo_dev_rs.task.jira_raw_data
  execute_sql.py select silo_dev_rs.adk.ingestor_state -w "table_name LIKE '%jira%'"
  execute_sql.py cdf silo_dev_rs.task.jira_raw_data 3 --limit 50
  execute_sql.py history silo_dev_rs.task.jira_raw_data --limit 5

\b
Global Options (work with all commands):
  -p, --profile   Databricks CLI profile [default: rstanhope]
  -n, --dry-run   Preview SQL without executing

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


@click.group(invoke_without_command=True, epilog=HELP_EPILOG)
@click.option(
    "--sql",
    help="SQL statement to execute. Use {table} as placeholder for table name.",
)
@click.option(
    "--sql-file",
    type=click.Path(exists=True, path_type=Path),
    help="File containing SQL statement(s) to execute.",
)
@click.option(
    "--table", "-t",
    metavar="CATALOG.SCHEMA.TABLE",
    help="Fully qualified table name. Replaces {table} placeholder in SQL.",
)
@click.option(
    "--profile", "-p",
    default=DEFAULT_PROFILE,
    show_default=True,
    envvar="DATABRICKS_PROFILE",
    help="Databricks CLI profile for authentication.",
)
@click.option(
    "--dry-run", "-n",
    is_flag=True,
    help="Show SQL without executing.",
)
@click.version_option(version="1.0.0", prog_name="execute_sql")
@click.pass_context
def cli(ctx: click.Context, sql: str, sql_file: Path, table: str, profile: str, dry_run: bool):
    """Execute SQL against Databricks Unity Catalog.

    A CLI tool for running SQL queries and common table operations against
    Databricks using the SQL Statement Execution API.

    \b
    MODES OF OPERATION:
      1. Direct SQL    → Use --sql or --sql-file options
      2. Subcommands   → Use built-in commands like 'select', 'history', 'cdf'

    Run 'execute_sql.py COMMAND --help' for subcommand-specific help.
    """
    ctx.ensure_object(dict)
    ctx.obj["profile"] = profile
    ctx.obj["dry_run"] = dry_run

    # If a subcommand is invoked, let it handle things
    if ctx.invoked_subcommand is not None:
        return

    # Otherwise, handle direct SQL execution
    if sql:
        final_sql = sql
    elif sql_file:
        final_sql = sql_file.read_text()
    else:
        click.echo(ctx.get_help())
        return

    # Replace {table} placeholder if table is provided
    if table:
        final_sql = final_sql.replace("{table}", table)

    click.secho("=" * 60, fg="cyan")
    click.secho("Databricks SQL Executor", fg="cyan", bold=True)
    click.secho("=" * 60, fg="cyan")
    click.echo(f"Profile: {profile}")
    if table:
        click.echo(f"Table: {table}")
    click.echo(f"Dry Run: {dry_run}")
    click.secho("=" * 60, fg="cyan")

    success, _ = execute_sql(sql=final_sql, profile=profile, dry_run=dry_run)
    if not success:
        sys.exit(1)


@cli.command()
@click.argument("table")
@click.option("--limit", "-l", default=10, show_default=True, help="Number of history records to show.")
@click.pass_context
def history(ctx: click.Context, table: str, limit: int):
    """Show version history for a Delta table.

    \b
    Example:
        uv run scripts/execute_sql.py history silo_dev_rs.task.jira_raw_data
        uv run scripts/execute_sql.py history silo_dev_rs.task.jira_raw_data --limit 5
    """
    sql = f"DESCRIBE HISTORY {table} LIMIT {limit}"
    success, _ = execute_sql(sql=sql, profile=ctx.obj["profile"], dry_run=ctx.obj["dry_run"])
    if not success:
        sys.exit(1)


@cli.command()
@click.argument("table")
@click.option("--limit", "-l", default=10, show_default=True, help="Maximum rows to return.")
@click.option("--where", "-w", help="WHERE clause condition.")
@click.option("--columns", "-c", default="*", show_default=True, help="Columns to select (comma-separated).")
@click.pass_context
def select(ctx: click.Context, table: str, limit: int, where: str, columns: str):
    """Query data from a table.

    \b
    Examples:
        uv run scripts/execute_sql.py select silo_dev_rs.adk.ingestor_state
        uv run scripts/execute_sql.py select silo_dev_rs.adk.ingestor_state -w "table_name LIKE '%jira%'"
        uv run scripts/execute_sql.py select silo_dev_rs.task.jira_raw_data -c "id,name" -l 5
    """
    sql = f"SELECT {columns} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {limit}"

    success, _ = execute_sql(sql=sql, profile=ctx.obj["profile"], dry_run=ctx.obj["dry_run"])
    if not success:
        sys.exit(1)


@cli.command()
@click.argument("table")
@click.pass_context
def describe(ctx: click.Context, table: str):
    """Show table schema and metadata.

    \b
    Example:
        uv run scripts/execute_sql.py describe silo_dev_rs.task.jira_raw_data
    """
    sql = f"DESCRIBE TABLE EXTENDED {table}"
    success, _ = execute_sql(sql=sql, profile=ctx.obj["profile"], dry_run=ctx.obj["dry_run"])
    if not success:
        sys.exit(1)


@cli.command()
@click.argument("table")
@click.pass_context
def properties(ctx: click.Context, table: str):
    """Show table properties (including CDF status).

    \b
    Example:
        uv run scripts/execute_sql.py properties silo_dev_rs.task.jira_raw_data
    """
    sql = f"SHOW TBLPROPERTIES {table}"
    success, _ = execute_sql(sql=sql, profile=ctx.obj["profile"], dry_run=ctx.obj["dry_run"])
    if not success:
        sys.exit(1)


@cli.command()
@click.argument("table")
@click.argument("start_version", type=int)
@click.option("--end-version", "-e", type=int, help="End version (defaults to latest).")
@click.option("--limit", "-l", default=100, show_default=True, help="Maximum rows to return.")
@click.pass_context
def cdf(ctx: click.Context, table: str, start_version: int, end_version: int, limit: int):
    """Read Change Data Feed (CDF) for a table.

    \b
    Examples:
        uv run scripts/execute_sql.py cdf silo_dev_rs.task.jira_raw_data 3
        uv run scripts/execute_sql.py cdf silo_dev_rs.task.jira_raw_data 3 -e 5
    """
    if end_version:
        sql = f"SELECT * FROM table_changes('{table}', {start_version}, {end_version}) LIMIT {limit}"
    else:
        sql = f"SELECT * FROM table_changes('{table}', {start_version}) LIMIT {limit}"

    success, _ = execute_sql(sql=sql, profile=ctx.obj["profile"], dry_run=ctx.obj["dry_run"])
    if not success:
        sys.exit(1)


@cli.command()
@click.argument("table")
@click.pass_context
def version(ctx: click.Context, table: str):
    """Get current version number of a Delta table.

    \b
    Example:
        uv run scripts/execute_sql.py version silo_dev_rs.task.jira_raw_data
    """
    sql = f"DESCRIBE HISTORY {table} LIMIT 1"
    success, results = execute_sql(sql=sql, profile=ctx.obj["profile"], dry_run=ctx.obj["dry_run"])
    if success and results:
        click.secho(f"\n→ Current version: {results[0][0]}", fg="green", bold=True)
    elif not success:
        sys.exit(1)


if __name__ == "__main__":
    cli()

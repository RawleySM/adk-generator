"""SQL Warehouse query execution for local mode.

This module provides a function to execute SQL against Databricks Unity Catalog
tables via the SQL Warehouse API, supporting both inline preview results and
large result sets returned as pandas DataFrames.

For local mode development, this replaces PySpark queries with direct SQL
Warehouse API calls, enabling:
- Quick data previews (INLINE disposition, JSON format)
- Full result retrieval as pandas DataFrames (EXTERNAL_LINKS, Arrow format)

Usage:
    from databricks_rlm_agent.sql_warehouse import execute_sql, SqlResult

    # Quick preview (default)
    result = execute_sql("SELECT * FROM catalog.schema.table")
    print(result.columns)  # ['col1', 'col2', ...]
    print(result.rows)     # [{'col1': 'val1', ...}, ...]

    # Full pandas DataFrame
    result = execute_sql("SELECT * FROM catalog.schema.table", as_pandas=True)
    df = result.df
    df.describe()

Environment Variables:
    ADK_SQL_WAREHOUSE_ID: SQL Warehouse ID to use (optional, auto-discovers if not set)
"""

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Default profile for authentication
DEFAULT_PROFILE = "rstanhope"


@dataclass
class SqlResult:
    """Result from SQL query execution.

    Attributes:
        columns: List of column names from the result schema.
        rows: List of row dictionaries (for preview mode).
        df: Optional pandas DataFrame (when as_pandas=True).
        truncated: True if results were truncated to preview_rows limit.
        row_count: Total number of rows returned (before truncation for preview).
        statement_id: Databricks statement execution ID for debugging.
    """

    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    df: Optional[Any] = None  # pandas.DataFrame when available
    truncated: bool = False
    row_count: int = 0
    statement_id: Optional[str] = None


def _get_workspace_client(profile: Optional[str] = None):
    """Get a Databricks WorkspaceClient.

    Args:
        profile: Databricks CLI profile name. Defaults to DEFAULT_PROFILE.

    Returns:
        WorkspaceClient instance.

    Raises:
        ImportError: If databricks-sdk is not installed.
        Exception: If authentication fails.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise ImportError(
            "databricks-sdk is required for SQL Warehouse queries. "
            "Install with: uv pip install databricks-sdk"
        ) from e

    effective_profile = profile or os.environ.get("DATABRICKS_PROFILE", DEFAULT_PROFILE)

    # Try profile-based auth first
    try:
        client = WorkspaceClient(profile=effective_profile)
        client.current_user.me()  # Verify auth
        logger.debug(f"Authenticated using profile: {effective_profile}")
        return client
    except Exception as e:
        logger.debug(f"Profile auth failed: {e}")

    # Fall back to default auth (env vars, run identity)
    try:
        client = WorkspaceClient()
        client.current_user.me()
        logger.debug("Using default Databricks authentication")
        return client
    except Exception as e:
        logger.debug(f"Default auth failed: {e}")

    raise RuntimeError(
        f"Could not authenticate to Databricks. Tried profile '{effective_profile}' "
        "and default credentials. Ensure either:\n"
        "  1. Run 'databricks auth login --profile <profile>', or\n"
        "  2. Set DATABRICKS_HOST and DATABRICKS_TOKEN env vars, or\n"
        "  3. Running in Databricks with proper run identity"
    )


def _get_sql_warehouse_id(client) -> str:
    """Get a SQL warehouse ID to execute statements.

    Prefers running warehouses; will attempt to start a stopped warehouse
    if no running warehouse is found.

    Args:
        client: WorkspaceClient instance.

    Returns:
        The ID of a SQL warehouse.

    Raises:
        RuntimeError: If no SQL warehouse is available.
    """
    # Check for explicit warehouse ID in environment
    warehouse_id = os.environ.get("ADK_SQL_WAREHOUSE_ID")
    if warehouse_id:
        logger.debug(f"Using SQL warehouse from ADK_SQL_WAREHOUSE_ID: {warehouse_id}")
        return warehouse_id

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
                    logger.info(f"SQL warehouse started: {wh.name}")
                    return wh.id
                time.sleep(10)

    raise RuntimeError(
        "No SQL warehouse available. Please create or start one, "
        "or set ADK_SQL_WAREHOUSE_ID environment variable."
    )


def execute_sql(
    sql: str,
    *,
    as_pandas: bool = False,
    preview_rows: int = 20,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    profile: Optional[str] = None,
) -> SqlResult:
    """Execute SQL via Databricks SQL Warehouse.

    For small results (preview mode): Uses INLINE disposition with JSON_ARRAY format.
    For large results (as_pandas=True): Uses EXTERNAL_LINKS disposition with
    ARROW_STREAM format, fetching chunks via presigned URLs.

    Args:
        sql: SQL statement to execute.
        as_pandas: If True, return full results as a pandas DataFrame.
                   If False (default), return preview rows as list of dicts.
        preview_rows: Maximum rows to return in preview mode (default: 20).
                      Ignored when as_pandas=True.
        catalog: Optional catalog context for the query (USE CATALOG).
        schema: Optional schema context for the query (USE SCHEMA).
        profile: Databricks CLI profile for authentication.

    Returns:
        SqlResult containing columns, rows (preview), df (pandas), and metadata.

    Raises:
        RuntimeError: If SQL execution fails.
        ImportError: If required dependencies are missing.

    Example:
        # Preview mode (default) - quick inspection
        result = execute_sql("SELECT * FROM my_table")
        for row in result.rows:
            print(row)

        # Pandas mode - full data analysis
        result = execute_sql("SELECT * FROM my_table", as_pandas=True)
        df = result.df
        print(df.describe())
    """
    from databricks.sdk.service.sql import (
        Disposition,
        Format,
        StatementState,
    )

    client = _get_workspace_client(profile)
    warehouse_id = _get_sql_warehouse_id(client)

    # Build full SQL with catalog/schema context if provided
    full_sql = sql
    if catalog or schema:
        context_statements = []
        if catalog:
            context_statements.append(f"USE CATALOG {catalog}")
        if schema:
            context_statements.append(f"USE SCHEMA {schema}")
        # Note: Statement execution API doesn't support multi-statement
        # We'll set catalog via the catalog parameter instead
        logger.debug(f"Query context: catalog={catalog}, schema={schema}")

    logger.info(f"Executing SQL (as_pandas={as_pandas}, preview_rows={preview_rows})")
    logger.debug(f"SQL: {sql[:200]}..." if len(sql) > 200 else f"SQL: {sql}")

    if as_pandas:
        # Large result mode: EXTERNAL_LINKS + ARROW_STREAM
        return _execute_as_pandas(
            client=client,
            warehouse_id=warehouse_id,
            sql=full_sql,
            catalog=catalog,
            schema=schema,
        )
    else:
        # Preview mode: INLINE + JSON_ARRAY
        return _execute_preview(
            client=client,
            warehouse_id=warehouse_id,
            sql=full_sql,
            preview_rows=preview_rows,
            catalog=catalog,
            schema=schema,
        )


def _execute_preview(
    client,
    warehouse_id: str,
    sql: str,
    preview_rows: int,
    catalog: Optional[str],
    schema: Optional[str],
) -> SqlResult:
    """Execute SQL and return preview rows as list of dicts.

    Uses INLINE disposition with JSON_ARRAY format for quick results.
    """
    from databricks.sdk.service.sql import (
        Disposition,
        Format,
        StatementState,
    )

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        catalog=catalog,
        schema=schema,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        row_limit=preview_rows,
        wait_timeout="50s",
    )

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

    # Extract columns from schema
    columns: list[str] = []
    if response.manifest and response.manifest.schema:
        columns = [col.name for col in response.manifest.schema.columns]

    # Extract rows
    rows: list[dict[str, Any]] = []
    truncated = False
    row_count = 0

    if response.result and response.result.data_array:
        data_array = response.result.data_array
        row_count = len(data_array)
        truncated = response.manifest.truncated if response.manifest else False

        # Convert array rows to dicts
        for row_array in data_array:
            row_dict = {}
            for i, value in enumerate(row_array):
                col_name = columns[i] if i < len(columns) else f"col_{i}"
                row_dict[col_name] = value
            rows.append(row_dict)

    logger.info(f"Preview result: {row_count} rows, {len(columns)} columns")

    return SqlResult(
        columns=columns,
        rows=rows,
        df=None,
        truncated=truncated,
        row_count=row_count,
        statement_id=response.statement_id,
    )


def _execute_as_pandas(
    client,
    warehouse_id: str,
    sql: str,
    catalog: Optional[str],
    schema: Optional[str],
) -> SqlResult:
    """Execute SQL and return full results as pandas DataFrame.

    Uses EXTERNAL_LINKS disposition with ARROW_STREAM format for large results.
    Fetches data chunks via presigned URLs and concatenates into a DataFrame.
    """
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.ipc as ipc
        import requests
    except ImportError as e:
        raise ImportError(
            "pandas and pyarrow are required for as_pandas=True. "
            "Install with: uv pip install pandas pyarrow"
        ) from e

    from databricks.sdk.service.sql import (
        Disposition,
        Format,
        StatementState,
    )

    # Submit statement with EXTERNAL_LINKS disposition
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        catalog=catalog,
        schema=schema,
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
        wait_timeout="50s",
    )

    # Check for errors
    if response.status and response.status.state == StatementState.FAILED:
        error_msg = "SQL execution failed"
        if response.status.error:
            error_msg = f"{error_msg}: {response.status.error.message}"
        raise RuntimeError(error_msg)

    # Handle pending state - poll until complete
    statement_id = response.statement_id
    while response.status and response.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        logger.debug(f"Statement {statement_id} still running, polling...")
        time.sleep(1)
        response = client.statement_execution.get_statement(statement_id)

    if response.status and response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(
            f"SQL execution did not succeed. State: {response.status.state}"
        )

    # Extract columns from schema
    columns: list[str] = []
    if response.manifest and response.manifest.schema:
        columns = [col.name for col in response.manifest.schema.columns]

    # Fetch Arrow data from external links
    arrow_tables: list[pa.Table] = []

    if response.result and response.result.external_links:
        for link in response.result.external_links:
            if link.external_link:
                logger.debug(f"Fetching chunk {link.chunk_index} from external link")
                arrow_data = _fetch_arrow_chunk(link.external_link)
                if arrow_data is not None:
                    arrow_tables.append(arrow_data)

    # Handle pagination if there are more chunks
    if response.manifest and response.manifest.total_chunk_count:
        total_chunks = response.manifest.total_chunk_count
        fetched_chunks = len(arrow_tables)

        while fetched_chunks < total_chunks:
            # Get next page of external links
            chunk_response = client.statement_execution.get_statement_result_chunk_n(
                statement_id=statement_id,
                chunk_index=fetched_chunks,
            )

            if chunk_response.external_links:
                for link in chunk_response.external_links:
                    if link.external_link:
                        logger.debug(
                            f"Fetching chunk {link.chunk_index} from external link"
                        )
                        arrow_data = _fetch_arrow_chunk(link.external_link)
                        if arrow_data is not None:
                            arrow_tables.append(arrow_data)
                            fetched_chunks += 1

    # Concatenate Arrow tables and convert to pandas
    if arrow_tables:
        combined_table = pa.concat_tables(arrow_tables)
        df = combined_table.to_pandas()
        row_count = len(df)
        logger.info(f"Pandas result: {row_count} rows, {len(columns)} columns")
    else:
        # Empty result - create empty DataFrame with schema
        df = pd.DataFrame(columns=columns)
        row_count = 0
        logger.info("Pandas result: 0 rows (empty result set)")

    # Create preview rows from DataFrame (first 20 rows)
    preview_rows = df.head(20).to_dict(orient="records")

    return SqlResult(
        columns=columns,
        rows=preview_rows,
        df=df,
        truncated=False,  # Full results, not truncated
        row_count=row_count,
        statement_id=statement_id,
    )


def _fetch_arrow_chunk(url: str) -> Optional[Any]:
    """Fetch an Arrow IPC stream chunk from a presigned URL.

    Args:
        url: Presigned URL for the Arrow data chunk.

    Returns:
        PyArrow Table, or None if fetch failed.
    """
    import io

    import pyarrow as pa
    import pyarrow.ipc as ipc
    import requests

    try:
        # Presigned URLs don't need auth headers
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        # Read Arrow IPC stream from response content
        # PyArrow's open_stream expects a file-like object, not raw bytes
        reader = ipc.open_stream(io.BytesIO(response.content))
        table = reader.read_all()
        return table

    except Exception as e:
        logger.error(f"Failed to fetch Arrow chunk: {e}")
        return None


# Convenience function for common use case
def query_table(
    table: str,
    *,
    columns: str = "*",
    where: Optional[str] = None,
    limit: int = 100,
    as_pandas: bool = False,
    profile: Optional[str] = None,
) -> SqlResult:
    """Query a Unity Catalog table with optional filtering.

    Convenience wrapper around execute_sql for common table queries.

    Args:
        table: Fully qualified table name (catalog.schema.table).
        columns: Column expression (default: "*").
        where: Optional WHERE clause condition.
        limit: Maximum rows to return (default: 100).
        as_pandas: If True, return full results as pandas DataFrame.
        profile: Databricks CLI profile for authentication.

    Returns:
        SqlResult with query results.

    Example:
        result = query_table(
            "silo_dev_rs.task.jira_raw_data",
            columns="id, name, status",
            where="status = 'open'",
            limit=50
        )
    """
    sql = f"SELECT {columns} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    sql += f" LIMIT {limit}"

    return execute_sql(
        sql,
        as_pandas=as_pandas,
        preview_rows=limit if not as_pandas else 20,
        profile=profile,
    )

"""Metadata keyword search tool for searching Unity Catalog table metadata.

This tool searches two metadata tables:
- silo_dev_rs.metadata.columnnames: Maps table paths to their column definitions
- silo_dev_rs.metadata.uc_provenance_index: Tracks table operations and version history
"""

import os
from google.adk.tools import ToolContext

DEFAULT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
MAX_DISPLAY_ROWS = 5


def _escape_sql_string(value: str) -> str:
    """Escape single quotes in SQL string values to prevent SQL injection."""
    if value is None:
        return value
    return value.replace("'", "''")


def metadata_keyword_search(
    keyword: str,
    table_type: str = "columnnames",
    operator: str = "LIKE",
    *,
    tool_context: ToolContext,
) -> dict:
    """
    Search Unity Catalog metadata tables for tables matching keyword patterns.

    This tool performs catalog-wide searches across metadata tables to help locate
    tables by name, column definitions, or operational history.

    TARGET TABLES:
    ==============

    1. silo_dev_rs.metadata.columnnames (42,057 rows)
       - Catalog-wide index of all table paths and their column schemas
       - Columns: path (string), column_array (array<string>)
       - Sample row:
         path: 'silo_mg.sm_mart_exceptions.report_cds__vendor_tax_id_export'
         column_array: '["VENDOR_NAME (STRING)","VENDOR_NUMBER (STRING)","FEDERAL_TAX_NUMBER (STRING)"]'

    2. silo_dev_rs.metadata.uc_provenance_index
       - Tracks all table operations (CREATE, INSERT, UPDATE, DELETE, etc.)
       - Columns: table_name, version, event_time, user_id, user_name, operation,
                  job_id, job_run_id, notebook_id, cluster_id, read_version,
                  user_metadata, operation_params_json, operation_metrics_json
       - Sample row:
         table_name: 'silo_chs.sm_erp.dim_company'
         version: 55
         operation: 'CHANGE COLUMN'
         user_name: 'mkreiner@spendmend.com'

    OPERATORS:
    ==========
    - LIKE: SQL LIKE pattern matching (use % for wildcards). Case-insensitive.
    - NOT LIKE: Exclude patterns. Case-insensitive.
    - =: Exact match (case-sensitive)
    - !=: Not equal (case-sensitive)

    OR PATTERNS:
    ============
    Use | in keyword to combine OR conditions. Only works with LIKE/NOT LIKE operators.
    Example: keyword="dim_%|fact_%" finds tables starting with 'dim_' OR 'fact_'.

    EXAMPLES:
    =========

    For columnnames table:
    ----------------------
    - keyword="vendor", operator="LIKE"
      → Finds tables with 'vendor' in path (case-insensitive)

    - keyword="%jira%", operator="LIKE"
      → Finds all JIRA-related tables

    - keyword="silo_dev_rs.task.%", operator="LIKE"
      → Finds all tables in silo_dev_rs.task schema

    - keyword="dim_%|fact_%", operator="LIKE"
      → Finds dimension or fact tables (OR pattern)

    For uc_provenance_index table:
    ------------------------------
    - keyword="rstanhope", operator="LIKE", table_type="provenance"
      → Finds operations by user containing 'rstanhope'

    - keyword="INSERT", operator="=", table_type="provenance"
      → Finds all INSERT operations

    - keyword="jira", operator="LIKE", table_type="provenance"
      → Finds provenance for tables containing 'jira'

    Args:
        keyword (str): The search pattern. Use SQL wildcards (%) for LIKE searches.
                       Use | to combine OR conditions (only works with LIKE/NOT LIKE).
        table_type (str): Which table to search:
                          - "columnnames" (default): Search table/column metadata
                          - "provenance": Search operation history
        operator (str): SQL operator - "LIKE", "NOT LIKE", "=", "!=" (default: "LIKE")
                        Note: | OR-patterns only work with LIKE/NOT LIKE operators.
        tool_context (ToolContext): Provided by ADK at runtime.

    Returns:
        dict: Search results with keys:
              - status: "success" or "error"
              - rows: List of matching rows (max 5 displayed)
              - total_count: Total matches found
              - message: Human-readable summary
              - suggestion: If >5 results, suggests refinement or delegate_code_results()
    """
    from databricks.sdk import WorkspaceClient
    import time

    # Normalize inputs
    table_type = table_type.lower().strip()
    operator = operator.upper().strip()

    # Validate operator
    valid_operators = ["LIKE", "NOT LIKE", "=", "!="]
    if operator not in valid_operators:
        return {
            "status": "error",
            "message": f"Invalid operator '{operator}'. Valid operators: {valid_operators}"
        }

    # Build query based on table type
    if table_type in ("columnnames", "columns", "col"):
        target_table = "silo_dev_rs.metadata.columnnames"
        search_column = "path"
        select_cols = "path, column_array"
    elif table_type in ("provenance", "prov", "history", "operations"):
        target_table = "silo_dev_rs.metadata.uc_provenance_index"
        search_column = "table_name"
        select_cols = "table_name, version, event_time, user_name, operation"
    else:
        return {
            "status": "error",
            "message": f"Invalid table_type '{table_type}'. Use 'columnnames' or 'provenance'."
        }

    # Escape single quotes to prevent SQL injection
    escaped_keyword = _escape_sql_string(keyword)

    # Handle OR patterns (| separator) - only valid for LIKE operators
    if "|" in escaped_keyword and operator in ("LIKE", "NOT LIKE"):
        patterns = [p.strip() for p in escaped_keyword.split("|")]
        pattern_clauses = []
        for p in patterns:
            # p is already escaped (from escaped_keyword), don't double-escape
            if "%" not in p:
                p = f"%{p}%"
            # Case-insensitive LIKE
            pattern_clauses.append(f"LOWER({search_column}) {operator} LOWER('{p}')")
        # NOT LIKE requires AND (exclude all patterns); LIKE uses OR (match any pattern)
        joiner = " AND " if operator == "NOT LIKE" else " OR "
        where_clause = joiner.join(pattern_clauses)
    elif "|" in escaped_keyword:
        # OR patterns with = or != don't make semantic sense, use first pattern
        patterns = [p.strip() for p in escaped_keyword.split("|")]
        search_pattern = patterns[0]
        where_clause = f"{search_column} {operator} '{search_pattern}'"
    else:
        # Single pattern search
        search_pattern = escaped_keyword
        if operator in ("LIKE", "NOT LIKE"):
            if "%" not in escaped_keyword:
                search_pattern = f"%{escaped_keyword}%"
            # Case-insensitive LIKE
            where_clause = f"LOWER({search_column}) {operator} LOWER('{search_pattern}')"
        else:
            # Exact match operators (=, !=)
            where_clause = f"{search_column} {operator} '{search_pattern}'"

    # Build count and select queries
    count_sql = f"SELECT COUNT(*) as cnt FROM {target_table} WHERE {where_clause}"
    select_sql = f"SELECT {select_cols} FROM {target_table} WHERE {where_clause} LIMIT {MAX_DISPLAY_ROWS + 1}"

    try:
        client = WorkspaceClient(profile=DEFAULT_PROFILE)

        # Get warehouse ID
        warehouses = list(client.warehouses.list())
        warehouse_id = None
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                warehouse_id = wh.id
                break

        if not warehouse_id:
            # Try starting a stopped warehouse
            for wh in warehouses:
                if wh.state and wh.state.value == "STOPPED":
                    client.warehouses.start(wh.id)
                    for _ in range(30):
                        status = client.warehouses.get(wh.id)
                        if status.state and status.state.value == "RUNNING":
                            warehouse_id = wh.id
                            break
                        time.sleep(10)
                    break

        if not warehouse_id:
            return {"status": "error", "message": "No SQL warehouse available"}

        # Execute count query
        count_response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=count_sql,
            wait_timeout="30s",
        )

        total_count = 0
        if count_response.result and count_response.result.data_array:
            total_count = int(count_response.result.data_array[0][0])

        if total_count == 0:
            return {
                "status": "success",
                "rows": [],
                "total_count": 0,
                "message": f"No matches found for '{keyword}' in {target_table}",
                "query_executed": select_sql
            }

        # Execute select query
        select_response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=select_sql,
            wait_timeout="30s",
        )

        rows = []
        columns = []
        if select_response.manifest and select_response.manifest.schema:
            columns = [col.name for col in select_response.manifest.schema.columns]

        if select_response.result and select_response.result.data_array:
            for row_data in select_response.result.data_array[:MAX_DISPLAY_ROWS]:
                row_dict = dict(zip(columns, row_data))
                rows.append(row_dict)

        result = {
            "status": "success",
            "rows": rows,
            "total_count": total_count,
            "displayed_count": len(rows),
            "columns": columns,
            "query_executed": select_sql
        }

        if total_count <= MAX_DISPLAY_ROWS:
            result["message"] = f"Found {total_count} matching rows in {target_table}"
        else:
            result["message"] = f"Found {total_count} total matches (showing first {MAX_DISPLAY_ROWS})"
            result["suggestion"] = (
                f"Results exceed display limit. Consider:\n"
                f"1. Refine your search with more specific keywords\n"
                f"2. Use delegate_code_results() to analyze the full result set by executing:\n"
                f"   SELECT * FROM {target_table} WHERE {where_clause}"
            )

        # Store search results in tool context for downstream tools
        if tool_context:
            tool_context.state["last_metadata_search_count"] = total_count
            tool_context.state["last_metadata_search_rows"] = rows
            tool_context.state["last_metadata_search_query"] = select_sql
            tool_context.state["last_metadata_search_table"] = target_table

        return result

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error executing search: {str(e)}",
            "query_attempted": select_sql
        }

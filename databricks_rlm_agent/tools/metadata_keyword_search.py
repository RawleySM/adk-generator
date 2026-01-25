"""Metadata keyword search tool for searching Unity Catalog table metadata.

This tool searches two metadata tables:
- silo_dev_rs.metadata.columnnames: Maps table paths to their column definitions
- silo_dev_rs.metadata.uc_provenance_index: Tracks table operations and version history
"""

import os
from typing import Optional
from google.adk.tools import ToolContext

DEFAULT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
MAX_DISPLAY_ROWS = 5


def metadata_keyword_search(
    keyword: str,
    table_type: str = "columnnames",
    operator: str = "LIKE",
    tool_context: Optional[ToolContext] = None,
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
    - LIKE: SQL LIKE pattern matching (use % for wildcards)
    - NOT LIKE: Exclude patterns
    - =: Exact match
    - !=: Not equal
    - AND: Combine with previous search (requires session state)
    - OR: Alternative match (use | in keyword)

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
                       Use | to combine OR conditions.
        table_type (str): Which table to search:
                          - "columnnames" (default): Search table/column metadata
                          - "provenance": Search operation history
        operator (str): SQL operator - "LIKE", "NOT LIKE", "=", "!=" (default: "LIKE")
        tool_context (ToolContext, optional): The tool context for state management.

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

    # Handle OR patterns (| separator)
    if "|" in keyword:
        patterns = [p.strip() for p in keyword.split("|")]
        where_clauses = [f"{search_column} {operator} '%{p}%'" for p in patterns]
        where_clause = " OR ".join(where_clauses)
    else:
        # Auto-add wildcards for LIKE if not present
        search_pattern = keyword
        if operator == "LIKE" and "%" not in keyword:
            search_pattern = f"%{keyword}%"
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

        return result

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error executing search: {str(e)}",
            "query_attempted": select_sql
        }

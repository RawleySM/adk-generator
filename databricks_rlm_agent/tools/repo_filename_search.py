"""Repository filename search tool for searching code repository file metadata.

This tool searches silo_dev_rs.repos.files to locate files within repositories,
with optional filtering by files that reference Unity Catalog Delta tables.
"""

import os
from typing import Optional
from google.adk.tools import ToolContext

DEFAULT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
MAX_DISPLAY_ROWS = 5


def repo_filename_search(
    keyword: str,
    search_field: str = "filename",
    operator: str = "LIKE",
    table_filter: Optional[str] = None,
    filetype_filter: Optional[str] = None,
    tool_context: Optional[ToolContext] = None,
) -> dict:
    """
    Search repository files for matching filenames, paths, or tables referenced.

    This tool searches the silo_dev_rs.repos.files table, which indexes all files
    across code repositories. It can also filter by files that reference specific
    Unity Catalog Delta tables (found via dataTables column).

    TARGET TABLE:
    =============

    silo_dev_rs.repos.files
    -----------------------
    Contains metadata about files in code repositories, useful for finding code
    that interacts with specific tables or locating files by name/type.

    Columns:
    - repo_name (string): Repository name (e.g., 'SDS-Kofax', 'sm-data-platform')
    - filename (string): File name (e.g., 'etl_pipeline.py', 'schema.sql')
    - filepath (string): Full path within repo (e.g., 'src/etl/etl_pipeline.py')
    - filetype (string): Extension (e.g., 'py', 'sql', 'yml', 'json')
    - filesize (bigint): Size in bytes
    - last_modified_by (string): Last committer
    - last_modified_timestamp (timestamp): Last modification time
    - dataTables (array<string>): UC Delta table paths found in the file

    Sample row:
      repo_name: 'SDS-Kofax'
      filename: 'Sds.Kofax.BaseLibrary.sln'
      filepath: 'Base Library.Sds.Kofax.BaseLibrary.Sds.Kofax.BaseLibrary.sln'
      filetype: 'sln'
      filesize: 1337
      dataTables: []

    OPERATORS:
    ==========
    - LIKE: SQL LIKE pattern matching (use % for wildcards, auto-added if missing)
    - NOT LIKE: Exclude patterns
    - =: Exact match
    - !=: Not equal

    SEARCH FIELDS:
    ==============
    - filename (default): Search by file name
    - filepath: Search by full path
    - repo_name: Search by repository name
    - filetype: Search by extension

    EXAMPLES:
    =========

    Basic filename searches:
    ------------------------
    - keyword="etl", search_field="filename"
      → Finds files with 'etl' in filename

    - keyword="%.py", search_field="filename", operator="LIKE"
      → Finds all Python files

    - keyword="jira", search_field="filepath"
      → Finds files in paths containing 'jira'

    Repository-specific searches:
    -----------------------------
    - keyword="sm-data-platform", search_field="repo_name"
      → Finds all files in sm-data-platform repo

    - keyword="sql", search_field="filetype"
      → Finds all SQL files across repos

    Filetype filtering:
    -------------------
    - keyword="vendor", filetype_filter="py"
      → Finds Python files with 'vendor' in filename

    - keyword="config", filetype_filter="yml|yaml|json"
      → Finds config files (YAML or JSON) matching 'config'

    Table reference searches:
    -------------------------
    - table_filter="silo_dev_rs.task.jira"
      → Finds files that reference jira tables in their code

    - keyword="etl", table_filter="vendor"
      → Finds ETL files that reference tables containing 'vendor'

    Combined searches:
    ------------------
    - keyword="loader", search_field="filename", filetype_filter="py", table_filter="jira"
      → Python loader files that reference JIRA tables

    Args:
        keyword (str): The search pattern. Use SQL wildcards (%) for LIKE searches.
                       Use | to combine OR conditions for the keyword.
        search_field (str): Field to search - "filename", "filepath", "repo_name",
                            or "filetype" (default: "filename")
        operator (str): SQL operator - "LIKE", "NOT LIKE", "=", "!=" (default: "LIKE")
        table_filter (str, optional): Filter by files that reference UC Delta tables
                                      matching this pattern (searches dataTables column).
        filetype_filter (str, optional): Filter by file extension(s). Use | for OR
                                         (e.g., "py|sql" for Python or SQL files).
        tool_context (ToolContext, optional): The tool context for state management.

    Returns:
        dict: Search results with keys:
              - status: "success" or "error"
              - rows: List of matching rows (max 5 displayed)
              - total_count: Total matches found
              - columns: Column names returned
              - message: Human-readable summary
              - suggestion: If >5 results, suggests refinement or delegate_code_results()
    """
    from databricks.sdk import WorkspaceClient
    import time

    target_table = "silo_dev_rs.repos.files"

    # Normalize inputs
    search_field = search_field.lower().strip()
    operator = operator.upper().strip()

    # Validate operator
    valid_operators = ["LIKE", "NOT LIKE", "=", "!="]
    if operator not in valid_operators:
        return {
            "status": "error",
            "message": f"Invalid operator '{operator}'. Valid operators: {valid_operators}"
        }

    # Validate search field
    valid_fields = ["filename", "filepath", "repo_name", "filetype"]
    if search_field not in valid_fields:
        return {
            "status": "error",
            "message": f"Invalid search_field '{search_field}'. Valid fields: {valid_fields}"
        }

    # Build WHERE clauses
    where_clauses = []

    # Main keyword search
    if keyword:
        if "|" in keyword:
            # Handle OR patterns
            patterns = [p.strip() for p in keyword.split("|")]
            or_clauses = []
            for p in patterns:
                if operator == "LIKE" and "%" not in p:
                    p = f"%{p}%"
                or_clauses.append(f"{search_field} {operator} '{p}'")
            where_clauses.append(f"({' OR '.join(or_clauses)})")
        else:
            search_pattern = keyword
            if operator == "LIKE" and "%" not in keyword:
                search_pattern = f"%{keyword}%"
            where_clauses.append(f"{search_field} {operator} '{search_pattern}'")

    # Filetype filter
    if filetype_filter:
        if "|" in filetype_filter:
            types = [t.strip() for t in filetype_filter.split("|")]
            type_clauses = [f"filetype = '{t}'" for t in types]
            where_clauses.append(f"({' OR '.join(type_clauses)})")
        else:
            where_clauses.append(f"filetype = '{filetype_filter}'")

    # Table reference filter (searches dataTables array)
    if table_filter:
        # Use array_contains or cast to string for search
        table_pattern = table_filter if "%" in table_filter else f"%{table_filter}%"
        where_clauses.append(f"CAST(dataTables AS STRING) LIKE '{table_pattern}'")

    if not where_clauses:
        return {
            "status": "error",
            "message": "At least one search criterion required (keyword, filetype_filter, or table_filter)"
        }

    where_clause = " AND ".join(where_clauses)

    # Build queries
    select_cols = "repo_name, filename, filepath, filetype, filesize, last_modified_timestamp, dataTables"
    count_sql = f"SELECT COUNT(*) as cnt FROM {target_table} WHERE {where_clause}"
    select_sql = f"SELECT {select_cols} FROM {target_table} WHERE {where_clause} ORDER BY last_modified_timestamp DESC LIMIT {MAX_DISPLAY_ROWS + 1}"

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
                "message": f"No files found matching criteria in {target_table}",
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
            result["message"] = f"Found {total_count} matching files"
        else:
            result["message"] = f"Found {total_count} total matches (showing first {MAX_DISPLAY_ROWS})"
            result["suggestion"] = (
                f"Results exceed display limit. Consider:\n"
                f"1. Add filetype_filter to narrow by extension (e.g., 'py', 'sql')\n"
                f"2. Add table_filter to find files referencing specific tables\n"
                f"3. Use more specific keywords\n"
                f"4. Use delegate_code_results() to analyze the full result set by executing:\n"
                f"   SELECT * FROM {target_table} WHERE {where_clause}"
            )

        return result

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error executing search: {str(e)}",
            "query_attempted": select_sql
        }

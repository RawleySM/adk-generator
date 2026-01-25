# GitHub Catalog Indexer

This directory contains the backup of the Python script used to index the Spendmend GitHub organization into a Databricks Delta table.

## Script Location
*   **Local:** `scripts/github_indexer/github_catalog_indexer.py`
*   **Databricks (DBFS):** `dbfs:/tmp/spendmend/github_catalog_indexer.py`

## Usage
The script runs as a Databricks Job task (`spark_python_task`) on the All-Purpose Cluster `1115-120035-jyzgoasz`.

### Arguments
1.  **GitHub Token:** (Passed via job parameter)
2.  **Organization:** `Spendmend`
3.  **Target Repo (Optional):** If provided, runs only for that repository (Test Mode). If omitted, scans the entire organization (Full Mode).

## Output Data
*   **Catalog:** `silo_dev_rs`
*   **Schema:** `repos`
*   **Table:** `files`
*   **Full Table Path:** `silo_dev_rs.repos.files`

### Schema
| Column | Type | Description |
| :--- | :--- | :--- |
| `repo_name` | String | Name of the GitHub repository |
| `filename` | String | Name of the file |
| `filepath` | String | Dot-separated path (e.g., `src.main.app.py`) |
| `filetype` | String | File extension |
| `filesize` | Long | Size in bytes |
| `last_modified_by` | String | "unknown" (placeholder to avoid API rate limits) |
| `last_modified_timestamp` | Timestamp | Repository last update time |
| `dataTables` | Array[String] | Empty list (placeholder for future use) |

## Performance & Stats
*   **Runtime:** ~1.8 minutes (108 seconds) for the full organization.
*   **Total Repositories Scanned:** 109
*   **Total Files Indexed:** 63,666

## Dependencies
*   **Python Libraries:** `requests`, `json`, `sys`, `datetime`
*   **Spark:** `pyspark.sql` (Standard Databricks Runtime)

## Notes
The script uses the GitHub Git Database API (Recursive Tree) to fetch the file structure efficiently. To prevent API rate limiting and excessive runtime (N+1 problem), `last_modified_timestamp` uses the *repository's* last update time rather than fetching commit history for every individual file.

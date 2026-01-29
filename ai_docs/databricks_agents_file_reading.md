# Databricks Agent File Reading Guide

This document provides definitive guidance for Databricks developers designing Python-based Agents (using Google ADK or similar frameworks) that need to read files from the Databricks filesystem. It synthesizes findings from extensive testing across deployment methods, file locations, and access techniques.

## General Agent Requirements

All ADK-based agents running on a Databricks Spark Driver (regardless of task type) must handle the following infrastructure constraints:

1.  **Async Event Loop Conflict:** The Databricks driver runs an active `asyncio` loop. Standard `asyncio.run(main())` will fail with `RuntimeError`.
    *   **Requirement:** Execute `import nest_asyncio; nest_asyncio.apply()` before running your agent logic.
2.  **Session Initialization:** `InMemorySessionService` starts empty; sessions must be created explicitly before the runner starts.

## Supported Capabilities Matrix

| Deployment Method | File Location | Read Method | Status | Critical Implementation Note |
| :--- | :--- | :--- | :--- | :--- |
| **Spark Script** | `/Volumes/...` | `subprocess` (cat/grep) | **PASS** | Native POSIX performance. Best for scanning. |
| **Spark Script** | `/Volumes/...` | `dbutils.fs.head` | **PASS** | Works natively. |
| **Spark Script** | `/Workspace/...` | `pathlib` / `open()` | **PASS** | Works via FUSE mount. |
| **Notebook Task** | `/Volumes/...` | `pathlib` | **PASS** | Use `dbutils.widgets` for params. |
| **Notebook Task** | `/Workspace/...` | `pathlib` | **PASS** | Works via FUSE mount. |
| **Notebook Task** | `/Workspace/...` | `dbutils.fs.head` | **conditional** | **MUST** prepend `file:/`. Standard paths fail. |

---

## Detailed Implementation Guidance

### 1. Deployment Method Considerations

#### Notebook Tasks
*   **Parameter Passing:** Command-line arguments (`sys.argv`) are not standard.
    *   **Requirement:** Use `dbutils.widgets.get("param_name")` to retrieve job parameters.
*   **Output Capture:** Logs are sometimes swallowed or truncated in the Jobs UI.
    *   **Requirement:** Use `dbutils.notebook.exit(json_result)` to cleanly return structured data.

#### Spark Python Scripts (`spark_python_task`)
*   **Parameter Passing:** Standard `sys.argv` works as expected.
*   **Identity:** Runs as the Job Owner (or Service Principal).

### 2. Handling File Locations & Read Methods

#### Unity Catalog Volumes (`/Volumes/...`)
*   **Behavior:** Behaves exactly like a standard POSIX filesystem.
*   **Recommendation:** **Primary choice for agent data.**
*   **Tools:**
    *   `subprocess`: Fully supported (`ls`, `grep`, `cat`).
    *   `pathlib`/`open()`: Fully supported.
    *   `dbutils`: Supported.

#### Workspace Files (`/Workspace/...`)
*   **Behavior:** Backed by a FUSE mount, but with specific constraints when using Spark APIs.
*   **Standard IO (`pathlib`, `subprocess`):**
    *   Works transparently on `/Workspace/path/to/file.py`.
    *   **LLM Insight:** LLMs often strip URI schemes (e.g., changing `file:/Workspace/...` to `/Workspace/...`). For standard IO, this is **beneficial/benign** as `open()` expects the path without the scheme.
*   **Databricks Utils (`dbutils`):**
    *   **CRITICAL CONSTRAINT:** `dbutils.fs` APIs default to `dbfs:/`. To access Workspace files, you **MUST** provide the `file:/` scheme.
    *   **Failure Mode:** `dbutils.fs.head("/Workspace/foo.py")` fails.
    *   **Success Mode:** `dbutils.fs.head("file:/Workspace/foo.py")` succeeds.
    *   **Tool logic:** Do **not** rely on the LLM to format this correctly. Your tool implementation must detect `/Workspace/` paths and strictly prepend `file:` if missing.

---

## Architectural Trade-offs

When designing agent tools, verify the following trade-offs to select the right approach:

### 1. Scanning Efficiency (Search vs. Read)
*   **`subprocess` (grep/find):** **Highest Efficiency.**
    *   Allows the agent to search *content* across thousands of files without loading them into context.
    *   Example: `grep -r "API_KEY" /Volumes/codebase` executes in O(1) memory for the agent.
    *   *Availability:* Only available via `subprocess` on Volumes/Workspace (FUSE). Not possible with `dbutils`.
*   **`pathlib` / `dbutils`:** **Low Efficiency for Search.**
    *   Requires reading file content into memory and LLM context to analyze. O(N) token cost.
    *   Only suitable for reading specific, identified files.

### 2. Robustness & Portability
*   **`pathlib` / `open()`:** **High.**
    *   Standard Python code. Portable to local dev, Cloud Run, or other environments.
    *   Resilient to path formatting (handles `/Workspace` natively).
*   **`dbutils`:** **Low.**
    *   Vendor-locked to Databricks.
    *   Brittle path requirements (needs `file:/` for Workspace).
    *   Mocking for local tests is difficult.

### 3. Observability
*   **Spark Script:** Stdout/Stderr are captured cleanly in driver logs. Easier to stream to external logging (CloudWatch/Splunk).
*   **Notebook:** Output is captured in "Notebook Output" or truncated JSON cells. Harder to parse programmatically without `dbutils.notebook.exit`.

## Final Recommendation
For a robust, high-performance Agent on Databricks:
1.  **Deployment:** Use **Spark Python Scripts** for production automation (cleaner logs, standard args). Use **Notebooks** only for human-in-the-loop debugging.
2.  **Storage:** Use **Unity Catalog Volumes** for all data and code repositories.
3.  **Tools:** Prefer **Standard Python IO (`pathlib`, `subprocess`)**.
    *   Use `subprocess` to enable "grep" capabilities for the agent (retrieval-augmented generation).
    *   Avoid `dbutils` for file IO unless strictly necessary for interacting with legacy DBFS mounts.

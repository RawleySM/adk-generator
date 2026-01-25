# E2E Testing Guide

This guide describes how to run end-to-end tests for the RLM Agent without relying on the ingestor polling mechanism.

## Overview

The RLM Agent uses a three-job pattern:
- **Job_A (Orchestrator)**: Control plane that runs the agent and generates code artifacts
- **Job_B (Executor)**: Execution plane that runs generated artifacts
- **Job_C (Ingestor)**: CDF polling plane that watches for new tasks

For development and debugging, you can bypass Job_C entirely by passing test tasks directly to the orchestrator.

## Test Tasks

Test tasks are defined in `databricks_rlm_agent/test_tasks.py` with progressive difficulty levels:

| Level | Issue Key | Description |
|-------|-----------|-------------|
| 1-2 | EVAL-001/002 | Simple queries, minimal joins |
| 3-4 | EVAL-003/004 | Aggregations, basic joins, filtering |
| 5-6 | EVAL-005/006 | Multi-table analysis, metadata exploration |
| 7-8 | EVAL-007/008 | Cross-schema investigation, data quality analysis |
| 9-10 | EVAL-009/010 | Complex workflow analysis, iterative exploration |

## Prompt Configuration

The orchestrator supports two ways to provide the initial prompt:

### Option 1: Inline Prompt (`ADK_PROMPT` / `--prompt`)

Pass the prompt text directly as a string:

```bash
rlm-orchestrator --prompt "Count all vendors in silo_dev_rs.dbo.vendors"
```

Or via job parameter:

```bash
uv run scripts/run_and_wait.py --job-id 12345 --param ADK_PROMPT="Count all vendors"
```

### Option 2: Prompt File (`ADK_PROMPT_FILE` / `--prompt-file`)

Load the prompt from a file in UC Volumes, DBFS, or local filesystem:

```bash
rlm-orchestrator --prompt-file /Volumes/silo_dev_rs/task/task_txt/task.txt
```

Or via job parameter:

```bash
uv run scripts/run_and_wait.py --job-id 12345 \
  --param ADK_PROMPT_FILE=/Volumes/silo_dev_rs/task/task_txt/task.txt
```

### Precedence Rules

**Literal prompt wins.** If both `ADK_PROMPT` and `ADK_PROMPT_FILE` are provided, the literal prompt (`ADK_PROMPT`) takes precedence. The file is only read when:

1. `--prompt` / `ADK_PROMPT` is empty or not provided, AND
2. `--prompt-file` / `ADK_PROMPT_FILE` points to a valid file

Default file path: `/Volumes/silo_dev_rs/task/task_txt/task.txt`

---

## Running Tests Locally

### Using `rlm-test` (Recommended)

The `rlm-test` command is a convenience wrapper with sensible defaults:

```bash
# List all available test tasks
rlm-test --list

# Run a specific test level
rlm-test --level 1

# Run with more iterations for complex tasks
rlm-test --level 5 --max-iterations 5

# Specify custom session ID
rlm-test --level 3 --session-id my_test_session

# Use different catalog/schema
rlm-test --level 1 --catalog silo_dev_rs --schema adk_test
```

### Using `rlm-orchestrator` Directly

You can also use the `--test-level` flag on the orchestrator:

```bash
# Run test level 3 with orchestrator
rlm-orchestrator --test-level 3

# Full example with all options
rlm-orchestrator \
  --test-level 5 \
  --catalog silo_dev_rs \
  --schema adk \
  --session-id test_session_001 \
  --max-iterations 5
```

## Running Tests on Databricks

### Option 1: Wheel Task with Parameters

Configure a Databricks job with `python_wheel_task`:

```json
{
  "tasks": [
    {
      "task_key": "e2e_test_level_3",
      "python_wheel_task": {
        "package_name": "databricks_rlm_agent",
        "entry_point": "rlm-orchestrator",
        "parameters": [
          "--test-level", "3",
          "--max-iterations", "5",
          "--catalog", "silo_dev_rs",
          "--schema", "adk"
        ]
      },
      "libraries": [
        {
          "whl": "dbfs:/Volumes/silo_dev_rs/adk/wheels/databricks_rlm_agent-0.1.34-py3-none-any.whl"
        }
      ],
      "existing_cluster_id": "your-cluster-id"
    }
  ]
}
```

### Option 2: Using Job Parameters

You can also use Databricks job parameters:

```json
{
  "job_parameters": [
    {"name": "TEST_LEVEL", "default": "3"},
    {"name": "MAX_ITERATIONS", "default": "5"}
  ],
  "tasks": [
    {
      "task_key": "e2e_test",
      "python_wheel_task": {
        "package_name": "databricks_rlm_agent",
        "entry_point": "rlm-test",
        "parameters": [
          "--level", "{{job.parameters.TEST_LEVEL}}",
          "--max-iterations", "{{job.parameters.MAX_ITERATIONS}}"
        ]
      }
    }
  ]
}
```

### Option 3: Notebook Wrapper

For interactive debugging, use a notebook:

```python
# Cell 1: Install the wheel
%pip install /Volumes/silo_dev_rs/adk/wheels/databricks_rlm_agent-0.1.34-py3-none-any.whl

# Cell 2: Run test
import sys
sys.argv = ["rlm-test", "--level", "3", "--max-iterations", "5"]

from databricks_rlm_agent.cli import test_main
test_main()
```

## Test Task Details

### Level 1: Basic Count Query
- **Task**: Count total vendors in master data
- **Tables**: `silo_dev_rs.dbo.vendors`
- **Purpose**: Verify basic SQL execution capability

### Level 2: Filtered Query
- **Task**: Find active vendors with websites
- **Tables**: `silo_dev_rs.dbo.vendors`
- **Purpose**: Test filtering and pattern observation

### Level 3: Aggregation Analysis
- **Task**: Analyze vendor distribution by status and class
- **Tables**: `silo_dev_rs.dbo.vendors`
- **Purpose**: Test GROUP BY and cross-tabulation

### Level 4: Two-Table JOIN
- **Task**: Analyze vendor-location relationship coverage
- **Tables**: `silo_dev_rs.dbo.vendors`, `silo_dev_rs.dbo.locations`
- **Purpose**: Test JOIN operations and coverage analysis

### Level 5: Multi-Table Analytics
- **Task**: Profile vendor match enrichment data quality
- **Tables**: `silo_dev_rs.task.ai_vendor_match_enriched`
- **Purpose**: Test comprehensive profiling with multiple metrics

### Level 6: Schema Discovery
- **Task**: Discover tables containing TIN/tax information
- **Tables**: `silo_dev_rs.metadata.columnnames`
- **Purpose**: Test metadata exploration and JSON parsing

### Level 7: Data Lineage Investigation
- **Task**: Trace data lineage for vendor matching workflow
- **Tables**: Multiple (view definitions, source tables)
- **Purpose**: Test view introspection and lineage documentation

### Level 8: Workflow State Analysis
- **Task**: Analyze workflow execution patterns and bottlenecks
- **Tables**: `silo_dev_rs.workflow.*`
- **Purpose**: Test JSON parsing and cross-table correlation

### Level 9: Data Quality Investigation
- **Task**: Investigate vendor data quality across master and client data
- **Tables**: Multiple vendor-related tables
- **Purpose**: Test pattern recognition and recommendation synthesis

### Level 10: Comprehensive Catalog Analysis
- **Task**: Build comprehensive data catalog documentation
- **Tables**: All tables in `silo_dev_rs.*`
- **Purpose**: Test iterative exploration with context management

## Debugging Tips

### View Session State

Query the Delta session table to see conversation history:

```sql
SELECT *
FROM silo_dev_rs.adk.adk_sessions
WHERE session_id LIKE 'test_level_%'
ORDER BY updated_at DESC
LIMIT 10;
```

### View Telemetry Events

Monitor execution events:

```sql
SELECT *
FROM silo_dev_rs.adk.adk_telemetry
WHERE run_id LIKE 'test_%'
ORDER BY timestamp DESC
LIMIT 50;
```

### Check Generated Artifacts

Artifacts are saved to UC Volumes:

```bash
ls /Volumes/silo_dev_rs/adk/artifacts/
```

## Adding New Test Tasks

To add a new test task, edit `databricks_rlm_agent/test_tasks.py`:

```python
TASK_LEVEL_N = TestTask(
    difficulty=N,
    issue_key="EVAL-0XX",
    summary="Brief description",
    description="""
Detailed task description with:
- Deliverables
- Acceptance criteria
- Hints if needed
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["catalog.schema.table"],
    delegation_benefit="Why delegate_code_results helps"
)

# Add to registry
TASKS[N] = TASK_LEVEL_N
```

Then rebuild the wheel:

```bash
cd databricks_rlm_agent
uv build --wheel
```

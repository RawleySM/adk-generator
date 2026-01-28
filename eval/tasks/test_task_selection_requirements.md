# Test Task Selection Requirements

## Purpose
This document defines the selection criteria for identifying a real, completed Jira ticket from `silo_dev_rs.task.jira_raw_dev` that will be used to create a new test task in `scripts/test_tasks.py`.

## Selection Criteria

### 1. Ticket Characteristics
- **Status**: Must be a COMPLETED/DONE ticket (not in progress or backlog)
- **Type**: Specification/planning ticket that involved creating a feature implementation plan
- **Ticket Pattern**: Must match `DEV-XXXX` format (constraint from task schema)
- **Has Attachments**: Must have text-based attachments (specs, requirements, plans) that would require `get_Jira_ticket_attachments.py`

### 2. Tool Integration Requirements
The ticket must naturally require use of these tools (without explicit instructions):

| Tool | Requirement |
|------|-------------|
| `repo_filename_search.py` | Task description uses 2 vague keywords that enable locating the right repository/files |
| `get_repo_file.py` | Associated GitHub files must exist that are relevant to the task |
| `get_Jira_ticket_attachments.py` | Must have downloadable text attachments (spec docs, plans, etc.) |
| `metadata_keyword_search.py` | Should reference Unity Catalog tables/data that facilitate searching |

### 3. Task Description Design
- **Vague but discoverable**: Description should NOT explicitly state which tools to use
- **Keyword-driven**: Should contain 2 keywords sufficient for `repo_filename_search`
- **Natural workflow**: Agent should organically discover the need to:
  1. Search for related code files
  2. Download GitHub files for context
  3. Download Jira attachments for specifications
  4. Query Unity Catalog data if referenced

### 4. Unity Catalog Integration
Ideal ticket characteristics:
- References specific tables in silo_dev_rs catalog
- Has data lineage or ETL components
- Involves metadata or schema changes

## Data Source
**Table**: `silo_dev_rs.task.jira_raw_dev`

### Expected Schema (to verify)
- `issue_key`: Jira ticket ID (e.g., DEV-1234)
- `summary`: Ticket title
- `description`: Full ticket description
- `status`: Current ticket status
- `issue_type`: Type (Story, Task, Spec, etc.)
- `attachments`: JSON array of attachment metadata
- Additional fields TBD from DESCRIBE

## Query Strategy

### Step 1: Explore Schema
```sql
DESCRIBE TABLE silo_dev_rs.task.jira_raw_dev
```

### Step 2: Find Completed Specs with Attachments
```sql
SELECT issue_key, summary, status, issue_type, attachments
FROM silo_dev_rs.task.jira_raw_dev
WHERE status IN ('Done', 'Closed', 'Complete', 'Resolved')
  AND (
    LOWER(summary) LIKE '%spec%' 
    OR LOWER(summary) LIKE '%plan%'
    OR LOWER(issue_type) LIKE '%spec%'
  )
  AND attachments IS NOT NULL
  AND CAST(attachments AS STRING) != '[]'
LIMIT 20
```

### Step 3: Identify Tickets with UC Table References
Look for tickets that mention:
- Table names (e.g., `silo_dev_rs.dbo.*`, `silo_dev_rs.task.*`)
- ETL/pipeline keywords
- Data model or schema changes

### Step 4: Create Temporary Views in silo_dev_rs.test
Store intermediate results as views for analysis:
```sql
CREATE OR REPLACE TEMPORARY VIEW silo_dev_rs.test.candidate_spec_tickets AS
SELECT ...
```

## Output
Selected ticket will be documented with:
1. Issue key (DEV-XXXX)
2. Summary/title
3. Why it meets criteria
4. Expected tool usage pattern
5. Vague description text for test task

---

## Final Selection: DEV-3202

### Ticket Details
- **Issue Key**: DEV-3202
- **Summary**: MVM | Run Batches for Vendor Matching using Perplexity
- **Status**: Done
- **Type**: Story
- **Description**: Need to run vendor matching job using Perplexity. Acceptance Criteria: Run Data batches for possible vendor matching using *Sonar-Pro Perplexity* model

### Why This Ticket Meets Criteria

| Criterion | How DEV-3202 Meets It |
|-----------|----------------------|
| Completed ticket | Status: Done |
| Text-based attachments | `Benchmark_Vendors.csv` (273KB), `perplexity-sonar-pro-05012025_processed.csv` (text/plain) |
| GitHub files needed | Related to `Master-Vendor-Alignment` repo and `ai_models/perplexity.py` code |
| Vague 2-keyword discovery | Keywords "perplexity" + "vendor" will find related code files |
| Unity Catalog data | References vendor tables: `silo_dev_rs.dbo.vendors`, `silo_dev_rs.dbo.jobmatchentities`, `silo_dev_rs.task.ai_vendor_match_enriched` |

### Expected Agent Tool Usage Pattern (Implicit)

1. **repo_filename_search**: Agent should search for "perplexity" or "vendor match" to find related code
2. **get_repo_file**: Download found Python files for context on implementation
3. **get_Jira_ticket_attachments**: Download the CSV benchmark data files attached to the ticket
4. **metadata_keyword_search**: Discover vendor-related tables in Unity Catalog
5. **delegate_code_results**: Query vendor matching results to understand data model

### Vague Task Description Design
The task description intentionally:
- Does NOT mention specific file downloads
- Does NOT name specific tools to use
- Uses general terms that require discovery
- References the ticket for context but requires investigation

### Stored View
Created: `silo_dev_rs.test.candidate_test_task_ticket`

---
*Document created: 2026-01-26*
*Author: Automated selection process*

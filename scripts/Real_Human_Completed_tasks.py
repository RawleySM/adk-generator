"""
Real-world completed Jira tasks for benchmarking databricks_analyst agent performance.

These tasks are sourced from silo_dev_rs.task.jira_raw_data and represent actual
work items that have been marked 'Done'. They serve as a benchmark for comparing
agent-generated plans/specs against human completion.

Tasks are numbered 11-15 to continue the sequence from test_tasks.py.

Difficulty Mapping (Approximate):
  11 (Level 4-5): Documentation & Metadata Mapping
  12 (Level 6): Logic & Data Joins (Scripting)
  13 (Level 7-8): New Module/Integration (Scraper)
  14 (Level 9): POC/Research (Embeddings)
  15 (Level 10): Full Stack App Spec (Streamlit)
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TestTask:
    """A test task for the databricks_analyst agent."""
    difficulty: int
    issue_key: str
    summary: str
    description: str
    priority: str = "Medium"
    story_points: float = 3.0
    expected_tables: list[str] = None  # Tables the agent should discover/use
    delegation_benefit: str = ""  # Why delegate_code_results helps for this task


# =============================================================================
# LEVEL 11 - Documentation & Metadata Mapping (DATA-721)
# =============================================================================
TASK_LEVEL_11 = TestTask(
    difficulty=11,
    issue_key="DATA-721",
    summary="Confluence Process Master Report Documentation",
    description="""
To make sure that each report testing confluence page has the following information: 

* DBT report name
* Remote Access, database and table name
* How its written to that table (i.e. created-once, update/insert, append-only, other?) 

**Deliverables:**
1. Identify the relevant DBT models and their corresponding Unity Catalog tables.
2. Determine the write mode for each table (e.g., by inspecting code or table history).
3. Generate a documentation summary for each report.

**Acceptance Criteria:**
- Accurate mapping of Report -> DBT Model -> UC Table
- Correct identification of write strategies
""",
    priority="High",
    story_points=8.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Summarize table metadata and report mappings to avoid output limits."
)


# =============================================================================
# LEVEL 12 - Logic & Data Joins (DATA-792)
# =============================================================================
TASK_LEVEL_12 = TestTask(
    difficulty=12,
    issue_key="DATA-792",
    summary="MVM - Create/Modify SM Common submit script",
    description="""
Summary: 

We need a script and databricks job that will submit unsubmitted vendors from SM Common to Master Vendor Management’s submission API. We should provide configurable parameters on vendors to process, such as min_volume, last_used_date, and volume_rank_percent etc. This should submit the unsubmitted vendors based on those filters. 

Example (default values):

* min_volume : $5000 
* spend_volume_rank: 95%
* Is_person: false 

This could be explained as to only include vendors with more than $5000 in spend, is within the top 95% of vendors spend, and is not marked as a person. 

Pre Requirements: 

* DBT Unsubmitted Vendors view (silo_{code}.sm.export_mvm__unsubmitted_vendors?)
* Unity Catalog Table, MVM Submitted Jobs, to join jobs and customer data. main.mvm.customer_jobs

Process Requirements:

* Query unsubmitted vendors
* Submit to MVM API
* Append Customer/Job mapping to MVM Submitted jobs

**Deliverables:**
1. Identify the correct source tables/views for unsubmitted vendors and submitted jobs.
2. Outline the logic for the submission script, including filtering and API interaction.
3. Spec out the Databricks job configuration.

**Acceptance Criteria:**
- Correctly identified source tables in UC.
- Logic covers all filtering requirements.
""",
    priority="High",
    story_points=8.0,
    expected_tables=[
        "silo_dev_rs.metadata.columnnames", 
        "main.mvm.customer_jobs"
    ],
    delegation_benefit="Summarize script logic and table dependencies."
)


# =============================================================================
# LEVEL 13 - New Module/Integration (DATA-817)
# =============================================================================
TASK_LEVEL_13 = TestTask(
    difficulty=13,
    issue_key="DATA-817",
    summary="Qualys Report Scraper Setup",
    description="""
*Description*: Develop a scraper to log into QualysGuard at [https://qualysguard.qg4.apps.qualys.com/|https://qualysguard.qg4.apps.qualys.com/], navigate to the VMDR reports section, and download the latest weekly report.

*Acceptance Criteria*:

* Scraper logs into Qualys using {{username=spend6ap}} and {{password='redacted'}}.
* Scraper navigates to "VMDR > Reports" and retrieves the latest report.
* Report is downloaded and securely stored in Azure Blob Storage.

**Deliverables:**
1. Research existing scraper patterns in the codebase.
2. Propose a design for the Qualys scraper (libraries, auth handling, storage).
3. Identify where this code should live in the repository.

**Acceptance Criteria:**
- Design follows existing repo patterns (if any).
- Secure handling of credentials and storage paths is addressed.
""",
    priority="Medium",
    story_points=8.0,
    expected_tables=[],
    delegation_benefit="Summarize scraper requirements and potential reuse of existing patterns."
)


# =============================================================================
# LEVEL 14 - POC/Research (DATA-851)
# =============================================================================
TASK_LEVEL_14 = TestTask(
    difficulty=14,
    issue_key="DATA-851",
    summary="MVM - POC Embedding Matching",
    description="""
We would like to do a proof of concept to see if using an embedding model can improve our overall matching efficiency either in or out of splink.

* Using Sbert
** [SentenceTransformers Documentation — Sentence Transformers documentation|https://www.sbert.net/]
** We can start with the model “{{all-MiniLM-L6-v2}}" 
* Create a playground notebook that demonstrates the creation of a name_embedding vector array and compares entries using cosine similarity.
* Review efficiency, discuss next steps.

Probable next steps:

* Test integration of TF-IDF into cosine similarity (i.e. penalize common occurring words)
* Create TI-IDF index based on Master Vendor dataset. Set refresh interval
* Create embedding hash map table( vendor_name, vector_array) to reduce runtime computation

**Deliverables:**
1. A plan for the POC notebook, including necessary libraries and data sources.
2. Identification of the Master Vendor dataset in UC to be used for testing.
3. Theoretical approach for integrating this into the existing matching pipeline.

**Acceptance Criteria:**
- Plan addresses Sbert usage and cosine similarity.
- Correct identification of vendor data tables.
""",
    priority="Medium",
    story_points=8.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Synthesize POC findings and library requirements."
)


# =============================================================================
# LEVEL 15 - Full Stack App Spec (DATA-864)
# =============================================================================
TASK_LEVEL_15 = TestTask(
    difficulty=15,
    issue_key="DATA-864",
    summary="PS Vendor Categorization Streamlit Initial",
    description="""
Summary: Create an initial working solution of a streamlit application that interacts with the Masterdata API to assign PS Vendor Category Tags (or more generally to any tag). 

Requirements:

* Input file (see attached)
* Tag Dropdown
* Generate table (Master Name, Tag)
** Tag is a searchable dropdown of all possible PS tags.
* On dropdown selection update tag to value. 
* Complete and Download button
** Should complete and download an excel with the changed tags applied.

Success Criteria: 

* No long wait times
* Only required columns should be Master Name and Master Id, (should amend all of same columns as input for export though)
* The dropdown selection should be based on API/Data not the file
* Tag updates should persist to the master vendor

**Deliverables:**
1. Detailed specification for the Streamlit app components and data flow.
2. Inventory of necessary APIs or UC tables for fetching tags and updating vendors.
3. Logic for the file processing and export.

**Acceptance Criteria:**
- Spec covers all UI requirements (dropdowns, buttons).
- Data backend (API/Tables) is correctly identified.
""",
    priority="Highest",
    story_points=8.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Synthesize full app requirements and data flow."
)


# =============================================================================
# Task Registry
# =============================================================================
TASKS = {
    11: TASK_LEVEL_11,
    12: TASK_LEVEL_12,
    13: TASK_LEVEL_13,
    14: TASK_LEVEL_14,
    15: TASK_LEVEL_15,
}


def get_task(difficulty: int) -> Optional[TestTask]:
    """Get a test task by difficulty level (11-15)."""
    return TASKS.get(difficulty)


def list_tasks() -> list[tuple[int, str, str]]:
    """List all tasks with (difficulty, issue_key, summary)."""
    return [(d, t.issue_key, t.summary) for d, t in sorted(TASKS.items())]


def format_task_as_prompt(task: TestTask) -> str:
    """Format a TestTask as a prompt string for direct agent invocation.

    This bypasses the ingestor polling mechanism for direct E2E testing.

    Args:
        task: A TestTask instance to format.

    Returns:
        Formatted prompt string including issue metadata and description.
    """
    return f"""## Task: {task.issue_key} - {task.summary}

**Priority:** {task.priority}
**Story Points:** {task.story_points}
**Difficulty Level:** {task.difficulty}/15

### Description
{task.description}

### Delegation Guidance
{task.delegation_benefit}
"""


def get_task_prompt(difficulty: int) -> Optional[str]:
    """Get a formatted prompt for a test task by difficulty level.

    Args:
        difficulty: Task difficulty level (11-15).

    Returns:
        Formatted prompt string, or None if level not found.
    """
    task = get_task(difficulty)
    if task:
        return format_task_as_prompt(task)
    return None


if __name__ == "__main__":
    # Print task summary when run directly
    print("=" * 70)
    print("AVAILABLE BENCHMARK TASKS (REAL COMPLETED JIRA TASKS)")
    print("=" * 70)
    for difficulty, issue_key, summary in list_tasks():
        task = TASKS[difficulty]
        print(f"\nLevel {difficulty}: {issue_key}")
        print(f"  Summary: {summary}")
        print(f"  Priority: {task.priority} | Story Points: {task.story_points}")
        print(f"  Delegation Benefit: {task.delegation_benefit[:60]}...")
    print("\n" + "=" * 70)

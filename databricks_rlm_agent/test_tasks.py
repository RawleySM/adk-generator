"""
Progressive difficulty test tasks for evaluating databricks_analyst agent.

Each task is designed to test deep exploration and probing behaviors:
- Codebase discovery via GitHub tools (repo_filename_search, get_repo_file)
- UC metadata mining via metadata_keyword_search tool
- Code execution and result processing via delegate_code_results
- Building specs and execution plans from vague Jira tasks
- Recursive exploration across code and data domains

IMPORTANT: Any files or views created during a test MUST be written to
/Volumes/silo_dev_rs/repos/test_dev/.

Available Tools:
- delegate_code_results: Delegates code execution to job_builder and result 
  processing to results_processor_agent
- metadata_keyword_search: Searches Unity Catalog metadata for columns/tables
- repo_filename_search: Searches repository files by pattern
- get_repo_file: Downloads files from GitHub repos to UC Volumes
- save_artifact_to_volumes: Saves artifacts to UC Volumes
- exit_loop: Terminates the LoopAgent iteration

Difficulty Scale (1-10):
  1-2: Simple queries, minimal joins
  3-4: Aggregations, basic joins, filtering
  5-6: Multi-table analysis, metadata exploration
  7-8: Cross-schema investigation, data quality analysis
  9-10: Complex workflow analysis, iterative exploration with synthesis
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
# LEVEL 1 - Basic UC + Codebase Probe
# =============================================================================
TASK_LEVEL_1 = TestTask(
    difficulty=1,
    issue_key="EVAL-001",
    summary="Warm-up probe: validate access + tiny catalog scan",
    description="""
Perform a minimal probe that validates UC and repo access before deeper tasks.

**Deliverables:**
- Run a simple UC query using delegate_code_results to confirm access.
- Use metadata_keyword_search to discover 3 random tables and validate schema discovery.
- Use repo_filename_search to find 3 files related to "agent" or "workflow" in any repo.
- Briefly summarize what domains you can access.
- A short access check report (1 paragraph)
- List of 3 UC tables found via metadata
- List of 3 repo files found via GitHub tooling

**Acceptance Criteria:**
- Evidence of both UC and repo exploration
- No large outputs; keep concise

This is a warmup task that proves baseline tool access.
""",
    priority="Low",
    story_points=1.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Minimal, but use delegate_code_results if output needs summarization"
)


# =============================================================================
# LEVEL 2 - Jira Task Sampling + Hypothesis Seeding
# =============================================================================
TASK_LEVEL_2 = TestTask(
    difficulty=2,
    issue_key="EVAL-002",
    summary="Sample Jira tasks and hypothesize data scope",
    description="""
Use delegate_code_results to sample 10 Jira tasks from
silo_dev_rs.task.jira_raw_data. Select a handful of fields that summarize each task.

**Deliverables:**
- A small table of 10 Jira tasks (key + summary + description snippet).
- For 3 tasks, infer likely data domains and code areas to explore next.
- List 3 UC tables and 3 repo files that look relevant to those tasks.
- 10-row Jira sample (short)
- 3 task-level hypotheses with next steps
- Relevant UC tables + repo files

**Acceptance Criteria:**
- Uses delegate_code_results for sampling
- Demonstrates cross-domain reasoning (Jira -> data + code)
""",
    priority="Low",
    story_points=2.0,
    expected_tables=["silo_dev_rs.task.jira_raw_data"],
    delegation_benefit="Summarize Jira sample and hypotheses without dumping large outputs"
)


# =============================================================================
# LEVEL 3 - Catalog-First Discovery
# =============================================================================
TASK_LEVEL_3 = TestTask(
    difficulty=3,
    issue_key="EVAL-003",
    summary="Discover data domains using columnnames metadata",
    description="""
Use metadata_keyword_search tool to discover tables that indicate
workflows, jobs, tasks, tickets, or audits. Use columnname pattern searches
like 'workflow', 'task', 'jira', 'audit', 'status', 'phase', 'owner'.

**Deliverables:**
1. A curated list of 8-12 tables grouped by domain (workflow, task, audit, etc.)
2. For each table, list key columns (3-5 per table) inferred from metadata
3. A short narrative explaining why these domains matter for agent behavior

**Acceptance Criteria:**
- Uses metadata_keyword_search as the primary discovery tool
- Clear grouping by domain
- Delegate summarization for compact output
""",
    priority="Medium",
    story_points=3.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Synthesize metadata matches into domain map"
)


# =============================================================================
# LEVEL 4 - GitHub Repo Recon + UC Crosswalk
# =============================================================================
TASK_LEVEL_4 = TestTask(
    difficulty=4,
    issue_key="EVAL-004",
    summary="Map code modules to likely data tables",
    description="""
Use repo_filename_search and get_repo_file to locate code related to
workflows, agents, tasks, or Jira ingestion across any available repos.
Then connect those code paths to relevant UC tables via metadata_keyword_search.

**Deliverables:**
1. 5-8 code files that appear relevant (with brief rationale)
2. For each file, list 2-3 UC tables that look related
3. A short "code-to-data" map describing possible data flows

**Acceptance Criteria:**
- Evidence of GitHub tools usage to inspect files
- A clear mapping between code artifacts and data tables
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Summarize repo findings and table mapping cleanly"
)


# =============================================================================
# LEVEL 5 - Vague Jira Story -> Exploration Plan
# =============================================================================
TASK_LEVEL_5 = TestTask(
    difficulty=5,
    issue_key="EVAL-005",
    summary="Convert vague Jira request into exploration plan",
    description="""
Pick 1 Jira task from silo_dev_rs.task.jira_raw_data that has vague wording.
Turn it into a concrete exploration plan spanning code, UC tables, and views.

**Deliverables:**
1. Jira task summary + missing details (assumptions list)
2. A step-by-step exploration plan (8-12 steps)
3. Initial list of UC tables and repo files to inspect
4. A list of targeted questions to answer via metadata + code reading

**Acceptance Criteria:**
- Jira-driven plan is concrete and cross-domain
- Includes metadata_keyword_search and GitHub tool usage
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["silo_dev_rs.task.jira_raw_data", "silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Summarize hypotheses and multi-step plan"
)


# =============================================================================
# LEVEL 6 - Recursive Discovery + UC Validation
# =============================================================================
TASK_LEVEL_6 = TestTask(
    difficulty=6,
    issue_key="EVAL-006",
    summary="Recursive probe: discover workflow tables and validate with samples",
    description="""
Start from metadata_keyword_search to discover workflow/task-related tables.
Then validate the top 5 candidates by sampling rows (limit 5) using
delegate_code_results.

**Deliverables:**
1. A ranked list of 10 candidate tables with reasons
2. Sample rows from the top 5 tables
3. Updated hypothesis: which tables are likely authoritative

**Acceptance Criteria:**
- Uses metadata_keyword_search for discovery
- Validates top candidates with data samples
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Parse metadata + summarize samples and ranking"
)


# =============================================================================
# LEVEL 7 - Code + Data Lineage Reconstruction
# =============================================================================
TASK_LEVEL_7 = TestTask(
    difficulty=7,
    issue_key="EVAL-007",
    summary="Trace lineage from Jira task to code to UC tables",
    description="""
Select a Jira task that references a workflow or automation outcome. Trace
the likely lineage from Jira -> code paths -> UC tables/views.

**Deliverables:**
1. Jira task summary + assumed system context
2. GitHub tool-based file discovery with 5-10 relevant files
3. UC table discovery using metadata_keyword_search
4. A text lineage map (code -> tables -> views)
5. Evidence: sample queries for key tables

**Acceptance Criteria:**
- Clear cross-domain lineage narrative
- Uses repo_filename_search, get_repo_file + metadata_keyword_search
- Uses delegate_code_results for sampling
""",
    priority="High",
    story_points=8.0,
    expected_tables=["silo_dev_rs.task.jira_raw_data", "silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Maintain lineage context across code + data exploration"
)


# =============================================================================
# LEVEL 8 - Spec a Complex Data App (Exploration-Only)
# =============================================================================
TASK_LEVEL_8 = TestTask(
    difficulty=8,
    issue_key="EVAL-008",
    summary="Spec a data application from sparse Jira signals",
    description="""
Using 2-3 Jira tasks with vague descriptions, define a proposed data application
spec. The work must be exploratory: probe codebases, inspect UC metadata, and
sample tables. The objective is to produce a spec and a plan, not to build.

**Deliverables:**
1. A data application specification (inputs, outputs, flows)
2. A table/view inventory that supports the spec
3. Code artifacts that appear to already implement pieces of the flow
4. A risk list (missing data, ambiguous definitions)
5. A deliverable plan with milestones

**Acceptance Criteria:**
- Spec is grounded in evidence from code + UC metadata
- Uses repo_filename_search, get_repo_file + delegate_code_results for sampling
""",
    priority="High",
    story_points=8.0,
    expected_tables=["silo_dev_rs.task.jira_raw_data", "silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Synthesize evidence into a coherent spec and plan"
)


# =============================================================================
# LEVEL 9 - Multi-Repo + UC Deep Recon
# =============================================================================
TASK_LEVEL_9 = TestTask(
    difficulty=9,
    issue_key="EVAL-009",
    summary="Deep recon: multi-repo probing + UC coverage map",
    description="""
Perform a deep exploration across any available repos and UC schemas to create
a coverage map for a hypothetical data application. Assume the Jira story is
vague and hints at "workflow automation" and "task intelligence".

**Investigation Areas:**

1. **Repo Recon:**
   - Use repo_filename_search to locate 20-30 candidate files
   - Pull 5-8 key files via get_repo_file and summarize their purpose
2. **UC Recon:**
   - Use metadata_keyword_search to identify tables for tasks, workflows, audits
   - Validate top 10 tables with delegate_code_results sampling
3. **Coverage Map:**
   - Create a cross-domain map: features -> code modules -> tables/views
4. **Gaps & Risks:**
   - Identify missing tables, unclear ownership, or weak metadata

**Acceptance Criteria:**
- Evidence of deep code + data probing
- A clear coverage map with gaps/risks
""",
    priority="High",
    story_points=13.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Summarize large recon findings and synthesize coverage map"
)


# =============================================================================
# LEVEL 10 - End-to-End Exploration Spec + Artifacts
# =============================================================================
TASK_LEVEL_10 = TestTask(
    difficulty=10,
    issue_key="EVAL-010",
    summary="Generate full exploration report + artifact outputs",
    description="""
Build a comprehensive exploration report for a complex data application,
based on vague Jira tasks, deep UC metadata exploration, and multi-repo code
probing. This task MUST produce artifacts saved under:
/Volumes/silo_dev_rs/repos/test_dev/

**Deliverables:**

1. **Exploration Report (Markdown):**
   - Executive summary
   - Jira task interpretations and assumptions
   - Code inventory with rationale
   - UC table/view inventory with evidence
   - Lineage and data flow diagrams (text-based)
   - Gaps/risks and remediation plan

2. **Artifacts Saved to Volumes:**
   - /Volumes/silo_dev_rs/repos/test_dev/exploration_report.md
   - /Volumes/silo_dev_rs/repos/test_dev/table_inventory.csv
   - /Volumes/silo_dev_rs/repos/test_dev/code_inventory.md

**Output Format:**
Produce a structured report suitable for engineers and data governance teams.

**Acceptance Criteria:**
- Artifacts written to the required UC Volume path
- Evidence of extensive code + data exploration

This is the most complex task requiring extensive exploration, synthesis, and
documentation. The agent MUST use delegate_code_results to:
- Summarize intermediate findings to avoid context rot
- Build progressive documentation
- Synthesize cross-schema patterns
- Generate the final structured report

Without delegate_code_results, context will exceed limits before completion.
""",
    priority="High",
    story_points=21.0,
    expected_tables=[
        "silo_dev_rs.metadata.columnnames",
        "silo_dev_rs.information_schema.*",
        "All tables in silo_dev_rs.*"
    ],
    delegation_benefit="Absolutely required - impossible to complete without intermediate summarization"
)


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
    1: TASK_LEVEL_1,
    2: TASK_LEVEL_2,
    3: TASK_LEVEL_3,
    4: TASK_LEVEL_4,
    5: TASK_LEVEL_5,
    6: TASK_LEVEL_6,
    7: TASK_LEVEL_7,
    8: TASK_LEVEL_8,
    9: TASK_LEVEL_9,
    10: TASK_LEVEL_10,
    11: TASK_LEVEL_11,
    12: TASK_LEVEL_12,
    13: TASK_LEVEL_13,
    14: TASK_LEVEL_14,
    15: TASK_LEVEL_15,
}


def get_task(difficulty: int) -> Optional[TestTask]:
    """Get a test task by difficulty level (1-15)."""
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
        difficulty: Task difficulty level (1-15).

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
    print("AVAILABLE TEST TASKS FOR DATABRICKS ANALYST EVALUATION")
    print("=" * 70)
    for difficulty, issue_key, summary in list_tasks():
        task = TASKS[difficulty]
        print(f"\nLevel {difficulty}: {issue_key}")
        print(f"  Summary: {summary}")
        print(f"  Priority: {task.priority} | Story Points: {task.story_points}")
        print(f"  Delegation Benefit: {task.delegation_benefit[:60]}...")
    print("\n" + "=" * 70)

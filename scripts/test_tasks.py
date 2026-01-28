"""
Progressive difficulty test tasks for evaluating databricks_analyst agent.

Each task is designed to benefit from the delegate_code_results tool to avoid context rot:
- Large result sets that need execution and summarization via downstream agents
- Schema discovery tasks requiring interpretation of metadata
- Multi-step analysis where intermediate findings inform next steps
- Pattern recognition across large datasets

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
# LEVEL 1 - Basic Count Query
# =============================================================================
TASK_LEVEL_1 = TestTask(
    difficulty=1,
    issue_key="EVAL-001",
    summary="Count total vendors in master data",
    description="""
Count the total number of vendors in the silo_dev_rs.dbo.vendors table.

**Deliverables:**
- Report the total vendor count
- Confirm the table exists and is accessible

**Acceptance Criteria:**
- Single number result with brief confirmation

This is a warmup task to verify basic SQL execution capability.
""",
    priority="Low",
    story_points=1.0,
    expected_tables=["silo_dev_rs.dbo.vendors"],
    delegation_benefit="Minimal - simple query, direct execution sufficient"
)


# =============================================================================
# LEVEL 2 - Filtered Query with Basic Interpretation
# =============================================================================
TASK_LEVEL_2 = TestTask(
    difficulty=2,
    issue_key="EVAL-002",
    summary="Find active vendors with websites",
    description="""
Query the vendors table to find vendors that are currently active (Status = 'Active')
AND have a website URL populated.

**Deliverables:**
- Count of vendors matching criteria
- Sample of 5 vendor names with their websites
- Brief observation about website URL patterns

**Acceptance Criteria:**
- Count reported
- Sample data provided
- Pattern observation (e.g., "most URLs are corporate domains")

**Hint:** The vendors table is at silo_dev_rs.dbo.vendors
""",
    priority="Low",
    story_points=2.0,
    expected_tables=["silo_dev_rs.dbo.vendors"],
    delegation_benefit="Use delegate_code_results to summarize URL patterns from sample data"
)


# =============================================================================
# LEVEL 3 - Aggregation with GROUP BY
# =============================================================================
TASK_LEVEL_3 = TestTask(
    difficulty=3,
    issue_key="EVAL-003",
    summary="Analyze vendor distribution by status and class",
    description="""
Analyze the distribution of vendors across different Status and Class values
in the silo_dev_rs.dbo.vendors table.

**Deliverables:**
1. Count of vendors by Status (top 10)
2. Count of vendors by Class (top 10)
3. Cross-tabulation of Status x Class (top combinations)
4. Summary insights about the vendor population

**Acceptance Criteria:**
- All three analyses completed
- Insights provided about data distribution patterns
- Identify any data quality issues (e.g., NULL values, unexpected categories)

Use delegate_code_results to synthesize findings into actionable insights rather than
dumping raw aggregation results.
""",
    priority="Medium",
    story_points=3.0,
    expected_tables=["silo_dev_rs.dbo.vendors"],
    delegation_benefit="Synthesize multiple aggregation results into coherent narrative"
)


# =============================================================================
# LEVEL 4 - Two-Table JOIN Analysis
# =============================================================================
TASK_LEVEL_4 = TestTask(
    difficulty=4,
    issue_key="EVAL-004",
    summary="Analyze vendor-location relationship coverage",
    description="""
Investigate the relationship between vendors and their locations by joining
silo_dev_rs.dbo.vendors with silo_dev_rs.dbo.locations.

**Deliverables:**
1. How many vendors have at least one location?
2. How many vendors have no locations?
3. What is the average number of locations per vendor?
4. Which vendors have the most locations (top 5)?
5. Geographic distribution summary (locations by State, top 10)

**Acceptance Criteria:**
- All five metrics calculated
- Join performed correctly (locations.VendorId = vendors.Id)
- Summary interpretation of vendor coverage

**Note:** Focus on non-deleted records (DeletedUTC IS NULL).
Use delegate_code_results to interpret findings and identify data quality concerns.
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["silo_dev_rs.dbo.vendors", "silo_dev_rs.dbo.locations"],
    delegation_benefit="Interpret geographic patterns and coverage gaps from multi-query results"
)


# =============================================================================
# LEVEL 5 - Multi-Table Analytics with Business Context
# =============================================================================
TASK_LEVEL_5 = TestTask(
    difficulty=5,
    issue_key="EVAL-005",
    summary="Profile vendor match enrichment data quality",
    description="""
Analyze the silo_dev_rs.task.ai_vendor_match_enriched view to understand
the quality and patterns of AI-assisted vendor matching.

**Deliverables:**
1. Total records and date range of matches
2. Distribution of ActionTaken values
3. Distribution of NumCandidates (how many options were presented)
4. Confidence score analysis:
   - Average SelectedConfidence
   - Distribution by confidence buckets (0-50, 50-80, 80-100)
5. Comparison: MaxConfidence vs SelectedConfidence (did humans pick best match?)
6. Top 5 users (smUser) by match volume

**Acceptance Criteria:**
- Comprehensive profile of the matching workflow
- Insights into human vs algorithmic matching behavior
- Data quality observations

This task requires multiple queries with synthesis. Use delegate_code_results to:
- Interpret confidence score patterns
- Summarize human decision-making patterns
- Identify potential process improvements
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["silo_dev_rs.task.ai_vendor_match_enriched"],
    delegation_benefit="Essential for synthesizing multi-faceted analysis into coherent insights"
)


# =============================================================================
# LEVEL 6 - Schema Discovery via Metadata
# =============================================================================
TASK_LEVEL_6 = TestTask(
    difficulty=6,
    issue_key="EVAL-006",
    summary="Discover tables containing TIN/tax information",
    description="""
Use the metadata catalog at silo_dev_rs.metadata.columnnames to discover
all tables in the silo_dev_rs catalog that contain tax-related information
(TIN, TaxIdentificationNumber, FederalTax, etc.).

**Deliverables:**
1. List of all tables containing TIN-related columns
2. For each table, list the relevant column names
3. Categorize tables by schema (dbo, task, etc.)
4. Recommend which tables would be authoritative sources for TIN data
5. Identify any potential data quality concerns (e.g., multiple TIN columns in one table)

**Acceptance Criteria:**
- Comprehensive discovery using metadata table
- Columns searched: TIN, Tax, TaxIdentification, FederalTax patterns
- Clear recommendation on authoritative sources

**Hint:** The columnnames table has columns: path (catalog.schema.table) and
column_array (JSON array of "COLUMN_NAME (TYPE)" strings).

Use delegate_code_results to parse the column_array JSON and categorize findings efficiently.
""",
    priority="Medium",
    story_points=5.0,
    expected_tables=["silo_dev_rs.metadata.columnnames"],
    delegation_benefit="Parse and categorize JSON column metadata, synthesize findings across many tables"
)


# =============================================================================
# LEVEL 7 - Cross-Schema Data Lineage Investigation
# =============================================================================
TASK_LEVEL_7 = TestTask(
    difficulty=7,
    issue_key="EVAL-007",
    summary="Trace data lineage for vendor matching workflow",
    description="""
Investigate the data lineage and relationships between tables involved in
the vendor matching process. Start from silo_dev_rs.task.ai_vendor_match_enriched
and trace back to source tables.

**Deliverables:**
1. Document the source tables used by ai_vendor_match_enriched view
   (examine the view definition)
2. For each source table, provide:
   - Row count
   - Key columns relevant to matching
   - Date range of data (if applicable)
3. Create a data lineage diagram (text-based) showing:
   - Source tables -> Intermediate tables -> Final view
4. Identify any potential data freshness issues
5. Document any tables referenced that might not exist or are empty

**Acceptance Criteria:**
- Complete lineage from raw data to enriched view
- Row counts and date ranges for all tables in the chain
- Identification of potential issues

This is a complex investigation requiring:
- Reading view definitions (DESCRIBE EXTENDED shows View Text)
- Multiple table explorations
- Synthesis into coherent lineage documentation

Use delegate_code_results to parse view SQL, extract table references, and build the lineage narrative.
""",
    priority="High",
    story_points=8.0,
    expected_tables=[
        "silo_dev_rs.task.ai_vendor_match_enriched",
        "silo_dev_rs.dbo.jobmatchentities",
        "silo_dev_rs.dbo.algorithmmatchresults",
        "silo_dev_rs.dbo.algorithmmatches",
        "silo_dev_rs.dbo.vendors",
        "silo_dev_rs.dbo.auditjobmatchentities"
    ],
    delegation_benefit="Parse complex view SQL, maintain context across many table explorations"
)


# =============================================================================
# LEVEL 8 - Workflow State Analysis
# =============================================================================
TASK_LEVEL_8 = TestTask(
    difficulty=8,
    issue_key="EVAL-008",
    summary="Analyze workflow execution patterns and bottlenecks",
    description="""
Perform a comprehensive analysis of the workflow system using tables in
silo_dev_rs.workflow schema to identify execution patterns and bottlenecks.

**Deliverables:**
1. **Workflow Inventory:**
   - Count of workflows by status (from workflow_state_v2)
   - Count by current_phase (discovery/planning/implementation)
   - Oldest and newest workflows

2. **Phase Duration Analysis:**
   - Parse phase JSON columns to extract timestamps
   - Calculate average time spent in each phase
   - Identify workflows stuck in a phase for abnormally long times

3. **Event Log Analysis:**
   - Query workflow_event_log_v2 for event patterns
   - Identify most common event types
   - Find any error events or failures

4. **Implementation Progress:**
   - Query v_implementation_progress view
   - Summarize completion rates
   - Identify stalled implementations

5. **Recommendations:**
   - Based on analysis, recommend process improvements
   - Identify potential automation opportunities

**Acceptance Criteria:**
- All four analysis areas completed
- Data-driven recommendations provided
- Clear identification of bottlenecks

This task requires extensive exploration and synthesis. Use delegate_code_results to:
- Parse JSON phase data
- Correlate findings across multiple tables
- Generate actionable recommendations
""",
    priority="High",
    story_points=8.0,
    expected_tables=[
        "silo_dev_rs.workflow.workflow_state_v2",
        "silo_dev_rs.workflow.workflow_event_log_v2",
        "silo_dev_rs.workflow.v_implementation_progress",
        "silo_dev_rs.workflow.v_workflow_summary"
    ],
    delegation_benefit="Critical for JSON parsing, cross-table correlation, and recommendation synthesis"
)


# =============================================================================
# LEVEL 9 - Data Quality Investigation
# =============================================================================
TASK_LEVEL_9 = TestTask(
    difficulty=9,
    issue_key="EVAL-009",
    summary="Investigate vendor data quality across master and client data",
    description="""
Conduct a thorough data quality investigation comparing vendor information
between master data (silo_dev_rs.dbo.vendors) and matching workflow data
(ai_vendor_match_enriched) to identify discrepancies and data quality issues.

**Investigation Areas:**

1. **Name Consistency Analysis:**
   - Compare Master_Name vs MatchedName in enriched view
   - Identify cases where names differ significantly
   - Calculate Levenshtein distance or similarity patterns
   - Categorize discrepancy types (abbreviations, typos, legal vs common name)

2. **Address Completeness:**
   - Profile completeness of address fields (Address1, City, State, ZipCode)
   - Compare Client vs Master address completeness
   - Identify patterns in missing data

3. **TIN Coverage Analysis:**
   - What percentage of matched vendors have TIN data?
   - Compare Client_TIN vs Master_TINLast4 availability
   - Identify potential TIN mismatches

4. **Confidence Score Calibration:**
   - For matches with SelectedConfidence < 80, investigate:
     - What attributes differ between client and master?
     - Are low-confidence matches accurate?
   - Sample 10 low-confidence matches and document findings

5. **Duplicate Detection:**
   - Look for potential duplicate vendors (same name, different IDs)
   - Identify vendors that might be parent-child relationships
   - Check for vendors merged via NewVendorID field

6. **Recommendations:**
   - Prioritized list of data quality issues
   - Suggested remediation actions
   - Estimated impact of each issue

**Acceptance Criteria:**
- All five investigation areas addressed
- Specific examples provided for each issue type
- Quantified findings (percentages, counts)
- Actionable recommendations

This is a complex, iterative investigation requiring many queries and synthesis.
Use delegate_code_results extensively to:
- Interpret name similarity patterns
- Categorize discrepancy types
- Build the prioritized recommendation list
""",
    priority="High",
    story_points=13.0,
    expected_tables=[
        "silo_dev_rs.dbo.vendors",
        "silo_dev_rs.task.ai_vendor_match_enriched",
        "silo_dev_rs.dbo.locations",
        "silo_dev_rs.dbo.vendortins"
    ],
    delegation_benefit="Essential for pattern recognition, categorization, and recommendation synthesis"
)


# =============================================================================
# LEVEL 10 - Comprehensive Catalog Analysis
# =============================================================================
TASK_LEVEL_10 = TestTask(
    difficulty=10,
    issue_key="EVAL-010",
    summary="Build comprehensive data catalog documentation for silo_dev_rs",
    description="""
Create comprehensive documentation for the silo_dev_rs Unity Catalog by
systematically exploring and documenting all schemas, tables, and their
relationships.

**Deliverables:**

1. **Schema Inventory:**
   - List all schemas in silo_dev_rs
   - Count of tables/views per schema
   - Brief description of each schema's purpose

2. **Table Documentation (for each schema):**
   - Table name, type (MANAGED/VIEW)
   - Row count (or estimate for large tables)
   - Column count and key columns
   - Table comment if available
   - Delta table properties (CDF enabled, etc.)

3. **Relationship Mapping:**
   - Using metadata.columnnames, identify potential FK relationships
   - Look for Id/[Table]Id patterns
   - Document key relationships between tables

4. **Data Freshness Report:**
   - For tables with timestamp columns, report latest data
   - Identify potentially stale tables
   - Note any tables with no recent updates

5. **Size and Optimization Analysis:**
   - Identify largest tables (by row count or bytes if available)
   - Note partitioning strategies used
   - Identify tables that might benefit from optimization

6. **Data Domain Classification:**
   - Categorize tables into domains:
     - Master Data (vendors, locations, contacts)
     - Transactional (matches, audits)
     - Analytics (task schema views)
     - Configuration (settings, types)
     - Logging/Telemetry (adk, logs)
   - Identify sensitive data columns (TIN, PII indicators)

7. **Quality Summary:**
   - Tables with no comments/descriptions
   - Schemas with incomplete documentation
   - Recommendations for metadata improvement

**Output Format:**
Produce a structured report suitable for a data governance team. Include:
- Executive summary (1 paragraph)
- Schema-by-schema breakdown
- Key findings and recommendations

**Acceptance Criteria:**
- All schemas documented
- Relationships identified
- Actionable recommendations for data governance

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
# LEVEL 11 - Multi-Source Investigation with Tool Discovery
# =============================================================================
TASK_LEVEL_11 = TestTask(
    difficulty=11,
    issue_key="DEV-3202",
    summary="Investigate Perplexity vendor matching implementation",
    description="""
A completed feature (DEV-3202) implemented AI-based vendor matching using Perplexity.
Your task is to investigate how this feature works and document the implementation.

**Investigation Goals:**

1. **Understand the Perplexity Integration:**
   - How is the Perplexity model integrated for vendor matching?
   - What prompts or configurations are used?
   - How does it compare to existing matching algorithms?

2. **Analyze the Benchmark Data:**
   - The ticket includes benchmark vendor data that was used to validate results
   - What metrics or criteria were used to evaluate matching quality?
   - What were the results of the batch runs?

3. **Document the Data Flow:**
   - Which Unity Catalog tables store vendor matching results?
   - How does the client vendor data flow to the AI model?
   - What fields from jobmatchentities and vendors tables are relevant?

4. **Code Architecture Review:**
   - Locate the implementation code for the Perplexity vendor matching
   - Document key functions and their responsibilities
   - Identify configuration parameters and dependencies

**Deliverables:**
- Summary of how Perplexity is integrated for vendor matching
- Analysis of benchmark data and results
- Data flow diagram (text-based) showing tables and transformations
- Code references with brief explanations of key components
- Recommendations for future improvements or monitoring

**Acceptance Criteria:**
- All four investigation areas addressed with evidence
- Code snippets or file references provided
- Clear understanding of how AI-assisted matching works
- Actionable insights for maintaining or improving the feature

**Hint:** The relevant Jira ticket and codebase both contain important context.
The vendor matching domain spans multiple schemas in silo_dev_rs.
""",
    priority="High",
    story_points=13.0,
    expected_tables=[
        "silo_dev_rs.dbo.vendors",
        "silo_dev_rs.dbo.jobmatchentities",
        "silo_dev_rs.dbo.algorithmmatchresults",
        "silo_dev_rs.task.ai_vendor_match_enriched",
        "silo_dev_rs.task.jira_raw_dev"
    ],
    delegation_benefit="Essential for synthesizing code review, data analysis, and documentation into coherent investigation report"
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
}


def get_task(difficulty: int) -> Optional[TestTask]:
    """Get a test task by difficulty level (1-10)."""
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
**Difficulty Level:** {task.difficulty}/10

### Description
{task.description}

### Delegation Guidance
{task.delegation_benefit}
"""


def get_task_prompt(difficulty: int) -> Optional[str]:
    """Get a formatted prompt for a test task by difficulty level.

    Args:
        difficulty: Task difficulty level (1-10).

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

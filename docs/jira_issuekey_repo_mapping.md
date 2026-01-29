# JIRA ISSUEKEY to GitHub Repository Mapping

**Generated:** 2026-01-29

## Overview

This document maps JIRA issue key prefixes to their corresponding GitHub repositories in the SpendMend organization. The mapping was discovered by:

1. Querying `silo_dev_rs.task.jira_pr_union` for merged PRs from each of the 7 distinct `source_table` values
2. Using the GitHub Search API to find PRs that reference each ISSUEKEY
3. Extracting the repository from matched PR URLs

## ISSUEKEY Prefix to Repository Mapping

| ISSUEKEY Prefix | source_table | GitHub Repository | Jira Project |
|-----------------|--------------|-------------------|--------------|
| **DATA** | data | SpendMend/SpendMend-Data-Explorer | Team: Data |
| **DEV** | dev | SpendMend/SMChat | Team: Development |
| **GAP** | gap | SpendMend/GenAIPharmacy | Gen AI - Pharmacy |
| **RI** | ri | SpendMend/RebateInsight-Web, SpendMend/RebateInsight-Functions | Rebate Insight |
| **TRL** | trl | SpendMend/TrullaDirectEngineering | Trulla Direct DEV |
| **TRUL** | trul | SpendMend/TrullaNew | Trulla |
| **TRULSW** | trulsw | SpendMend/TrullaNew | Trulla Procurement DEV |

## Key Findings

### Shared Repositories

1. **TRUL & TRULSW → TrullaNew**: Both prefixes map to the same repository. They originate from different Jira projects ("Trulla" vs "Trulla Procurement DEV") but share a single codebase.

2. **RI → Multiple Repos**: The Rebate Insight project spans two repositories:
   - `RebateInsight-Web` - Frontend/Web application
   - `RebateInsight-Functions` - Backend/Azure Functions

### PR Volume by Prefix

Based on GitHub search results:
- **RI**: 692+ PRs
- **GAP**: 692+ PRs  
- **TRUL**: 25+ PRs
- **TRULSW**: Shared with TRUL
- **DATA**, **DEV**, **TRL**: Lower volume

## Data Source

### SQL Query Used

```sql
WITH merged_data AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY source_table ORDER BY RAND()) as rn
  FROM silo_dev_rs.task.jira_pr_union 
  WHERE DEVELOPMENT_DATA LIKE '%state=MERGED%' 
     OR DEVELOPMENT_DATA LIKE '%"state":"MERGED"%'
)
SELECT ISSUEKEY, SUMMARY, DESCRIPTION, ISSUE_TYPE, STATUS, STATUS_CATEGORY, 
       RESOLUTION, PRIORITY, ASSIGNEE, REPORTER, CREATOR, CREATED, UPDATED, 
       RESOLVED, PROJECT, LABELS, PARENT, DEVELOPMENT_DATA, source_table
FROM merged_data 
WHERE rn = 1
ORDER BY source_table
```

### Table Structure

The `jira_pr_union` view unions data from 7 raw Jira tables:
- `jira.raw.dev` → source_table='dev'
- `jira.raw.gap` → source_table='gap'
- `jira.raw.ri` → source_table='ri'
- `jira.raw.data` → source_table='data'
- `jira.raw.trulsw` → source_table='trulsw'
- `jira.raw.trul` → source_table='trul'
- `jira.raw.trl` → source_table='trl'

Only records with `DEVELOPMENT_DATA LIKE '%pullrequest%'` are included.

## Generated Scripts

### Main Script: `scripts/query_jira_pr_repos.py`

A Python script that:
1. Queries Databricks for merged PR samples from each source_table
2. Extracts ISSUEKEY prefixes
3. Searches GitHub API for PRs referencing each ISSUEKEY
4. Maps prefixes to repositories
5. Outputs JSON files with results

**Usage:**
```bash
uv run scripts/query_jira_pr_repos.py
```

**Dependencies:**
- databricks-sdk>=0.20.0
- requests>=2.31.0
- python-dotenv>=1.0.0

**Environment Variables Required:**
- `GITHUB_TOKEN` - GitHub Personal Access Token
- `DATABRICKS_PROFILE` - Databricks CLI profile (default: rstanhope)

## Output Files

| File | Description |
|------|-------------|
| `docs/jira_pr_samples.json` | Full JIRA issue data for 7 random merged PR samples |
| `docs/issuekey_repo_mapping.json` | ISSUEKEY prefix to GitHub repo mapping |
| `docs/github_pr_results.json` | Detailed PR info from GitHub API |

## Sample JIRA Issues (One per source_table)

| ISSUEKEY | Summary | source_table |
|----------|---------|--------------|
| DATA-1080 | DE - Feature Requests and Wrap up | data |
| DEV-3384 | SM Chat/Devlabs: Not getting reply from docx file | dev |
| GAP-1663 | Build PUT APIs for Billing Claim Form UB04 & CMS1500 | gap |
| RI-688 | UI Global Style Corrections & Updates | ri |
| TRL-240 | Develop Function to Check Servotron Health | trl |
| TRUL-1688 | Shopping Cart Cleanup | trul |
| TRULSW-116 | Browser Header Abbreviation to capitals | trulsw |

## GitHub PR Examples

| Issue Key | PR URL | Repository |
|-----------|--------|------------|
| DATA-1080 | https://github.com/SpendMend/SpendMend-Data-Explorer/pull/25 | SpendMend-Data-Explorer |
| DEV-3384 | https://github.com/SpendMend/SMChat/pull/160 | SMChat |
| GAP-1663 | https://github.com/SpendMend/GenAIPharmacy/pull/749 | GenAIPharmacy |
| TRL-240 | https://github.com/SpendMend/TrullaDirectEngineering/pull/27 | TrullaDirectEngineering |
| TRULSW-116 | https://github.com/SpendMend/TrullaNew/pull/316 | TrullaNew |

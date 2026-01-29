#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
#   "requests>=2.31.0",
#   "python-dotenv>=1.0.0",
#   "click>=8.1.0",
# ]
# ///
"""Query JIRA PR data and map ISSUEKEY prefixes to GitHub repos.

This script:
1. Queries silo_dev_rs.task.jira_pr_union for merged PRs from each source_table
2. Extracts as JSON
3. Uses GitHub API to find which repos have PRs linked to each ISSUEKEY prefix
4. Populates pr_number column for JIRA issues that reference GitHub PRs
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import requests
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()

DEFAULT_PROFILE = "rstanhope"
GITHUB_ORG = "SpendMend"  # Organization to search within

# Mapping of ISSUEKEY prefixes to GitHub repositories
# Discovered via GitHub Search API - see docs/jira_issuekey_repo_mapping.md
ISSUEKEY_REPO_MAPPING: dict[str, list[str]] = {
    "DATA": ["SpendMend/SpendMend-Data-Explorer"],
    "DEV": ["SpendMend/SMChat"],
    "GAP": ["SpendMend/GenAIPharmacy"],
    "RI": ["SpendMend/RebateInsight-Web", "SpendMend/RebateInsight-Functions"],
    "TRL": ["SpendMend/TrullaDirectEngineering"],
    "TRUL": ["SpendMend/TrullaNew"],
    "TRULSW": ["SpendMend/TrullaNew"],
}


def get_sql_warehouse_id(client) -> str:
    """Get a SQL warehouse ID to execute statements."""
    warehouses = list(client.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.info(f"Using SQL warehouse: {wh.name} (id: {wh.id})")
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value == "STOPPED":
            logger.info(f"Starting SQL warehouse: {wh.name}")
            client.warehouses.start(wh.id)
            for _ in range(30):
                status = client.warehouses.get(wh.id)
                if status.state and status.state.value == "RUNNING":
                    return wh.id
                time.sleep(10)
    raise RuntimeError("No SQL warehouse available.")


def execute_sql(sql: str, profile: str = DEFAULT_PROFILE) -> list[dict[str, Any]] | None:
    """Execute SQL and return results as list of dicts."""
    from databricks.sdk import WorkspaceClient

    logger.info(f"Executing SQL:\n{sql}")
    client = WorkspaceClient(profile=profile)

    try:
        warehouse_id = get_sql_warehouse_id(client)
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s",
        )

        if response.status and response.status.state.value == "SUCCEEDED":
            if response.manifest and response.manifest.schema and response.result and response.result.data_array:
                columns = [col.name for col in response.manifest.schema.columns]
                results = []
                for row in response.result.data_array:
                    results.append(dict(zip(columns, row)))
                logger.info(f"Got {len(results)} rows")
                return results
            return []
        else:
            error = response.status.error if response.status else "Unknown error"
            logger.error(f"SQL execution failed: {error}")
            return None
    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        return None


def query_merged_pr_samples() -> list[dict[str, Any]]:
    """Query one random merged PR row from each source_table."""
    sql = """
    WITH merged_data AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY source_table ORDER BY RAND()) as rn
      FROM silo_dev_rs.task.jira_pr_union 
      WHERE DEVELOPMENT_DATA LIKE '%state=MERGED%' OR DEVELOPMENT_DATA LIKE '%"state":"MERGED"%'
    )
    SELECT ISSUEKEY, SUMMARY, DESCRIPTION, ISSUE_TYPE, STATUS, STATUS_CATEGORY, 
           RESOLUTION, PRIORITY, ASSIGNEE, REPORTER, CREATOR, CREATED, UPDATED, 
           RESOLVED, PROJECT, LABELS, PARENT, DEVELOPMENT_DATA, source_table
    FROM merged_data 
    WHERE rn = 1
    ORDER BY source_table
    """
    return execute_sql(sql) or []


def search_github_prs_for_issue(
    issue_key: str, 
    github_token: str,
    org: str = GITHUB_ORG
) -> list[dict[str, Any]]:
    """Search GitHub for PRs mentioning an issue key.
    
    Uses GitHub Search API to find PRs in the organization that reference
    the given JIRA issue key.
    """
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    # Search for PRs mentioning this issue key in the org
    # Search in title, body, and comments
    search_query = f'org:{org} is:pr "{issue_key}"'
    
    url = "https://api.github.com/search/issues"
    params = {
        "q": search_query,
        "per_page": 10,
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        results = []
        for item in data.get("items", []):
            # Extract repo from the URL
            # html_url looks like: https://github.com/SpendMend/RepoName/pull/123
            html_url = item.get("html_url", "")
            repo_match = re.search(r"github\.com/([^/]+/[^/]+)/pull", html_url)
            repo = repo_match.group(1) if repo_match else "unknown"
            
            results.append({
                "issue_key": issue_key,
                "pr_number": item.get("number"),
                "pr_title": item.get("title"),
                "pr_url": html_url,
                "repo": repo,
                "state": item.get("state"),
                "created_at": item.get("created_at"),
                "closed_at": item.get("closed_at"),
            })
        
        return results
    except requests.exceptions.RequestException as e:
        logger.error(f"GitHub API error for {issue_key}: {e}")
        return []


def search_github_prs_in_repos(
    issue_key: str,
    repos: list[str],
    github_token: str,
) -> list[dict[str, Any]]:
    """Search specific GitHub repos for PRs mentioning an issue key.
    
    Args:
        issue_key: The JIRA issue key to search for (e.g., "GAP-1234")
        repos: List of repos to search (e.g., ["SpendMend/GenAIPharmacy"])
        github_token: GitHub personal access token
        
    Returns:
        List of PR info dicts with pr_number, pr_url, repo, state
    """
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    results = []
    
    for repo in repos:
        # Search in this specific repo
        search_query = f'repo:{repo} is:pr "{issue_key}"'
        
        url = "https://api.github.com/search/issues"
        params = {
            "q": search_query,
            "per_page": 10,
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("items", []):
                html_url = item.get("html_url", "")
                
                results.append({
                    "issue_key": issue_key,
                    "pr_number": item.get("number"),
                    "pr_title": item.get("title"),
                    "pr_url": html_url,
                    "repo": repo,
                    "state": item.get("state"),
                    "created_at": item.get("created_at"),
                    "closed_at": item.get("closed_at"),
                })
                
        except requests.exceptions.RequestException as e:
            logger.error(f"GitHub API error for {issue_key} in {repo}: {e}")
            continue
    
    return results


def query_issues_without_pr_number(
    limit: int = 100,
    profile: str = DEFAULT_PROFILE,
) -> list[dict[str, Any]]:
    """Query JIRA issues that don't have a PR number yet.
    
    Returns issues from jira_pr_union where pr_number is NULL.
    """
    sql = f"""
    SELECT DISTINCT ISSUEKEY, source_table
    FROM silo_dev_rs.task.jira_pr_union
    WHERE pr_number IS NULL
    LIMIT {limit}
    """
    return execute_sql(sql, profile) or []


def upsert_pr_number(
    issue_key: str,
    pr_number: int,
    github_repo: str,
    pr_url: str,
    pr_state: str,
    profile: str = DEFAULT_PROFILE,
) -> bool:
    """Insert or update a PR number for an ISSUEKEY in the lookup table.
    
    Uses MERGE to handle both insert and update cases.
    """
    updated_at = datetime.now(timezone.utc).isoformat()
    
    sql = f"""
    MERGE INTO silo_dev_rs.task.jira_pr_numbers AS target
    USING (SELECT '{issue_key}' AS ISSUEKEY) AS source
    ON target.ISSUEKEY = source.ISSUEKEY
    WHEN MATCHED THEN
        UPDATE SET 
            pr_number = {pr_number},
            github_repo = '{github_repo}',
            pr_url = '{pr_url}',
            pr_state = '{pr_state}',
            updated_at = '{updated_at}'
    WHEN NOT MATCHED THEN
        INSERT (ISSUEKEY, pr_number, github_repo, pr_url, pr_state, updated_at)
        VALUES ('{issue_key}', {pr_number}, '{github_repo}', '{pr_url}', '{pr_state}', '{updated_at}')
    """
    
    result = execute_sql(sql, profile)
    return result is not None


def populate_pr_numbers(
    limit: int = 100,
    profile: str = DEFAULT_PROFILE,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate pr_number for JIRA issues that don't have one yet.
    
    Args:
        limit: Maximum number of issues to process
        profile: Databricks CLI profile
        dry_run: If True, search GitHub but don't write to database
        
    Returns:
        Summary dict with counts of found, updated, not_found, errors
    """
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        logger.error("GITHUB_TOKEN not found in environment")
        return {"error": "GITHUB_TOKEN not found"}
    
    logger.info("=" * 60)
    logger.info("Populating PR Numbers")
    logger.info("=" * 60)
    
    # Get issues without PR numbers
    logger.info(f"\n[Step 1] Querying up to {limit} issues without PR numbers...")
    issues = query_issues_without_pr_number(limit=limit, profile=profile)
    
    if not issues:
        logger.info("No issues found without PR numbers")
        return {"found": 0, "updated": 0, "not_found": 0, "errors": 0}
    
    logger.info(f"Found {len(issues)} issues to process")
    
    summary = {
        "found": 0,
        "updated": 0,
        "not_found": 0,
        "errors": 0,
        "skipped_no_mapping": 0,
        "details": [],
    }
    
    for i, issue in enumerate(issues):
        issue_key = issue["ISSUEKEY"]
        source_table = issue["source_table"]
        prefix = issue_key.split("-")[0]
        
        logger.info(f"\n[{i+1}/{len(issues)}] Processing {issue_key} (source: {source_table})")
        
        # Get repos for this prefix
        repos = ISSUEKEY_REPO_MAPPING.get(prefix, [])
        if not repos:
            logger.warning(f"  No repo mapping for prefix: {prefix}")
            summary["skipped_no_mapping"] += 1
            continue
        
        # Add delay to avoid GitHub rate limiting
        if i > 0:
            time.sleep(0.5)
        
        # Search GitHub for PRs
        pr_results = search_github_prs_in_repos(issue_key, repos, github_token)
        
        if pr_results:
            # Take the first (most relevant) PR
            pr = pr_results[0]
            summary["found"] += 1
            
            logger.info(f"  Found PR #{pr['pr_number']} in {pr['repo']} (state: {pr['state']})")
            
            if not dry_run:
                success = upsert_pr_number(
                    issue_key=issue_key,
                    pr_number=pr["pr_number"],
                    github_repo=pr["repo"],
                    pr_url=pr["pr_url"],
                    pr_state=pr["state"],
                    profile=profile,
                )
                if success:
                    summary["updated"] += 1
                    logger.info(f"  Updated pr_number to {pr['pr_number']}")
                else:
                    summary["errors"] += 1
                    logger.error(f"  Failed to update pr_number")
            else:
                logger.info(f"  [DRY RUN] Would update pr_number to {pr['pr_number']}")
            
            summary["details"].append({
                "issue_key": issue_key,
                "pr_number": pr["pr_number"],
                "repo": pr["repo"],
                "state": pr["state"],
            })
        else:
            summary["not_found"] += 1
            logger.info(f"  No PRs found for {issue_key}")
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info(f"  Issues processed: {len(issues)}")
    logger.info(f"  PRs found: {summary['found']}")
    logger.info(f"  PRs updated: {summary['updated']}")
    logger.info(f"  Not found: {summary['not_found']}")
    logger.info(f"  Errors: {summary['errors']}")
    logger.info(f"  Skipped (no mapping): {summary['skipped_no_mapping']}")
    
    return summary


def get_org_repos(github_token: str, org: str = GITHUB_ORG) -> list[str]:
    """Get list of repos in the organization."""
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    
    repos = []
    page = 1
    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        params = {"per_page": 100, "page": page}
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
                
            repos.extend([repo["full_name"] for repo in data])
            page += 1
            
            if len(data) < 100:
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching repos: {e}")
            break
    
    return repos


def discover_repo_mapping():
    """Discover ISSUEKEY to repo mapping by sampling merged PRs."""
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        logger.error("GITHUB_TOKEN not found in environment")
        return
    
    logger.info("=" * 60)
    logger.info("JIRA PR to GitHub Repo Mapper")
    logger.info("=" * 60)
    
    # Step 1: Query Databricks for merged PR samples
    logger.info("\n[Step 1] Querying merged PR samples from each source_table...")
    samples = query_merged_pr_samples()
    
    if not samples:
        logger.error("No samples retrieved from database")
        return
    
    # Extract and display the samples as JSON
    logger.info(f"\n[Step 2] Retrieved {len(samples)} samples:")
    samples_json = json.dumps(samples, indent=2, default=str)
    print("\n--- JIRA PR Samples JSON ---")
    print(samples_json)
    print("--- End JSON ---\n")
    
    # Save samples to file
    output_dir = Path(__file__).parent
    samples_file = output_dir / "jira_pr_samples.json"
    with open(samples_file, "w") as f:
        json.dump(samples, f, indent=2, default=str)
    logger.info(f"Saved samples to {samples_file}")
    
    # Step 3: Extract unique ISSUEKEY prefixes
    issue_keys = [s["ISSUEKEY"] for s in samples]
    prefixes = list(set(key.split("-")[0] for key in issue_keys))
    logger.info(f"\n[Step 3] Found ISSUEKEY prefixes: {prefixes}")
    
    # Step 4: Search GitHub for PRs linked to each issue
    logger.info("\n[Step 4] Searching GitHub for PRs linked to each issue...")
    
    prefix_to_repos: dict[str, set[str]] = {prefix: set() for prefix in prefixes}
    all_pr_results = []
    
    for sample in samples:
        issue_key = sample["ISSUEKEY"]
        prefix = issue_key.split("-")[0]
        source_table = sample["source_table"]
        
        logger.info(f"  Searching for {issue_key} (source: {source_table})...")
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
        
        pr_results = search_github_prs_for_issue(issue_key, github_token)
        
        if pr_results:
            for pr in pr_results:
                repo = pr["repo"]
                prefix_to_repos[prefix].add(repo)
                pr["source_table"] = source_table
                all_pr_results.append(pr)
                logger.info(f"    Found PR: {pr['pr_url']} in {repo}")
        else:
            logger.info(f"    No PRs found for {issue_key}")
    
    # Step 5: Display the mapping
    logger.info("\n" + "=" * 60)
    logger.info("[Step 5] ISSUEKEY Prefix to GitHub Repo Mapping")
    logger.info("=" * 60)
    
    mapping_results = {}
    for prefix in sorted(prefix_to_repos.keys()):
        repos = sorted(prefix_to_repos[prefix])
        mapping_results[prefix] = repos
        if repos:
            logger.info(f"\n  {prefix}:")
            for repo in repos:
                logger.info(f"    - {repo}")
        else:
            logger.info(f"\n  {prefix}: No repos found (may need manual investigation)")
    
    # Save mapping to file
    mapping_file = output_dir / "issuekey_repo_mapping.json"
    with open(mapping_file, "w") as f:
        json.dump(mapping_results, f, indent=2)
    logger.info(f"\nSaved mapping to {mapping_file}")
    
    # Save all PR results
    pr_results_file = output_dir / "github_pr_results.json"
    with open(pr_results_file, "w") as f:
        json.dump(all_pr_results, f, indent=2)
    logger.info(f"Saved PR results to {pr_results_file}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    print("\n--- ISSUEKEY to Repo Mapping ---")
    print(json.dumps(mapping_results, indent=2))
    print("--- End Mapping ---")


# ============================================================================
# CLI Interface
# ============================================================================

@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context):
    """Query JIRA PR data and populate GitHub PR numbers.
    
    \b
    Commands:
      discover    Discover ISSUEKEY prefix to GitHub repo mapping
      populate    Populate pr_number for JIRA issues without one
    
    \b
    Examples:
      uv run scripts/query_jira_pr_repos.py discover
      uv run scripts/query_jira_pr_repos.py populate --limit 50
      uv run scripts/query_jira_pr_repos.py populate --dry-run
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
def discover():
    """Discover ISSUEKEY prefix to GitHub repo mapping.
    
    Queries merged PR samples from each source_table, searches GitHub,
    and outputs the mapping to issuekey_repo_mapping.json.
    """
    discover_repo_mapping()


@cli.command()
@click.option(
    "--limit", "-l",
    default=100,
    show_default=True,
    help="Maximum number of issues to process.",
)
@click.option(
    "--profile", "-p",
    default=DEFAULT_PROFILE,
    show_default=True,
    help="Databricks CLI profile.",
)
@click.option(
    "--dry-run", "-n",
    is_flag=True,
    help="Search GitHub but don't write to database.",
)
def populate(limit: int, profile: str, dry_run: bool):
    """Populate pr_number for JIRA issues that don't have one yet.
    
    Queries silo_dev_rs.task.jira_pr_union for issues without PR numbers,
    searches GitHub using the ISSUEKEY prefix to repo mapping, and updates
    the jira_pr_numbers lookup table.
    
    \b
    Examples:
      uv run scripts/query_jira_pr_repos.py populate
      uv run scripts/query_jira_pr_repos.py populate --limit 50
      uv run scripts/query_jira_pr_repos.py populate --dry-run
    """
    summary = populate_pr_numbers(limit=limit, profile=profile, dry_run=dry_run)
    
    # Output summary as JSON for programmatic use
    if summary.get("details"):
        output_dir = Path(__file__).parent
        output_file = output_dir / "populate_pr_results.json"
        with open(output_file, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"\nSaved results to {output_file}")


if __name__ == "__main__":
    cli()

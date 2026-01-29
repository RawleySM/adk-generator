#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
#   "requests>=2.31.0",
#   "python-dotenv>=1.0.0",
#   "click>=8.1.0",
#   "pandas>=2.0.0",
# ]
# ///
"""Populate pr_url column for JIRA issues using JIRA's Development Information API.

This script:
1. Loads JIRA credentials from .env (JIRA_API_KEY, USER_NAME)
2. Queries silo_dev_rs.task.jira_pr_union for issues with PR data
3. Calls JIRA's dev-status API to get actual PR URLs
4. Updates the jira_pr_numbers lookup table with pr_url

The JIRA dev-status API returns linked pull requests for each issue.
"""

import base64
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import pandas as pd
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
JIRA_BASE_URL = "https://spendmend.atlassian.net"


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

    logger.debug(f"Executing SQL:\n{sql}")
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


def load_batch_to_dataframe(
    limit: int = 10,
    profile: str = DEFAULT_PROFILE,
    where_clause: str | None = None,
) -> pd.DataFrame:
    """Load a batch of rows from jira_pr_union into a pandas DataFrame.
    
    Args:
        limit: Number of rows to load
        profile: Databricks CLI profile
        where_clause: Optional WHERE clause conditions
        
    Returns:
        DataFrame with ISSUEKEY, DEVELOPMENT_DATA, source_table, and empty pr_url column
    """
    sql = """
    SELECT DISTINCT ISSUEKEY, DEVELOPMENT_DATA, source_table
    FROM silo_dev_rs.task.jira_pr_union
    WHERE DEVELOPMENT_DATA LIKE '%pullrequest%'
    """
    if where_clause:
        sql += f" AND ({where_clause})"
    sql += f" LIMIT {limit}"
    
    results = execute_sql(sql, profile)
    
    if results:
        df = pd.DataFrame(results)
        df["pr_url"] = None  # Add empty pr_url column
        return df
    return pd.DataFrame()


def get_jira_auth_header(user_name: str, api_key: str) -> dict[str, str]:
    """Create Basic Auth header for JIRA API.
    
    JIRA Cloud uses Basic Auth with email:api_token format.
    """
    credentials = f"{user_name}:{api_key}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_jira_issue_id(
    issue_key: str,
    headers: dict[str, str],
) -> str | None:
    """Get the internal JIRA issue ID from the issue key.
    
    The dev-status API requires the numeric issue ID, not the key.
    """
    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}"
    params = {"fields": "id"}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("id")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting issue ID for {issue_key}: {e}")
        return None


def get_pr_urls_from_jira(
    issue_id: str,
    headers: dict[str, str],
) -> list[dict[str, Any]]:
    """Get pull request URLs from JIRA's dev-status API.
    
    Args:
        issue_id: The numeric JIRA issue ID
        headers: Auth headers for the API
        
    Returns:
        List of PR info dicts with url, name, status, etc.
    """
    url = f"{JIRA_BASE_URL}/rest/dev-status/1.0/issue/detail"
    params = {
        "issueId": issue_id,
        "applicationType": "GitHub",
        "dataType": "pullrequest",
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        pr_list = []
        # Navigate the response structure
        detail = data.get("detail", [])
        for provider in detail:
            pullRequests = provider.get("pullRequests", [])
            for pr in pullRequests:
                pr_list.append({
                    "url": pr.get("url"),
                    "name": pr.get("name"),
                    "status": pr.get("status"),
                    "source_branch": pr.get("source", {}).get("branch"),
                    "destination_branch": pr.get("destination", {}).get("branch"),
                    "author": pr.get("author", {}).get("name"),
                    "last_update": pr.get("lastUpdate"),
                })
        
        return pr_list
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting PR URLs for issue ID {issue_id}: {e}")
        return []


def process_batch_with_jira_api(
    df: pd.DataFrame,
    user_name: str,
    api_key: str,
    delay_seconds: float = 0.5,
) -> pd.DataFrame:
    """Process a batch of rows, fetching PR URLs from JIRA API.
    
    Args:
        df: DataFrame with ISSUEKEY column
        user_name: JIRA username (email)
        api_key: JIRA API token
        delay_seconds: Delay between API calls to avoid rate limiting
        
    Returns:
        DataFrame with pr_url column populated
    """
    headers = get_jira_auth_header(user_name, api_key)
    
    for idx, row in df.iterrows():
        issue_key = row["ISSUEKEY"]
        logger.info(f"[{idx + 1}/{len(df)}] Processing {issue_key}...")
        
        # Get the numeric issue ID first
        issue_id = get_jira_issue_id(issue_key, headers)
        if not issue_id:
            logger.warning(f"  Could not get issue ID for {issue_key}")
            continue
        
        # Rate limit delay
        if idx > 0:
            time.sleep(delay_seconds)
        
        # Get PR URLs
        pr_list = get_pr_urls_from_jira(issue_id, headers)
        
        if pr_list:
            # Take the first PR URL (or could concatenate multiple)
            pr_url = pr_list[0].get("url")
            df.at[idx, "pr_url"] = pr_url
            logger.info(f"  Found PR: {pr_url}")
            
            # Log all PRs if multiple
            if len(pr_list) > 1:
                logger.info(f"  (Total {len(pr_list)} PRs found)")
                for pr in pr_list[1:]:
                    logger.debug(f"    Additional PR: {pr.get('url')}")
        else:
            logger.info(f"  No PRs found for {issue_key}")
    
    return df


def test_small_batch(
    limit: int = 10,
    profile: str = DEFAULT_PROFILE,
) -> pd.DataFrame:
    """Test PR URL retrieval with a small batch.
    
    Args:
        limit: Number of rows to test
        profile: Databricks CLI profile
        
    Returns:
        DataFrame with pr_url column populated
    """
    # Load credentials from .env
    user_name = os.environ.get("USER_NAME")
    api_key = os.environ.get("JIRA_API_KEY")
    
    if not user_name or not api_key:
        logger.error("USER_NAME and JIRA_API_KEY must be set in .env")
        return pd.DataFrame()
    
    logger.info("=" * 60)
    logger.info(f"Testing PR URL retrieval with {limit} rows")
    logger.info("=" * 60)
    logger.info(f"JIRA User: {user_name}")
    logger.info(f"JIRA Base URL: {JIRA_BASE_URL}")
    
    # Load batch
    logger.info(f"\n[Step 1] Loading {limit} rows from jira_pr_union...")
    df = load_batch_to_dataframe(limit=limit, profile=profile)
    
    if df.empty:
        logger.error("No rows loaded from database")
        return df
    
    logger.info(f"Loaded {len(df)} rows")
    print("\n--- Initial DataFrame ---")
    print(df[["ISSUEKEY", "source_table"]].to_string())
    print("---")
    
    # Process with JIRA API
    logger.info(f"\n[Step 2] Fetching PR URLs from JIRA API...")
    df = process_batch_with_jira_api(df, user_name, api_key)
    
    # Display results
    logger.info("\n[Step 3] Results:")
    print("\n--- DataFrame with PR URLs ---")
    print(df[["ISSUEKEY", "source_table", "pr_url"]].to_string())
    print("---")
    
    # Summary
    found_count = df["pr_url"].notna().sum()
    logger.info(f"\nSummary: Found {found_count}/{len(df)} PR URLs")
    
    return df


def upsert_pr_url(
    issue_key: str,
    pr_url: str,
    pr_number: int | None,
    github_repo: str | None,
    pr_state: str | None,
    profile: str = DEFAULT_PROFILE,
) -> bool:
    """Insert or update a PR URL for an ISSUEKEY in jira_pr_numbers.
    
    Uses MERGE to handle both insert and update cases.
    """
    updated_at = datetime.now(timezone.utc).isoformat()
    
    # Build the MERGE statement with proper escaping
    pr_url_escaped = pr_url.replace("'", "''") if pr_url else ""
    github_repo_escaped = github_repo.replace("'", "''") if github_repo else ""
    pr_state_escaped = pr_state.replace("'", "''") if pr_state else ""
    
    # Extract pr_number from URL if not provided
    if pr_number is None and pr_url:
        import re
        match = re.search(r'/pull/(\d+)', pr_url)
        if match:
            pr_number = int(match.group(1))
    
    pr_number_str = str(pr_number) if pr_number else "NULL"
    
    sql = f"""
    MERGE INTO silo_dev_rs.task.jira_pr_numbers AS target
    USING (SELECT '{issue_key}' AS ISSUEKEY) AS source
    ON target.ISSUEKEY = source.ISSUEKEY
    WHEN MATCHED THEN
        UPDATE SET 
            pr_url = '{pr_url_escaped}',
            pr_number = {pr_number_str},
            github_repo = '{github_repo_escaped}',
            pr_state = '{pr_state_escaped}',
            updated_at = '{updated_at}'
    WHEN NOT MATCHED THEN
        INSERT (ISSUEKEY, pr_url, pr_number, github_repo, pr_state, updated_at)
        VALUES ('{issue_key}', '{pr_url_escaped}', {pr_number_str}, '{github_repo_escaped}', '{pr_state_escaped}', '{updated_at}')
    """
    
    result = execute_sql(sql, profile)
    return result is not None


def call_jira_api_with_backoff(
    func,
    *args,
    max_retries: int = 5,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    **kwargs,
) -> tuple[Any, bool]:
    """Call a JIRA API function with exponential backoff retry.
    
    Args:
        func: Function to call
        *args: Positional arguments for func
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        backoff_factor: Multiplier for delay after each retry
        **kwargs: Keyword arguments for func
        
    Returns:
        Tuple of (result, success). Result is None if all retries failed.
    """
    delay = initial_delay
    
    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            return result, True
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            
            # 429 = Rate limited, retry with backoff
            # 403 = Could be daily limit
            # 401 = Auth error, don't retry
            if status_code == 401:
                logger.error(f"Authentication error (401) - check JIRA_API_KEY")
                return None, False
            elif status_code == 403:
                logger.warning(f"Forbidden (403) - may have hit daily API limit")
                return None, False
            elif status_code == 429:
                if attempt < max_retries:
                    logger.warning(f"Rate limited (429), retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                else:
                    logger.error(f"Rate limited after {max_retries} retries")
                    return None, False
            else:
                logger.error(f"HTTP error {status_code}: {e}")
                return None, False
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                logger.warning(f"Request error, retrying in {delay:.1f}s: {e}")
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
            else:
                logger.error(f"Request failed after {max_retries} retries: {e}")
                return None, False
    
    return None, False


def get_pr_urls_from_jira_with_backoff(
    issue_id: str,
    headers: dict[str, str],
    max_retries: int = 5,
) -> tuple[list[dict[str, Any]], bool]:
    """Get PR URLs with exponential backoff retry.
    
    Returns:
        Tuple of (pr_list, success). Empty list with success=True means no PRs found.
        Empty list with success=False means API call failed.
    """
    url = f"{JIRA_BASE_URL}/rest/dev-status/1.0/issue/detail"
    params = {
        "issueId": issue_id,
        "applicationType": "GitHub",
        "dataType": "pullrequest",
    }
    
    delay = 1.0
    max_delay = 60.0
    backoff_factor = 2.0
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            pr_list = []
            detail = data.get("detail", [])
            for provider in detail:
                pullRequests = provider.get("pullRequests", [])
                for pr in pullRequests:
                    pr_list.append({
                        "url": pr.get("url"),
                        "name": pr.get("name"),
                        "status": pr.get("status"),
                        "source_branch": pr.get("source", {}).get("branch"),
                        "destination_branch": pr.get("destination", {}).get("branch"),
                        "author": pr.get("author", {}).get("name"),
                        "last_update": pr.get("lastUpdate"),
                    })
            
            return pr_list, True
            
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            
            if status_code == 401:
                logger.error(f"Authentication error (401)")
                return [], False
            elif status_code == 403:
                logger.warning(f"Forbidden (403) - potential daily limit")
                return [], False
            elif status_code == 429:
                if attempt < max_retries:
                    logger.warning(f"Rate limited (429), retrying in {delay:.1f}s")
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                    continue
                else:
                    logger.error(f"Rate limited after {max_retries} retries")
                    return [], False
            else:
                logger.error(f"HTTP error {status_code}: {e}")
                return [], False
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                logger.warning(f"Request error, retrying in {delay:.1f}s: {e}")
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
            else:
                logger.error(f"Request failed after {max_retries} retries: {e}")
                return [], False
    
    return [], False


def populate_all_pr_urls(
    batch_size: int = 100,
    max_consecutive_failures: int = 10,
    profile: str = DEFAULT_PROFILE,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Populate pr_url for all JIRA issues without one.
    
    Features:
    - Exponential backoff for rate limiting
    - Stops after max_consecutive_failures (indicates daily API limit)
    - Updates jira_pr_numbers table with pr_url
    
    Args:
        batch_size: Number of rows to process per batch
        max_consecutive_failures: Exit after this many consecutive API failures
        profile: Databricks CLI profile
        dry_run: If True, don't write to database
        
    Returns:
        Summary dict with processing statistics
    """
    # Load credentials
    user_name = os.environ.get("USER_NAME")
    api_key = os.environ.get("JIRA_API_KEY")
    
    if not user_name or not api_key:
        logger.error("USER_NAME and JIRA_API_KEY must be set")
        return {"error": "Missing credentials", "exit_code": 1}
    
    headers = get_jira_auth_header(user_name, api_key)
    
    logger.info("=" * 70)
    logger.info("JIRA PR URL Population Job")
    logger.info("=" * 70)
    logger.info(f"JIRA User: {user_name}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Max consecutive failures: {max_consecutive_failures}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("=" * 70)
    
    summary = {
        "processed": 0,
        "found": 0,
        "updated": 0,
        "not_found": 0,
        "api_failures": 0,
        "consecutive_failures": 0,
        "exit_reason": None,
        "exit_code": 0,
    }
    
    total_processed = 0
    consecutive_failures = 0
    
    while True:
        # Query next batch of issues without pr_url
        sql = f"""
        SELECT DISTINCT j.ISSUEKEY, j.DEVELOPMENT_DATA, j.source_table
        FROM silo_dev_rs.task.jira_pr_union j
        LEFT JOIN silo_dev_rs.task.jira_pr_numbers p ON j.ISSUEKEY = p.ISSUEKEY
        WHERE (p.pr_url IS NULL OR p.pr_url = '')
          AND j.DEVELOPMENT_DATA LIKE '%pullrequest%'
        LIMIT {batch_size}
        """
        
        results = execute_sql(sql, profile)
        
        if not results:
            logger.info("No more issues to process")
            summary["exit_reason"] = "completed"
            break
        
        logger.info(f"\n--- Batch: {len(results)} issues ---")
        
        for i, row in enumerate(results):
            issue_key = row["ISSUEKEY"]
            total_processed += 1
            
            logger.info(f"[{total_processed}] Processing {issue_key}...")
            
            # Get issue ID
            try:
                response = requests.get(
                    f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}",
                    headers=headers,
                    params={"fields": "id"},
                    timeout=30,
                )
                response.raise_for_status()
                issue_id = response.json().get("id")
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response else None
                logger.error(f"  Failed to get issue ID (HTTP {status_code})")
                consecutive_failures += 1
                summary["api_failures"] += 1
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"\n{'='*70}")
                    logger.error(f"STOPPING: {consecutive_failures} consecutive API failures")
                    logger.error("This likely indicates a daily API rate limit has been reached.")
                    logger.error(f"{'='*70}")
                    summary["exit_reason"] = "daily_limit_reached"
                    summary["exit_code"] = 2
                    summary["consecutive_failures"] = consecutive_failures
                    summary["processed"] = total_processed
                    return summary
                continue
            except requests.exceptions.RequestException as e:
                logger.error(f"  Request error getting issue ID: {e}")
                consecutive_failures += 1
                summary["api_failures"] += 1
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"\n{'='*70}")
                    logger.error(f"STOPPING: {consecutive_failures} consecutive failures")
                    logger.error(f"{'='*70}")
                    summary["exit_reason"] = "consecutive_failures"
                    summary["exit_code"] = 2
                    summary["consecutive_failures"] = consecutive_failures
                    summary["processed"] = total_processed
                    return summary
                continue
            
            if not issue_id:
                logger.warning(f"  No issue ID found")
                consecutive_failures += 1
                continue
            
            # Small delay between API calls
            time.sleep(0.3)
            
            # Get PR URLs with backoff
            pr_list, success = get_pr_urls_from_jira_with_backoff(issue_id, headers)
            
            if not success:
                consecutive_failures += 1
                summary["api_failures"] += 1
                
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"\n{'='*70}")
                    logger.error(f"STOPPING: {consecutive_failures} consecutive API failures")
                    logger.error("This likely indicates a daily API rate limit has been reached.")
                    logger.error(f"{'='*70}")
                    summary["exit_reason"] = "daily_limit_reached"
                    summary["exit_code"] = 2
                    summary["consecutive_failures"] = consecutive_failures
                    summary["processed"] = total_processed
                    return summary
                continue
            
            # Reset consecutive failures on success
            consecutive_failures = 0
            
            if pr_list:
                pr = pr_list[0]
                pr_url = pr.get("url", "")
                pr_status = pr.get("status", "")
                
                # Extract repo from URL
                import re
                repo_match = re.search(r"github\.com/([^/]+/[^/]+)/pull", pr_url)
                github_repo = repo_match.group(1) if repo_match else ""
                
                # Extract PR number
                pr_num_match = re.search(r"/pull/(\d+)", pr_url)
                pr_number = int(pr_num_match.group(1)) if pr_num_match else None
                
                summary["found"] += 1
                logger.info(f"  Found: {pr_url}")
                
                if not dry_run:
                    if upsert_pr_url(
                        issue_key=issue_key,
                        pr_url=pr_url,
                        pr_number=pr_number,
                        github_repo=github_repo,
                        pr_state=pr_status,
                        profile=profile,
                    ):
                        summary["updated"] += 1
                    else:
                        logger.error(f"  Failed to update database")
                else:
                    logger.info(f"  [DRY RUN] Would update: {pr_url}")
            else:
                summary["not_found"] += 1
                logger.info(f"  No PRs found")
        
        # Log progress
        summary["processed"] = total_processed
        logger.info(f"\n--- Progress: {total_processed} processed, {summary['found']} found, {summary['updated']} updated ---")
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"  Total processed: {summary['processed']}")
    logger.info(f"  PRs found: {summary['found']}")
    logger.info(f"  PRs updated: {summary['updated']}")
    logger.info(f"  Not found: {summary['not_found']}")
    logger.info(f"  API failures: {summary['api_failures']}")
    logger.info(f"  Exit reason: {summary['exit_reason']}")
    logger.info("=" * 70)
    
    return summary


# ============================================================================
# CLI Interface
# ============================================================================

@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context):
    """Populate pr_url for JIRA issues using JIRA's Development API.
    
    \b
    Commands:
      test       Test PR URL retrieval with a small batch
      populate   Populate pr_url for all issues (used by Databricks job)
    
    \b
    Examples:
      uv run scripts/populate_jira_pr_urls.py test
      uv run scripts/populate_jira_pr_urls.py test --limit 5
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.option(
    "--limit", "-l",
    default=10,
    show_default=True,
    help="Number of rows to test.",
)
@click.option(
    "--profile", "-p",
    default=DEFAULT_PROFILE,
    show_default=True,
    help="Databricks CLI profile.",
)
def test(limit: int, profile: str):
    """Test PR URL retrieval with a small batch.
    
    \b
    Examples:
      uv run scripts/populate_jira_pr_urls.py test
      uv run scripts/populate_jira_pr_urls.py test --limit 5
    """
    df = test_small_batch(limit=limit, profile=profile)
    
    if not df.empty:
        # Save results to JSON for inspection
        output_dir = Path(__file__).parent
        output_file = output_dir / "test_pr_urls_results.json"
        records = df[["ISSUEKEY", "source_table", "pr_url"]].to_dict(orient="records")
        with open(output_file, "w") as f:
            json.dump(records, f, indent=2)
        logger.info(f"\nSaved results to {output_file}")


@cli.command()
@click.option(
    "--batch-size", "-b",
    default=100,
    show_default=True,
    help="Number of rows to process per batch.",
)
@click.option(
    "--max-failures", "-m",
    default=10,
    show_default=True,
    help="Stop after this many consecutive API failures (indicates daily limit).",
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
    help="Fetch PR URLs but don't write to database.",
)
def populate(batch_size: int, max_failures: int, profile: str, dry_run: bool):
    """Populate pr_url for all JIRA issues.
    
    Features exponential backoff for rate limiting and stops after
    consecutive failures indicating a daily API limit has been reached.
    
    \b
    Examples:
      uv run scripts/populate_jira_pr_urls.py populate
      uv run scripts/populate_jira_pr_urls.py populate --batch-size 50
      uv run scripts/populate_jira_pr_urls.py populate --dry-run
    """
    summary = populate_all_pr_urls(
        batch_size=batch_size,
        max_consecutive_failures=max_failures,
        profile=profile,
        dry_run=dry_run,
    )
    
    # Exit with appropriate code
    exit_code = summary.get("exit_code", 0)
    if exit_code != 0:
        logger.warning(f"Exiting with code {exit_code}: {summary.get('exit_reason')}")
    sys.exit(exit_code)


# ============================================================================
# Databricks Job Entry Point
# ============================================================================

def run_databricks_job():
    """Entry point for running as a Databricks Python task.
    
    Reads configuration from environment variables:
    - USER_NAME: JIRA username (email)
    - JIRA_API_KEY: JIRA API token
    - BATCH_SIZE: Number of rows per batch (default: 100)
    - MAX_CONSECUTIVE_FAILURES: Stop threshold (default: 10)
    
    This function is called when the script runs in Databricks.
    """
    # In Databricks, secrets are loaded via dbutils or environment
    user_name = os.environ.get("USER_NAME")
    api_key = os.environ.get("JIRA_API_KEY")
    
    if not user_name or not api_key:
        logger.error("USER_NAME and JIRA_API_KEY must be set")
        logger.error("In Databricks, use secret scope: dbutils.secrets.get('adk-secrets', 'JIRA_API_KEY')")
        sys.exit(1)
    
    batch_size = int(os.environ.get("BATCH_SIZE", "100"))
    max_failures = int(os.environ.get("MAX_CONSECUTIVE_FAILURES", "10"))
    
    summary = populate_all_pr_urls(
        batch_size=batch_size,
        max_consecutive_failures=max_failures,
        profile=DEFAULT_PROFILE,
        dry_run=False,
    )
    
    # Write summary for job tracking
    logger.info(f"Job summary: {json.dumps(summary, indent=2)}")
    
    exit_code = summary.get("exit_code", 0)
    if exit_code == 2:
        # Daily limit reached - this is expected, not a failure
        logger.warning("Daily API limit reached - job will resume on next run")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    # Check if running in Databricks (no CLI args typically)
    import sys
    if len(sys.argv) == 1 and os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        run_databricks_job()
    else:
        cli()

# Databricks notebook source
# MAGIC %md
# MAGIC # Populate JIRA PR URLs
# MAGIC 
# MAGIC This notebook populates the `pr_url` column in `silo_dev_rs.task.jira_pr_numbers`
# MAGIC by calling the JIRA Development Information API for each issue.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Exponential backoff for rate limiting
# MAGIC - Stops after 10 consecutive failures (indicates daily API limit)
# MAGIC - Resumes from where it left off on subsequent runs

# COMMAND ----------

import base64
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load credentials from Databricks secrets
JIRA_USER = dbutils.secrets.get(scope="adk-secrets", key="USER_NAME")
JIRA_API_KEY = dbutils.secrets.get(scope="adk-secrets", key="JIRA_API_KEY")

# Configuration
JIRA_BASE_URL = "https://spendmend.atlassian.net"
BATCH_SIZE = 100
MAX_CONSECUTIVE_FAILURES = 10  # Stop after this many consecutive failures (daily limit)
API_DELAY_SECONDS = 0.3  # Delay between API calls

print(f"JIRA User: {JIRA_USER}")
print(f"JIRA Base URL: {JIRA_BASE_URL}")
print(f"Batch size: {BATCH_SIZE}")
print(f"Max consecutive failures: {MAX_CONSECUTIVE_FAILURES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_jira_auth_header():
    """Create Basic Auth header for JIRA API."""
    credentials = f"{JIRA_USER}:{JIRA_API_KEY}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def get_jira_issue_id(issue_key, headers):
    """Get the internal JIRA issue ID from the issue key."""
    url = f"{JIRA_BASE_URL}/rest/api/2/issue/{issue_key}"
    params = {"fields": "id"}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get("id")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting issue ID for {issue_key}: {e}")
        return None


def get_pr_urls_from_jira(issue_id, headers, max_retries=5):
    """Get PR URLs with exponential backoff retry.
    
    Returns:
        Tuple of (pr_list, success).
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
                    })
            
            return pr_list, True
            
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            
            if status_code in (401, 403):
                logger.error(f"Auth/Forbidden error ({status_code})")
                return [], False
            elif status_code == 429:
                if attempt < max_retries:
                    logger.warning(f"Rate limited (429), retrying in {delay:.1f}s")
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                    continue
                return [], False
            else:
                logger.error(f"HTTP error {status_code}: {e}")
                return [], False
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                logger.warning(f"Request error, retrying: {e}")
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
            else:
                return [], False
    
    return [], False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Processing Loop

# COMMAND ----------

def populate_pr_urls():
    """Main function to populate PR URLs."""
    headers = get_jira_auth_header()
    
    # Statistics
    stats = {
        "processed": 0,
        "found": 0,
        "updated": 0,
        "not_found": 0,
        "api_failures": 0,
    }
    consecutive_failures = 0
    
    print("=" * 70)
    print("Starting JIRA PR URL Population")
    print("=" * 70)
    
    while True:
        # Query next batch of issues without pr_url
        query = f"""
        SELECT DISTINCT j.ISSUEKEY
        FROM silo_dev_rs.task.jira_pr_union j
        LEFT JOIN silo_dev_rs.task.jira_pr_numbers p ON j.ISSUEKEY = p.ISSUEKEY
        WHERE (p.pr_url IS NULL OR p.pr_url = '')
          AND j.DEVELOPMENT_DATA LIKE '%pullrequest%'
        LIMIT {BATCH_SIZE}
        """
        
        df = spark.sql(query)
        rows = df.collect()
        
        if not rows:
            print("\n✓ All issues processed!")
            break
        
        print(f"\n--- Processing batch of {len(rows)} issues ---")
        
        for row in rows:
            issue_key = row["ISSUEKEY"]
            stats["processed"] += 1
            
            print(f"[{stats['processed']}] {issue_key}...", end=" ")
            
            # Get issue ID
            issue_id = get_jira_issue_id(issue_key, headers)
            if not issue_id:
                consecutive_failures += 1
                stats["api_failures"] += 1
                print("✗ (no issue ID)")
                
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"\n{'='*70}")
                    print(f"STOPPING: {consecutive_failures} consecutive API failures")
                    print("This likely indicates a daily API rate limit.")
                    print(f"{'='*70}")
                    stats["exit_reason"] = "daily_limit_reached"
                    return stats
                continue
            
            time.sleep(API_DELAY_SECONDS)
            
            # Get PR URLs
            pr_list, success = get_pr_urls_from_jira(issue_id, headers)
            
            if not success:
                consecutive_failures += 1
                stats["api_failures"] += 1
                print("✗ (API error)")
                
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"\n{'='*70}")
                    print(f"STOPPING: {consecutive_failures} consecutive API failures")
                    print(f"{'='*70}")
                    stats["exit_reason"] = "daily_limit_reached"
                    return stats
                continue
            
            # Reset consecutive failures on success
            consecutive_failures = 0
            
            if pr_list:
                pr = pr_list[0]
                pr_url = pr.get("url", "")
                pr_status = pr.get("status", "")
                
                # Extract repo and PR number from URL
                repo_match = re.search(r"github\.com/([^/]+/[^/]+)/pull", pr_url)
                github_repo = repo_match.group(1) if repo_match else ""
                
                pr_num_match = re.search(r"/pull/(\d+)", pr_url)
                pr_number = int(pr_num_match.group(1)) if pr_num_match else None
                
                # Upsert to database
                updated_at = datetime.now(timezone.utc).isoformat()
                
                merge_sql = f"""
                MERGE INTO silo_dev_rs.task.jira_pr_numbers AS target
                USING (SELECT '{issue_key}' AS ISSUEKEY) AS source
                ON target.ISSUEKEY = source.ISSUEKEY
                WHEN MATCHED THEN
                    UPDATE SET 
                        pr_url = '{pr_url}',
                        pr_number = {pr_number if pr_number else 'NULL'},
                        github_repo = '{github_repo}',
                        pr_state = '{pr_status}',
                        updated_at = '{updated_at}'
                WHEN NOT MATCHED THEN
                    INSERT (ISSUEKEY, pr_url, pr_number, github_repo, pr_state, updated_at)
                    VALUES ('{issue_key}', '{pr_url}', {pr_number if pr_number else 'NULL'}, '{github_repo}', '{pr_status}', '{updated_at}')
                """
                
                spark.sql(merge_sql)
                stats["found"] += 1
                stats["updated"] += 1
                print(f"✓ {pr_url}")
            else:
                stats["not_found"] += 1
                print("- (no PRs)")
    
    stats["exit_reason"] = "completed"
    return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Job

# COMMAND ----------

# Run the population job
result = populate_pr_urls()

# Print summary
print("\n" + "=" * 70)
print("FINAL SUMMARY")
print("=" * 70)
print(f"  Total processed: {result['processed']}")
print(f"  PRs found: {result['found']}")
print(f"  PRs updated: {result['updated']}")
print(f"  Not found: {result['not_found']}")
print(f"  API failures: {result['api_failures']}")
print(f"  Exit reason: {result.get('exit_reason', 'unknown')}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

# Check current state of the table
display(spark.sql("""
SELECT 
    COUNT(*) as total_issues,
    COUNT(pr_url) as with_pr_url,
    COUNT(*) - COUNT(pr_url) as missing_pr_url
FROM silo_dev_rs.task.jira_pr_union j
LEFT JOIN silo_dev_rs.task.jira_pr_numbers p ON j.ISSUEKEY = p.ISSUEKEY
"""))

# COMMAND ----------

# Sample of recently updated rows
display(spark.sql("""
SELECT ISSUEKEY, pr_url, pr_number, github_repo, pr_state, updated_at
FROM silo_dev_rs.task.jira_pr_numbers
WHERE pr_url IS NOT NULL
ORDER BY updated_at DESC
LIMIT 10
"""))

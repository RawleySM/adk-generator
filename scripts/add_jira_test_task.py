#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "databricks-sql-connector",
#     "click",
# ]
# ///
"""Add Jira-style tasks to the Databricks table for agent evaluation.

Usage:
    # Add a custom task
    uv run scripts/add_jira_test_task.py --issue-key AGENT-101 --summary "..." --description "..."

    # Add a pre-built test task by difficulty (1-10)
    uv run scripts/add_jira_test_task.py --testtask 5

    # List available test tasks
    uv run scripts/add_jira_test_task.py --list-tasks
"""
import subprocess
import json
import sys
from pathlib import Path

import click
from databricks import sql

# Import test tasks module
sys.path.insert(0, str(Path(__file__).parent))
from test_tasks import get_task, list_tasks as list_tasks_fn, TASKS

def get_access_token():
    """Retrieves a short-lived access token using the Databricks CLI."""
    try:
        # We don't print here to keep CLI output clean for the main command
        result = subprocess.run(
            ["databricks", "auth", "token", "--profile", "rstanhope"],
            capture_output=True,
            text=True,
            check=True
        )
        return json.loads(result.stdout)["access_token"]
    except subprocess.CalledProcessError as e:
        raise click.ClickException(f"Failed to get Databricks token: {e.stderr}")

HOST = "adb-6983841757863745.5.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/57f6389fdcdefbc0"

@click.command()
@click.option('--issue-key', help='The unique identifier for the Jira task (e.g., AGENT-101).')
@click.option('--summary', help='A concise summary or title of the task.')
@click.option('--description', help='Detailed instructions and context for the agent.')
@click.option('--priority', default='Medium', show_default=True, help='The priority level (e.g., High, Medium, Low).')
@click.option('--story-points', type=float, default=None, help='Complexity estimate (e.g., 1, 3, 5, 8).')
@click.option('--due-date', default=None, help='Due date in YYYY-MM-DD format.')
@click.option('--assignee', default='databricks-rlm-agent', show_default=True, help='The assignee for the task.')
@click.option('--testtask', '-t', type=int, help='Use a pre-built test task by difficulty level (1-10).')
@click.option('--list-tasks', '-l', is_flag=True, help='List all available test tasks and exit.')
def add_task(issue_key, summary, description, priority, story_points, due_date, assignee, testtask, list_tasks):
    """
    Adds a new Jira-style task to the Databricks table 'silo_dev_rs.task.jira_raw_data'.

    This tool is useful for creating test cases or assigning work to agents via the database.

    \b
    Modes:
      1. Custom task: Provide --issue-key, --summary, and --description
      2. Pre-built test task: Use --testtask N (where N is 1-10 difficulty)
      3. List tasks: Use --list-tasks to see available pre-built tasks

    \b
    Examples:
      uv run scripts/add_jira_test_task.py --testtask 3
      uv run scripts/add_jira_test_task.py --list-tasks
      uv run scripts/add_jira_test_task.py --issue-key AGENT-101 --summary "Test" --description "Details"
    """
    # Handle --list-tasks
    if list_tasks:
        click.echo(click.style("=" * 70, fg='cyan'))
        click.echo(click.style("AVAILABLE TEST TASKS FOR DATABRICKS ANALYST EVALUATION", fg='cyan', bold=True))
        click.echo(click.style("=" * 70, fg='cyan'))
        for difficulty, task_key, task_summary in list_tasks_fn():
            task = TASKS[difficulty]
            click.echo(f"\n{click.style(f'Level {difficulty}:', fg='green', bold=True)} {task_key}")
            click.echo(f"  {click.style('Summary:', fg='blue')} {task_summary}")
            click.echo(f"  {click.style('Priority:', fg='blue')} {task.priority} | "
                      f"{click.style('Story Points:', fg='blue')} {task.story_points}")
            # Truncate llm_query_benefit for display
            benefit = task.llm_query_benefit[:70] + "..." if len(task.llm_query_benefit) > 70 else task.llm_query_benefit
            click.echo(f"  {click.style('LLM Query Benefit:', fg='yellow')} {benefit}")
        click.echo("\n" + click.style("=" * 70, fg='cyan'))
        click.echo(f"\nUsage: {click.style('uv run scripts/add_jira_test_task.py --testtask <level>', fg='green')}")
        return

    # Handle --testtask
    if testtask is not None:
        if testtask < 1 or testtask > 10:
            raise click.ClickException("Test task difficulty must be between 1 and 10")

        task = get_task(testtask)
        if task is None:
            raise click.ClickException(f"No test task defined for difficulty level {testtask}")

        # Use values from the test task
        issue_key = task.issue_key
        summary = task.summary
        description = task.description.strip()
        priority = task.priority
        story_points = task.story_points

        click.echo(click.style(f"\nUsing pre-built test task (Level {testtask}):", fg='cyan', bold=True))
        click.echo(f"  Issue Key: {issue_key}")
        click.echo(f"  Summary: {summary}")
        click.echo(f"  Priority: {priority} | Story Points: {story_points}")
        click.echo("")

    # Validate required fields for custom tasks
    elif not all([issue_key, summary, description]):
        raise click.ClickException(
            "Must provide either --testtask OR all of: --issue-key, --summary, --description"
        )

    # Default story_points if not set
    if story_points is None:
        story_points = 3.0

    try:
        token = get_access_token()
        click.echo(f"Connecting to Databricks Warehouse at {HOST}...")
        
        with sql.connect(
            server_hostname=HOST,
            http_path=HTTP_PATH,
            access_token=token
        ) as connection:
            
            with connection.cursor() as cursor:
                # Query mapping arguments to their respective columns
                # Columns selected based on importance for agent context and planning
                insert_query = """
                INSERT INTO silo_dev_rs.task.jira_raw_data (
                    ISSUEKEY,
                    SUMMARY__summary,
                    DESCRIPTION__description,
                    PRIORITY__priority,
                    STORY_POINTS__customfield_10028,
                    DUE_DATE__duedate,
                    ASSIGNEE__assignee,
                    STATUS__status,
                    PROJECT__project
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Default values for fields not exposed as main options
                status = "To Do"
                project = "AGENT-TEST"

                # Execute with parameter binding
                cursor.execute(insert_query, (
                    issue_key,
                    summary,
                    description,
                    priority,
                    story_points,
                    due_date,
                    assignee,
                    status,
                    project
                ))
                
                click.echo(click.style(f"Success! Task {issue_key} added to silo_dev_rs.task.jira_raw_data.", fg='green'))

    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg='red'), err=True)
        raise click.Abort()

if __name__ == "__main__":
    add_task()
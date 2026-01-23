#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "databricks-sql-connector",
#     "click",
# ]
# ///
import subprocess
import json
import click
from databricks import sql

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
@click.option('--issue-key', required=True, help='The unique identifier for the Jira task (e.g., AGENT-101).')
@click.option('--summary', required=True, help='A concise summary or title of the task.')
@click.option('--description', required=True, help='Detailed instructions and context for the agent.')
@click.option('--priority', default='Medium', show_default=True, help='The priority level (e.g., High, Medium, Low).')
@click.option('--story-points', type=float, default=3.0, show_default=True, help='Complexity estimate (e.g., 1, 3, 5, 8).')
@click.option('--due-date', default=None, help='Due date in YYYY-MM-DD format.')
@click.option('--assignee', default='databricks-rlm-agent', show_default=True, help='The assignee for the task.')
def add_task(issue_key, summary, description, priority, story_points, due_date, assignee):
    """
    Adds a new Jira-style task to the Databricks table 'silo_dev_rs.task.jira_raw_data'.
    
    This tool is useful for creating test cases or assigning work to agents via the database.
    """
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
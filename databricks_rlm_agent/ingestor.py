"""CDF-based ingestor for the RLM Agent.

This module polls a trigger table (e.g., jira_raw_data) using Databricks
Change Data Feed (CDF) to detect new tasks assigned to the RLM agent.
When new tasks are found, it triggers the orchestrator job via the Jobs API.

Key Components:
    - State Table: Tracks the last processed commit version (watermark)
    - CDF Polling: Uses table_changes() to read only new inserts
    - Trigger Logic: Calls Jobs API to start orchestrator with task context

Tables:
    - Trigger Table: silo_dev_rs.task.jira_raw_data (source of new tasks)
    - State Table: ${catalog}.${schema}.ingestor_state (watermark tracking)
    - Telemetry: ${catalog}.${schema}.telemetry (event logging)

Usage:
    from databricks_rlm_agent.ingestor import IngestorService

    service = IngestorService(
        spark=spark,
        catalog="silo_dev_rs",
        schema="adk",
        trigger_table="silo_dev_rs.task.jira_raw_data",
        assignee_filter="databricks-rlm-agent",
    )

    new_tasks = service.poll_for_new_tasks()
    for task in new_tasks:
        service.trigger_orchestrator(task)
"""

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from .utils.jira_attachments import download_jira_attachments, DEFAULT_TARGET_VOLUME

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Default table name for state tracking
STATE_TABLE_NAME = "ingestor_state"


@dataclass
class JiraTask:
    """Represents a JIRA task to be processed by the RLM agent."""

    issue_key: str
    summary: str
    description: str
    assignee: str
    status: str
    commit_version: int
    raw_data: dict[str, Any]
    has_attachments: bool = False
    attachment_count: int = 0
    downloaded_attachments: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "issue_key": self.issue_key,
            "summary": self.summary,
            "description": self.description,
            "assignee": self.assignee,
            "status": self.status,
            "commit_version": self.commit_version,
            "has_attachments": self.has_attachments,
            "attachment_count": self.attachment_count,
            "downloaded_attachments": self.downloaded_attachments,
        }


class IngestorService:
    """Service for polling CDF and triggering orchestrator jobs."""

    def __init__(
        self,
        spark: "SparkSession",
        catalog: str,
        schema: str,
        trigger_table: str,
        assignee_filter: str = "databricks-rlm-agent",
        orchestrator_job_id: Optional[int] = None,
    ):
        """Initialize the ingestor service.

        Args:
            spark: Active SparkSession.
            catalog: Unity Catalog name for state/telemetry tables.
            schema: Schema name within the catalog.
            trigger_table: Fully qualified name of the trigger table to poll.
            assignee_filter: Assignee value to filter for (default: databricks-rlm-agent).
            orchestrator_job_id: Job ID of the orchestrator to trigger.
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.trigger_table = trigger_table
        self.assignee_filter = assignee_filter
        self.orchestrator_job_id = orchestrator_job_id

        self._state_table = f"{catalog}.{schema}.{STATE_TABLE_NAME}"

    def ensure_state_table(self) -> None:
        """Create the state table if it doesn't exist.

        The state table tracks the last processed commit version (watermark)
        for the CDF polling.
        """
        logger.info(f"Ensuring state table exists: {self._state_table}")

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._state_table} (
                table_name STRING NOT NULL,
                last_commit_version BIGINT NOT NULL,
                last_poll_time TIMESTAMP NOT NULL,
                tasks_found INT,
                tasks_triggered INT,
                updated_time TIMESTAMP NOT NULL
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """

        try:
            self.spark.sql(create_sql)
            logger.info(f"State table ready: {self._state_table}")
        except Exception as e:
            logger.error(f"Failed to create state table: {e}")
            raise

    def get_last_commit_version(self) -> int:
        """Get the last processed commit version for the trigger table.

        Returns:
            The last commit version, or 0 if no previous state exists.
        """
        query = f"""
            SELECT last_commit_version
            FROM {self._state_table}
            WHERE table_name = '{self.trigger_table}'
            ORDER BY updated_time DESC
            LIMIT 1
        """

        try:
            df = self.spark.sql(query)
            rows = df.collect()

            if rows:
                version = rows[0].last_commit_version
                logger.info(f"Last commit version for {self.trigger_table}: {version}")
                return version
            else:
                logger.info(f"No previous state for {self.trigger_table}, starting from 0")
                return 0

        except Exception as e:
            # Table might not have any data yet
            logger.warning(f"Could not read state: {e}")
            return 0

    def get_current_table_version(self) -> int:
        """Get the current (latest) version of the trigger table.

        Returns:
            The current table version.
        """
        query = f"DESCRIBE HISTORY {self.trigger_table} LIMIT 1"

        try:
            df = self.spark.sql(query)
            rows = df.collect()

            if rows:
                version = rows[0].version
                logger.info(f"Current table version: {version}")
                return version
            else:
                logger.warning("No history found for trigger table")
                return 0

        except Exception as e:
            logger.error(f"Could not get table version: {e}")
            raise

    def update_commit_version(
        self,
        commit_version: int,
        tasks_found: int = 0,
        tasks_triggered: int = 0,
    ) -> None:
        """Update the watermark with the new commit version.

        Args:
            commit_version: The new commit version to record.
            tasks_found: Number of tasks found in this poll.
            tasks_triggered: Number of tasks that triggered orchestrator runs.
        """
        now = datetime.now(timezone.utc)

        # Use MERGE to upsert the state
        merge_sql = f"""
            MERGE INTO {self._state_table} AS target
            USING (
                SELECT
                    '{self.trigger_table}' AS table_name,
                    {commit_version} AS last_commit_version,
                    TIMESTAMP '{now.isoformat()}' AS last_poll_time,
                    {tasks_found} AS tasks_found,
                    {tasks_triggered} AS tasks_triggered,
                    TIMESTAMP '{now.isoformat()}' AS updated_time
            ) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN
                UPDATE SET
                    last_commit_version = source.last_commit_version,
                    last_poll_time = source.last_poll_time,
                    tasks_found = source.tasks_found,
                    tasks_triggered = source.tasks_triggered,
                    updated_time = source.updated_time
            WHEN NOT MATCHED THEN
                INSERT (table_name, last_commit_version, last_poll_time, tasks_found, tasks_triggered, updated_time)
                VALUES (source.table_name, source.last_commit_version, source.last_poll_time, source.tasks_found, source.tasks_triggered, source.updated_time)
        """

        try:
            self.spark.sql(merge_sql)
            logger.info(f"Updated watermark to version {commit_version}")
        except Exception as e:
            logger.error(f"Failed to update watermark: {e}")
            raise

    def poll_for_new_tasks(self) -> list[JiraTask]:
        """Poll for new tasks using CDF.

        Reads changes from the trigger table since the last commit version,
        filters for inserts with the configured assignee, and returns the
        matching tasks.

        Returns:
            List of JiraTask objects for new tasks to process.
        """
        last_version = self.get_last_commit_version()
        current_version = self.get_current_table_version()

        if current_version <= last_version:
            logger.info(f"No new changes (current={current_version}, last={last_version})")
            return []

        logger.info(f"Reading CDF from version {last_version + 1} to {current_version}")

        # Query table_changes for new inserts
        # Note: The assignee field name may vary - common patterns are:
        # - ASSIGNEE__assignee (nested struct)
        # - assignee
        # - assignee_name
        cdf_query = f"""
            SELECT
                *,
                _commit_version,
                _change_type
            FROM table_changes('{self.trigger_table}', {last_version + 1}, {current_version})
            WHERE _change_type = 'insert'
        """

        try:
            df = self.spark.sql(cdf_query)

            # Check which assignee column exists
            columns = df.columns
            assignee_col = None
            for col in ["ASSIGNEE__assignee", "assignee", "assignee_name", "ASSIGNEE"]:
                if col in columns:
                    assignee_col = col
                    break

            if assignee_col is None:
                logger.warning(f"No assignee column found. Available columns: {columns}")
                # Still process all tasks if no assignee filter can be applied
                filtered_df = df
            else:
                logger.info(f"Using assignee column: {assignee_col}")
                filtered_df = df.filter(df[assignee_col] == self.assignee_filter)

            rows = filtered_df.collect()
            logger.info(f"Found {len(rows)} new tasks assigned to {self.assignee_filter}")

            tasks = []
            for row in rows:
                # Extract fields with fallbacks for different schemas
                issue_key = (
                    getattr(row, "ISSUEKEY", None) or
                    getattr(row, "KEY", None) or
                    getattr(row, "key", None) or
                    getattr(row, "issue_key", None) or
                    str(uuid.uuid4())[:8]
                )
                summary = (
                    getattr(row, "SUMMARY__summary", None) or
                    getattr(row, "SUMMARY", None) or
                    getattr(row, "summary", None) or
                    ""
                )
                description = (
                    getattr(row, "DESCRIPTION__description", None) or
                    getattr(row, "DESCRIPTION", None) or
                    getattr(row, "description", None) or
                    ""
                )
                status = (
                    getattr(row, "STATUS__status", None) or
                    getattr(row, "STATUS", None) or
                    getattr(row, "status", None) or
                    "Unknown"
                )
                assignee = getattr(row, assignee_col, self.assignee_filter) if assignee_col else self.assignee_filter
                commit_version = row._commit_version

                # Check for attachments - look for common column patterns
                attachment_count = 0
                has_attachments = False
                for att_col in ["ATTACHMENT__attachment", "attachment", "ATTACHMENT", "attachments"]:
                    if att_col in columns:
                        att_val = getattr(row, att_col, None)
                        if att_val is not None:
                            # Attachment value could be a count, list, or non-null marker
                            if isinstance(att_val, (list, tuple)):
                                attachment_count = len(att_val)
                            elif isinstance(att_val, int):
                                attachment_count = att_val
                            elif isinstance(att_val, str) and att_val.strip():
                                # Non-empty string indicates attachments exist
                                attachment_count = 1
                            else:
                                # Any other truthy value
                                attachment_count = 1 if att_val else 0
                            has_attachments = attachment_count > 0
                            break

                task = JiraTask(
                    issue_key=issue_key,
                    summary=summary,
                    description=description,
                    assignee=assignee,
                    status=status,
                    commit_version=commit_version,
                    raw_data=row.asDict(),
                    has_attachments=has_attachments,
                    attachment_count=attachment_count,
                )
                tasks.append(task)

            return tasks

        except Exception as e:
            logger.error(f"CDF query failed: {e}")
            # Check if CDF is not enabled
            if "enableChangeDataFeed" in str(e).lower() or "change data feed" in str(e).lower():
                logger.error(
                    f"CDF is not enabled on {self.trigger_table}. "
                    "Run: ALTER TABLE {self.trigger_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                )
            raise

    def download_task_attachments(
        self,
        task: JiraTask,
        target_volume: str = DEFAULT_TARGET_VOLUME,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Download attachments for a task if it has any.

        Args:
            task: The JiraTask with potential attachments.
            target_volume: UC Volumes directory to write downloads under.
            dry_run: If True, log but don't actually download.

        Returns:
            Download result dict with status, files, etc.
        """
        if not task.has_attachments:
            logger.debug(f"Task {task.issue_key} has no attachments to download")
            return {
                "status": "skipped",
                "message": "No attachments on this task",
                "issue_key": task.issue_key,
            }

        if dry_run:
            logger.info(f"[DRY RUN] Would download attachments for task: {task.issue_key}")
            return {
                "status": "dry_run",
                "message": f"Would download attachments for {task.issue_key}",
                "issue_key": task.issue_key,
                "attachment_count": task.attachment_count,
            }

        logger.info(f"Downloading attachments for task {task.issue_key}")

        # Generate session_id for artifact registry
        session_id = f"jira_{task.issue_key}_{task.commit_version}"

        result = download_jira_attachments(
            tickets_csv=task.issue_key,
            target_volume=target_volume,
            session_id=session_id,
            invocation_id=f"ingest_{task.issue_key}",
            iteration=0,
            register_artifacts=True,
        )

        # Update task with downloaded attachment paths
        if result.get("status") in ("success", "partial"):
            task.downloaded_attachments = [
                f.get("output_path")
                for f in result.get("files", [])
                if f.get("success") and f.get("output_path")
            ]
            logger.info(
                f"Downloaded {len(task.downloaded_attachments)} attachments for {task.issue_key}"
            )
        else:
            logger.warning(
                f"Failed to download attachments for {task.issue_key}: {result.get('message')}"
            )

        return result

    def trigger_orchestrator(
        self,
        task: JiraTask,
        dry_run: bool = False,
    ) -> Optional[tuple[int, str]]:
        """Trigger the orchestrator job for a task.

        Args:
            task: The JiraTask to process.
            dry_run: If True, log but don't actually trigger.

        Returns:
            Tuple of (run_id, run_url) if triggered, None if dry_run or no job configured.
        """
        if self.orchestrator_job_id is None:
            logger.warning("No orchestrator job ID configured - cannot trigger")
            return None

        if dry_run:
            logger.info(f"[DRY RUN] Would trigger orchestrator for task: {task.issue_key}")
            return None

        from .jobs_api import _get_workspace_client

        client = _get_workspace_client()

        # Build job parameters with task context
        job_params = {
            "ADK_SESSION_ID": f"jira_{task.issue_key}_{task.commit_version}",
            "ADK_PROMPT": f"Process JIRA task {task.issue_key}: {task.summary}\n\n{task.description}",
            "ADK_DELTA_CATALOG": self.catalog,
            "ADK_DELTA_SCHEMA": self.schema,
            "ADK_TASK_ISSUE_KEY": task.issue_key,
        }

        # Include attachment paths if any were downloaded
        if task.downloaded_attachments:
            job_params["ADK_ATTACHMENT_PATHS"] = json.dumps(task.downloaded_attachments)
            job_params["ADK_ATTACHMENT_COUNT"] = str(len(task.downloaded_attachments))

        logger.info(f"Triggering orchestrator job {self.orchestrator_job_id} for task {task.issue_key}")

        try:
            response = client.jobs.run_now(
                job_id=self.orchestrator_job_id,
                job_parameters=job_params,
            )

            run_id = response.run_id
            host = client.config.host.rstrip("/")
            run_url = f"{host}/#job/{self.orchestrator_job_id}/run/{run_id}"

            logger.info(f"Orchestrator triggered: run_id={run_id}, url={run_url}")

            return run_id, run_url

        except Exception as e:
            logger.error(f"Failed to trigger orchestrator: {e}")
            raise

    def run_poll_cycle(self, dry_run: bool = False) -> dict[str, Any]:
        """Run a complete poll cycle.

        This is the main entry point for the ingestor:
        1. Ensure state table exists
        2. Poll for new tasks
        3. Trigger orchestrator for each task
        4. Update watermark

        Args:
            dry_run: If True, don't trigger jobs or update state.

        Returns:
            Summary of the poll cycle.
        """
        result = {
            "start_time": datetime.now(timezone.utc).isoformat(),
            "trigger_table": self.trigger_table,
            "assignee_filter": self.assignee_filter,
            "tasks_found": 0,
            "tasks_triggered": 0,
            "triggered_runs": [],
            "errors": [],
            "dry_run": dry_run,
        }

        try:
            # Ensure state table
            self.ensure_state_table()

            # Get versions
            last_version = self.get_last_commit_version()
            current_version = self.get_current_table_version()
            result["last_version"] = last_version
            result["current_version"] = current_version

            # Poll for new tasks
            tasks = self.poll_for_new_tasks()
            result["tasks_found"] = len(tasks)

            # Download attachments and trigger orchestrator for each task
            for task in tasks:
                try:
                    # Download attachments if the task has any
                    if task.has_attachments:
                        attachment_result = self.download_task_attachments(
                            task, dry_run=dry_run
                        )
                        if attachment_result.get("status") == "error":
                            logger.warning(
                                f"Attachment download failed for {task.issue_key}, "
                                f"proceeding with orchestrator trigger anyway"
                            )
                            result.setdefault("attachment_errors", []).append({
                                "issue_key": task.issue_key,
                                "error": attachment_result.get("message"),
                            })

                    trigger_result = self.trigger_orchestrator(task, dry_run=dry_run)
                    if trigger_result:
                        run_id, run_url = trigger_result
                        result["triggered_runs"].append({
                            "issue_key": task.issue_key,
                            "run_id": run_id,
                            "run_url": run_url,
                            "attachments_downloaded": len(task.downloaded_attachments),
                        })
                        result["tasks_triggered"] += 1
                    elif dry_run:
                        result["tasks_triggered"] += 1  # Count dry run as triggered
                except Exception as e:
                    logger.error(f"Error triggering task {task.issue_key}: {e}")
                    result["errors"].append({
                        "issue_key": task.issue_key,
                        "error": str(e),
                    })

            # Update watermark (unless dry run)
            if not dry_run and current_version > last_version:
                self.update_commit_version(
                    commit_version=current_version,
                    tasks_found=result["tasks_found"],
                    tasks_triggered=result["tasks_triggered"],
                )

        except Exception as e:
            logger.exception(f"Poll cycle failed: {e}")
            result["errors"].append({"error": str(e)})

        result["end_time"] = datetime.now(timezone.utc).isoformat()
        result["status"] = "success" if not result["errors"] else "partial_failure"

        return result


def get_orchestrator_job_id(spark: "SparkSession", secret_scope: str) -> Optional[int]:
    """Get the orchestrator job ID from secrets.

    Args:
        spark: Active SparkSession.
        secret_scope: Name of the secret scope.

    Returns:
        The job ID as an integer, or None if not found.
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        job_id_str = dbutils.secrets.get(scope=secret_scope, key="rlm-orchestrator-job-id")
        return int(job_id_str) if job_id_str else None
    except Exception as e:
        logger.debug(f"Could not read orchestrator job ID from secrets: {e}")
        return None

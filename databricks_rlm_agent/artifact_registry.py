"""Artifact Registry for RLM Workflow.

This module provides a hybrid artifact storage system:
- Delta table for structured metadata tracking (artifact_id, status, timestamps, metadata)
- ADK ArtifactService for large blob storage (code files, stdout/stderr logs)

The registry decouples code generation from execution, enabling proper state
propagation between agents in the RLM workflow.

Schema:
    artifact_registry Delta table stores metadata with references to ADK artifacts:
    - artifact_id: Unique identifier for the artifact
    - session_id: ADK session identifier
    - invocation_id: Agent invocation identifier
    - iteration: Loop iteration number
    - artifact_type: Type of artifact (delegation_request, executor_result, processor_response)
    - sublm_instruction: Small inline instruction text
    - code_artifact_key: Reference to ADK ArtifactService for code
    - stdout_artifact_key: Reference for stdout logs
    - stderr_artifact_key: Reference for stderr logs
    - status: Artifact status (pending, executing, completed, consumed)
    - metadata_json: Additional metadata as JSON string
    - created_time, updated_time, consumed_time: Timestamps
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Delta table configuration from environment
DEFAULT_CATALOG = os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs")
DEFAULT_SCHEMA = os.environ.get("ADK_DELTA_SCHEMA", "adk")
DEFAULT_TABLE = os.environ.get("ADK_ARTIFACT_REGISTRY_TABLE", "artifact_registry")


@dataclass
class Artifact:
    """Represents an artifact in the registry."""

    artifact_id: str
    session_id: str
    invocation_id: str
    iteration: int
    artifact_type: str
    sublm_instruction: Optional[str] = None
    code_artifact_key: Optional[str] = None
    stdout_artifact_key: Optional[str] = None
    stderr_artifact_key: Optional[str] = None
    status: str = "pending"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_time: Optional[datetime] = None
    updated_time: Optional[datetime] = None
    consumed_time: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert artifact to dictionary for Delta table insertion."""
        return {
            "artifact_id": self.artifact_id,
            "session_id": self.session_id,
            "invocation_id": self.invocation_id,
            "iteration": self.iteration,
            "artifact_type": self.artifact_type,
            "sublm_instruction": self.sublm_instruction,
            "code_artifact_key": self.code_artifact_key,
            "stdout_artifact_key": self.stdout_artifact_key,
            "stderr_artifact_key": self.stderr_artifact_key,
            "status": self.status,
            "metadata_json": json.dumps(self.metadata) if self.metadata else None,
            "created_time": self.created_time or datetime.now(timezone.utc),
            "updated_time": self.updated_time or datetime.now(timezone.utc),
            "consumed_time": self.consumed_time,
        }

    @classmethod
    def from_row(cls, row: Any) -> "Artifact":
        """Create an Artifact from a Delta table row."""
        metadata = {}
        if row.metadata_json:
            try:
                metadata = json.loads(row.metadata_json)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse metadata_json for artifact {row.artifact_id}")

        return cls(
            artifact_id=row.artifact_id,
            session_id=row.session_id,
            invocation_id=row.invocation_id,
            iteration=row.iteration,
            artifact_type=row.artifact_type,
            sublm_instruction=row.sublm_instruction,
            code_artifact_key=row.code_artifact_key,
            stdout_artifact_key=row.stdout_artifact_key,
            stderr_artifact_key=row.stderr_artifact_key,
            status=row.status,
            metadata=metadata,
            created_time=row.created_time,
            updated_time=row.updated_time,
            consumed_time=row.consumed_time,
        )


class ArtifactRegistry:
    """Registry for managing artifacts in a Delta table.

    This class provides CRUD operations for artifacts, tracking their lifecycle
    from creation through execution to consumption.

    Example:
        >>> registry = ArtifactRegistry(spark, "silo_dev_rs", "adk")
        >>> registry.ensure_table()
        >>> artifact_id = registry.create_artifact(
        ...     session_id="session_001",
        ...     invocation_id="inv_001",
        ...     iteration=1,
        ...     artifact_type="delegation_request",
        ...     sublm_instruction="Analyze the data",
        ...     code_artifact_key="artifact_001_code.py",
        ... )
    """

    # DDL template for creating the artifact registry table
    CREATE_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        artifact_id STRING NOT NULL,
        session_id STRING NOT NULL,
        invocation_id STRING NOT NULL,
        iteration INT NOT NULL,
        artifact_type STRING NOT NULL,
        sublm_instruction STRING,
        code_artifact_key STRING,
        stdout_artifact_key STRING,
        stderr_artifact_key STRING,
        status STRING NOT NULL,
        metadata_json STRING,
        created_time TIMESTAMP NOT NULL,
        updated_time TIMESTAMP NOT NULL,
        consumed_time TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (session_id)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion' = '1',
        'delta.minWriterVersion' = '2'
    )
    """

    def __init__(
        self,
        spark: "SparkSession",
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
        table: str = DEFAULT_TABLE,
    ):
        """Initialize the artifact registry.

        Args:
            spark: SparkSession for Delta operations.
            catalog: Unity Catalog name.
            schema: Schema name within the catalog.
            table: Table name for the artifact registry.
        """
        self._spark = spark
        self._catalog = catalog
        self._schema = schema
        self._table = table
        self._full_table_name = f"{catalog}.{schema}.{table}"

    @property
    def full_table_name(self) -> str:
        """Get the fully qualified table name."""
        return self._full_table_name

    def ensure_table(self) -> None:
        """Ensure the artifact registry table exists, creating it if necessary."""
        ddl = self.CREATE_TABLE_DDL.format(
            catalog=self._catalog,
            schema=self._schema,
            table=self._table,
        )
        logger.info(f"Ensuring artifact registry table exists: {self._full_table_name}")
        self._spark.sql(ddl)

    def create_artifact(
        self,
        session_id: str,
        invocation_id: str,
        iteration: int,
        artifact_type: str,
        sublm_instruction: Optional[str] = None,
        code_artifact_key: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        artifact_id: Optional[str] = None,
    ) -> str:
        """Create a new artifact in the registry.

        Args:
            session_id: ADK session identifier.
            invocation_id: Agent invocation identifier.
            iteration: Loop iteration number.
            artifact_type: Type of artifact (delegation_request, executor_result, etc.).
            sublm_instruction: Optional instruction text for sub-LM processing.
            code_artifact_key: Optional reference to code stored in ArtifactService.
            metadata: Optional additional metadata dictionary.
            artifact_id: Optional specific artifact ID (generated if not provided).

        Returns:
            The artifact_id of the created artifact.
        """
        if artifact_id is None:
            artifact_id = f"art_{uuid.uuid4().hex[:12]}"

        now = datetime.now(timezone.utc)

        artifact = Artifact(
            artifact_id=artifact_id,
            session_id=session_id,
            invocation_id=invocation_id,
            iteration=iteration,
            artifact_type=artifact_type,
            sublm_instruction=sublm_instruction,
            code_artifact_key=code_artifact_key,
            status="pending",
            metadata=metadata or {},
            created_time=now,
            updated_time=now,
        )

        # Insert into Delta table
        data = [artifact.to_dict()]
        df = self._spark.createDataFrame(data)
        df.write.format("delta").mode("append").saveAsTable(self._full_table_name)

        logger.info(f"Created artifact: {artifact_id} (type={artifact_type}, session={session_id})")
        return artifact_id

    def get_artifact(self, artifact_id: str) -> Optional[Artifact]:
        """Get an artifact by ID.

        Args:
            artifact_id: The artifact identifier.

        Returns:
            The Artifact if found, None otherwise.
        """
        df = self._spark.sql(f"""
            SELECT * FROM {self._full_table_name}
            WHERE artifact_id = '{artifact_id}'
        """)

        rows = df.collect()
        if not rows:
            return None

        return Artifact.from_row(rows[0])

    def get_artifact_sync(self, artifact_id: str) -> Optional[dict[str, Any]]:
        """Synchronous version of get_artifact returning a dict.

        This is useful for contexts where an Artifact dataclass is not needed.

        Args:
            artifact_id: The artifact identifier.

        Returns:
            Dictionary with artifact data, or None if not found.
        """
        artifact = self.get_artifact(artifact_id)
        if artifact is None:
            return None
        return artifact.to_dict()

    def get_pending_artifacts(
        self,
        session_id: Optional[str] = None,
        artifact_type: Optional[str] = None,
    ) -> list[Artifact]:
        """Get all pending artifacts, optionally filtered.

        Args:
            session_id: Optional session ID to filter by.
            artifact_type: Optional artifact type to filter by.

        Returns:
            List of pending Artifact instances.
        """
        where_clauses = ["status = 'pending'"]
        if session_id:
            where_clauses.append(f"session_id = '{session_id}'")
        if artifact_type:
            where_clauses.append(f"artifact_type = '{artifact_type}'")

        where_sql = " AND ".join(where_clauses)

        df = self._spark.sql(f"""
            SELECT * FROM {self._full_table_name}
            WHERE {where_sql}
            ORDER BY created_time ASC
        """)

        return [Artifact.from_row(row) for row in df.collect()]

    def update_artifact(
        self,
        artifact_id: str,
        status: Optional[str] = None,
        stdout_artifact_key: Optional[str] = None,
        stderr_artifact_key: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Update an artifact in the registry.

        Args:
            artifact_id: The artifact identifier.
            status: Optional new status.
            stdout_artifact_key: Optional reference to stdout in ArtifactService.
            stderr_artifact_key: Optional reference to stderr in ArtifactService.
            metadata: Optional metadata to merge with existing.

        Returns:
            True if update was successful, False otherwise.
        """
        # Build SET clauses dynamically
        set_clauses = ["updated_time = current_timestamp()"]

        if status:
            set_clauses.append(f"status = '{status}'")
        if stdout_artifact_key:
            set_clauses.append(f"stdout_artifact_key = '{stdout_artifact_key}'")
        if stderr_artifact_key:
            set_clauses.append(f"stderr_artifact_key = '{stderr_artifact_key}'")

        # Handle metadata merge
        if metadata:
            # Get existing metadata and merge
            existing = self.get_artifact(artifact_id)
            if existing:
                merged_metadata = {**existing.metadata, **metadata}
                metadata_json = json.dumps(merged_metadata).replace("'", "''")
                set_clauses.append(f"metadata_json = '{metadata_json}'")

        set_sql = ", ".join(set_clauses)

        update_sql = f"""
            UPDATE {self._full_table_name}
            SET {set_sql}
            WHERE artifact_id = '{artifact_id}'
        """

        try:
            self._spark.sql(update_sql)
            logger.info(f"Updated artifact: {artifact_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update artifact {artifact_id}: {e}")
            return False

    def update_artifact_sync(
        self,
        artifact_id: str,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        status: Optional[str] = None,
    ) -> bool:
        """Synchronous artifact update with stdout/stderr as direct values.

        Note: This stores references to where the actual content should be stored
        in the ArtifactService. The caller is responsible for saving the actual
        content to the ArtifactService before calling this method.

        Args:
            artifact_id: The artifact identifier.
            stdout: Reference key for stdout artifact.
            stderr: Reference key for stderr artifact.
            status: New status.

        Returns:
            True if successful.
        """
        return self.update_artifact(
            artifact_id=artifact_id,
            status=status,
            stdout_artifact_key=stdout,
            stderr_artifact_key=stderr,
        )

    def mark_consumed(self, artifact_id: str) -> bool:
        """Mark an artifact as consumed.

        Args:
            artifact_id: The artifact identifier.

        Returns:
            True if successful.
        """
        update_sql = f"""
            UPDATE {self._full_table_name}
            SET status = 'consumed',
                consumed_time = current_timestamp(),
                updated_time = current_timestamp()
            WHERE artifact_id = '{artifact_id}'
        """

        try:
            self._spark.sql(update_sql)
            logger.info(f"Marked artifact as consumed: {artifact_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to mark artifact {artifact_id} as consumed: {e}")
            return False

    def get_latest_artifact_for_session(
        self,
        session_id: str,
        artifact_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Optional[Artifact]:
        """Get the most recent artifact for a session.

        Args:
            session_id: The session identifier.
            artifact_type: Optional type filter.
            status: Optional status filter.

        Returns:
            The most recent Artifact, or None if not found.
        """
        where_clauses = [f"session_id = '{session_id}'"]
        if artifact_type:
            where_clauses.append(f"artifact_type = '{artifact_type}'")
        if status:
            where_clauses.append(f"status = '{status}'")

        where_sql = " AND ".join(where_clauses)

        df = self._spark.sql(f"""
            SELECT * FROM {self._full_table_name}
            WHERE {where_sql}
            ORDER BY created_time DESC
            LIMIT 1
        """)

        rows = df.collect()
        if not rows:
            return None

        return Artifact.from_row(rows[0])


# Convenience function to get a registry instance
def get_artifact_registry(
    spark: "SparkSession",
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    table: str = DEFAULT_TABLE,
    ensure_exists: bool = True,
) -> ArtifactRegistry:
    """Get an ArtifactRegistry instance.

    Args:
        spark: SparkSession for Delta operations.
        catalog: Unity Catalog name.
        schema: Schema name.
        table: Table name.
        ensure_exists: Whether to create the table if it doesn't exist.

    Returns:
        Configured ArtifactRegistry instance.
    """
    registry = ArtifactRegistry(spark, catalog, schema, table)
    if ensure_exists:
        registry.ensure_table()
    return registry

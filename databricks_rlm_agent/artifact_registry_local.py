"""Local Artifact Registry for RLM Workflow.

This module provides a local implementation of the artifact registry using DuckDB
for metadata storage and local filesystem for artifact content storage.

The local registry mirrors the interface of ArtifactRegistry but operates entirely
on local resources, enabling development and testing without Databricks connectivity.

Schema:
    artifact_registry DuckDB table stores metadata with references to local artifacts:
    - artifact_id: Unique identifier for the artifact
    - session_id: ADK session identifier
    - invocation_id: Agent invocation identifier
    - iteration: Loop iteration number
    - artifact_type: Type of artifact (delegation_request, executor_result, processor_response)
    - sublm_instruction: Small inline instruction text
    - code_artifact_key: Reference to local artifact file for code
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

from databricks_rlm_agent.artifact_registry import Artifact

logger = logging.getLogger(__name__)

# Default paths from environment
DEFAULT_DB_PATH = os.environ.get("ADK_LOCAL_DB_PATH", ".adk_local/adk.duckdb")
DEFAULT_ARTIFACTS_PATH = os.environ.get("ADK_LOCAL_ARTIFACTS_PATH", ".adk_local/artifacts")


class LocalArtifactRegistry:
    """Local registry for managing artifacts in DuckDB.

    This class provides CRUD operations for artifacts using DuckDB for metadata
    and local filesystem for artifact content storage. It implements the same
    interface as ArtifactRegistry for seamless switching between local and
    Databricks modes.

    Example:
        >>> registry = LocalArtifactRegistry(
        ...     db_path=".adk_local/adk.duckdb",
        ...     artifacts_path=".adk_local/artifacts"
        ... )
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

    # DDL template for creating the artifact registry table in DuckDB
    CREATE_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS artifact_registry (
        artifact_id VARCHAR NOT NULL PRIMARY KEY,
        session_id VARCHAR NOT NULL,
        invocation_id VARCHAR NOT NULL,
        iteration INTEGER NOT NULL,
        artifact_type VARCHAR NOT NULL,
        sublm_instruction VARCHAR,
        code_artifact_key VARCHAR,
        stdout_artifact_key VARCHAR,
        stderr_artifact_key VARCHAR,
        status VARCHAR NOT NULL,
        metadata_json VARCHAR,
        created_time TIMESTAMP NOT NULL,
        updated_time TIMESTAMP NOT NULL,
        consumed_time TIMESTAMP
    )
    """

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        artifacts_path: str = DEFAULT_ARTIFACTS_PATH,
    ):
        """Initialize the local artifact registry.

        Args:
            db_path: Path to the DuckDB database file.
            artifacts_path: Path to the directory for artifact content storage.
        """
        if duckdb is None:
            raise ImportError(
                "duckdb is required for LocalArtifactRegistry. "
                "Install it with: pip install duckdb"
            )

        self._db_path = Path(db_path)
        self._artifacts_path = Path(artifacts_path)

        # Ensure parent directories exist
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._artifacts_path.mkdir(parents=True, exist_ok=True)

        # Connect to DuckDB
        self._conn = duckdb.connect(str(self._db_path))

    @property
    def db_path(self) -> Path:
        """Get the database file path."""
        return self._db_path

    @property
    def artifacts_path(self) -> Path:
        """Get the artifacts directory path."""
        return self._artifacts_path

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "LocalArtifactRegistry":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def ensure_table(self) -> None:
        """Ensure the artifact registry table exists, creating it if necessary."""
        logger.info(f"Ensuring local artifact registry table exists in: {self._db_path}")
        self._conn.execute(self.CREATE_TABLE_DDL)

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
            code_artifact_key: Optional reference to code stored locally.
            metadata: Optional additional metadata dictionary.
            artifact_id: Optional specific artifact ID (generated if not provided).

        Returns:
            The artifact_id of the created artifact.
        """
        if artifact_id is None:
            artifact_id = f"art_{uuid.uuid4().hex[:12]}"

        now = datetime.now(timezone.utc)
        metadata_json = json.dumps(metadata) if metadata else None

        self._conn.execute(
            """
            INSERT INTO artifact_registry (
                artifact_id, session_id, invocation_id, iteration, artifact_type,
                sublm_instruction, code_artifact_key, stdout_artifact_key, stderr_artifact_key,
                status, metadata_json, created_time, updated_time, consumed_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'pending', ?, ?, ?, NULL)
            """,
            [
                artifact_id,
                session_id,
                invocation_id,
                iteration,
                artifact_type,
                sublm_instruction,
                code_artifact_key,
                metadata_json,
                now,
                now,
            ],
        )

        logger.info(f"Created artifact: {artifact_id} (type={artifact_type}, session={session_id})")
        return artifact_id

    def _row_to_artifact(self, row: tuple) -> Artifact:
        """Convert a DuckDB row tuple to an Artifact object.

        Args:
            row: Tuple of column values from the query.

        Returns:
            Artifact instance.
        """
        # Column order matches: artifact_id, session_id, invocation_id, iteration,
        # artifact_type, sublm_instruction, code_artifact_key, stdout_artifact_key,
        # stderr_artifact_key, status, metadata_json, created_time, updated_time, consumed_time
        metadata = {}
        if row[10]:  # metadata_json
            try:
                metadata = json.loads(row[10])
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse metadata_json for artifact {row[0]}")

        return Artifact(
            artifact_id=row[0],
            session_id=row[1],
            invocation_id=row[2],
            iteration=row[3],
            artifact_type=row[4],
            sublm_instruction=row[5],
            code_artifact_key=row[6],
            stdout_artifact_key=row[7],
            stderr_artifact_key=row[8],
            status=row[9],
            metadata=metadata,
            created_time=row[11],
            updated_time=row[12],
            consumed_time=row[13],
        )

    def get_artifact(self, artifact_id: str) -> Optional[Artifact]:
        """Get an artifact by ID.

        Args:
            artifact_id: The artifact identifier.

        Returns:
            The Artifact if found, None otherwise.
        """
        result = self._conn.execute(
            "SELECT * FROM artifact_registry WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone()

        if not result:
            return None

        return self._row_to_artifact(result)

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
        query = "SELECT * FROM artifact_registry WHERE status = 'pending'"
        params: list[Any] = []

        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)
        if artifact_type:
            query += " AND artifact_type = ?"
            params.append(artifact_type)

        query += " ORDER BY created_time ASC"

        results = self._conn.execute(query, params).fetchall()
        return [self._row_to_artifact(row) for row in results]

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
            stdout_artifact_key: Optional reference to stdout artifact.
            stderr_artifact_key: Optional reference to stderr artifact.
            metadata: Optional metadata to merge with existing.

        Returns:
            True if update was successful, False otherwise.
        """
        now = datetime.now(timezone.utc)

        # Build SET clauses dynamically
        set_parts = ["updated_time = ?"]
        params: list[Any] = [now]

        if status:
            set_parts.append("status = ?")
            params.append(status)
        if stdout_artifact_key:
            set_parts.append("stdout_artifact_key = ?")
            params.append(stdout_artifact_key)
        if stderr_artifact_key:
            set_parts.append("stderr_artifact_key = ?")
            params.append(stderr_artifact_key)

        # Handle metadata merge
        if metadata:
            existing = self.get_artifact(artifact_id)
            if existing:
                merged_metadata = {**existing.metadata, **metadata}
                set_parts.append("metadata_json = ?")
                params.append(json.dumps(merged_metadata))

        set_sql = ", ".join(set_parts)
        params.append(artifact_id)

        try:
            self._conn.execute(
                f"UPDATE artifact_registry SET {set_sql} WHERE artifact_id = ?",
                params,
            )
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
        locally. The caller is responsible for saving the actual content to the
        artifacts directory before calling this method.

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
        now = datetime.now(timezone.utc)

        try:
            self._conn.execute(
                """
                UPDATE artifact_registry
                SET status = 'consumed', consumed_time = ?, updated_time = ?
                WHERE artifact_id = ?
                """,
                [now, now, artifact_id],
            )
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
        query = "SELECT * FROM artifact_registry WHERE session_id = ?"
        params: list[Any] = [session_id]

        if artifact_type:
            query += " AND artifact_type = ?"
            params.append(artifact_type)
        if status:
            query += " AND status = ?"
            params.append(status)

        query += " ORDER BY created_time DESC LIMIT 1"

        result = self._conn.execute(query, params).fetchone()
        if not result:
            return None

        return self._row_to_artifact(result)

    # Local artifact content storage methods

    def save_artifact_content(self, artifact_key: str, content: str | bytes) -> Path:
        """Save artifact content to local filesystem.

        Args:
            artifact_key: Unique key for the artifact (e.g., "artifact_001_code.py").
            content: The content to save (string or bytes).

        Returns:
            Path to the saved artifact file.
        """
        artifact_path = self._artifacts_path / artifact_key

        # Ensure parent directories exist for nested keys
        artifact_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(content, str):
            artifact_path.write_text(content)
        else:
            artifact_path.write_bytes(content)

        logger.debug(f"Saved artifact content to: {artifact_path}")
        return artifact_path

    def load_artifact_content(self, artifact_key: str) -> Optional[str]:
        """Load artifact content from local filesystem.

        Args:
            artifact_key: Unique key for the artifact.

        Returns:
            The content as a string, or None if not found.
        """
        artifact_path = self._artifacts_path / artifact_key

        if not artifact_path.exists():
            logger.warning(f"Artifact not found: {artifact_path}")
            return None

        return artifact_path.read_text()

    def load_artifact_content_bytes(self, artifact_key: str) -> Optional[bytes]:
        """Load artifact content as bytes from local filesystem.

        Args:
            artifact_key: Unique key for the artifact.

        Returns:
            The content as bytes, or None if not found.
        """
        artifact_path = self._artifacts_path / artifact_key

        if not artifact_path.exists():
            logger.warning(f"Artifact not found: {artifact_path}")
            return None

        return artifact_path.read_bytes()

    def delete_artifact_content(self, artifact_key: str) -> bool:
        """Delete artifact content from local filesystem.

        Args:
            artifact_key: Unique key for the artifact.

        Returns:
            True if deleted successfully, False otherwise.
        """
        artifact_path = self._artifacts_path / artifact_key

        if not artifact_path.exists():
            logger.warning(f"Artifact not found for deletion: {artifact_path}")
            return False

        try:
            artifact_path.unlink()
            logger.debug(f"Deleted artifact content: {artifact_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete artifact {artifact_path}: {e}")
            return False

    def artifact_content_exists(self, artifact_key: str) -> bool:
        """Check if artifact content exists on local filesystem.

        Args:
            artifact_key: Unique key for the artifact.

        Returns:
            True if the artifact exists, False otherwise.
        """
        artifact_path = self._artifacts_path / artifact_key
        return artifact_path.exists()


# Convenience function to get a registry instance
def get_local_artifact_registry(
    db_path: str = DEFAULT_DB_PATH,
    artifacts_path: str = DEFAULT_ARTIFACTS_PATH,
    ensure_exists: bool = True,
) -> LocalArtifactRegistry:
    """Get a LocalArtifactRegistry instance.

    Args:
        db_path: Path to the DuckDB database file.
        artifacts_path: Path to the directory for artifact content storage.
        ensure_exists: Whether to create the table if it doesn't exist.

    Returns:
        Configured LocalArtifactRegistry instance.
    """
    registry = LocalArtifactRegistry(db_path=db_path, artifacts_path=artifacts_path)
    if ensure_exists:
        registry.ensure_table()
    return registry

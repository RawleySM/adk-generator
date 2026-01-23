"""DeltaSessionService for persisting ADK sessions to Databricks Unity Catalog Delta tables.

This module provides a custom SessionService implementation that stores session
data in Delta tables, enabling durable state persistence for ADK agents running
on Databricks Lakeflow Jobs.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any, Optional, TYPE_CHECKING

from google.adk.events.event import Event
from google.adk.sessions.base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from google.adk.sessions.session import Session
from google.adk.sessions.state import State
from typing_extensions import override

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger("adk_generator.sessions.delta")

# Apply nest_asyncio for Databricks compatibility
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass


def _extract_state_delta(state: Optional[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Extract app, user, and session state deltas from a state dictionary.

    Separates state keys by their prefixes:
    - "app:*" -> app state (prefix removed)
    - "user:*" -> user state (prefix removed)
    - "temp:*" -> ignored (temporary state)
    - other -> session state
    """
    deltas = {"app": {}, "user": {}, "session": {}}
    if state:
        for key, value in state.items():
            if key.startswith(State.APP_PREFIX):
                deltas["app"][key.removeprefix(State.APP_PREFIX)] = value
            elif key.startswith(State.USER_PREFIX):
                deltas["user"][key.removeprefix(State.USER_PREFIX)] = value
            elif not key.startswith(State.TEMP_PREFIX):
                deltas["session"][key] = value
    return deltas


def _merge_state(
    app_state: dict[str, Any],
    user_state: dict[str, Any],
    session_state: dict[str, Any],
) -> dict[str, Any]:
    """Merge app, user, and session states into a single state dictionary.

    Adds appropriate prefixes to app and user state keys.
    """
    merged = copy.deepcopy(session_state)
    for key, value in app_state.items():
        merged[State.APP_PREFIX + key] = value
    for key, value in user_state.items():
        merged[State.USER_PREFIX + key] = value
    return merged


class DeltaSessionService(BaseSessionService):
    """A session service that persists sessions to Databricks Unity Catalog Delta tables.

    This implementation stores sessions, events, app states, and user states in
    four Delta tables within a Unity Catalog schema. It supports:

    - Durable session persistence across job runs
    - Optimistic concurrency control via version counters
    - Idempotent event appends via MERGE operations
    - Session rewind with logical pointers (no physical deletes)
    - Soft deletes for session recovery

    Tables created:
    - {catalog}.{schema}.sessions - Session metadata and state
    - {catalog}.{schema}.events - Event history
    - {catalog}.{schema}.app_states - Application-level state
    - {catalog}.{schema}.user_states - User-level state

    Example:
        ```python
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        service = DeltaSessionService(
            spark=spark,
            catalog="silo_dev_rs",
            schema="adk"
        )

        session = await service.create_session(
            app_name="my_agent",
            user_id="user123"
        )
        ```
    """

    # Retry configuration for optimistic concurrency conflicts
    MAX_RETRIES = 3
    RETRY_DELAYS_MS = [100, 200, 300]

    def __init__(
        self,
        spark: "SparkSession",
        catalog: str = "silo_dev_rs",
        schema: str = "adk",
    ):
        """Initialize the DeltaSessionService.

        Args:
            spark: Active SparkSession for executing SQL
            catalog: Unity Catalog name
            schema: Schema name within the catalog
        """
        self._spark = spark
        self._catalog = catalog
        self._schema = schema

        # Build fully-qualified table names
        self._sessions_table = f"{catalog}.{schema}.sessions"
        self._events_table = f"{catalog}.{schema}.events"
        self._app_states_table = f"{catalog}.{schema}.app_states"
        self._user_states_table = f"{catalog}.{schema}.user_states"

        # Table creation flag and lock
        self._tables_created = False
        self._table_creation_lock = asyncio.Lock()

        logger.info(
            "DeltaSessionService initialized with catalog=%s, schema=%s",
            catalog, schema
        )

    async def _ensure_tables_exist(self) -> None:
        """Create tables if they don't exist.

        Uses CREATE TABLE IF NOT EXISTS with Delta format and appropriate
        partitioning for query performance.
        """
        if self._tables_created:
            return

        async with self._table_creation_lock:
            if self._tables_created:
                return

            # Create sessions table
            # Note: DEFAULT values removed for Delta compatibility (POC)
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._sessions_table} (
                    app_name STRING NOT NULL,
                    user_id STRING NOT NULL,
                    session_id STRING NOT NULL,
                    state_json STRING,
                    created_time TIMESTAMP NOT NULL,
                    update_time TIMESTAMP NOT NULL,
                    version BIGINT NOT NULL,
                    is_deleted BOOLEAN NOT NULL,
                    deleted_time TIMESTAMP,
                    rewind_to_event_id STRING,
                    last_write_nonce STRING
                )
                USING DELTA
                PARTITIONED BY (app_name)
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)

            # Create events table
            # Note: DEFAULT values removed for Delta compatibility (POC)
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._events_table} (
                    app_name STRING NOT NULL,
                    user_id STRING NOT NULL,
                    session_id STRING NOT NULL,
                    event_id STRING NOT NULL,
                    invocation_id STRING NOT NULL,
                    author STRING NOT NULL,
                    event_timestamp TIMESTAMP NOT NULL,
                    sequence_num BIGINT NOT NULL,
                    event_data_json STRING NOT NULL,
                    state_delta_json STRING,
                    has_state_delta BOOLEAN NOT NULL,
                    created_time TIMESTAMP NOT NULL,
                    is_after_rewind BOOLEAN NOT NULL
                )
                USING DELTA
                PARTITIONED BY (app_name, user_id)
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
            """)

            # Create app_states table
            # Note: DEFAULT values removed for Delta compatibility (POC)
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._app_states_table} (
                    app_name STRING NOT NULL,
                    state_json STRING,
                    update_time TIMESTAMP NOT NULL,
                    version BIGINT NOT NULL
                )
                USING DELTA
            """)

            # Create user_states table
            # Note: DEFAULT values removed for Delta compatibility (POC)
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._user_states_table} (
                    app_name STRING NOT NULL,
                    user_id STRING NOT NULL,
                    state_json STRING,
                    update_time TIMESTAMP NOT NULL,
                    version BIGINT NOT NULL
                )
                USING DELTA
                PARTITIONED BY (app_name)
            """)

            self._tables_created = True
            logger.info("Delta tables created/verified successfully")

    def _escape_sql_string(self, s: str) -> str:
        """Escape single quotes in a string for SQL injection prevention."""
        if s is None:
            return ""
        return s.replace("'", "''")

    def _to_json(self, obj: Any) -> str:
        """Serialize an object to JSON string."""
        if obj is None:
            return "{}"
        if hasattr(obj, "model_dump"):
            return json.dumps(obj.model_dump(mode="json", by_alias=True))
        return json.dumps(obj)

    def _from_json(self, json_str: Optional[str]) -> dict[str, Any]:
        """Deserialize a JSON string to a dictionary."""
        if not json_str:
            return {}
        try:
            return json.loads(json_str, strict=False)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}. String: {json_str[:100]}...")
            raise

    async def _get_app_state(self, app_name: str) -> dict[str, Any]:
        """Get the app state for the given app_name."""
        escaped_app = self._escape_sql_string(app_name)
        result = self._spark.sql(f"""
            SELECT state_json FROM {self._app_states_table}
            WHERE app_name = '{escaped_app}'
        """).collect()

        if result:
            return self._from_json(result[0]["state_json"])
        return {}

    async def _get_user_state(self, app_name: str, user_id: str) -> dict[str, Any]:
        """Get the user state for the given app_name and user_id."""
        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        result = self._spark.sql(f"""
            SELECT state_json FROM {self._user_states_table}
            WHERE app_name = '{escaped_app}' AND user_id = '{escaped_user}'
        """).collect()

        if result:
            return self._from_json(result[0]["state_json"])
        return {}

    async def _upsert_app_state(
        self,
        app_name: str,
        state_delta: dict[str, Any],
    ) -> None:
        """Upsert app state with the given delta."""
        if not state_delta:
            return

        escaped_app = self._escape_sql_string(app_name)
        now = datetime.now(timezone.utc).isoformat()

        # Get current state
        current_state = await self._get_app_state(app_name)
        new_state = {**current_state, **state_delta}
        state_json = self._escape_sql_string(json.dumps(new_state))

        self._spark.sql(f"""
            MERGE INTO {self._app_states_table} AS target
            USING (SELECT '{escaped_app}' AS app_name) AS source
            ON target.app_name = source.app_name
            WHEN MATCHED THEN UPDATE SET
                state_json = '{state_json}',
                update_time = TIMESTAMP '{now}',
                version = target.version + 1
            WHEN NOT MATCHED THEN INSERT (app_name, state_json, update_time, version)
            VALUES ('{escaped_app}', '{state_json}', TIMESTAMP '{now}', 1)
        """)

    async def _upsert_user_state(
        self,
        app_name: str,
        user_id: str,
        state_delta: dict[str, Any],
    ) -> None:
        """Upsert user state with the given delta."""
        if not state_delta:
            return

        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        now = datetime.now(timezone.utc).isoformat()

        # Get current state
        current_state = await self._get_user_state(app_name, user_id)
        new_state = {**current_state, **state_delta}
        state_json = self._escape_sql_string(json.dumps(new_state))

        self._spark.sql(f"""
            MERGE INTO {self._user_states_table} AS target
            USING (SELECT '{escaped_app}' AS app_name, '{escaped_user}' AS user_id) AS source
            ON target.app_name = source.app_name AND target.user_id = source.user_id
            WHEN MATCHED THEN UPDATE SET
                state_json = '{state_json}',
                update_time = TIMESTAMP '{now}',
                version = target.version + 1
            WHEN NOT MATCHED THEN INSERT (app_name, user_id, state_json, update_time, version)
            VALUES ('{escaped_app}', '{escaped_user}', '{state_json}', TIMESTAMP '{now}', 1)
        """)

    @override
    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ) -> Session:
        """Create a new session.

        Args:
            app_name: The name of the app
            user_id: The id of the user
            state: Optional initial state (may include app:, user:, temp: prefixed keys)
            session_id: Optional client-provided session ID (UUID generated if not provided)

        Returns:
            The newly created Session instance

        Raises:
            ValueError: If a session with the given ID already exists
        """
        await self._ensure_tables_exist()

        # Generate session ID if not provided
        if not session_id:
            session_id = str(uuid.uuid4())

        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        escaped_session = self._escape_sql_string(session_id)

        # Check if session already exists
        existing = self._spark.sql(f"""
            SELECT 1 FROM {self._sessions_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND is_deleted = FALSE
        """).collect()

        if existing:
            raise ValueError(f"Session with id {session_id} already exists.")

        # Extract state deltas by prefix
        state_deltas = _extract_state_delta(state)
        app_state_delta = state_deltas["app"]
        user_state_delta = state_deltas["user"]
        session_state = state_deltas["session"]

        # Upsert app and user states
        await self._upsert_app_state(app_name, app_state_delta)
        await self._upsert_user_state(app_name, user_id, user_state_delta)

        # Insert session row
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        session_state_json = self._escape_sql_string(json.dumps(session_state))

        self._spark.sql(f"""
            INSERT INTO {self._sessions_table}
            (app_name, user_id, session_id, state_json, created_time, update_time, version, is_deleted)
            VALUES (
                '{escaped_app}',
                '{escaped_user}',
                '{escaped_session}',
                '{session_state_json}',
                TIMESTAMP '{now_iso}',
                TIMESTAMP '{now_iso}',
                1,
                FALSE
            )
        """)

        # Fetch app and user states for the merged response
        app_state = await self._get_app_state(app_name)
        user_state = await self._get_user_state(app_name, user_id)
        merged_state = _merge_state(app_state, user_state, session_state)

        return Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged_state,
            events=[],
            last_update_time=now.timestamp(),
        )

    @override
    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        config: Optional[GetSessionConfig] = None,
    ) -> Optional[Session]:
        """Get a session by ID.

        Args:
            app_name: The name of the app
            user_id: The id of the user
            session_id: The session ID
            config: Optional configuration for filtering events

        Returns:
            The Session if found, None otherwise
        """
        await self._ensure_tables_exist()

        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        escaped_session = self._escape_sql_string(session_id)

        # Get session row
        session_result = self._spark.sql(f"""
            SELECT session_id, state_json, update_time, version, rewind_to_event_id
            FROM {self._sessions_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND is_deleted = FALSE
        """).collect()

        if not session_result:
            return None

        session_row = session_result[0]
        session_state = self._from_json(session_row["state_json"])
        update_time = session_row["update_time"]
        rewind_to_event_id = session_row["rewind_to_event_id"]

        # Build events query with optional filters
        events_query = f"""
            SELECT event_id, invocation_id, author, event_timestamp, sequence_num,
                   event_data_json, state_delta_json, has_state_delta, is_after_rewind
            FROM {self._events_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND is_after_rewind = FALSE
        """

        if config and config.after_timestamp:
            after_dt = datetime.fromtimestamp(config.after_timestamp, timezone.utc)
            events_query += f" AND event_timestamp >= TIMESTAMP '{after_dt.isoformat()}'"

        # Use (sequence_num, created_time, event_id) as composite sort key for deterministic ordering
        # This ensures consistent ordering even if sequence_num has duplicates under high concurrency
        events_query += " ORDER BY sequence_num ASC, created_time ASC, event_id ASC"

        if config and config.num_recent_events:
            # For recent events, we need to order desc first, limit, then reorder
            events_query = f"""
                SELECT * FROM (
                    SELECT event_id, invocation_id, author, event_timestamp, sequence_num,
                           event_data_json, state_delta_json, has_state_delta, is_after_rewind,
                           created_time
                    FROM {self._events_table}
                    WHERE app_name = '{escaped_app}'
                      AND user_id = '{escaped_user}'
                      AND session_id = '{escaped_session}'
                      AND is_after_rewind = FALSE
            """
            if config.after_timestamp:
                after_dt = datetime.fromtimestamp(config.after_timestamp, timezone.utc)
                events_query += f" AND event_timestamp >= TIMESTAMP '{after_dt.isoformat()}'"

            events_query += f"""
                    ORDER BY sequence_num DESC, created_time DESC, event_id DESC
                    LIMIT {config.num_recent_events}
                ) subq ORDER BY sequence_num ASC, created_time ASC, event_id ASC
            """

        events_result = self._spark.sql(events_query).collect()

        # Reconstruct Event objects
        events = []
        for row in events_result:
            event_data = self._from_json(row["event_data_json"])
            event = Event.model_validate(event_data)
            events.append(event)

        # Fetch app and user states
        app_state = await self._get_app_state(app_name)
        user_state = await self._get_user_state(app_name, user_id)
        merged_state = _merge_state(app_state, user_state, session_state)

        # Convert update_time to timestamp
        if isinstance(update_time, datetime):
            last_update_time = update_time.timestamp()
        else:
            last_update_time = float(update_time)

        return Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=merged_state,
            events=events,
            last_update_time=last_update_time,
        )

    @override
    async def list_sessions(
        self,
        *,
        app_name: str,
        user_id: Optional[str] = None,
    ) -> ListSessionsResponse:
        """List sessions for an app, optionally filtered by user.

        Args:
            app_name: The name of the app
            user_id: Optional user ID to filter by

        Returns:
            ListSessionsResponse containing the sessions (without events)
        """
        await self._ensure_tables_exist()

        escaped_app = self._escape_sql_string(app_name)

        query = f"""
            SELECT session_id, user_id, state_json, update_time
            FROM {self._sessions_table}
            WHERE app_name = '{escaped_app}'
              AND is_deleted = FALSE
        """

        if user_id is not None:
            escaped_user = self._escape_sql_string(user_id)
            query += f" AND user_id = '{escaped_user}'"

        query += " ORDER BY update_time DESC"

        result = self._spark.sql(query).collect()

        # Fetch app state
        app_state = await self._get_app_state(app_name)

        # Build user states map
        user_states_map: dict[str, dict[str, Any]] = {}
        if user_id is not None:
            user_states_map[user_id] = await self._get_user_state(app_name, user_id)
        else:
            # Fetch all user states for this app
            user_states_result = self._spark.sql(f"""
                SELECT user_id, state_json FROM {self._user_states_table}
                WHERE app_name = '{escaped_app}'
            """).collect()
            for row in user_states_result:
                user_states_map[row["user_id"]] = self._from_json(row["state_json"])

        sessions = []
        for row in result:
            session_state = self._from_json(row["state_json"])
            user_state = user_states_map.get(row["user_id"], {})
            merged_state = _merge_state(app_state, user_state, session_state)

            update_time = row["update_time"]
            if isinstance(update_time, datetime):
                last_update_time = update_time.timestamp()
            else:
                last_update_time = float(update_time)

            sessions.append(Session(
                id=row["session_id"],
                app_name=app_name,
                user_id=row["user_id"],
                state=merged_state,
                events=[],  # list_sessions doesn't include events
                last_update_time=last_update_time,
            ))

        return ListSessionsResponse(sessions=sessions)

    @override
    async def delete_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> None:
        """Soft-delete a session.

        Sets is_deleted=TRUE and deleted_time on the session row.
        Events are preserved for audit trail.

        Args:
            app_name: The name of the app
            user_id: The id of the user
            session_id: The session ID to delete
        """
        await self._ensure_tables_exist()

        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        escaped_session = self._escape_sql_string(session_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        self._spark.sql(f"""
            UPDATE {self._sessions_table}
            SET is_deleted = TRUE,
                deleted_time = TIMESTAMP '{now_iso}',
                update_time = TIMESTAMP '{now_iso}'
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
        """)

    @override
    async def append_event(self, session: Session, event: Event) -> Event:
        """Append an event to a session.

        This method:
        1. Skips partial events (streaming chunks)
        2. Validates the session is not stale
        3. Merges the event row idempotently (duplicate event_id is safe)
        4. Applies state_delta to app/user/session states
        5. Updates the session row with version check
        6. Retries on optimistic concurrency conflicts

        Args:
            session: The session to append to
            event: The event to append

        Returns:
            The appended event

        Raises:
            ValueError: If the session is stale
        """
        await self._ensure_tables_exist()

        # Skip partial events (streaming chunks)
        if event.partial:
            return event

        # Trim temp state before persisting
        event = self._trim_temp_delta_state(event)

        for attempt in range(self.MAX_RETRIES):
            try:
                return await self._append_event_with_retry(session, event)
            except ValueError as e:
                if "stale" in str(e).lower() or "version" in str(e).lower():
                    if attempt < self.MAX_RETRIES - 1:
                        delay = self.RETRY_DELAYS_MS[attempt] / 1000.0
                        logger.warning(
                            "Concurrency conflict on attempt %d, retrying in %dms",
                            attempt + 1, self.RETRY_DELAYS_MS[attempt]
                        )
                        await asyncio.sleep(delay)
                        continue
                raise

        # This should not be reached, but just in case
        return await self._append_event_with_retry(session, event)

    async def _append_event_with_retry(self, session: Session, event: Event) -> Event:
        """Internal method to append event with proper atomicity and OCC.

        Atomicity strategy:
        1. Read session state and version
        2. Check staleness
        3. UPDATE session row with OCC (version check) FIRST
        4. Verify OCC succeeded with exact match on version AND update_time
        5. INSERT event ONLY after OCC success (no orphaned events)
        6. Apply app/user state deltas last

        Sequencing strategy:
        - sequence_num is best-effort (may have duplicates under high concurrency)
        - Deterministic ordering guaranteed by (sequence_num, created_time, event_id)
        """
        escaped_app = self._escape_sql_string(session.app_name)
        escaped_user = self._escape_sql_string(session.user_id)
        escaped_session = self._escape_sql_string(session.id)

        # STEP 1: Get current session state and check for staleness
        session_result = self._spark.sql(f"""
            SELECT state_json, update_time, version
            FROM {self._sessions_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND is_deleted = FALSE
        """).collect()

        if not session_result:
            raise ValueError(f"Session {session.id} not found or deleted")

        session_row = session_result[0]
        stored_update_time = session_row["update_time"]
        stored_version = session_row["version"]

        # Convert to timestamp for comparison
        if isinstance(stored_update_time, datetime):
            stored_timestamp = stored_update_time.timestamp()
        else:
            stored_timestamp = float(stored_update_time)

        # STEP 2: Check for stale session
        if stored_timestamp > session.last_update_time:
            raise ValueError(
                f"Session is stale: stored update_time {stored_timestamp} > "
                f"session.last_update_time {session.last_update_time}"
            )

        # Get current session state
        current_session_state = self._from_json(session_row["state_json"])

        # Extract state deltas from event (all deferred until after OCC succeeds)
        state_delta_json: Optional[str] = None
        has_state_delta = False
        pending_app_delta: dict[str, Any] = {}
        pending_user_delta: dict[str, Any] = {}

        if event.actions and event.actions.state_delta:
            state_deltas = _extract_state_delta(event.actions.state_delta)
            pending_app_delta = state_deltas["app"]
            pending_user_delta = state_deltas["user"]
            session_state_delta = state_deltas["session"]

            # Update session state (local only, will be persisted in UPDATE)
            if session_state_delta:
                current_session_state = {**current_session_state, **session_state_delta}

            state_delta_json = self._escape_sql_string(
                json.dumps(event.actions.state_delta)
            )
            has_state_delta = True

        # Generate unique nonce for OCC verification
        # This avoids timestamp string comparison issues across Spark/Python
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        occ_nonce = str(uuid.uuid4())

        # STEP 3: UPDATE session row with OCC FIRST (before event insert)
        # This ensures no event is written unless session update succeeds
        new_state_json = self._escape_sql_string(json.dumps(current_session_state))
        escaped_nonce = self._escape_sql_string(occ_nonce)

        self._spark.sql(f"""
            UPDATE {self._sessions_table}
            SET state_json = '{new_state_json}',
                update_time = TIMESTAMP '{now_iso}',
                version = version + 1,
                last_write_nonce = '{escaped_nonce}'
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND version = {stored_version}
        """)

        # STEP 4: Verify OCC succeeded by checking our unique nonce was written
        # This is more reliable than timestamp string comparison
        verify_result = self._spark.sql(f"""
            SELECT version, last_write_nonce FROM {self._sessions_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND is_deleted = FALSE
              AND version = {stored_version + 1}
        """).collect()

        if not verify_result:
            raise ValueError(
                f"Version conflict: OCC failed for version {stored_version} -> {stored_version + 1}"
            )

        # Verify our exact nonce was written (not another concurrent writer)
        actual_nonce = verify_result[0]["last_write_nonce"]
        if actual_nonce != occ_nonce:
            raise ValueError(
                f"Version conflict: nonce mismatch. Expected {occ_nonce}, got {actual_nonce}"
            )

        # STEP 5: OCC succeeded - NOW insert the event (no orphaned events possible)
        event_data_json = self._escape_sql_string(self._to_json(event))
        escaped_event_id = self._escape_sql_string(event.id)
        escaped_invocation_id = self._escape_sql_string(event.invocation_id)
        escaped_author = self._escape_sql_string(event.author)
        event_timestamp_iso = datetime.fromtimestamp(event.timestamp, timezone.utc).isoformat()
        state_delta_value = f"'{state_delta_json}'" if state_delta_json else "NULL"

        # Use session version as sequence base to reduce collision probability
        # sequence_num = (session_version * 1000) + local_offset
        # This leverages OCC: only one writer can have this exact session version
        sequence_base = (stored_version + 1) * 1000

        self._spark.sql(f"""
            MERGE INTO {self._events_table} AS target
            USING (
                SELECT
                    '{escaped_app}' AS app_name,
                    '{escaped_user}' AS user_id,
                    '{escaped_session}' AS session_id,
                    '{escaped_event_id}' AS event_id,
                    {sequence_base} + COALESCE(
                        (SELECT COUNT(*) FROM {self._events_table}
                         WHERE app_name = '{escaped_app}'
                           AND user_id = '{escaped_user}'
                           AND session_id = '{escaped_session}'
                           AND sequence_num >= {sequence_base}
                           AND sequence_num < {sequence_base + 1000}),
                        0
                    ) AS next_seq
            ) AS source
            ON target.app_name = source.app_name
               AND target.user_id = source.user_id
               AND target.session_id = source.session_id
               AND target.event_id = source.event_id
            WHEN NOT MATCHED THEN INSERT (
                app_name, user_id, session_id, event_id, invocation_id, author,
                event_timestamp, sequence_num, event_data_json, state_delta_json,
                has_state_delta, created_time, is_after_rewind
            ) VALUES (
                '{escaped_app}', '{escaped_user}', '{escaped_session}',
                '{escaped_event_id}', '{escaped_invocation_id}', '{escaped_author}',
                TIMESTAMP '{event_timestamp_iso}', source.next_seq, '{event_data_json}',
                {state_delta_value}, {str(has_state_delta).upper()},
                TIMESTAMP '{now_iso}', FALSE
            )
        """)

        # STEP 6: Apply deferred app/user state deltas (after all critical writes)
        if pending_app_delta:
            await self._upsert_app_state(session.app_name, pending_app_delta)
        if pending_user_delta:
            await self._upsert_user_state(session.app_name, session.user_id, pending_user_delta)

        # Update in-memory session
        session.last_update_time = now.timestamp()
        self._update_session_state(session, event)
        session.events.append(event)

        return event

    async def rewind_session(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        target_event_id: str,
    ) -> Session:
        """Rewind a session to a specific event.

        This sets a logical rewind pointer - events after the target are marked
        as is_after_rewind=TRUE and excluded from get_session queries.
        The session state is reconstructed by replaying events up to the target.

        Args:
            app_name: The name of the app
            user_id: The id of the user
            session_id: The session ID
            target_event_id: The event ID to rewind to

        Returns:
            The rewound Session

        Raises:
            ValueError: If the session or target event is not found
        """
        await self._ensure_tables_exist()

        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        escaped_session = self._escape_sql_string(session_id)
        escaped_target = self._escape_sql_string(target_event_id)

        # Get target event sequence number
        target_result = self._spark.sql(f"""
            SELECT sequence_num FROM {self._events_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND event_id = '{escaped_target}'
        """).collect()

        if not target_result:
            raise ValueError(f"Target event {target_event_id} not found")

        target_seq = target_result[0]["sequence_num"]

        # Mark events after target as rewound
        now_iso = datetime.now(timezone.utc).isoformat()
        self._spark.sql(f"""
            UPDATE {self._events_table}
            SET is_after_rewind = TRUE
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND sequence_num > {target_seq}
        """)

        # Reconstruct state by replaying events up to target
        events_result = self._spark.sql(f"""
            SELECT event_data_json, state_delta_json, has_state_delta
            FROM {self._events_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
              AND sequence_num <= {target_seq}
            ORDER BY sequence_num ASC, created_time ASC, event_id ASC
        """).collect()

        # Rebuild session state from events
        session_state: dict[str, Any] = {}
        for row in events_result:
            if row["has_state_delta"] and row["state_delta_json"]:
                delta = self._from_json(row["state_delta_json"])
                state_deltas = _extract_state_delta(delta)
                session_state = {**session_state, **state_deltas["session"]}

        # Update session row
        state_json = self._escape_sql_string(json.dumps(session_state))
        self._spark.sql(f"""
            UPDATE {self._sessions_table}
            SET state_json = '{state_json}',
                update_time = TIMESTAMP '{now_iso}',
                rewind_to_event_id = '{escaped_target}',
                version = version + 1
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
        """)

        # Return the rewound session
        return await self.get_session(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
        )

    async def clear_rewind(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> Session:
        """Clear the rewind state and restore all events.

        Args:
            app_name: The name of the app
            user_id: The id of the user
            session_id: The session ID

        Returns:
            The session with all events restored
        """
        await self._ensure_tables_exist()

        escaped_app = self._escape_sql_string(app_name)
        escaped_user = self._escape_sql_string(user_id)
        escaped_session = self._escape_sql_string(session_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        # Clear is_after_rewind flags
        self._spark.sql(f"""
            UPDATE {self._events_table}
            SET is_after_rewind = FALSE
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
        """)

        # Rebuild full state from all events
        events_result = self._spark.sql(f"""
            SELECT state_delta_json, has_state_delta
            FROM {self._events_table}
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
            ORDER BY sequence_num ASC, created_time ASC, event_id ASC
        """).collect()

        session_state: dict[str, Any] = {}
        for row in events_result:
            if row["has_state_delta"] and row["state_delta_json"]:
                delta = self._from_json(row["state_delta_json"])
                state_deltas = _extract_state_delta(delta)
                session_state = {**session_state, **state_deltas["session"]}

        # Update session row
        state_json = self._escape_sql_string(json.dumps(session_state))
        self._spark.sql(f"""
            UPDATE {self._sessions_table}
            SET state_json = '{state_json}',
                update_time = TIMESTAMP '{now_iso}',
                rewind_to_event_id = NULL,
                version = version + 1
            WHERE app_name = '{escaped_app}'
              AND user_id = '{escaped_user}'
              AND session_id = '{escaped_session}'
        """)

        return await self.get_session(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
        )

    async def close(self) -> None:
        """Close the session service (no-op for Delta, included for interface compatibility)."""
        pass

    async def __aenter__(self) -> "DeltaSessionService":
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()

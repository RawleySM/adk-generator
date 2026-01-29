"""LocalSessionService for persisting ADK sessions to a local DuckDB database.

This module provides a custom SessionService implementation that stores session
data in a local DuckDB database, enabling durable state persistence for ADK agents
running in local development mode without requiring Databricks infrastructure.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
from google.adk.events.event import Event
from google.adk.sessions.base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from google.adk.sessions.session import Session
from google.adk.sessions.state import State
from typing_extensions import override

logger = logging.getLogger("adk_generator.sessions.local")


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


def _apply_state_delta(
    current_state: dict[str, Any],
    state_delta: dict[str, Any],
) -> dict[str, Any]:
    """Apply a state delta with deletion-on-None semantics.

    When a value in state_delta is None, the key is deleted from current_state.
    Otherwise, the value is set/updated.

    Args:
        current_state: The current state dictionary.
        state_delta: The delta to apply. None values indicate deletion.

    Returns:
        New state dictionary with delta applied.
    """
    new_state = copy.deepcopy(current_state)
    for key, value in state_delta.items():
        if value is None:
            # Delete the key if it exists
            new_state.pop(key, None)
        else:
            # Set/update the value
            new_state[key] = value
    return new_state


class LocalSessionService(BaseSessionService):
    """A session service that persists sessions to a local DuckDB database.

    This implementation stores sessions, events, app states, and user states in
    four tables within a DuckDB database. It supports:

    - Durable session persistence for local development
    - Same interface as DeltaSessionService for seamless switching
    - Idempotent event inserts via primary key
    - Session rewind with logical pointers (no physical deletes)
    - Soft deletes for session recovery

    Tables created:
    - sessions - Session metadata and state
    - events - Event history
    - app_states - Application-level state
    - user_states - User-level state

    Example:
        ```python
        service = LocalSessionService(
            db_path=".adk_local/adk.duckdb"
        )

        session = await service.create_session(
            app_name="my_agent",
            user_id="user123"
        )
        ```
    """

    def __init__(
        self,
        db_path: str = ".adk_local/adk.duckdb",
    ):
        """Initialize the LocalSessionService.

        Args:
            db_path: Path to the DuckDB database file.
                     Defaults to .adk_local/adk.duckdb
        """
        self._db_path = db_path

        # Ensure parent directory exists
        db_dir = Path(db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        # Create connection
        self._conn = duckdb.connect(db_path)

        # Table creation flag and lock
        self._tables_created = False
        self._table_creation_lock = asyncio.Lock()

        logger.info(
            "LocalSessionService initialized with db_path=%s",
            db_path
        )

    async def _ensure_tables_exist(self) -> None:
        """Create tables if they don't exist.

        Uses CREATE TABLE IF NOT EXISTS with appropriate schemas matching
        the DeltaSessionService for compatibility.
        """
        if self._tables_created:
            return

        async with self._table_creation_lock:
            if self._tables_created:
                return

            # Create sessions table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    app_name VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    session_id VARCHAR NOT NULL,
                    state_json VARCHAR,
                    created_time TIMESTAMP NOT NULL,
                    update_time TIMESTAMP NOT NULL,
                    version BIGINT NOT NULL,
                    is_deleted BOOLEAN NOT NULL,
                    deleted_time TIMESTAMP,
                    rewind_to_event_id VARCHAR,
                    last_write_nonce VARCHAR,
                    PRIMARY KEY (app_name, user_id, session_id)
                )
            """)

            # Create events table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    app_name VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    session_id VARCHAR NOT NULL,
                    event_id VARCHAR NOT NULL,
                    invocation_id VARCHAR NOT NULL,
                    author VARCHAR NOT NULL,
                    event_timestamp TIMESTAMP NOT NULL,
                    sequence_num BIGINT NOT NULL,
                    event_data_json VARCHAR NOT NULL,
                    state_delta_json VARCHAR,
                    has_state_delta BOOLEAN NOT NULL,
                    created_time TIMESTAMP NOT NULL,
                    is_after_rewind BOOLEAN NOT NULL,
                    PRIMARY KEY (app_name, user_id, session_id, event_id)
                )
            """)

            # Create app_states table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS app_states (
                    app_name VARCHAR NOT NULL PRIMARY KEY,
                    state_json VARCHAR,
                    update_time TIMESTAMP NOT NULL,
                    version BIGINT NOT NULL
                )
            """)

            # Create user_states table
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS user_states (
                    app_name VARCHAR NOT NULL,
                    user_id VARCHAR NOT NULL,
                    state_json VARCHAR,
                    update_time TIMESTAMP NOT NULL,
                    version BIGINT NOT NULL,
                    PRIMARY KEY (app_name, user_id)
                )
            """)

            self._tables_created = True
            logger.info("Local DuckDB tables created/verified successfully")

    def _to_json(self, obj: Any) -> str:
        """Serialize an object to JSON string."""
        if obj is None:
            return "{}"
        if hasattr(obj, "model_dump"):
            return json.dumps(obj.model_dump(mode="json", by_alias=True))
        return json.dumps(obj)

    def _from_json(self, json_str: Optional[str], recover_on_error: bool = True) -> dict[str, Any]:
        """Deserialize a JSON string to a dictionary.

        Args:
            json_str: JSON string to deserialize
            recover_on_error: If True, return empty dict on JSON errors instead of raising.
                This allows graceful recovery from corrupted state.
        """
        if not json_str:
            return {}
        try:
            return json.loads(json_str, strict=False)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}. String: {json_str[:100]}...")
            if recover_on_error:
                logger.warning("Recovering from corrupted JSON by returning empty state")
                return {}
            raise

    async def _get_app_state(self, app_name: str) -> dict[str, Any]:
        """Get the app state for the given app_name."""
        result = self._conn.execute(
            "SELECT state_json FROM app_states WHERE app_name = ?",
            [app_name]
        ).fetchone()

        if result:
            return self._from_json(result[0])
        return {}

    async def _get_user_state(self, app_name: str, user_id: str) -> dict[str, Any]:
        """Get the user state for the given app_name and user_id."""
        result = self._conn.execute(
            "SELECT state_json FROM user_states WHERE app_name = ? AND user_id = ?",
            [app_name, user_id]
        ).fetchone()

        if result:
            return self._from_json(result[0])
        return {}

    async def _upsert_app_state(
        self,
        app_name: str,
        state_delta: dict[str, Any],
    ) -> None:
        """Upsert app state with the given delta.

        Uses deletion-on-None semantics: if a value in state_delta is None,
        the key is removed from the persisted state.
        """
        if not state_delta:
            return

        now = datetime.now(timezone.utc)

        # Get current state and apply delta with deletion semantics
        current_state = await self._get_app_state(app_name)
        new_state = _apply_state_delta(current_state, state_delta)
        state_json = json.dumps(new_state)

        # Use INSERT OR REPLACE for upsert semantics in DuckDB
        self._conn.execute("""
            INSERT INTO app_states (app_name, state_json, update_time, version)
            VALUES (?, ?, ?, 1)
            ON CONFLICT (app_name) DO UPDATE SET
                state_json = EXCLUDED.state_json,
                update_time = EXCLUDED.update_time,
                version = app_states.version + 1
        """, [app_name, state_json, now])

    async def _upsert_user_state(
        self,
        app_name: str,
        user_id: str,
        state_delta: dict[str, Any],
    ) -> None:
        """Upsert user state with the given delta.

        Uses deletion-on-None semantics: if a value in state_delta is None,
        the key is removed from the persisted state.
        """
        if not state_delta:
            return

        now = datetime.now(timezone.utc)

        # Get current state and apply delta with deletion semantics
        current_state = await self._get_user_state(app_name, user_id)
        new_state = _apply_state_delta(current_state, state_delta)
        state_json = json.dumps(new_state)

        # Use INSERT OR REPLACE for upsert semantics in DuckDB
        self._conn.execute("""
            INSERT INTO user_states (app_name, user_id, state_json, update_time, version)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT (app_name, user_id) DO UPDATE SET
                state_json = EXCLUDED.state_json,
                update_time = EXCLUDED.update_time,
                version = user_states.version + 1
        """, [app_name, user_id, state_json, now])

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

        # Check if session already exists
        existing = self._conn.execute("""
            SELECT 1 FROM sessions
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            AND is_deleted = FALSE
        """, [app_name, user_id, session_id]).fetchone()

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
        session_state_json = json.dumps(session_state)

        self._conn.execute("""
            INSERT INTO sessions
            (app_name, user_id, session_id, state_json, created_time, update_time, version, is_deleted)
            VALUES (?, ?, ?, ?, ?, ?, 1, FALSE)
        """, [app_name, user_id, session_id, session_state_json, now, now])

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

        # Get session row
        session_result = self._conn.execute("""
            SELECT session_id, state_json, update_time, version, rewind_to_event_id
            FROM sessions
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            AND is_deleted = FALSE
        """, [app_name, user_id, session_id]).fetchone()

        if not session_result:
            return None

        session_state = self._from_json(session_result[1])
        update_time = session_result[2]

        # Build events query with optional filters
        params: list[Any] = [app_name, user_id, session_id]
        events_query = """
            SELECT event_id, invocation_id, author, event_timestamp, sequence_num,
                   event_data_json, state_delta_json, has_state_delta, is_after_rewind
            FROM events
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            AND is_after_rewind = FALSE
        """

        if config and config.after_timestamp:
            after_dt = datetime.fromtimestamp(config.after_timestamp, timezone.utc)
            events_query += " AND event_timestamp >= ?"
            params.append(after_dt)

        if config and config.num_recent_events:
            # For recent events, order desc first, limit, then reorder
            events_query = f"""
                SELECT * FROM (
                    SELECT event_id, invocation_id, author, event_timestamp, sequence_num,
                           event_data_json, state_delta_json, has_state_delta, is_after_rewind,
                           created_time
                    FROM events
                    WHERE app_name = ? AND user_id = ? AND session_id = ?
                    AND is_after_rewind = FALSE
            """
            params = [app_name, user_id, session_id]

            if config.after_timestamp:
                after_dt = datetime.fromtimestamp(config.after_timestamp, timezone.utc)
                events_query += " AND event_timestamp >= ?"
                params.append(after_dt)

            events_query += f"""
                    ORDER BY sequence_num DESC, created_time DESC, event_id DESC
                    LIMIT {config.num_recent_events}
                ) subq ORDER BY sequence_num ASC, created_time ASC, event_id ASC
            """
        else:
            events_query += " ORDER BY sequence_num ASC, created_time ASC, event_id ASC"

        events_result = self._conn.execute(events_query, params).fetchall()

        # Reconstruct Event objects
        events = []
        for row in events_result:
            event_data = self._from_json(row[5])  # event_data_json
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

        params: list[Any] = [app_name]
        query = """
            SELECT session_id, user_id, state_json, update_time
            FROM sessions
            WHERE app_name = ? AND is_deleted = FALSE
        """

        if user_id is not None:
            query += " AND user_id = ?"
            params.append(user_id)

        query += " ORDER BY update_time DESC"

        result = self._conn.execute(query, params).fetchall()

        # Fetch app state
        app_state = await self._get_app_state(app_name)

        # Build user states map
        user_states_map: dict[str, dict[str, Any]] = {}
        if user_id is not None:
            user_states_map[user_id] = await self._get_user_state(app_name, user_id)
        else:
            # Fetch all user states for this app
            user_states_result = self._conn.execute(
                "SELECT user_id, state_json FROM user_states WHERE app_name = ?",
                [app_name]
            ).fetchall()
            for row in user_states_result:
                user_states_map[row[0]] = self._from_json(row[1])

        sessions = []
        for row in result:
            sess_session_id = row[0]
            sess_user_id = row[1]
            session_state = self._from_json(row[2])
            sess_update_time = row[3]

            user_state = user_states_map.get(sess_user_id, {})
            merged_state = _merge_state(app_state, user_state, session_state)

            if isinstance(sess_update_time, datetime):
                last_update_time = sess_update_time.timestamp()
            else:
                last_update_time = float(sess_update_time)

            sessions.append(Session(
                id=sess_session_id,
                app_name=app_name,
                user_id=sess_user_id,
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

        now = datetime.now(timezone.utc)

        self._conn.execute("""
            UPDATE sessions
            SET is_deleted = TRUE,
                deleted_time = ?,
                update_time = ?
            WHERE app_name = ? AND user_id = ? AND session_id = ?
        """, [now, now, app_name, user_id, session_id])

    @override
    async def append_event(self, session: Session, event: Event) -> Event:
        """Append an event to a session.

        This method:
        1. Skips partial events (streaming chunks)
        2. Validates the session is not stale
        3. Inserts the event row idempotently (duplicate event_id is safe via primary key)
        4. Applies state_delta to app/user/session states
        5. Updates the session row

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

        return await self._append_event_internal(session, event)

    async def _append_event_internal(self, session: Session, event: Event) -> Event:
        """Internal method to append event."""
        # Get current session state and check for staleness
        session_result = self._conn.execute("""
            SELECT state_json, update_time, version
            FROM sessions
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            AND is_deleted = FALSE
        """, [session.app_name, session.user_id, session.id]).fetchone()

        if not session_result:
            raise ValueError(f"Session {session.id} not found or deleted")

        current_session_state = self._from_json(session_result[0])
        stored_update_time = session_result[1]
        stored_version = session_result[2]

        # Convert to timestamp for comparison
        if isinstance(stored_update_time, datetime):
            stored_timestamp = stored_update_time.timestamp()
        else:
            stored_timestamp = float(stored_update_time)

        # Check for stale session
        if stored_timestamp > session.last_update_time:
            raise ValueError(
                f"Session is stale: stored update_time {stored_timestamp} > "
                f"session.last_update_time {session.last_update_time}"
            )

        # Extract state deltas from event
        state_delta_json: Optional[str] = None
        has_state_delta = False
        app_state_delta: dict[str, Any] = {}
        user_state_delta: dict[str, Any] = {}

        if event.actions and event.actions.state_delta:
            state_deltas = _extract_state_delta(event.actions.state_delta)
            app_state_delta = state_deltas["app"]
            user_state_delta = state_deltas["user"]
            session_state_delta = state_deltas["session"]

            # Update session state with deletion-on-None semantics
            if session_state_delta:
                current_session_state = _apply_state_delta(
                    current_session_state, session_state_delta
                )

            state_delta_json = json.dumps(event.actions.state_delta)
            has_state_delta = True

        now = datetime.now(timezone.utc)

        # Get next sequence number
        seq_result = self._conn.execute("""
            SELECT COALESCE(MAX(sequence_num), 0) + 1
            FROM events
            WHERE app_name = ? AND user_id = ? AND session_id = ?
        """, [session.app_name, session.user_id, session.id]).fetchone()
        next_seq = seq_result[0] if seq_result else 1

        # Insert event (idempotent via primary key - ON CONFLICT DO NOTHING)
        event_data_json = self._to_json(event)
        event_timestamp = datetime.fromtimestamp(event.timestamp, timezone.utc)

        self._conn.execute("""
            INSERT INTO events (
                app_name, user_id, session_id, event_id, invocation_id, author,
                event_timestamp, sequence_num, event_data_json, state_delta_json,
                has_state_delta, created_time, is_after_rewind
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
            ON CONFLICT (app_name, user_id, session_id, event_id) DO NOTHING
        """, [
            session.app_name, session.user_id, session.id,
            event.id, event.invocation_id, event.author,
            event_timestamp, next_seq, event_data_json, state_delta_json,
            has_state_delta, now
        ])

        # Update session row
        new_state_json = json.dumps(current_session_state)
        self._conn.execute("""
            UPDATE sessions
            SET state_json = ?,
                update_time = ?,
                version = version + 1
            WHERE app_name = ? AND user_id = ? AND session_id = ?
        """, [new_state_json, now, session.app_name, session.user_id, session.id])

        # Apply app/user state deltas
        if app_state_delta:
            await self._upsert_app_state(session.app_name, app_state_delta)
        if user_state_delta:
            await self._upsert_user_state(session.app_name, session.user_id, user_state_delta)

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

        # Get target event sequence number
        target_result = self._conn.execute("""
            SELECT sequence_num FROM events
            WHERE app_name = ? AND user_id = ? AND session_id = ? AND event_id = ?
        """, [app_name, user_id, session_id, target_event_id]).fetchone()

        if not target_result:
            raise ValueError(f"Target event {target_event_id} not found")

        target_seq = target_result[0]

        # Mark events after target as rewound
        now = datetime.now(timezone.utc)
        self._conn.execute("""
            UPDATE events
            SET is_after_rewind = TRUE
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            AND sequence_num > ?
        """, [app_name, user_id, session_id, target_seq])

        # Reconstruct state by replaying events up to target
        events_result = self._conn.execute("""
            SELECT event_data_json, state_delta_json, has_state_delta
            FROM events
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            AND sequence_num <= ?
            ORDER BY sequence_num ASC, created_time ASC, event_id ASC
        """, [app_name, user_id, session_id, target_seq]).fetchall()

        # Rebuild session state from events with deletion-on-None semantics
        session_state: dict[str, Any] = {}
        for row in events_result:
            if row[2] and row[1]:  # has_state_delta and state_delta_json
                delta = self._from_json(row[1])
                state_deltas = _extract_state_delta(delta)
                session_state = _apply_state_delta(session_state, state_deltas["session"])

        # Update session row
        state_json = json.dumps(session_state)
        self._conn.execute("""
            UPDATE sessions
            SET state_json = ?,
                update_time = ?,
                rewind_to_event_id = ?,
                version = version + 1
            WHERE app_name = ? AND user_id = ? AND session_id = ?
        """, [state_json, now, target_event_id, app_name, user_id, session_id])

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

        now = datetime.now(timezone.utc)

        # Clear is_after_rewind flags
        self._conn.execute("""
            UPDATE events
            SET is_after_rewind = FALSE
            WHERE app_name = ? AND user_id = ? AND session_id = ?
        """, [app_name, user_id, session_id])

        # Rebuild full state from all events
        events_result = self._conn.execute("""
            SELECT state_delta_json, has_state_delta
            FROM events
            WHERE app_name = ? AND user_id = ? AND session_id = ?
            ORDER BY sequence_num ASC, created_time ASC, event_id ASC
        """, [app_name, user_id, session_id]).fetchall()

        # Rebuild state with deletion-on-None semantics
        session_state: dict[str, Any] = {}
        for row in events_result:
            if row[1] and row[0]:  # has_state_delta and state_delta_json
                delta = self._from_json(row[0])
                state_deltas = _extract_state_delta(delta)
                session_state = _apply_state_delta(session_state, state_deltas["session"])

        # Update session row
        state_json = json.dumps(session_state)
        self._conn.execute("""
            UPDATE sessions
            SET state_json = ?,
                update_time = ?,
                rewind_to_event_id = NULL,
                version = version + 1
            WHERE app_name = ? AND user_id = ? AND session_id = ?
        """, [state_json, now, app_name, user_id, session_id])

        return await self.get_session(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
        )

    async def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("LocalSessionService connection closed")

    async def __aenter__(self) -> "LocalSessionService":
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager."""
        await self.close()

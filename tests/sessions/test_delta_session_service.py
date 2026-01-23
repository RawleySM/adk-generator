"""Unit tests for DeltaSessionService.

These tests use a mock SparkSession to verify the service logic
without requiring a Databricks connection.
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, AsyncMock
from typing import Any

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.session import Session
from google.adk.sessions.state import State

from adk_generator.sessions.delta_session_service import (
    DeltaSessionService,
    _extract_state_delta,
    _merge_state,
)


class MockRow:
    """Mock Spark Row object."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def asDict(self) -> dict[str, Any]:
        return self._data


class MockDataFrame:
    """Mock Spark DataFrame."""

    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = [MockRow(r) for r in rows]

    def collect(self) -> list[MockRow]:
        return self._rows


class MockSparkSession:
    """Mock SparkSession for unit testing."""

    def __init__(self):
        self._executed_sql: list[str] = []
        self._mock_results: dict[str, list[dict[str, Any]]] = {}
        self._tables_data: dict[str, list[dict[str, Any]]] = {
            "sessions": [],
            "events": [],
            "app_states": [],
            "user_states": [],
        }

    def set_result(self, pattern: str, rows: list[dict[str, Any]]) -> None:
        """Set a mock result for SQL containing the pattern."""
        self._mock_results[pattern] = rows

    def sql(self, query: str) -> MockDataFrame:
        """Execute a mock SQL query."""
        self._executed_sql.append(query)

        # Return mock results based on query patterns
        for pattern, rows in self._mock_results.items():
            if pattern.lower() in query.lower():
                return MockDataFrame(rows)

        # Default empty result
        return MockDataFrame([])

    def get_executed_sql(self) -> list[str]:
        """Get all executed SQL statements."""
        return self._executed_sql

    def clear_executed_sql(self) -> None:
        """Clear the executed SQL log."""
        self._executed_sql = []


class TestExtractStateDelta:
    """Tests for _extract_state_delta function."""

    def test_empty_state(self):
        result = _extract_state_delta(None)
        assert result == {"app": {}, "user": {}, "session": {}}

        result = _extract_state_delta({})
        assert result == {"app": {}, "user": {}, "session": {}}

    def test_app_prefix(self):
        state = {"app:setting": "value", "app:config": 123}
        result = _extract_state_delta(state)
        assert result["app"] == {"setting": "value", "config": 123}
        assert result["user"] == {}
        assert result["session"] == {}

    def test_user_prefix(self):
        state = {"user:preference": "dark", "user:lang": "en"}
        result = _extract_state_delta(state)
        assert result["app"] == {}
        assert result["user"] == {"preference": "dark", "lang": "en"}
        assert result["session"] == {}

    def test_temp_prefix_ignored(self):
        state = {"temp:cache": "data", "temp:buffer": [1, 2, 3]}
        result = _extract_state_delta(state)
        assert result["app"] == {}
        assert result["user"] == {}
        assert result["session"] == {}

    def test_session_keys(self):
        state = {"counter": 1, "history": ["a", "b"]}
        result = _extract_state_delta(state)
        assert result["app"] == {}
        assert result["user"] == {}
        assert result["session"] == {"counter": 1, "history": ["a", "b"]}

    def test_mixed_prefixes(self):
        state = {
            "app:global": True,
            "user:name": "Alice",
            "temp:scratch": "ignore",
            "local": "session_data",
        }
        result = _extract_state_delta(state)
        assert result["app"] == {"global": True}
        assert result["user"] == {"name": "Alice"}
        assert result["session"] == {"local": "session_data"}


class TestMergeState:
    """Tests for _merge_state function."""

    def test_empty_states(self):
        result = _merge_state({}, {}, {})
        assert result == {}

    def test_app_state_prefixed(self):
        result = _merge_state({"setting": "value"}, {}, {})
        assert result == {"app:setting": "value"}

    def test_user_state_prefixed(self):
        result = _merge_state({}, {"pref": "dark"}, {})
        assert result == {"user:pref": "dark"}

    def test_session_state_unchanged(self):
        result = _merge_state({}, {}, {"counter": 5})
        assert result == {"counter": 5}

    def test_all_states_merged(self):
        result = _merge_state(
            {"global": True},
            {"name": "Bob"},
            {"local": "data", "count": 10},
        )
        assert result == {
            "app:global": True,
            "user:name": "Bob",
            "local": "data",
            "count": 10,
        }


class TestDeltaSessionServiceInit:
    """Tests for DeltaSessionService initialization."""

    def test_init_default_schema(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)

        assert service._catalog == "silo_dev_rs"
        assert service._schema == "adk"
        assert service._sessions_table == "silo_dev_rs.adk.sessions"
        assert service._events_table == "silo_dev_rs.adk.events"

    def test_init_custom_schema(self):
        spark = MockSparkSession()
        service = DeltaSessionService(
            spark=spark,
            catalog="my_catalog",
            schema="my_schema",
        )

        assert service._catalog == "my_catalog"
        assert service._schema == "my_schema"
        assert service._sessions_table == "my_catalog.my_schema.sessions"


class TestDeltaSessionServiceHelpers:
    """Tests for DeltaSessionService helper methods."""

    def test_escape_sql_string(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)

        assert service._escape_sql_string("hello") == "hello"
        assert service._escape_sql_string("it's") == "it''s"
        assert service._escape_sql_string("a'b'c") == "a''b''c"
        assert service._escape_sql_string(None) == ""

    def test_to_json(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)

        assert service._to_json(None) == "{}"
        assert service._to_json({"a": 1}) == '{"a": 1}'
        assert service._to_json([1, 2, 3]) == "[1, 2, 3]"

    def test_from_json(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)

        assert service._from_json(None) == {}
        assert service._from_json("") == {}
        assert service._from_json("{}") == {}
        assert service._from_json('{"a": 1}') == {"a": 1}


@pytest.mark.asyncio
class TestDeltaSessionServiceCreateSession:
    """Tests for create_session method."""

    async def test_create_session_generates_id(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True  # Skip table creation

        session = await service.create_session(
            app_name="test_app",
            user_id="user1",
        )

        assert session.app_name == "test_app"
        assert session.user_id == "user1"
        assert session.id is not None
        assert len(session.id) == 36  # UUID format
        assert session.events == []

    async def test_create_session_with_provided_id(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = await service.create_session(
            app_name="test_app",
            user_id="user1",
            session_id="custom-id-123",
        )

        assert session.id == "custom-id-123"

    async def test_create_session_with_initial_state(self):
        spark = MockSparkSession()

        # Mock app and user state reads to return the state we upserted
        spark.set_result("FROM silo_dev_rs.adk.app_states", [
            {"state_json": '{"global": true}'}
        ])
        spark.set_result("FROM silo_dev_rs.adk.user_states", [
            {"state_json": '{"name": "Alice"}'}
        ])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = await service.create_session(
            app_name="test_app",
            user_id="user1",
            state={
                "app:global": True,
                "user:name": "Alice",
                "counter": 0,
            },
        )

        # State should be merged with prefixes
        assert "app:global" in session.state
        assert "user:name" in session.state
        assert "counter" in session.state

    async def test_create_session_duplicate_raises(self):
        spark = MockSparkSession()
        spark.set_result("SELECT 1", [{"1": 1}])  # Simulate existing session

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        with pytest.raises(ValueError, match="already exists"):
            await service.create_session(
                app_name="test_app",
                user_id="user1",
                session_id="existing-id",
            )


@pytest.mark.asyncio
class TestDeltaSessionServiceGetSession:
    """Tests for get_session method."""

    async def test_get_session_not_found(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = await service.get_session(
            app_name="test_app",
            user_id="user1",
            session_id="nonexistent",
        )

        assert session is None

    async def test_get_session_found(self):
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session result
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": '{"counter": 5}',
            "update_time": now,
            "version": 1,
            "rewind_to_event_id": None,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = await service.get_session(
            app_name="test_app",
            user_id="user1",
            session_id="session-123",
        )

        assert session is not None
        assert session.id == "session-123"
        assert session.app_name == "test_app"
        assert session.user_id == "user1"


@pytest.mark.asyncio
class TestDeltaSessionServiceListSessions:
    """Tests for list_sessions method."""

    async def test_list_sessions_empty(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        response = await service.list_sessions(app_name="test_app")

        assert response.sessions == []

    async def test_list_sessions_with_results(self):
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        spark.set_result("FROM silo_dev_rs.adk.sessions", [
            {
                "session_id": "session-1",
                "user_id": "user1",
                "state_json": "{}",
                "update_time": now,
            },
            {
                "session_id": "session-2",
                "user_id": "user1",
                "state_json": "{}",
                "update_time": now,
            },
        ])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        response = await service.list_sessions(app_name="test_app")

        assert len(response.sessions) == 2


@pytest.mark.asyncio
class TestDeltaSessionServiceDeleteSession:
    """Tests for delete_session method."""

    async def test_delete_session_executes_update(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        await service.delete_session(
            app_name="test_app",
            user_id="user1",
            session_id="session-123",
        )

        # Verify UPDATE was called with soft delete
        sql_executed = spark.get_executed_sql()
        assert any("UPDATE" in sql and "is_deleted = TRUE" in sql for sql in sql_executed)


@pytest.mark.asyncio
class TestDeltaSessionServiceAppendEvent:
    """Tests for append_event method."""

    async def test_append_event_skips_partial(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
        )
        event = Event(
            author="agent",
            partial=True,
        )

        result = await service.append_event(session, event)

        assert result.partial is True
        # No SQL should be executed for partial events
        assert len(spark.get_executed_sql()) == 0

    async def test_append_event_trims_temp_state(self):
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session lookup
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=now.timestamp(),
        )
        event = Event(
            author="agent",
            actions=EventActions(
                state_delta={
                    "temp:cache": "should_be_removed",
                    "persistent": "should_stay",
                }
            ),
        )

        result = await service.append_event(session, event)

        # temp: keys should be removed
        assert "temp:cache" not in result.actions.state_delta
        assert "persistent" in result.actions.state_delta

    async def test_append_event_stale_session_raises(self):
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)
        earlier = datetime.fromtimestamp(now.timestamp() - 100, timezone.utc)

        # Mock session with newer update_time than the session object
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=earlier.timestamp(),  # Old timestamp
        )
        event = Event(author="agent")

        with pytest.raises(ValueError, match="stale"):
            await service.append_event(session, event)


@pytest.mark.asyncio
class TestDeltaSessionServiceRewind:
    """Tests for rewind_session and clear_rewind methods."""

    async def test_rewind_session_marks_events(self):
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock target event lookup to return sequence number
        event_data = {
            "id": "event-5",
            "author": "test_agent",
            "invocation_id": "inv-1",
            "timestamp": now.timestamp(),
        }
        spark.set_result("FROM silo_dev_rs.adk.events", [
            {
                "sequence_num": 5,
                "event_id": "event-5",
                "invocation_id": "inv-1",
                "author": "test_agent",
                "event_timestamp": now,
                "event_data_json": json.dumps(event_data),
                "state_delta_json": None,
                "has_state_delta": False,
                "is_after_rewind": False,
            }
        ])

        # Mock session for get_session
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
            "rewind_to_event_id": "event-5",
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        await service.rewind_session(
            app_name="test_app",
            user_id="user1",
            session_id="session-123",
            target_event_id="event-5",
        )

        # Verify UPDATE was called to mark events
        sql_executed = spark.get_executed_sql()
        assert any("is_after_rewind = TRUE" in sql for sql in sql_executed)

    async def test_rewind_nonexistent_event_raises(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        with pytest.raises(ValueError, match="not found"):
            await service.rewind_session(
                app_name="test_app",
                user_id="user1",
                session_id="session-123",
                target_event_id="nonexistent",
            )

    async def test_clear_rewind_restores_events(self):
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session for get_session
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
            "rewind_to_event_id": None,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        await service.clear_rewind(
            app_name="test_app",
            user_id="user1",
            session_id="session-123",
        )

        # Verify UPDATE was called to clear rewind flags
        sql_executed = spark.get_executed_sql()
        assert any("is_after_rewind = FALSE" in sql for sql in sql_executed)


@pytest.mark.asyncio
class TestDeltaSessionServiceTableCreation:
    """Tests for table creation logic."""

    async def test_ensure_tables_creates_all_tables(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)

        await service._ensure_tables_exist()

        sql_executed = spark.get_executed_sql()

        # Verify all four tables are created
        assert any("CREATE TABLE IF NOT EXISTS" in sql and "sessions" in sql for sql in sql_executed)
        assert any("CREATE TABLE IF NOT EXISTS" in sql and "events" in sql for sql in sql_executed)
        assert any("CREATE TABLE IF NOT EXISTS" in sql and "app_states" in sql for sql in sql_executed)
        assert any("CREATE TABLE IF NOT EXISTS" in sql and "user_states" in sql for sql in sql_executed)

    async def test_ensure_tables_only_creates_once(self):
        spark = MockSparkSession()
        service = DeltaSessionService(spark=spark)

        await service._ensure_tables_exist()
        spark.clear_executed_sql()

        await service._ensure_tables_exist()

        # No SQL should be executed on second call
        assert len(spark.get_executed_sql()) == 0


@pytest.mark.asyncio
class TestDeltaSessionServiceContextManager:
    """Tests for async context manager support."""

    async def test_context_manager(self):
        spark = MockSparkSession()

        async with DeltaSessionService(spark=spark) as service:
            assert service is not None
            assert isinstance(service, DeltaSessionService)


@pytest.mark.asyncio
class TestDeltaSessionServiceOCC:
    """Tests for optimistic concurrency control enforcement."""

    async def test_occ_version_conflict_raises(self):
        """Verify that version mismatch after UPDATE raises ValueError."""
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session lookup - returns version 1
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        # Mock version verification to return version 1 (not incremented)
        # This simulates another process having updated the row
        spark.set_result("SELECT version FROM", [{
            "version": 1,  # Should be 2 if update succeeded
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=now.timestamp(),
        )
        event = Event(author="agent")

        with pytest.raises(ValueError, match="Version conflict"):
            await service.append_event(session, event)

    async def test_occ_success_when_version_increments(self):
        """Verify successful append when version increments correctly."""
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session lookup - returns version 1
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        # Mock version verification to return version 2 (correctly incremented)
        spark.set_result("SELECT version FROM", [{
            "version": 2,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=now.timestamp(),
        )
        event = Event(author="agent")

        # Should not raise
        result = await service.append_event(session, event)
        assert result.author == "agent"


@pytest.mark.asyncio
class TestDeltaSessionServiceSequenceOrdering:
    """Tests for event sequence ordering."""

    async def test_events_ordered_by_sequence_and_created_time(self):
        """Verify get_session orders events by (sequence_num, created_time)."""
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session result
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
            "rewind_to_event_id": None,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        await service.get_session(
            app_name="test_app",
            user_id="user1",
            session_id="session-123",
        )

        # Verify the ORDER BY clause includes both sequence_num and created_time
        sql_executed = spark.get_executed_sql()
        events_query = [sql for sql in sql_executed if "FROM" in sql and "events" in sql]
        assert len(events_query) > 0
        assert any("ORDER BY sequence_num ASC, created_time ASC" in sql for sql in events_query)

    async def test_sequence_num_computed_in_merge(self):
        """Verify sequence_num is computed atomically inside MERGE."""
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session lookup
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        # Mock version verification
        spark.set_result("SELECT version FROM", [{
            "version": 2,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=now.timestamp(),
        )
        event = Event(author="agent")

        await service.append_event(session, event)

        # Verify MERGE contains inline sequence calculation
        sql_executed = spark.get_executed_sql()
        merge_sql = [sql for sql in sql_executed if "MERGE INTO" in sql and "events" in sql]
        assert len(merge_sql) > 0
        assert any("COALESCE" in sql and "MAX(sequence_num)" in sql for sql in merge_sql)


@pytest.mark.asyncio
class TestDeltaSessionServiceAtomicity:
    """Tests for atomicity of state updates."""

    async def test_app_user_state_not_updated_on_version_conflict(self):
        """Verify app/user state deltas are NOT applied if OCC fails."""
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session lookup
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        # Mock version verification to fail (version not incremented)
        spark.set_result("SELECT version FROM", [{
            "version": 1,  # Should be 2 if update succeeded
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=now.timestamp(),
        )
        event = Event(
            author="agent",
            actions=EventActions(
                state_delta={
                    "app:global_setting": "new_value",
                    "user:preference": "dark",
                    "session_counter": 1,
                }
            ),
        )

        with pytest.raises(ValueError, match="Version conflict"):
            await service.append_event(session, event)

        # Verify NO app_states or user_states MERGE was executed
        sql_executed = spark.get_executed_sql()
        app_state_merges = [sql for sql in sql_executed if "MERGE INTO" in sql and "app_states" in sql]
        user_state_merges = [sql for sql in sql_executed if "MERGE INTO" in sql and "user_states" in sql]

        assert len(app_state_merges) == 0, "App state should not be updated on OCC failure"
        assert len(user_state_merges) == 0, "User state should not be updated on OCC failure"

    async def test_app_user_state_updated_after_occ_success(self):
        """Verify app/user state deltas ARE applied after OCC succeeds."""
        spark = MockSparkSession()
        now = datetime.now(timezone.utc)

        # Mock session lookup
        spark.set_result("FROM silo_dev_rs.adk.sessions", [{
            "session_id": "session-123",
            "state_json": "{}",
            "update_time": now,
            "version": 1,
        }])

        # Mock version verification to succeed
        spark.set_result("SELECT version FROM", [{
            "version": 2,
        }])

        service = DeltaSessionService(spark=spark)
        service._tables_created = True

        session = Session(
            id="session-123",
            app_name="test_app",
            user_id="user1",
            last_update_time=now.timestamp(),
        )
        event = Event(
            author="agent",
            actions=EventActions(
                state_delta={
                    "app:global_setting": "new_value",
                    "user:preference": "dark",
                    "session_counter": 1,
                }
            ),
        )

        await service.append_event(session, event)

        # Verify app_states and user_states MERGE were executed
        sql_executed = spark.get_executed_sql()
        app_state_merges = [sql for sql in sql_executed if "MERGE INTO" in sql and "app_states" in sql]
        user_state_merges = [sql for sql in sql_executed if "MERGE INTO" in sql and "user_states" in sql]

        assert len(app_state_merges) > 0, "App state should be updated on OCC success"
        assert len(user_state_merges) > 0, "User state should be updated on OCC success"

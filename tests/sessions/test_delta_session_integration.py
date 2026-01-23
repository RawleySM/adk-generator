"""Integration tests for DeltaSessionService against Databricks Unity Catalog.

These tests require:
1. A Databricks connection via SparkSession
2. Write access to silo_dev_rs.adk_test schema

To run these tests:
    uv run pytest tests/sessions/test_delta_session_integration.py -v -m databricks

Set DATABRICKS_INTEGRATION_TESTS=1 environment variable to enable these tests.
"""

import asyncio
import os
import uuid
import pytest
from datetime import datetime, timezone
from typing import Generator

from google.adk.events.event import Event
from google.adk.events.event_actions import EventActions
from google.adk.sessions.session import Session
from google.adk.sessions.base_session_service import GetSessionConfig

# Skip all tests in this module if not running integration tests
pytestmark = pytest.mark.skipif(
    os.environ.get("DATABRICKS_INTEGRATION_TESTS") != "1",
    reason="Set DATABRICKS_INTEGRATION_TESTS=1 to run Databricks integration tests"
)

# Try to import PySpark - tests will be skipped if not available
try:
    from pyspark.sql import SparkSession
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = None

if HAS_PYSPARK:
    from adk_generator.sessions.delta_session_service import DeltaSessionService


@pytest.fixture(scope="module")
def spark() -> Generator["SparkSession", None, None]:
    """Create a SparkSession connected to Databricks.

    This fixture creates a SparkSession that connects to Databricks
    using the configured profile.
    """
    if not HAS_PYSPARK:
        pytest.skip("PySpark not available")

    # Create SparkSession - assumes Databricks Connect or cluster environment
    try:
        spark = SparkSession.builder \
            .appName("DeltaSessionService-Integration-Tests") \
            .getOrCreate()
        yield spark
    finally:
        # Don't stop the session as it may be shared
        pass


@pytest.fixture(scope="module")
def test_schema() -> str:
    """Use a test schema to avoid polluting production data."""
    return "adk_test"


@pytest.fixture(scope="module")
def service(spark: "SparkSession", test_schema: str) -> Generator[DeltaSessionService, None, None]:
    """Create a DeltaSessionService instance for testing."""
    service = DeltaSessionService(
        spark=spark,
        catalog="silo_dev_rs",
        schema=test_schema,
    )
    yield service


@pytest.fixture
def unique_app_name() -> str:
    """Generate a unique app name for test isolation."""
    return f"test_app_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_user_id() -> str:
    """Generate a unique user ID for test isolation."""
    return f"user_{uuid.uuid4().hex[:8]}"


@pytest.mark.databricks
@pytest.mark.asyncio
class TestDeltaSessionIntegration:
    """Integration tests for DeltaSessionService."""

    async def test_full_session_lifecycle(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test complete session lifecycle: create, get, list, delete."""
        # Create session
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={"counter": 0},
        )

        assert session.id is not None
        assert session.app_name == unique_app_name
        assert session.user_id == unique_user_id
        assert session.state.get("counter") == 0

        # Get session
        retrieved = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        assert retrieved is not None
        assert retrieved.id == session.id
        assert retrieved.state.get("counter") == 0

        # List sessions
        response = await service.list_sessions(
            app_name=unique_app_name,
            user_id=unique_user_id,
        )

        assert len(response.sessions) >= 1
        session_ids = [s.id for s in response.sessions]
        assert session.id in session_ids

        # Delete session
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        # Verify deletion
        deleted = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )
        assert deleted is None

    async def test_session_with_state_prefixes(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test sessions with app:, user:, and session-level state."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={
                "app:global_setting": "enabled",
                "user:preference": "dark_mode",
                "local_counter": 0,
            },
        )

        assert "app:global_setting" in session.state
        assert "user:preference" in session.state
        assert "local_counter" in session.state

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

    async def test_append_event(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test appending events to a session."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={"counter": 0},
        )

        # Create and append event with state delta
        event = Event(
            invocation_id=str(uuid.uuid4()),
            author="test_agent",
            actions=EventActions(
                state_delta={"counter": 1}
            ),
        )

        result = await service.append_event(session, event)

        assert result.id == event.id

        # Retrieve session and verify event is stored
        retrieved = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        assert len(retrieved.events) == 1
        assert retrieved.events[0].id == event.id
        assert retrieved.state.get("counter") == 1

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

    async def test_append_multiple_events(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test appending multiple events with accumulating state."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={"counter": 0},
        )

        # Append 5 events, each incrementing the counter
        for i in range(1, 6):
            event = Event(
                invocation_id=str(uuid.uuid4()),
                author="test_agent",
                actions=EventActions(
                    state_delta={"counter": i}
                ),
            )
            await service.append_event(session, event)

        # Verify final state
        retrieved = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        assert len(retrieved.events) == 5
        assert retrieved.state.get("counter") == 5

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

    async def test_idempotent_event_append(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test that appending the same event twice is idempotent."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={"counter": 0},
        )

        # Create event with fixed ID
        event = Event(
            id="fixed-event-id-123",
            invocation_id=str(uuid.uuid4()),
            author="test_agent",
            actions=EventActions(
                state_delta={"counter": 1}
            ),
        )

        # Append same event twice
        await service.append_event(session, event)

        # Re-get session to get fresh timestamp
        session = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        # Create same event again (simulating retry)
        event2 = Event(
            id="fixed-event-id-123",
            invocation_id=event.invocation_id,
            author="test_agent",
            actions=EventActions(
                state_delta={"counter": 1}
            ),
        )
        await service.append_event(session, event2)

        # Verify only one event is stored
        retrieved = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        # Due to MERGE ON event_id, duplicate should not be inserted
        assert len(retrieved.events) == 1

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

    async def test_get_session_with_event_filters(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test get_session with num_recent_events and after_timestamp filters."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
        )

        # Append 10 events
        for i in range(10):
            event = Event(
                invocation_id=str(uuid.uuid4()),
                author="test_agent",
            )
            await service.append_event(session, event)

            # Refresh session
            session = await service.get_session(
                app_name=unique_app_name,
                user_id=unique_user_id,
                session_id=session.id,
            )

        # Test num_recent_events filter
        limited = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
            config=GetSessionConfig(num_recent_events=3),
        )

        assert len(limited.events) == 3

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

    async def test_rewind_and_clear(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test rewind_session and clear_rewind functionality."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={"counter": 0},
        )

        # Append 5 events, tracking the 3rd event's ID
        target_event_id = None
        for i in range(1, 6):
            event = Event(
                invocation_id=str(uuid.uuid4()),
                author="test_agent",
                actions=EventActions(
                    state_delta={"counter": i}
                ),
            )
            await service.append_event(session, event)

            if i == 3:
                target_event_id = event.id

            # Refresh session
            session = await service.get_session(
                app_name=unique_app_name,
                user_id=unique_user_id,
                session_id=session.id,
            )

        # Rewind to event 3
        rewound = await service.rewind_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
            target_event_id=target_event_id,
        )

        # Should only see first 3 events
        assert len(rewound.events) == 3
        assert rewound.state.get("counter") == 3

        # Clear rewind
        restored = await service.clear_rewind(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        # Should see all 5 events again
        assert len(restored.events) == 5
        assert restored.state.get("counter") == 5

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

    async def test_list_sessions_all_users(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
    ):
        """Test listing sessions for all users in an app."""
        # Create sessions for different users
        user1 = f"user_{uuid.uuid4().hex[:8]}"
        user2 = f"user_{uuid.uuid4().hex[:8]}"

        session1 = await service.create_session(
            app_name=unique_app_name,
            user_id=user1,
        )
        session2 = await service.create_session(
            app_name=unique_app_name,
            user_id=user2,
        )

        # List all sessions for app
        response = await service.list_sessions(
            app_name=unique_app_name,
        )

        assert len(response.sessions) >= 2
        session_ids = [s.id for s in response.sessions]
        assert session1.id in session_ids
        assert session2.id in session_ids

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=user1,
            session_id=session1.id,
        )
        await service.delete_session(
            app_name=unique_app_name,
            user_id=user2,
            session_id=session2.id,
        )


@pytest.mark.databricks
@pytest.mark.asyncio
class TestDeltaSessionConcurrency:
    """Concurrency tests for DeltaSessionService."""

    async def test_concurrent_append_different_sessions(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test concurrent appends to different sessions (should all succeed)."""
        # Create multiple sessions
        sessions = []
        for i in range(3):
            session = await service.create_session(
                app_name=unique_app_name,
                user_id=unique_user_id,
                session_id=f"concurrent-session-{i}-{uuid.uuid4().hex[:8]}",
            )
            sessions.append(session)

        # Append to all sessions concurrently
        async def append_to_session(session: Session):
            event = Event(
                invocation_id=str(uuid.uuid4()),
                author="test_agent",
            )
            return await service.append_event(session, event)

        results = await asyncio.gather(
            *[append_to_session(s) for s in sessions]
        )

        assert len(results) == 3
        assert all(r is not None for r in results)

        # Cleanup
        for session in sessions:
            await service.delete_session(
                app_name=unique_app_name,
                user_id=unique_user_id,
                session_id=session.id,
            )


@pytest.mark.databricks
@pytest.mark.asyncio
class TestDeltaSessionPerformance:
    """Performance tests for DeltaSessionService."""

    async def test_session_with_many_events(
        self,
        service: DeltaSessionService,
        unique_app_name: str,
        unique_user_id: str,
    ):
        """Test session with 100 events for performance baseline."""
        session = await service.create_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            state={"counter": 0},
        )

        # Append 100 events
        for i in range(100):
            event = Event(
                invocation_id=str(uuid.uuid4()),
                author="test_agent",
                actions=EventActions(
                    state_delta={"counter": i + 1, f"key_{i}": f"value_{i}"}
                ),
            )
            await service.append_event(session, event)

            # Refresh session periodically
            if i % 10 == 0:
                session = await service.get_session(
                    app_name=unique_app_name,
                    user_id=unique_user_id,
                    session_id=session.id,
                )

        # Final retrieval
        final = await service.get_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

        assert len(final.events) == 100
        assert final.state.get("counter") == 100

        # Cleanup
        await service.delete_session(
            app_name=unique_app_name,
            user_id=unique_user_id,
            session_id=session.id,
        )

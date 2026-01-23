# Context, State, and Memory Management

Effective context management is crucial for coherent, multi-turn conversations.

## The `Session` Object & `SessionService`

*   **`Session`**: The container for a single, ongoing conversation (`id`, `state`, `events`).
*   **`SessionService`**: Manages the lifecycle of `Session` objects (`create_session`, `get_session`, `append_event`).
*   **Implementations**: `InMemorySessionService` (dev), `VertexAiSessionService` (prod), `DatabaseSessionService` (self-managed).

## `State`: The Conversational Scratchpad

A mutable dictionary within `session.state` for short-term, dynamic data.

*   **Update Mechanism**: Always update via `context.state` (in callbacks/tools) or `LlmAgent.output_key`.
*   **Prefixes for Scope**:
    *   **(No prefix)**: Session-specific (e.g., `session.state['booking_step']`).
    *   `user:`: Persistent for a `user_id` across all their sessions (e.g., `session.state['user:preferred_currency']`).
    *   `app:`: Persistent for `app_name` across all users and sessions.
    *   `temp:`: Ephemeral state that only exists for the current **invocation** (one user request -> final agent response cycle). It is discarded afterwards.

## `Memory`: Long-Term Knowledge & Retrieval

For knowledge beyond a single conversation.

*   **`BaseMemoryService`**: Defines the interface (`add_session_to_memory`, `search_memory`).
*   **Implementations**: `InMemoryMemoryService`, `VertexAiRagMemoryService`.
*   **Usage**: Agents interact via tools (e.g., the built-in `load_memory` tool).

## `Artifacts`: Binary Data Management

For named, versioned binary data (files, images).

*   **Representation**: `google.genai.types.Part` (containing a `Blob` with `data: bytes` and `mime_type: str`).
*   **`BaseArtifactService`**: Manages storage (`save_artifact`, `load_artifact`).
*   **Implementations**: `InMemoryArtifactService`, `GcsArtifactService`.

## Session Service Types

### 1. InMemorySessionService (Development)

```python
from google.adk.sessions import InMemorySessionService
from google.adk.runners import Runner

session_service = InMemorySessionService()

runner = Runner(
    agent=root_agent,
    app_name="my_agent",
    session_service=session_service
)
```

### 2. Database-backed (Production)

```python
from google.adk.sessions import DatabaseSessionService

session_service = DatabaseSessionService(
    connection_string="sqlite:///sessions.db"
)

runner = Runner(
    agent=root_agent,
    app_name="my_agent",
    session_service=session_service
)
```

### 3. VertexAI (Cloud)

```python
from google.adk.sessions import VertexAISessionService

session_service = VertexAISessionService(
    project_id="my-project",
    location="us-central1"
)

runner = Runner(
    agent=root_agent,
    app_name="my_agent",
    session_service=session_service
)
```

## Event Compaction (for long conversations)

```python
from google.adk.apps import App, EventsCompactionConfig
from google.adk.apps import LlmEventsSummarizer

app = App(
    name="my_agent",
    root_agent=root_agent,
    events_compaction_config=EventsCompactionConfig(
        summarizer=LlmEventsSummarizer(model="gemini-2.5-flash"),
        compaction_interval=10,  # Compact every 10 invocations
        overlap_size=2  # Keep 2 previous invocations for context
    )
)
```

## Context Caching (for large prompts)

```python
from google.adk.agents import ContextCacheConfig

context_cache_config = ContextCacheConfig(
    enabled=True,
    ttl_seconds=3600  # Cache for 1 hour
)

app = App(
    name="my_agent",
    root_agent=root_agent,
    context_cache_config=context_cache_config
)
```

## RAG/Long-term Memory (if needed)

```python
from google.adk.memory import VectorStore
from google.adk.tools import FunctionTool

# Set up vector store
vector_store = VectorStore(
    embedding_model="text-embedding-004",
    collection_name="knowledge_base"
)

def search_memory(query: str) -> str:
    """Search long-term memory for relevant information."""
    results = vector_store.search(query, top_k=5)
    return "\n".join([r.content for r in results])

search_memory_tool = FunctionTool(search_memory)

# Add to agent's tools
root_agent.tools.append(search_memory_tool)
```

## Best Practices

- Use InMemorySessionService for development
- Use database-backed sessions for production
- Enable event compaction for long conversations
- Use context caching for large, repeated prompts
- Implement RAG for knowledge-intensive applications


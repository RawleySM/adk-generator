# Runtime, Events, and Execution Flow

The `Runner` is the central orchestrator of an ADK application.

## Runtime Configuration (`RunConfig`)

Passed to `run` or `run_live` to control execution limits and output formats.

```python
from google.adk.agents.run_config import RunConfig
from google.genai import types

config = RunConfig(
    # Safety limits
    max_llm_calls=100,  # Prevent infinite agent loops
    
    # Streaming & Modality
    response_modalities=["AUDIO", "TEXT"], # Request specific output formats
    
    # Voice configuration (for AUDIO modality)
    speech_config=types.SpeechConfig(
        voice_config=types.VoiceConfig(
            prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name="Kore")
        )
    ),
    
    # Debugging
    save_input_blobs_as_artifacts=True # Save uploaded files to ArtifactService
)
```

## The `Runner`: The Orchestrator

*   **Role**: Manages the agent's lifecycle, the event loop, and coordinates with services.
*   **Entry Point**: `runner.run_async(user_id, session_id, new_message)`.

## The Event Loop: Core Execution Flow

1.  User input becomes a `user` `Event`.
2.  `Runner` calls `agent.run_async(invocation_context)`.
3.  Agent `yield`s an `Event` (e.g., tool call, text response). Execution pauses.
4.  `Runner` processes the `Event` (applies state changes, etc.) and yields it to the client.
5.  Execution resumes. This cycle repeats until the agent is done.

## `Event` Object: The Communication Backbone

`Event` objects carry all information and signals.

*   `Event.author`: Source of the event (`'user'`, agent name, `'system'`).
*   `Event.content`: The primary payload (text, function calls, function responses).
*   `Event.actions`: Signals side effects (`state_delta`, `transfer_to_agent`, `escalate`).
*   `Event.is_final_response()`: Helper to identify the complete, displayable message.

## Asynchronous Programming (Python Specific)

ADK is built on `asyncio`. Use `async def`, `await`, and `async for` for all I/O-bound operations.

## Testing the Output of an Agent

The following script demonstrates how to programmatically test an agent's output:

```python
import asyncio

from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from app.agent import root_agent
from google.genai import types as genai_types


async def main():
    """Runs the agent with a sample query."""
    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name="app", user_id="test_user", session_id="test_session"
    )
    runner = Runner(
        agent=root_agent, app_name="app", session_service=session_service
    )
    query = "I want a recipe for pancakes"
    async for event in runner.run_async(
        user_id="test_user",
        session_id="test_session",
        new_message=genai_types.Content(
            role="user", 
            parts=[genai_types.Part.from_text(text=query)]
        ),
    ):
        if event.is_final_response():
            print(event.content.parts[0].text)


if __name__ == "__main__":
    asyncio.run(main())
```


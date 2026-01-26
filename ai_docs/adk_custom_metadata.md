## Miscellaneous Controls

ADK provides additional RunConfig options to control session behavior, manage costs, and persist audio data for debugging and compliance purposes.

```python
run_config = RunConfig(
    # Limit total LLM calls per invocation
    max_llm_calls=500,  # Default: 500 (prevents runaway loops)
                        # 0 or negative = unlimited (use with caution)

    # Save audio/video artifacts for debugging/compliance
    save_live_blob=True,  # Default: False

    # Attach custom metadata to events
    custom_metadata={"user_tier": "premium", "session_type": "support"},  # Default: None

    # Enable compositional function calling (experimental)
    support_cfc=True  # Default: False (Gemini 2.x models only)
)
```

### custom_metadata

This parameter allows you to attach arbitrary key-value metadata to events generated during the current invocation. The metadata is stored in the `Event.custom_metadata` field and persisted to session storage, enabling you to tag events with application-specific context for analytics, debugging, routing, or compliance tracking.

**Configuration:**

```python
from google.adk.agents.run_config import RunConfig

# Attach metadata to all events in this invocation
run_config = RunConfig(
    custom_metadata={
        "user_tier": "premium",
        "session_type": "customer_support",
        "campaign_id": "promo_2025",
        "ab_test_variant": "variant_b"
    }
)
```

**How it works:**

When you provide `custom_metadata` in RunConfig:

1. **Metadata attachment**: The dictionary is attached to every `Event` generated during the invocation
2. **Session persistence**: Events with metadata are stored in the session service (database, Vertex AI, or in-memory)
3. **Event access**: Retrieve metadata from any event via `event.custom_metadata`
4. **A2A integration**: For Agent-to-Agent (A2A) communication, ADK automatically propagates A2A request metadata to this field

**Type specification:**

```python
custom_metadata: Optional[dict[str, Any]] = None
```

The metadata is a flexible dictionary accepting any JSON-serializable values (strings, numbers, booleans, nested objects, arrays).

**Use cases:**

- **User segmentation**: Tag events with user tier, subscription level, or cohort information
- **Session classification**: Label sessions by type (support, sales, onboarding) for analytics
- **Campaign tracking**: Associate events with marketing campaigns or experiments
- **A/B testing**: Track which variant of your application generated the event
- **Compliance**: Attach jurisdiction, consent flags, or data retention policies
- **Debugging**: Add trace IDs, feature flags, or environment identifiers
- **Analytics**: Store custom dimensions for downstream analysis

**Example - Retrieving metadata from events:**

```python
async for event in runner.run_live(
    session=session,
    live_request_queue=queue,
    run_config=RunConfig(
        custom_metadata={"user_id": "user_123", "experiment": "new_ui"}
    )
):
    if event.custom_metadata:
        print(f"User: {event.custom_metadata.get('user_id')}")
        print(f"Experiment: {event.custom_metadata.get('experiment')}")
```

**Agent-to-Agent (A2A) integration:**

When using `RemoteA2AAgent`, ADK automatically extracts metadata from A2A requests and populates `custom_metadata`:

```python
# A2A request metadata is automatically mapped to custom_metadata
# Source: a2a/converters/request_converter.py
custom_metadata = {
    "a2a_metadata": {
        # Original A2A request metadata appears here
    }
}
```

This enables seamless metadata propagation across agent boundaries in multi-agent architectures.

**Best practices:**

- Use consistent key naming conventions across your application
- Avoid storing sensitive data (PII, credentials) in metadata—use encryption if necessary
- Keep metadata size reasonable to minimize storage overhead
- Document your metadata schema for team consistency
- Consider using metadata for session filtering and search in production debugging


# Control Flow with Callbacks

Callbacks are functions that intercept and control agent execution at specific points.

## Callback Mechanism: Interception & Control

*   **Definition**: A Python function assigned to an agent's `callback` parameter (e.g., `after_agent_callback=my_func`).
*   **Context**: Receives a `CallbackContext` (or `ToolContext`) with runtime info.
*   **Return Value**: **Crucially determines flow.**
    *   `return None`: Allow the default action to proceed.
    *   `return <Specific Object>`: **Override** the default action/result.

## Types of Callbacks

1.  **Agent Lifecycle**: `before_agent_callback`, `after_agent_callback`.
2.  **LLM Interaction**: `before_model_callback`, `after_model_callback`.
3.  **Tool Execution**: `before_tool_callback`, `after_tool_callback`.

## Callback Best Practices

*   **Keep Focused**: Each callback for a single purpose.
*   **Performance**: Avoid blocking I/O or heavy computation.
*   **Error Handling**: Use `try...except` to prevent crashes.

## Example 1: Data Aggregation with `after_agent_callback`

This callback runs after an agent, inspects the `session.events` to find structured data from tool calls (like `google_search` results), and saves it to state for later use.

```python
from google.adk.agents.callback_context import CallbackContext

def collect_research_sources_callback(callback_context: CallbackContext) -> None:
    """Collects and organizes web research sources from agent events."""
    session = callback_context._invocation_context.session
    # Get existing sources from state to append to them.
    url_to_short_id = callback_context.state.get("url_to_short_id", {})
    sources = callback_context.state.get("sources", {})
    id_counter = len(url_to_short_id) + 1

    # Iterate through all events in the session to find grounding metadata.
    for event in session.events:
        if not (event.grounding_metadata and event.grounding_metadata.grounding_chunks):
            continue
        # ... logic to parse grounding_chunks and grounding_supports ...
        # (See full implementation in the original code snippet)

    # Save the updated source map back to state.
    callback_context.state["url_to_short_id"] = url_to_short_id
    callback_context.state["sources"] = sources

# Used in an agent like this:
# section_researcher = LlmAgent(..., after_agent_callback=collect_research_sources_callback)
```

## Example 2: Output Transformation with `after_agent_callback`

This callback takes an LLM's raw output (containing custom tags), uses Python to format it into markdown, and returns the modified content, overriding the original.

```python
import re
from google.adk.agents.callback_context import CallbackContext
from google.genai import types as genai_types

def citation_replacement_callback(callback_context: CallbackContext) -> genai_types.Content:
    """Replaces <cite> tags in a report with Markdown-formatted links."""
    # 1. Get raw report and sources from state.
    final_report = callback_context.state.get("final_cited_report", "")
    sources = callback_context.state.get("sources", {})

    # 2. Define a replacer function for regex substitution.
    def tag_replacer(match: re.Match) -> str:
        short_id = match.group(1)
        if not (source_info := sources.get(short_id)):
            return "" # Remove invalid tags
        title = source_info.get("title", short_id)
        return f" [{title}]({source_info['url']})"

    # 3. Use regex to find all <cite> tags and replace them.
    processed_report = re.sub(
        r'<cite\s+source\s*=\s*["\']?(src-\d+)["\']?\s*/>',
        tag_replacer,
        final_report,
    )
    processed_report = re.sub(r"\s+([.,;:])", r"\1", processed_report) # Fix spacing

    # 4. Save the new version to state and return it to override the original agent output.
    callback_context.state["final_report_with_citations"] = processed_report
    return genai_types.Content(parts=[genai_types.Part(text=processed_report)])

# Used in an agent like this:
# report_composer = LlmAgent(..., after_agent_callback=citation_replacement_callback)
```

## Global Control with Plugins

Plugins are stateful, reusable modules for implementing cross-cutting concerns that apply globally to all agents, tools, and model calls managed by a `Runner`. Unlike Callbacks which are configured per-agent, Plugins are registered once on the `Runner`.

*   **Use Cases**: Ideal for universal logging, application-wide policy enforcement, global caching, and collecting metrics.
*   **Execution Order**: Plugin callbacks run **before** their corresponding agent-level callbacks. If a plugin callback returns a value, the agent-level callback is skipped.

### Defining a Plugin

Inherit from `BasePlugin` and implement callback methods.

```python
from google.adk.plugins import BasePlugin
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_request import LlmRequest

class AuditLoggingPlugin(BasePlugin):
    def __init__(self):
        super().__init__(name="audit_logger")

    async def before_model_callback(self, callback_context: CallbackContext, llm_request: LlmRequest):
        # Log every prompt sent to any LLM
        print(f"[AUDIT] Agent {callback_context.agent_name} calling LLM with: {llm_request.contents[-1]}")

    async def on_tool_error_callback(self, tool, error, **kwargs):
        # Global error handler for all tools
        print(f"[ALERT] Tool {tool.name} failed: {error}")
        # Optionally return a dict to suppress the exception and provide fallback
        return {"status": "error", "message": "An internal error occurred, handled by plugin."}
```

### Registering a Plugin

```python
from google.adk.runners import Runner
# runner = Runner(agent=root_agent, ..., plugins=[AuditLoggingPlugin()])
```

*   **Error Handling Callbacks**: Plugins support unique error hooks like `on_model_error_callback` and `on_tool_error_callback` for centralized error management.
*   **Limitation**: Plugins are not supported by the `adk web` interface.


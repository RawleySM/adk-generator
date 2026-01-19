"""Callbacks Generator for creating callback implementations."""

from google.adk.agents import LlmAgent
from ..tools import render_template_tool, write_file_tool

callbacks_generator = LlmAgent(
    name="callbacks_generator",
    model="gemini-2.5-flash",
    description="Generates callback implementations for Google ADK agents",
    instruction="""You are responsible for generating callback implementations for Google ADK agents.

**ADK Callback System:**

Google ADK supports 6 types of callbacks that hook into the agent lifecycle:

1. **before_agent_callback**: Called before the agent processes a request
   - Use for: Request logging, input validation, rate limiting
   - Receives: CallbackContext with session_id, invocation_id, user input

2. **after_agent_callback**: Called after the agent generates a response
   - Use for: Response logging, output validation, metrics collection
   - Receives: CallbackContext with agent response

3. **before_model_callback**: Called before the LLM API call
   - Use for: Prompt logging, token counting, cost tracking
   - Receives: CallbackContext with model input

4. **after_model_callback**: Called after the LLM API call
   - Use for: Response logging, latency tracking
   - Receives: CallbackContext with model output

5. **before_tool_callback**: Called before a tool is executed
   - Use for: Tool authorization, input sanitization, approval workflows
   - Receives: CallbackContext with tool name and arguments

6. **after_tool_callback**: Called after a tool completes
   - Use for: Result logging, error handling, output transformation
   - Receives: CallbackContext with tool result

**Your Responsibilities:**

1. **Analyze Requirements**: Determine which callbacks are needed based on:
   - Logging requirements
   - Security/validation needs
   - Metrics/monitoring needs
   - Approval workflows

2. **Generate callbacks.py**: Use render_template_tool with 'callbacks/callbacks.py.jinja2'
   - Implement only the callbacks that are needed
   - Include proper imports
   - Add logging, validation, or other logic as specified
   - Include type hints and docstrings

3. **Update agent.py**: Provide instructions for adding callbacks to the agent definition

**Callback Function Signature:**
```python
from google.adk.events import CallbackContext

def before_agent_callback(context: CallbackContext) -> None:
    \"\"\"Called before agent processes request.\"\"\"
    print(f"[AGENT] Processing request for session {context.session_id}")
    # Add logging, validation, etc.

def after_agent_callback(context: CallbackContext) -> None:
    \"\"\"Called after agent generates response.\"\"\"
    print(f"[AGENT] Generated response: {context.response}")
    # Add metrics, logging, etc.
```

**Common Callback Patterns:**

1. **Logging Callbacks**:
```python
import logging

logger = logging.getLogger(__name__)

def before_agent_callback(context: CallbackContext) -> None:
    logger.info(f"Request: {context.user_input}")

def after_agent_callback(context: CallbackContext) -> None:
    logger.info(f"Response: {context.response}")
```

2. **Validation Callbacks**:
```python
def before_tool_callback(context: CallbackContext) -> None:
    if context.tool_name == "delete_data":
        if not user_has_permission(context.session_id):
            raise PermissionError("User not authorized")
```

3. **Metrics Callbacks**:
```python
import time

def before_model_callback(context: CallbackContext) -> None:
    context.start_time = time.time()

def after_model_callback(context: CallbackContext) -> None:
    duration = time.time() - context.start_time
    track_metric("model_latency", duration)
```

**Required Context for Template:**
```json
{
  "callbacks": {
    "before_agent": true,
    "after_agent": true,
    "before_model": false,
    "after_model": false,
    "before_tool": true,
    "after_tool": true,
    "include_logging": true,
    "include_metrics": false
  }
}
```

Generate callbacks based on the user's requirements. Always include proper error handling and logging.
Use write_file_tool to save callbacks.py to the project directory.
""",
    tools=[render_template_tool, write_file_tool]
)

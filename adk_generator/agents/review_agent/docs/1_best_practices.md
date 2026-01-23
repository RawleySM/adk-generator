# General Best Practices & Common Pitfalls

## Development Best Practices

*   **Start Simple**: Begin with `LlmAgent`, mock tools, and `InMemorySessionService`. Gradually add complexity.
*   **Iterative Development**: Build small features, test, debug, refine.
*   **Modular Design**: Use agents and tools to encapsulate logic.
*   **Clear Naming**: Descriptive names for agents, tools, state keys.
*   **Error Handling**: Implement robust `try...except` blocks in tools and callbacks. Guide LLMs on how to handle tool errors.
*   **Testing**: Write unit tests for tools/callbacks, integration tests for agent flows (`pytest`, `adk eval`).
*   **Dependency Management**: Use virtual environments (`venv`) and `requirements.txt`.
*   **Secrets Management**: Never hardcode API keys. Use `.env` for local dev, environment variables or secret managers (Google Cloud Secret Manager) for production.

## Common Pitfalls to Avoid

### 1. Infinite Loops

Especially with `LoopAgent` or complex LLM tool-calling chains. Use `max_iterations`, `max_llm_calls`, and strong instructions.

### 2. Missing None Checks

Always check for `None` or `Optional` values when accessing nested properties:

```python
# ❌ Wrong - may crash
text = event.content.parts[0].text

# ✅ Correct - safe access
if event.content and event.content.parts and event.content.parts[0].text:
    text = event.content.parts[0].text
```

### 3. Immutability of Events

Events are immutable records. If you need to change something *before* it's processed, do so in a `before_*` callback and return a *new* modified object.

### 4. Confusing `output_key` vs. Direct State Writes

*   `output_key`: For the agent's *final conversational* output.
*   Direct `tool_context.state['key'] = value`: For *any other* data you want to save.

## Debugging, Logging & Observability

### `adk web` UI

Best first step. Provides visual trace, session history, and state inspection.

### Event Stream Logging

Iterate `runner.run_async()` events and print relevant fields:

```python
async for event in runner.run_async(...):
    print(f"[{event.author}] Event ID: {event.id}, Invocation: {event.invocation_id}")
    if event.content and event.content.parts:
        if event.content.parts[0].text:
            print(f"  Text: {event.content.parts[0].text[:100]}...")
        if event.get_function_calls():
            print(f"  Tool Call: {event.get_function_calls()[0].name} with {event.get_function_calls()[0].args}")
        if event.get_function_responses():
            print(f"  Tool Response: {event.get_function_responses()[0].response}")
    if event.actions:
        if event.actions.state_delta:
            print(f"  State Delta: {event.actions.state_delta}")
        if event.actions.transfer_to_agent:
            print(f"  TRANSFER TO: {event.actions.transfer_to_agent}")
    if event.error_message:
        print(f"  ERROR: {event.error_message}")
```

### Tool/Callback Logging

Simple logging directly within your functions.

### Python Logging Module

Use Python's standard `logging` module. Control verbosity with `adk web --log_level DEBUG` or `adk web -v`.

### One-Line Observability Integrations

ADK has native hooks for popular tracing platforms:

**AgentOps**:
```python
import agentops
agentops.init(api_key="...") # Automatically instruments ADK agents
```

**Arize Phoenix**:
```python
from phoenix.otel import register
register(project_name="my_agent", auto_instrument=True)
```

**Google Cloud Trace**: Enable via flag during deployment:
```bash
adk deploy [cloud_run|agent_engine] --trace_to_cloud ...
```

### Session History

`session.events` is persisted for detailed post-mortem analysis.

## Performance Optimization

*   **Model Selection**: Choose the smallest model that meets requirements (e.g., `gemini-2.5-flash` for simple tasks).
*   **Instruction Prompt Engineering**: Concise, clear instructions reduce tokens and improve accuracy.
*   **Tool Use Optimization**:
    *   Design efficient tools (fast API calls, optimize database queries).
    *   Cache tool results (e.g., using `before_tool_callback` or `tool_context.state`).
*   **State Management**: Store only necessary data in state to avoid large context windows.
*   **`include_contents='none'`**: For stateless utility agents, saves LLM context window.
*   **Parallelization**: Use `ParallelAgent` for independent tasks.
*   **Streaming**: Use `StreamingMode.SSE` or `BIDI` for perceived latency reduction.
*   **`max_llm_calls`**: Limit LLM calls to prevent runaway agents and control costs.

## Example Agents

Find practical examples and reference implementations in the [ADK Samples repository](https://github.com/google/adk-samples).


An `LlmAgent` within a `LoopAgent` needs a function tool to escalate/exit the loop, not just a keyword phrase. The `LoopAgent` specifically checks for `event.actions.escalate` to determine when to stop looping [1](#0-0) .

## How Loop Exit Works

The `LoopAgent` terminates when either:
1. `max_iterations` is reached, or
2. Any sub-agent yields an `Event` with `escalate=True` in its `EventActions` [2](#0-1) 

## Implementation Pattern

The standard approach is to provide the `LlmAgent` with an `exit_loop` tool that sets `tool_context.actions.escalate = True` [3](#0-2) :

```python
def exit_loop(tool_context: ToolContext):
    """Call this function ONLY when the iterative process should end."""
    tool_context.actions.escalate = True
    return {}
```

The `LlmAgent`'s instructions should explicitly tell it when to call this tool [4](#0-3) .

## Alternative: Custom Agent

You could also use a custom `BaseAgent` that checks state and yields an `Event` with `escalate=True` [5](#0-4) , but this requires a separate agent rather than just the `LlmAgent` outputting a phrase.

## Notes

Simply having an `LlmAgent` output a keyword phrase like "DONE" won't exit the loop unless another agent interprets that phrase and triggers the escalate action. The `LoopAgent` only responds to the explicit `escalate=True` flag in events [1](#0-0) .

### Citations

**File:** src/google/adk/agents/loop_agent.py (L102-103)
```python
            if event.actions.escalate:
              should_exit = True
```

**File:** llms-full.txt (L2082-2083)
```text
      * **Termination:** The loop stops if the optional `max_iterations` is reached, or if any sub-agent returns an [`Event`](../events/index.md) with `escalate=True` in it's Event Actions.
      * **Context & State:** Passes the *same* `InvocationContext` in each iteration, allowing state changes (e.g., counters, flags) to persist across loops.
```

**File:** llms-full.txt (L2456-2460)
```text
    class CheckStatusAndEscalate(BaseAgent):
        async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
            status = ctx.session.state.get("quality_status", "fail")
            should_stop = (status == "pass")
            yield Event(author=self.name, actions=EventActions(escalate=should_stop))
```

**File:** llms-full.txt (L2652-2657)
```text
        def exit_loop(tool_context: ToolContext):
          """Call this function ONLY when the critique indicates no further changes are needed, signaling the iterative process should end."""
          print(f"  [Tool Call] exit_loop triggered by {tool_context.agent_name}")
          tool_context.actions.escalate = True
          # Return empty dict as tools should typically return JSON-serializable output
          return {}
```

**File:** llms-full.txt (L2710-2715)
```text
            Analyze the 'Critique/Suggestions'.
            IF the critique is *exactly* "{COMPLETION_PHRASE}":
            You MUST call the 'exit_loop' function. Do not output any text.
            ELSE (the critique contains actionable feedback):
            Carefully apply the suggestions to improve the 'Current Document'. Output *only* the refined document text.
            Do not add explanations. Either output the refined document OR call the exit_loop function.
```

# Building Custom Agents (`BaseAgent`)

For unique orchestration logic that doesn't fit standard workflow agents, inherit directly from `BaseAgent`.

## When to Use Custom Agents

*   **Complex Conditional Logic**: `if/else` branching based on multiple state variables.
*   **Dynamic Agent Selection**: Choosing which sub-agent to run based on runtime evaluation.
*   **Direct External Integrations**: Calling external APIs or libraries directly within the orchestration flow.
*   **Custom Loop/Retry Logic**: More sophisticated iteration patterns than `LoopAgent`, such as the `EscalationChecker` example.

## Implementing `_run_async_impl`

This is the core asynchronous method you must override.

### Example: A Custom Agent for Loop Control

This agent reads state, applies simple Python logic, and yields an `Event` with an `escalate` action to control a `LoopAgent`.

```python
from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from typing import AsyncGenerator
import logging

class EscalationChecker(BaseAgent):
    """Checks research evaluation and escalates to stop the loop if grade is 'pass'."""

    def __init__(self, name: str):
        super().__init__(name=name)

    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        # 1. Read from session state.
        evaluation_result = ctx.session.state.get("research_evaluation")

        # 2. Apply custom Python logic.
        if evaluation_result and evaluation_result.get("grade") == "pass":
            logging.info(
                f"[{self.name}] Research passed. Escalating to stop loop."
            )
            # 3. Yield an Event with a control Action.
            yield Event(author=self.name, actions=EventActions(escalate=True))
        else:
            logging.info(
                f"[{self.name}] Research failed or not found. Loop continues."
            )
            # Yielding an event without actions lets the flow continue.
            yield Event(author=self.name)
```

## Key Implementation Notes

*   **Asynchronous Generator**: `async def ... yield Event`. This allows pausing and resuming execution.
*   **`ctx: InvocationContext`**: Provides access to all session state (`ctx.session.state`).
*   **Calling Sub-Agents**: Use `async for event in self.sub_agent_instance.run_async(ctx): yield event`.
*   **Control Flow**: Use standard Python `if/else`, `for/while` loops for complex logic.

## Example: Conditional Branching Agent

```python
from google.adk.agents import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event
from typing import AsyncGenerator

class ConditionalRouter(BaseAgent):
    """Routes to different sub-agents based on state conditions."""
    
    def __init__(self, name: str, positive_agent, negative_agent):
        super().__init__(name=name, sub_agents=[positive_agent, negative_agent])
        self.positive_agent = positive_agent
        self.negative_agent = negative_agent
    
    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        # Read condition from state
        sentiment = ctx.session.state.get("sentiment_analysis", {})
        
        # Route based on condition
        if sentiment.get("score", 0) > 0.5:
            target_agent = self.positive_agent
        else:
            target_agent = self.negative_agent
        
        # Execute the selected agent
        async for event in target_agent.run_async(ctx):
            yield event
```

## Example: Dynamic Retry Agent

```python
class RetryAgent(BaseAgent):
    """Retries a sub-agent until success or max retries."""
    
    def __init__(self, name: str, target_agent, max_retries: int = 3):
        super().__init__(name=name, sub_agents=[target_agent])
        self.target_agent = target_agent
        self.max_retries = max_retries
    
    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        for attempt in range(self.max_retries):
            ctx.session.state["retry_attempt"] = attempt + 1
            
            async for event in self.target_agent.run_async(ctx):
                yield event
            
            # Check if successful
            if ctx.session.state.get("operation_success"):
                return
            
            logging.warning(f"Attempt {attempt + 1} failed, retrying...")
        
        # All retries exhausted
        yield Event(
            author=self.name,
            content=Content(parts=[Part(text="Max retries exceeded")])
        )
```


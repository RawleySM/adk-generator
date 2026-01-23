# Orchestration with Workflow Agents

Workflow agents (`SequentialAgent`, `ParallelAgent`, `LoopAgent`) provide deterministic control flow, combining LLM capabilities with structured execution. They do **not** use an LLM for their own orchestration logic.

## `SequentialAgent`: Linear Execution

Executes `sub_agents` one after another in the order defined. The `InvocationContext` is passed along, allowing state changes to be visible to subsequent agents.

```python
from google.adk.agents import SequentialAgent, Agent

# Agent 1: Summarizes a document and saves to state
summarizer = Agent(
    name="DocumentSummarizer",
    model="gemini-3-flash-preview",
    instruction="Summarize the provided document in 3 sentences.",
    output_key="document_summary" # Output saved to session.state['document_summary']
)

# Agent 2: Generates questions based on the summary from state
question_generator = Agent(
    name="QuestionGenerator",
    model="gemini-3-flash-preview",
    instruction="Generate 3 comprehension questions based on this summary: {document_summary}",
    # 'document_summary' is dynamically injected from session.state
)

document_pipeline = SequentialAgent(
    name="SummaryQuestionPipeline",
    sub_agents=[summarizer, question_generator], # Order matters!
    description="Summarizes a document then generates questions."
)
```

## `ParallelAgent`: Concurrent Execution

Executes `sub_agents` simultaneously. Useful for independent tasks to reduce overall latency. All sub-agents share the same `session.state`.

```python
from google.adk.agents import ParallelAgent, Agent, SequentialAgent

# Agents to fetch data concurrently
fetch_stock_price = Agent(name="StockPriceFetcher", ..., output_key="stock_data")
fetch_news_headlines = Agent(name="NewsFetcher", ..., output_key="news_data")
fetch_social_sentiment = Agent(name="SentimentAnalyzer", ..., output_key="sentiment_data")

# Agent to merge results (runs after ParallelAgent, usually in a SequentialAgent)
merger_agent = Agent(
    name="ReportGenerator",
    model="gemini-3-flash-preview",
    instruction="Combine stock data: {stock_data}, news: {news_data}, and sentiment: {sentiment_data} into a market report."
)

# Pipeline to run parallel fetching then sequential merging
market_analysis_pipeline = SequentialAgent(
    name="MarketAnalyzer",
    sub_agents=[
        ParallelAgent(
            name="ConcurrentFetch",
            sub_agents=[fetch_stock_price, fetch_news_headlines, fetch_social_sentiment]
        ),
        merger_agent # Runs after all parallel agents complete
    ]
)
```

*   **Concurrency Caution**: When parallel agents write to the same `state` key, race conditions can occur. Always use distinct `output_key`s or manage concurrent writes explicitly.

## `LoopAgent`: Iterative Processes

Repeatedly executes its `sub_agents` (sequentially within each loop iteration) until a condition is met or `max_iterations` is reached.

### Termination of `LoopAgent`

A `LoopAgent` terminates when:
1.  `max_iterations` is reached.
2.  Any `Event` yielded by a sub-agent (or a tool within it) sets `actions.escalate = True`. This provides dynamic, content-driven loop termination.

### Example: Iterative Refinement Loop with a Custom `BaseAgent` for Control

This example shows a loop that continues until a condition, determined by an evaluation agent, is met.

```python
from google.adk.agents import LoopAgent, Agent, BaseAgent
from google.adk.events import Event, EventActions
from google.adk.agents.invocation_context import InvocationContext
from typing import AsyncGenerator

# An LLM Agent that evaluates research and produces structured JSON output
research_evaluator = Agent(
    name="research_evaluator",
    # ... configuration from Section 2.2 ...
    output_schema=Feedback,
    output_key="research_evaluation",
)

# An LLM Agent that performs additional searches based on feedback
enhanced_search_executor = Agent(
    name="enhanced_search_executor",
    instruction="Execute the follow-up queries from 'research_evaluation' and combine with existing findings.",
    # ... other configurations ...
)

# A custom BaseAgent to check the evaluation and stop the loop
class EscalationChecker(BaseAgent):
    """Checks research evaluation and escalates to stop the loop if grade is 'pass'."""
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        evaluation = ctx.session.state.get("research_evaluation")
        if evaluation and evaluation.get("grade") == "pass":
            # The key to stopping the loop: yield an Event with escalate=True
            yield Event(author=self.name, actions=EventActions(escalate=True))
        else:
            # Let the loop continue
            yield Event(author=self.name)

# Define the loop
iterative_refinement_loop = LoopAgent(
    name="IterativeRefinementLoop",
    sub_agents=[
        research_evaluator, # Step 1: Evaluate
        EscalationChecker(name="EscalationChecker"), # Step 2: Check and maybe stop
        enhanced_search_executor, # Step 3: Refine (only runs if loop didn't stop)
    ],
    max_iterations=5, # Fallback to prevent infinite loops
    description="Iteratively evaluates and refines research until it passes quality checks."
)
```


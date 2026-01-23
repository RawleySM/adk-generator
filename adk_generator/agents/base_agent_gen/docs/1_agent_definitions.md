# Agent Definitions (`LlmAgent`)

The `LlmAgent` is the cornerstone of intelligent behavior, leveraging an LLM for reasoning and decision-making.

## Basic `LlmAgent` Setup

```python
from google.adk.agents import Agent

def get_current_time(city: str) -> dict:
    """Returns the current time in a specified city."""
    # Mock implementation
    if city.lower() == "new york":
        return {"status": "success", "time": "10:30 AM EST"}
    return {"status": "error", "message": f"Time for {city} not available."}

my_first_llm_agent = Agent(
    name="time_teller_agent",
    model="gemini-3-flash-preview", # Essential: The LLM powering the agent
    instruction="You are a helpful assistant that tells the current time in cities. Use the 'get_current_time' tool for this purpose.",
    description="Tells the current time in a specified city.", # Crucial for multi-agent delegation
    tools=[get_current_time] # List of callable functions/tool instances
)
```

## Advanced `LlmAgent` Configuration

*   **`generate_content_config`**: Controls LLM generation parameters (temperature, token limits, safety).
    ```python
    from google.genai import types as genai_types
    from google.adk.agents import Agent

    gen_config = genai_types.GenerateContentConfig(
        temperature=0.2,            # Controls randomness (0.0-1.0), lower for more deterministic.
        top_p=0.9,                  # Nucleus sampling: sample from top_p probability mass.
        top_k=40,                   # Top-k sampling: sample from top_k most likely tokens.
        max_output_tokens=1024,     # Max tokens in LLM's response.
        stop_sequences=["## END"]   # LLM will stop generating if these sequences appear.
    )
    agent = Agent(
        # ... basic config ...
        generate_content_config=gen_config
    )
    ```

*   **`output_key`**: Automatically saves the agent's final text or structured (if `output_schema` is used) response to the `session.state` under this key. Facilitates data flow between agents.
    ```python
    agent = Agent(
        # ... basic config ...
        output_key="llm_final_response_text"
    )
    # After agent runs, session.state['llm_final_response_text'] will contain its output.
    ```

*   **`input_schema` & `output_schema`**: Define strict JSON input/output formats using Pydantic models.
    > **Warning**: Using `output_schema` forces the LLM to generate JSON and **disables** its ability to use tools or delegate to other agents.

### Example: Defining and Using Structured Output

This is the most reliable way to make an LLM produce predictable, parseable JSON, which is essential for multi-agent workflows.

1.  **Define the Schema with Pydantic:**
    ```python
    from pydantic import BaseModel, Field
    from typing import Literal

    class SearchQuery(BaseModel):
        """Model representing a specific search query for web search."""
        search_query: str = Field(
            description="A highly specific and targeted query for web search."
        )

    class Feedback(BaseModel):
        """Model for providing evaluation feedback on research quality."""
        grade: Literal["pass", "fail"] = Field(
            description="Evaluation result. 'pass' if the research is sufficient, 'fail' if it needs revision."
        )
        comment: str = Field(
            description="Detailed explanation of the evaluation, highlighting strengths and/or weaknesses of the research."
        )
        follow_up_queries: list[SearchQuery] | None = Field(
            default=None,
            description="A list of specific, targeted follow-up search queries needed to fix research gaps. This should be null or empty if the grade is 'pass'."
        )
    ```
    *   **`BaseModel` & `Field`**: Define data types, defaults, and crucial `description` fields. These descriptions are sent to the LLM to guide its output.
    *   **`Literal`**: Enforces strict enum-like values (`"pass"` or `"fail"`), preventing the LLM from hallucinating unexpected values.

2.  **Assign the Schema to an `LlmAgent`:**
    ```python
    research_evaluator = LlmAgent(
        name="research_evaluator",
        model="gemini-3-pro-preview",
        instruction="""You are a meticulous quality assurance analyst. Evaluate the research findings in 'section_research_findings' and be very critical.
        If you find significant gaps, assign a grade of 'fail', write a detailed comment, and generate 5-7 specific follow-up queries.
        If the research is thorough, grade it 'pass'.
        Your response must be a single, raw JSON object validating against the 'Feedback' schema.
        """,
        output_schema=Feedback, # This forces the LLM to output JSON matching the Feedback model.
        output_key="research_evaluation", # The resulting JSON object will be saved to state.
        disallow_transfer_to_peers=True, # Prevents this agent from delegating. Its job is only to evaluate.
    )
    ```

*   **`include_contents`**: Controls whether the conversation history is sent to the LLM.
    *   `'default'` (default): Sends relevant history.
    *   `'none'`: Sends no history; agent operates purely on current turn's input and `instruction`. Useful for stateless API wrapper agents.
    ```python
    agent = Agent(..., include_contents='none')
    ```

*   **`planner`**: Assign a `BasePlanner` instance to enable multi-step reasoning.
    *   **`BuiltInPlanner`**: Leverages a model's native "thinking" or planning capabilities (e.g., Gemini).
        ```python
        from google.adk.planners import BuiltInPlanner
        from google.genai.types import ThinkingConfig

        agent = Agent(
            model="gemini-3-flash-preview",
            planner=BuiltInPlanner(
                thinking_config=ThinkingConfig(include_thoughts=True)
            ),
            # ... tools ...
        )
        ```
    *   **`PlanReActPlanner`**: Instructs the model to follow a structured Plan-Reason-Act output format, useful for models without built-in planning.

*   **`code_executor`**: Assign a `BaseCodeExecutor` to allow the agent to execute code blocks.
    *   **`BuiltInCodeExecutor`**: The standard, sandboxed code executor provided by ADK for safe execution.
        ```python
        from google.adk.code_executors import BuiltInCodeExecutor
        agent = Agent(
            name="code_agent",
            model="gemini-3-flash-preview",
            instruction="Write and execute Python code to solve math problems.",
            code_executor=BuiltInCodeExecutor() # Corrected from a list to an instance
        )
        ```

*   **Callbacks**: Hooks for observing and modifying agent behavior at key lifecycle points (`before_model_callback`, `after_tool_callback`, etc.).

## LLM Instruction Crafting (`instruction`)

The `instruction` is critical. It guides the LLM's behavior, persona, and tool usage.

**Best Practices & Examples:**

*   **Be Specific & Concise**: Avoid ambiguity.
*   **Define Persona & Role**: Give the LLM a clear role.
*   **Constrain Behavior & Tool Use**: Explicitly state what the LLM *and should not* do.
*   **Define Output Format**: Tell the LLM *exactly* what its output should look like, especially when not using `output_schema`.
*   **Dynamic Injection**: Use `{state_key}` to inject runtime data from `session.state` into the prompt.
*   **Iteration**: Test, observe, and refine instructions.

**Example 1: Constraining Tool Use and Output Format**
```python
import datetime
from google.adk.tools import google_search   


plan_generator = LlmAgent(
    model="gemini-3-flash-preview",
    name="plan_generator",
    description="Generates a 4-5 line action-oriented research plan.",
    instruction=f"""
    You are a research strategist. Your job is to create a high-level RESEARCH PLAN, not a summary.
    **RULE: Your output MUST be a bulleted list of 4-5 action-oriented research goals or key questions.**
    - A good goal starts with a verb like "Analyze," "Identify," "Investigate."
    - A bad output is a statement of fact like "The event was in April 2024."
    **TOOL USE IS STRICTLY LIMITED:**
    Your goal is to create a generic, high-quality plan *without searching*.
    Only use `google_search` if a topic is ambiguous and you absolutely cannot create a plan without it.
    You are explicitly forbidden from researching the *content* or *themes* of the topic.
    Current date: {datetime.datetime.now().strftime("%Y-%m-%d")}
    """,
    tools=[google_search],
)
```

**Example 2: Injecting Data from State and Specifying Custom Tags**
This agent's `instruction` relies on data placed in `session.state` by previous agents.
```python
report_composer = LlmAgent(
    model="gemini-3-pro-preview",
    name="report_composer_with_citations",
    include_contents="none", # History not needed; all data is injected.
    description="Transforms research data and a markdown outline into a final, cited report.",
    instruction="""
    Transform the provided data into a polished, professional, and meticulously cited research report.

    ---
    ### INPUT DATA
    *   Research Plan: `{research_plan}`
    *   Research Findings: `{section_research_findings}`
    *   Citation Sources: `{sources}`
    *   Report Structure: `{report_sections}`

    ---
    ### CRITICAL: Citation System
    To cite a source, you MUST insert a special citation tag directly after the claim it supports.

    **The only correct format is:** `<cite source="src-ID_NUMBER" />`

    ---
    ### Final Instructions
    Generate a comprehensive report using ONLY the `<cite source="src-ID_NUMBER" />` tag system for all citations.
    The final report must strictly follow the structure provided in the **Report Structure** markdown outline.
    Do not include a "References" or "Sources" section; all citations must be in-line.
    """,
    output_key="final_cited_report",
)
```


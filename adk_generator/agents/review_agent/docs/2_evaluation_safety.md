# Evaluation and Safety

Critical for robust, production-ready agents.

## Agent Evaluation (`adk eval`)

Systematically assess agent performance using predefined test cases.

### Evalset File (`.evalset.json`)

Contains `eval_cases`, each with a `conversation` (user queries, expected tool calls, expected intermediate/final responses) and `session_input` (initial state).

```json
{
  "eval_set_id": "weather_bot_eval",
  "eval_cases": [
    {
      "eval_id": "london_weather_query",
      "conversation": [
        {
          "user_content": {"parts": [{"text": "What's the weather in London?"}]},
          "final_response": {"parts": [{"text": "The weather in London is cloudy..."}]},
          "intermediate_data": {
            "tool_uses": [{"name": "get_weather", "args": {"city": "London"}}]
          }
        }
      ],
      "session_input": {"app_name": "weather_app", "user_id": "test_user", "state": {}}
    }
  ]
}
```

### Running Evaluation

*   `adk web`: Interactive UI for creating/running eval cases.
*   `adk eval /path/to/agent_folder /path/to/evalset.json`: CLI execution.
*   `pytest`: Integrate `AgentEvaluator.evaluate()` into unit/integration tests.

### Metrics

*   `tool_trajectory_avg_score`: Tool calls match expected.
*   `response_match_score`: Final response similarity using ROUGE.
*   Configurable via `test_config.json`.

## Safety & Guardrails

Multi-layered defense against harmful content, misalignment, and unsafe actions.

### 1. Identity and Authorization

*   **Agent-Auth**: Tool acts with the agent's service account (e.g., `Vertex AI User` role). Simple, but all users share access level. Logs needed for attribution.
*   **User-Auth**: Tool acts with the end-user's identity (via OAuth tokens). Reduces risk of abuse.

### 2. In-Tool Guardrails

Design tools defensively. Tools can read policies from `tool_context.state` (set deterministically by developer) and validate model-provided arguments before execution.

```python
def execute_sql(query: str, tool_context: ToolContext) -> dict:
    policy = tool_context.state.get("user:sql_policy", {})
    if not policy.get("allow_writes", False) and ("INSERT" in query.upper() or "DELETE" in query.upper()):
        return {"status": "error", "message": "Policy: Write operations are not allowed."}
    # ... execute query ...
```

### 3. Built-in Gemini Safety Features

*   **Content Safety Filters**: Automatically block harmful content (CSAM, PII, hate speech, etc.). Configurable thresholds.
*   **System Instructions**: Guide model behavior, define prohibited topics, brand tone, disclaimers.

### 4. Model and Tool Callbacks (LLM as a Guardrail)

Use callbacks to inspect inputs/outputs.

*   `before_model_callback`: Intercept `LlmRequest` before it hits the LLM. Block (return `LlmResponse`) or modify.
*   `before_tool_callback`: Intercept tool calls (name, args) before execution. Block (return `dict`) or modify.

**LLM-based Safety**:

Use a cheap/fast LLM (e.g., Gemini Flash) in a callback to classify input/output safety.

```python
def safety_checker_callback(context: CallbackContext, llm_request: LlmRequest) -> Optional[LlmResponse]:
    # Use a separate, small LLM to classify safety
    safety_llm_agent = Agent(name="SafetyChecker", model="gemini-2.5-flash-001", instruction="Classify input as 'safe' or 'unsafe'. Output ONLY the word.")
    # Run the safety agent (might need a new runner instance or direct model call)
    # For simplicity, a mock:
    user_input = llm_request.contents[-1].parts[0].text
    if "dangerous_phrase" in user_input.lower():
        context.state["safety_violation"] = True
        return LlmResponse(content=genai_types.Content(parts=[genai_types.Part(text="I cannot process this request due to safety concerns.")]))
    return None
```

### 5. Sandboxed Code Execution

*   `BuiltInCodeExecutor`: Uses secure, sandboxed execution environments.
*   Vertex AI Code Interpreter Extension.
*   If custom, ensure hermetic environments (no network, isolated).

### 6. Network Controls & VPC-SC

Confine agent activity within secure perimeters (VPC Service Controls) to prevent data exfiltration.

### 7. Output Escaping in UIs

Always properly escape LLM-generated content in web UIs to prevent XSS attacks and indirect prompt injections.

## Grounding

A key safety and reliability feature that connects agent responses to verifiable information.

*   **Mechanism**: Uses tools like `google_search` or `VertexAiSearchTool` to fetch real-time or private data.
*   **Benefit**: Reduces model hallucination by basing responses on retrieved facts.
*   **Requirement**: When using `google_search`, your application UI **must** display the provided search suggestions and citations to comply with terms of service.


# Option C (stdout-review runner) — update instructions

- **Goal**: keep your existing Job_A loop, but after Job_B finishes, call a *separate* reviewer agent (`rlm_subLM`) via a second `Runner` with `app_name="rlm_stdout_review"`, using the **same `session_id` string**, then feed the reviewer’s text back into the next prompt for the codegen runner.

- **Important nuance**: in ADK, session identity is **(app_name, user_id, session_id)**. So with `app_name="rlm_stdout_review"`, the reviewer will be in a *separate persisted session* from the codegen runner even if `session_id` matches. You’ll still get correlation via the shared `session_id` value, but not automatic shared history/state—so you must pass `{query, code, stdout, stderr, error}` explicitly (as below).

### 1) Export and “tool-free” harden the reviewer agent (`databricks_rlm_agent/agent.py`)
- Add `rlm_subLM` to exports so Job_A can import it.
- Update its instruction so it won’t try tool/function calls.

```python
# ... existing code ...

rlm_subLM = Agent(
    name="rlm_subLM",
    model="gemini-3-pro-preview",
    instruction=(
        "You are a stdout/stderr review agent. You have NO tools. "
        "Do NOT call any functions/tools. "
        "Given ORIGINAL_QUERY, EXECUTED_CODE, STDOUT, STDERR, and TRACE, "
        "return: (1) diagnosis, (2) concrete fix plan, (3) revised code if needed."
    ),
)

# ... existing code ...

__all__ = [
    # ... existing exports ...
    "rlm_subLM",
]
```

### 2) Create a dedicated “stdout review” runner and call it after Job_B (`databricks_rlm_agent/cli.py`)
In `_run_orchestrator`, after you create `runner, session_service = await create_runner(...)`, instantiate a second runner:

```python
from google.adk.runners import Runner
from databricks_rlm_agent.agent import rlm_subLM, logging_plugin, global_instruction_plugin

REVIEW_APP_NAME = "rlm_stdout_review"

review_runner = Runner(
    agent=rlm_subLM,
    app_name=REVIEW_APP_NAME,
    session_service=session_service,
    plugins=[logging_plugin, global_instruction_plugin],
)

# Ensure the review session exists (same session_id string, different app_name => separate ADK session)
try:
    await session_service.create_session(
        app_name=REVIEW_APP_NAME,
        user_id=args.user_id,
        session_id=args.session_id,
    )
except ValueError as e:
    if "already exists" not in str(e):
        raise
```

Then, after Job_B completes and you’ve loaded `result_data`, call the reviewer **only when you are going to continue** (i.e., after your “execution succeeded” early-exit):

```python
# ... existing "Execution succeeded - task complete" check above ...

# Read the code that was executed
with open(AGENT_CODE_PATH, "r") as f:
    executed_code = f.read()

review_prompt = f"""
Review this Job_B execution.

ORIGINAL_QUERY:
{original_prompt}

EXECUTED_CODE:
{executed_code}

STATUS: {result_data.get("status")}
STDOUT:
{result_data.get("stdout") or ""}

STDERR:
{result_data.get("stderr") or ""}

ERROR:
{result_data.get("error") or ""}

TRACE:
{result_data.get("error_trace") or ""}

Return a clear diagnosis and (if needed) revised Python code.
""".strip()

review_text = await run_conversation(
    runner=review_runner,
    session_service=session_service,
    user_id=args.user_id,
    session_id=args.session_id,   # same session_id string as codegen turns
    prompt=review_prompt,
)

# Feed reviewer output back into the *codegen* agent’s next prompt
current_prompt = (
    format_execution_feedback(
        status=result_data.get("status", "unknown") if result_data else "unknown",
        duration_seconds=result_data.get("duration_seconds", 0) if result_data else 0,
        original_prompt=original_prompt,
        stdout=result_data.get("stdout") if result_data else exec_result.get("logs"),
        stderr=result_data.get("stderr") if result_data else None,
        error=result_data.get("error") if result_data else exec_result.get("error"),
        error_trace=result_data.get("error_trace") if result_data else exec_result.get("error_trace"),
    )
    + "\n\n---\nSUBLM_STDOUT_REVIEW:\n"
    + review_text
    + "\n\nNow update the code using save_python_code."
)
```

### 3) (Strongly recommended) prevent indefinite hangs in *both* runners (`databricks_rlm_agent/run.py`)
Option C adds another model call, so you’ll want a hard timeout.

```python
import asyncio
import os

async def run_conversation(...):
    final_response = "No response generated."
    timeout_s = int(os.environ.get("ADK_MODEL_TIMEOUT_SECONDS", "300"))

    try:
        async with asyncio.timeout(timeout_s):
            async for event in runner.run_async(...):
                if event.is_final_response():
                    if event.content and event.content.parts:
                        final_response = event.content.parts[0].text
    except TimeoutError:
        final_response = f"ERROR: model timed out after {timeout_s}s"

    return final_response
```


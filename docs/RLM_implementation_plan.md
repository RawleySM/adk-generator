# RLM Workflow Implementation Plan

## Overview

Implement the 13-step RLM (Recursive Language Model) workflow that syncs the vision in `plans/RLM_overview.md` with the current `databricks_rlm_agent/` codebase. The core innovation is an **Artifact Registry** Delta table that decouples code generation from execution, enabling proper state propagation between agents.

---

## Workflow Summary

```
databricks_analyst → delegate_code_results() → [validation plugins] → artifact registry
                                                                           ↓
                                                                     job_builder (BaseAgent)
                                                                           ↓
                                                                      Job_B executor
                                                                           ↓
                                                             results_processor_agent (with injected context)
                                                                           ↓
                                                                     context pruning → loop continues
```

### Agent Sequence in LoopAgent

```
root_agent (LoopAgent)
  └── sub_agents:
        1. databricks_analyst (LlmAgent)       - generates code, calls delegate_code_results()
        2. job_builder (BaseAgent)             - deterministic job submission
        3. results_processor_agent (LlmAgent)  - processes results with instruction
```

---

## Phase 1: Artifact Registry Foundation (Hybrid ADK + Delta)

### Design Decision: Two-Layer Artifact Storage

| Layer | Storage | Purpose | Data |
|-------|---------|---------|------|
| **Metadata Registry** | Delta Table | Structured tracking | artifact_id, status, timestamps, metadata |
| **Binary Content** | ADK ArtifactService | Large blob storage | agent_code files, stdout/stderr logs |

**Benefits:**
- Delta table provides SQL-queryable metadata and status tracking
- ADK ArtifactService handles versioned binary storage (code files, logs)
- Can swap `InMemoryArtifactService` (dev) → custom `DeltaArtifactService` (prod)

### 1.1 Create `databricks_rlm_agent/artifact_registry.py`

**Delta table schema (metadata only):**
```sql
CREATE TABLE {catalog}.{schema}.artifact_registry (
    artifact_id STRING NOT NULL,
    session_id STRING NOT NULL,
    invocation_id STRING NOT NULL,
    iteration INT NOT NULL,
    artifact_type STRING NOT NULL,  -- 'delegation_request', 'executor_result', 'processor_response'
    sublm_instruction STRING,        -- Small, inline
    code_artifact_key STRING,        -- Reference to ADK ArtifactService: "{artifact_id}_code.py"
    stdout_artifact_key STRING,      -- Reference to ADK ArtifactService: "{artifact_id}_stdout.txt"
    stderr_artifact_key STRING,      -- Reference to ADK ArtifactService: "{artifact_id}_stderr.txt"
    status STRING NOT NULL,          -- 'pending', 'executing', 'completed', 'consumed'
    metadata_json STRING,
    created_time TIMESTAMP NOT NULL,
    updated_time TIMESTAMP NOT NULL,
    consumed_time TIMESTAMP
) USING DELTA PARTITIONED BY (session_id)
```

**CRUD methods:**
- `create_artifact(..., code_artifact_key) -> artifact_id`
- `get_artifact(artifact_id) -> dict` (metadata only)
- `update_artifact(artifact_id, stdout_artifact_key, stderr_artifact_key, status)`
- `mark_consumed(artifact_id)`

### 1.2 Configure ADK ArtifactService in Runner

```python
from google.adk.artifacts import InMemoryArtifactService
# Future: from databricks_rlm_agent.artifacts import DeltaArtifactService

# Development: In-memory (ephemeral)
artifact_service = InMemoryArtifactService()

# Production: Could implement DeltaArtifactService that uses UC Volumes
# artifact_service = DeltaArtifactService(spark, catalog, schema)

runner = Runner(
    app_name=APP_NAME,
    agent=root_agent,
    session_service=session_service,
    artifact_service=artifact_service,  # Enables context.save_artifact/load_artifact
)
```

### 1.3 Saving Artifacts in delegate_code_results Tool

```python
import types

def delegate_code_results(code: str, tool_context: ToolContext) -> dict:
    # ... parse blob ...

    # Save code to ADK ArtifactService
    code_part = types.Part.from_text(text=parsed.agent_code)
    code_key = f"{artifact_id}_code.py"
    version = tool_context.save_artifact(filename=code_key, artifact=code_part)

    # Insert metadata to Delta registry
    registry.create_artifact(
        artifact_id=artifact_id,
        code_artifact_key=code_key,
        sublm_instruction=parsed.sublm_instruction,  # Small, inline
        ...
    )
```

### 1.4 Loading Artifacts in job_builder

```python
# In job_builder or executor
code_key = artifact["code_artifact_key"]
code_part = ctx.load_artifact(filename=code_key)
agent_code = code_part.text  # or code_part.inline_data for binary
```

### 1.5 Create `databricks_rlm_agent/utils/docstring_parser.py`

```python
def parse_delegation_blob(blob: str) -> ParsedBlob:
    """Parse '''<instruction>'''\n<code> format into (sublm_instruction, agent_code)"""
```

---

## Phase 2: Create delegate_code_results Tool

### Create `databricks_rlm_agent/tools/delegate_code_results.py`

The tool must:
1. Parse docstring header → `sublm_instruction` + `agent_code`
2. Insert row to artifact registry
3. Set state keys (invocation-scoped `temp:rlm:*` for glue, session-scoped `rlm:iteration`):
   - `tool_context.state["temp:rlm:artifact_id"] = artifact_id`
   - `tool_context.state["temp:rlm:sublm_instruction"] = sublm_instruction`
   - `tool_context.state["temp:rlm:has_agent_code"] = bool(agent_code)`
   - `tool_context.state["rlm:iteration"] += 1`  # session-scoped counter
4. Trigger escalation: `tool_context.actions.escalate = True`

> **Note:** The `temp:rlm:*` prefix ensures glue keys auto-discard after invocation (see `plans/refactor_key_glue.md`).

---

## Phase 3: Validation Plugins

### 3.1 Create `databricks_rlm_agent/plugins/formatting_check_plugin.py`

`before_tool_callback` for `delegate_code_results`:
- Validate docstring format is parseable
- Block with structured error if malformed

### 3.2 Create `databricks_rlm_agent/plugins/code_linting_plugin.py`

`before_tool_callback` for `delegate_code_results` and `save_python_code`:
- Run `ast.parse()` on code portion
- Block with syntax error location if invalid

### 3.3 Wire `UcToolExecutionSafetyPlugin`

Already exists - just add to App plugin list.

---

## Phase 4: Context Injection Plugin

### Create `databricks_rlm_agent/plugins/rlm_context_injection_plugin.py`

`before_agent_callback` for `results_processor` agent:
1. Read `temp:rlm:artifact_id` from `callback_context.state` (via `get_rlm_state()` dual-read helper)
2. Load full output from `result.json` in UC Volumes (via `temp:rlm:result_json_path`)
3. Return `types.Content` with:
   - `sublm_instruction` as analysis request
   - `stdout`/`stderr` as execution output

> **Note:** Uses dual-read pattern (`temp:rlm:*` first, fallback to `rlm:*`) for migration compatibility.

---

## Phase 5: Context Pruning Plugin

### Create `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py`

`after_agent_callback` for `results_processor` agent:
1. Mark artifact as consumed in registry
2. Clear both `temp:rlm:*` and legacy `rlm:*` keys (defensive during migration)
3. Preserve `rlm:iteration` counter (session-scoped)

> **Note:** After migration, `temp:rlm:*` keys auto-discard so explicit pruning is defensive. The plugin clears both variants to handle older sessions.

---

## Phase 6: job_builder CustomAgent (Deterministic Job Submission)

### Why job_builder(BaseAgent) instead of cli.py

| cli.py approach | job_builder(BaseAgent) approach |
|-----------------|--------------------------------|
| Job submission outside agent system | Job submission is an observable agent step |
| Synchronous, one job at a time | **Future**: Can launch parallel jobs |
| Logic not in session events/telemetry | Full ADK callback observability |
| Mixes orchestration with entrypoint | Clean separation of concerns |

### 6.1 Create `databricks_rlm_agent/agents/job_builder.py`

```python
from google.adk.agents.base_agent import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.genai import types

class JobBuilderAgent(BaseAgent):
    """
    Deterministic agent that submits Job_B executor runs.

    No LLM - pure Python logic:
    1. Read temp:rlm:artifact_id from state (via dual-read helper)
    2. Build job JSON with session parameters
    3. Submit via jobs.run_now()
    4. Wait for completion via get_run_output()
    5. Parse stdout between RLM markers
    6. Write results to artifact registry
    7. Set temp:rlm:execution_* keys for results_processor_agent
    """

    def __init__(self, name: str = "job_builder"):
        super().__init__(name=name, description="Deterministic job submission agent")

    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        # Step 1: Check if we have an artifact to execute
        # Uses dual-read: temp:rlm:* first, fallback to legacy rlm:*
        artifact_id = get_rlm_state(ctx.session.state, "artifact_id")
        has_code = get_rlm_state(ctx.session.state, "has_agent_code", False)

        if not artifact_id or not has_code:
            # No code to execute - skip to next agent
            yield self._create_skip_event(ctx, "No artifact with code to execute")
            return

        # Step 2: Load artifact from registry
        artifact = self._registry.get_artifact_sync(artifact_id)

        # Step 3: Build job JSON dynamically
        job_json = self._build_job_json(
            artifact_id=artifact_id,
            session_id=ctx.session.id,
            iteration=ctx.session.state.get("rlm:iteration", 1),  # session-scoped
        )

        # Step 4: Submit job via Jobs API
        run_id = self._submit_job(job_json)

        # Step 5: Wait for completion
        run_output = self._wait_for_completion(run_id)

        # Step 6: Parse stdout between markers
        stdout = self._parse_rlm_markers(run_output.logs)

        # Step 7: Update artifact registry
        self._registry.update_artifact_sync(
            artifact_id=artifact_id,
            stdout=stdout,
            stderr=run_output.error,
            status="completed",
        )

        # Step 8: Escalate to let LoopAgent invoke results_processor_agent
        yield self._create_completion_event(ctx, artifact_id, run_id)
```

### 6.2 Simplify `databricks_rlm_agent/cli.py`

cli.py becomes **just an entrypoint** - no job management logic:

```python
async def _run_orchestrator(args, logger) -> int:
    """Simplified orchestrator - just starts the agent loop."""

    # Create runner with DeltaSessionService
    runner, session_service = await create_runner(spark, catalog, schema)

    # Create or resume session
    session = await get_or_create_session(session_service, args)

    # Run the agent loop - job_builder handles execution internally
    response = await run_conversation(
        runner=runner,
        session_service=session_service,
        user_id=args.user_id,
        session_id=args.session_id,
        prompt=args.prompt,
    )

    # Record telemetry
    # ...

    return 0
```

### 6.3 Modify `databricks_rlm_agent/executor.py`

- Add `execute_from_registry(artifact_id)` function
- Print RLM markers for log parsing:
  ```
  ===RLM_EXEC_START artifact_id={id}===
  <agent code output>
  ===RLM_EXEC_END artifact_id={id} status={success|failed}===
  ```

### 6.4 Update `databricks_rlm_agent/agent.py`

Add job_builder to LoopAgent sub_agents:

```python
from databricks_rlm_agent.agents.job_builder import JobBuilderAgent

job_builder = JobBuilderAgent(name="job_builder")
results_processor_agent = LlmAgent(
    name="results_processor",
    model="gemini-3-pro-preview",
    instruction="""You are a specialist sub-agent for processing execution results.
Your role is to analyze code execution output based on the provided instruction.""",
)

root_agent = LoopAgent(
    name="orchestrator_loop",
    max_iterations=10,
    sub_agents=[
        databricks_analyst,       # 1. Generates code, calls delegate_code_results()
        job_builder,              # 2. Submits Job_B, waits, writes results
        results_processor_agent,  # 3. Processes with injected context
    ]
)
```

---

## Phase 7: Wire Plugins

### Modify `databricks_rlm_agent/agent.py`

```python
app = App(
    name="adk_poc_plugins",
    root_agent=root_agent,
    plugins=[
        safety_plugin,              # Step 1: Block destructive operations
        formatting_plugin,          # Step 2: Validate docstring format
        linting_plugin,             # Step 3: Validate Python syntax
        logging_plugin,             # Telemetry
        global_instruction_plugin,
        delegation_escalation_plugin,  # Escalates after delegate_code_results
        context_injection_plugin,      # Step 12: Inject context to results_processor_agent
        context_pruning_plugin,        # Step 13: Clear state after processing
    ]
)
```

---

## Files to Create

| File | Purpose |
|------|---------|
| `databricks_rlm_agent/artifact_registry.py` | Delta table CRUD for artifacts |
| `databricks_rlm_agent/agents/__init__.py` | Agents subpackage |
| `databricks_rlm_agent/agents/job_builder.py` | **Deterministic BaseAgent for Job_B submission** |
| `databricks_rlm_agent/utils/__init__.py` | Utils package |
| `databricks_rlm_agent/utils/docstring_parser.py` | Parse sublm_instruction from blob |
| `databricks_rlm_agent/tools/delegate_code_results.py` | **New tool** for code+instruction delegation |
| `databricks_rlm_agent/plugins/formatting_check_plugin.py` | Validate docstring format |
| `databricks_rlm_agent/plugins/code_linting_plugin.py` | Validate Python syntax |
| `databricks_rlm_agent/plugins/rlm_context_injection_plugin.py` | Inject context to results_processor_agent |
| `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py` | Clear state after processing |

## Files to Modify

| File | Changes |
|------|---------|
| `databricks_rlm_agent/agent.py` | Add job_builder + results_processor to LoopAgent, add plugins |
| `databricks_rlm_agent/run.py` | Add `artifact_service=InMemoryArtifactService()` to Runner |
| `databricks_rlm_agent/cli.py` | **Simplify to entrypoint only** - remove job logic |
| `databricks_rlm_agent/executor.py` | Read from registry, add RLM markers, load code from artifact_key |
| `databricks_rlm_agent/jobs_api.py` | Accept artifact_id parameter |
| `databricks_rlm_agent/plugins/__init__.py` | Export new plugins |
| `databricks_rlm_agent/tools/__init__.py` | Export delegate_code_results |

---

## Verification Plan

### Unit Tests
1. `test_docstring_parser.py` - Parse various blob formats
2. `test_artifact_registry.py` - CRUD operations roundtrip
3. `test_formatting_plugin.py` - Block vs allow scenarios
4. `test_linting_plugin.py` - Syntax error detection

### Integration Tests (Local)
1. `delegate_code_results()` → registry insert → state keys set
2. Plugin chain: safety → format → lint → tool execution
3. Context injection: artifact load → Content creation

### End-to-End Test (Databricks)
1. Submit prompt to orchestrator
2. Verify artifact registry row created
3. Verify Job_B reads from registry by artifact_id
4. Verify results_processor_agent receives injected context
5. Verify state keys cleared after processing

### Telemetry Events to Add
- `delegate_code_results_saved`
- `executor_started` / `executor_completed`
- `results_processor_agent_started` / `results_processor_agent_completed`
- `artifact_consumed`

---

## Key ADK Patterns Used

1. **State Propagation**: Invocation glue uses `temp:rlm:*` keys (auto-discarded after invocation), while `rlm:iteration` is session-scoped. Keys propagate via `EventActions.state_delta`.
2. **Escalation Signal**: `tool_context.actions.escalate = True` stops current agent
3. **Context Injection**: `before_agent_callback` returns `types.Content` to inject user message
4. **State Pruning**: `after_agent_callback` sets state keys to `None` to delete (defensive for `temp:` keys)
5. **Temp State**: `temp:rlm:*` for within-invocation glue, `temp:parsed_blob` for delegation blob caching
6. **ArtifactService**: `context.save_artifact(filename, part)` / `context.load_artifact(filename)` for binary storage
7. **Hybrid Storage**: Delta table for metadata + ADK ArtifactService for binary content
8. **Dual-Read Pattern**: `get_rlm_state()` helper reads `temp:rlm:*` first, falls back to legacy `rlm:*` during migration

---

## Future Enhancements

### Dynamic Job Parallelism

The `job_builder(BaseAgent)` design enables future parallel job execution:

```python
# Future enhancement (not in current scope)
class ParallelJobBuilderAgent(BaseAgent):
    """
    Enhanced job_builder that can launch multiple executor jobs.

    The upstream agent can specify parallelism:
    - tool_context.state["temp:rlm:parallel_artifacts"] = [artifact_id_1, artifact_id_2, ...]
    - job_builder launches N jobs concurrently
    - Waits for all completions via asyncio.gather()
    - Aggregates results for downstream
    """
```

### DeltaArtifactService (Production Persistence)

Replace `InMemoryArtifactService` with a custom implementation for production:

```python
# Future: databricks_rlm_agent/artifacts/delta_artifact_service.py
from google.adk.artifacts.base_artifact_service import BaseArtifactService

class DeltaArtifactService(BaseArtifactService):
    """
    ArtifactService implementation using Unity Catalog Volumes.

    Stores artifacts as files in UC Volumes:
    - Path: /Volumes/{catalog}/{schema}/artifacts/{app_name}/{user_id}/{session_id}/{filename}.v{version}

    Benefits:
    - Durable storage across job restarts
    - Accessible from Job_B executor
    - Auditable via Delta table metadata
    """
```

This is **not in current scope** - use `InMemoryArtifactService` for initial development.

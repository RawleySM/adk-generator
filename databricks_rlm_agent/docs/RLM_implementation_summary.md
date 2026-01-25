# RLM Workflow Implementation Summary

## Overview

This document summarizes the implementation of the 13-step RLM (Recursive Language Model) workflow that enables code generation, execution, and result processing through a coordinated agent system.

The core innovation is an **Artifact Registry** Delta table that decouples code generation from execution, enabling proper state propagation between agents.

## Architecture

### Workflow Diagram

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
        3. results_processor_agent (LlmAgent)  - processes results with injected context
```

## Files Created

### Phase 1: Artifact Registry Foundation

| File | Purpose |
|------|---------|
| `artifact_registry.py` | Delta table CRUD for artifact metadata tracking |
| `utils/__init__.py` | Utils package initialization |
| `utils/docstring_parser.py` | Parse `'''<instruction>'''\n<code>` format into components |

### Phase 2: delegate_code_results Tool

| File | Purpose |
|------|---------|
| `tools/delegate_code_results.py` | Tool for delegating code execution with state propagation |

### Phase 3: Validation Plugins

| File | Purpose |
|------|---------|
| `plugins/formatting_check_plugin.py` | Validates delegation blob docstring format |
| `plugins/code_linting_plugin.py` | Validates Python syntax using ast.parse() |

### Phase 4-5: Context Management Plugins

| File | Purpose |
|------|---------|
| `plugins/rlm_context_injection_plugin.py` | Injects execution results into results_processor_agent |
| `plugins/rlm_context_pruning_plugin.py` | Clears state after results_processor_agent completes |

### Phase 6: job_builder Agent

| File | Purpose |
|------|---------|
| `agents/__init__.py` | Agents package initialization |
| `agents/job_builder.py` | Deterministic BaseAgent for Job_B submission and result collection |

## Files Modified

| File | Changes |
|------|---------|
| `agent.py` | Added job_builder, results_processor_agent to LoopAgent; wired all new plugins |
| `run.py` | Added InMemoryArtifactService to Runner configuration |
| `executor.py` | Added `execute_from_registry()` function and RLM markers for log parsing |
| `plugins/__init__.py` | Exported new plugins (FormattingCheckPlugin, CodeLintingPlugin, RlmContextInjectionPlugin, RlmContextPruningPlugin) |
| `tools/__init__.py` | Exported delegate_code_results tool |

## Key ADK Patterns Used

### 1. State Propagation
```python
tool_context.state["rlm:artifact_id"] = artifact_id
tool_context.state["rlm:sublm_instruction"] = parsed.sublm_instruction
tool_context.state["rlm:has_agent_code"] = bool(agent_code)
tool_context.state["rlm:iteration"] += 1
```
State keys persist via `EventActions.state_delta`.

### 2. Escalation Signal
```python
tool_context.actions.escalate = True
```
Stops the current agent and lets LoopAgent invoke the next sub-agent.

### 3. Context Injection

Context injection reads from **state keys** (not directly from the registry):
```python
async def before_agent_callback(self, *, callback_context):
    # Read from state (set by job_builder)
    artifact_id = callback_context.state.get("rlm:artifact_id")
    stdout = callback_context.state.get("rlm:execution_stdout")
    stderr = callback_context.state.get("rlm:execution_stderr")

    # Return types.Content to inject as user message
    return types.Content(
        role="user",
        parts=[types.Part.from_text(text=content_text)],
    )
```

> **Note:** The plugin attempts to load stdout/stderr from ArtifactService keys if available, falling back to direct state values. This "state-driven" approach works because `job_builder` sets execution results in state before `results_processor_agent` runs.

### 4. State Pruning
```python
async def after_agent_callback(self, *, callback_context):
    # Set to None to delete keys
    callback_context.state["rlm:artifact_id"] = None
```

### 5. ArtifactService Integration
```python
from google.adk.artifacts import InMemoryArtifactService

artifact_service = InMemoryArtifactService()
runner = Runner(
    agent=root_agent,
    artifact_service=artifact_service,
    ...
)
```

### 6. Hybrid Storage Design

| Layer | Storage | Purpose |
|-------|---------|---------|
| Metadata Registry | Delta Table | Structured tracking (artifact_id, status, timestamps) |
| Binary Content | ADK ArtifactService | Large blob storage within orchestrator runtime |
| **Cross-Job Handoff** | UC Volumes | Code files written to `/Volumes/{catalog}/{schema}/artifacts/` |

> **Important:** `InMemoryArtifactService` is **ephemeral** - it doesn't persist across job boundaries. For Job_A → Job_B code handoff, `job_builder` writes code to **UC Volumes paths** which the executor reads. The registry stores a `code_artifact_key` reference, but the executor resolves this to a Volumes path.

## Plugin Execution Order

### Plugin Wiring (App vs Runner)

Plugins are configured in **two locations** with different scopes:

**App plugins** (`agent.py`) - RLM workflow-specific plugins:
1. **safety_plugin** - Block destructive operations first
2. **formatting_plugin** - Validate docstring format
3. **linting_plugin** - Validate Python syntax
4. **logging_plugin** - Telemetry and logging
5. **global_instruction_plugin** - Inject global instructions
6. **context_injection_plugin** - Inject context to results_processor
7. **context_pruning_plugin** - Clear state after processing

**Runner plugins** (`run.py`) - Core infrastructure plugins:
- `logging_plugin` - Session-level logging
- `global_instruction_plugin` - Global context injection

> **Note:** ADK combines App and Runner plugins. The App plugin list is the **authoritative chain** for RLM workflow behavior.

## State Keys

### RLM Workflow State Keys

| Key | Type | Description | Set By | Cleared By |
|-----|------|-------------|--------|------------|
| `rlm:artifact_id` | str | Current artifact identifier | delegate_code_results | pruning_plugin |
| `rlm:sublm_instruction` | str | Instruction for results_processor_agent | delegate_code_results | pruning_plugin |
| `rlm:has_agent_code` | bool | Whether artifact has code to execute | delegate_code_results | pruning_plugin |
| `rlm:iteration` | int | Current loop iteration | delegate_code_results | **Preserved** |
| `rlm:code_artifact_key` | str | Reference to code in ArtifactService | delegate_code_results | pruning_plugin |
| `rlm:session_id` | str | Session identifier | delegate_code_results | pruning_plugin |
| `rlm:invocation_id` | str | Invocation identifier | delegate_code_results | pruning_plugin |
| `rlm:execution_stdout` | str | Captured stdout from execution | job_builder | pruning_plugin |
| `rlm:execution_stderr` | str | Captured stderr from execution | job_builder | pruning_plugin |
| `rlm:execution_success` | bool | Whether execution succeeded | job_builder | pruning_plugin |
| `rlm:databricks_run_id` | str | Databricks job run ID | job_builder | pruning_plugin |
| `rlm:run_url` | str | URL to Databricks run | job_builder | pruning_plugin |

### Temporary State Keys

| Key | Type | Description |
|-----|------|-------------|
| `temp:parsed_blob` | dict | Parsed delegation blob (within invocation) |

## RLM Markers for Log Parsing

The executor wraps output in markers for reliable log parsing:

```
===RLM_EXEC_START artifact_id={id}===
<agent code output>
===RLM_EXEC_END artifact_id={id} status={success|failed}===
```

## Delta Table Schema

### artifact_registry Table

```sql
CREATE TABLE {catalog}.{schema}.artifact_registry (
    artifact_id STRING NOT NULL,
    session_id STRING NOT NULL,
    invocation_id STRING NOT NULL,
    iteration INT NOT NULL,
    artifact_type STRING NOT NULL,
    sublm_instruction STRING,
    code_artifact_key STRING,
    stdout_artifact_key STRING,
    stderr_artifact_key STRING,
    status STRING NOT NULL,
    metadata_json STRING,
    created_time TIMESTAMP NOT NULL,
    updated_time TIMESTAMP NOT NULL,
    consumed_time TIMESTAMP
) USING DELTA PARTITIONED BY (session_id)
```

## Future Enhancements

### Dynamic Job Parallelism

The `job_builder(BaseAgent)` design enables future parallel job execution:

```python
class ParallelJobBuilderAgent(BaseAgent):
    """
    Enhanced job_builder that can launch multiple executor jobs.
    - tool_context.state["rlm:parallel_artifacts"] = [artifact_id_1, ...]
    - Launches N jobs concurrently
    - Waits via asyncio.gather()
    """
```

### DeltaArtifactService (Production Persistence)

Replace `InMemoryArtifactService` with a custom implementation:

```python
class DeltaArtifactService(BaseArtifactService):
    """
    ArtifactService using Unity Catalog Volumes.
    Path: /Volumes/{catalog}/{schema}/artifacts/{app_name}/{user_id}/{session_id}/{filename}
    """
```

## Testing Checklist

### Unit Tests
- [ ] `test_docstring_parser.py` - Parse various blob formats
- [ ] `test_artifact_registry.py` - CRUD operations roundtrip
- [ ] `test_formatting_plugin.py` - Block vs allow scenarios
- [ ] `test_linting_plugin.py` - Syntax error detection

### Integration Tests
- [ ] `delegate_code_results()` → registry insert → state keys set
- [ ] Plugin chain: safety → format → lint → tool execution
- [ ] Context injection: artifact load → Content creation

### End-to-End Test (Databricks)
- [ ] Submit prompt to orchestrator
- [ ] Verify artifact registry row created
- [ ] Verify Job_B reads from registry by artifact_id
- [ ] Verify results_processor_agent receives injected context
- [ ] Verify state keys cleared after processing

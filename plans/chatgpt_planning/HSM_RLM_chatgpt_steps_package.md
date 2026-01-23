Here’s a **drop-in markdown “Plugin Cookbook”** you can paste into your repo as-is (e.g., `docs/adk/plugins_cookbook.md`). Everything below is **Plugin Modules + plugin callbacks only** (no agent callbacks).

```markdown
# ADK Plugins Cookbook (Plugin Callbacks Only)

This doc is a practical cookbook for **Google ADK (Agent Development Kit)** **Plugin Modules** (`BasePlugin`) and **plugin callback hooks** (global, runner/app-level). It intentionally avoids **Agent Callbacks**.

Core reference: ADK Plugins docs + prebuilt plugins (Logging, ContextFilter, GlobalInstruction, SaveFilesAsArtifacts, ReflectAndRetryTool). 

---

## 0) Mental model: where plugins sit

Plugins are **cross-cutting**. Register once on the **Runner/App**, and they apply to **every Agent / Model / Tool** under that runner. 

### Lifecycle sketch (typical)
```

User message
└─ on_user_message_callback
└─ before_run_callback
└─ before_agent_callback
└─ before_model_callback
└─ (model call)
└─ after_model_callback
└─ before_tool_callback
└─ (tool call)
└─ after_tool_callback
└─ after_agent_callback
└─ after_run_callback
Streaming events → on_event_callback
Errors → on_model_error_callback / on_tool_error_callback

````

**Power move:** In `on_tool_error_callback`, you can return a `dict` to **suppress the exception** and keep the run moving (and `after_tool_callback` still triggers). 

---

## 1) Callback selection matrix (use this to avoid plugin spaghetti)

| Concern / Goal | Primary callbacks | Notes |
|---|---|---|
| Global guardrails / “system policy” | `before_agent_callback` (and/or `before_model_callback`) | Use prebuilt **GlobalInstructionPlugin** as blueprint.  |
| Context hygiene / compaction | `before_model_callback` / `after_model_callback` | Use **ContextFilterPlugin** blueprint.  |
| Observability / logging / tracing | `before_*` + `after_*` + `on_event_callback` + `on_*_error_callback` | Prebuilt **LoggingPlugin** shows the coverage approach.  |
| Tool resilience (retry/fallback/circuit breaker) | `on_tool_error_callback` (+ `before_tool_callback`) | Returning `dict` suppresses exception.  |
| Artifact capture / provenance | `after_tool_callback` (sometimes `before_tool_callback`) | Blueprint: **SaveFilesAsArtifactsPlugin**.  |
| Rate limits / budgets | `before_model_callback`, `before_tool_callback` | Block/slowdown early; record metrics on errors too. |
| Caching | `before_model_callback` (serve cache), `after_model_callback` (populate) | Make key deterministic. |
| PII redaction | `on_user_message_callback`, `before_model_callback`, `on_event_callback` | Redact inbound and outbound. |
| Output schema enforcement | `after_model_callback`, `after_tool_callback` | Validate + rewrite loop (or raise). |

---

## 2) Recipe: Reflect + Retry + Fallback (Tools)

### When to use
- Tools hit flaky APIs (429s, timeouts), transient infra errors, or known brittle endpoints.
- You want consistent retry logic across *all* tools.

### Pattern
- Categorize errors (transient vs permanent).
- Retry with bounded backoff.
- Optional: fallback to an alternate tool.
- Optional: circuit breaker (fail-fast after N failures per tool/service).

> ADK-supported superpower: return a `dict` from `on_tool_error_callback` to suppress the exception and continue. 

```python
from google.adk.plugins import BasePlugin
from google.adk.tools import BaseTool, ToolContext

class ToolResiliencePlugin(BasePlugin):
    def __init__(self, *, max_retries=2):
        super().__init__(name="tool_resilience")
        self.max_retries = max_retries
        self.fail_counts = {}  # (tool_name -> int) optional circuit breaker

    async def before_tool_callback(self, *, tool: BaseTool, tool_args: dict, tool_context: ToolContext):
        # Example: circuit breaker gate
        if self.fail_counts.get(tool.name, 0) >= 5:
            raise RuntimeError(f"Circuit open for tool={tool.name}")

    async def on_tool_error_callback(self, *, tool: BaseTool, tool_args: dict, tool_context: ToolContext, error: Exception):
        # 1) classify
        transient = isinstance(error, TimeoutError) or "429" in str(error)

        # 2) bounded retry loop (pseudo)
        if transient:
            attempt = tool_context.state.get("attempt", 0) + 1
            tool_context.state["attempt"] = attempt
            if attempt <= self.max_retries:
                # Option A: rethrow and let caller/tool runner retry if your system supports it
                # Option B: directly return a fallback result (suppresses exception)
                return {"status": "retry_scheduled", "attempt": attempt}

        # 3) record and fail (or fallback)
        self.fail_counts[tool.name] = self.fail_counts.get(tool.name, 0) + 1
        # Return dict to suppress exception with a graceful fallback:
        return {"status": "failed", "tool": tool.name, "error": str(error)}
````

**Notes**

* Keep retries **bounded** and **idempotent**.
* Favor returning a structured failure result over crashing the whole run in production flows.

---

## 3) Recipe: Global instruction injection (real guardrails)

### When to use

* You want a single, authoritative set of constraints (safety, style, compliance, tool usage) applied everywhere.
* You’re tired of copying “policy prompts” into every agent.

Blueprint: **GlobalInstructionPlugin**.

```python
from google.adk.plugins import BasePlugin

class GuardrailsPlugin(BasePlugin):
    def __init__(self, *, instruction: str):
        super().__init__(name="guardrails")
        self.instruction = instruction

    async def before_agent_callback(self, *, invocation_context, agent):
        # Attach a global instruction in the way your agent runtime expects.
        # (Exact mechanism depends on ADK agent config objects.)
        invocation_context.state["global_instruction"] = self.instruction
```

**Tips**

* Separate “policy” vs “product behavior” instructions so you can version them independently.
* Treat this plugin as **immutable** in production (config changes must be reviewed).

---

## 4) Recipe: Context filtering / compaction (token sanity)

Blueprint: **ContextFilterPlugin**.

### When to use

* Long-running runs where tool outputs are large.
* You see “orphaned” tool/function artifacts polluting context.

```python
from google.adk.plugins import BasePlugin

class ContextHygienePlugin(BasePlugin):
    def __init__(self, *, max_chars=100_000):
        super().__init__(name="context_hygiene")
        self.max_chars = max_chars

    async def before_model_callback(self, *, invocation_context, model, messages):
        # Example: truncate huge tool outputs, keep only summaries/headers
        compacted = []
        total = 0
        for m in reversed(messages):
            s = str(m)
            if total + len(s) > self.max_chars:
                break
            compacted.append(m)
            total += len(s)
        return list(reversed(compacted))  # return replacement messages if supported
```

**Tips**

* Always preserve: system instructions, last user request, and any “contract” messages (schemas, constraints).
* Consider hashing + artifacting large tool outputs instead of keeping them in context.

---

## 5) Recipe: Save tool outputs as artifacts (provenance)

Blueprint: **SaveFilesAsArtifactsPlugin**.

### When to use

* Tools read/write files (reports, CSVs, PDFs, images).
* You need traceability for audits or reruns.

```python
from google.adk.plugins import BasePlugin

class ArtifactCapturePlugin(BasePlugin):
    def __init__(self, *, artifact_service):
        super().__init__(name="artifact_capture")
        self.artifacts = artifact_service

    async def after_tool_callback(self, *, tool, tool_args, tool_context, tool_response):
        # Detect file outputs; persist with metadata (tool name, args hash, timestamps)
        paths = tool_response.get("paths", []) if isinstance(tool_response, dict) else []
        for p in paths:
            self.artifacts.save(path=p, metadata={"tool": tool.name, "args": tool_args})
```

**Tips**

* Store a content hash; avoid duplicating identical files.
* Attach the artifact reference ID back into the tool response for downstream steps.

---

## 6) Recipe: Observability plugin (structured logs + events + errors)

Blueprint: **LoggingPlugin**.

### When to use

* You need consistent logs across all agents/tools/models.
* You want to debug “why did it do that” using traces instead of vibes.

```python
from google.adk.plugins import BasePlugin

class ObservabilityPlugin(BasePlugin):
    def __init__(self, *, sink):
        super().__init__(name="observability")
        self.sink = sink  # e.g., stdout JSON, MLflow, OpenTelemetry collector

    async def before_model_callback(self, *, invocation_context, model, messages):
        self.sink.write({"evt":"before_model", "model": getattr(model, "name", None), "n_msgs": len(messages)})

    async def after_model_callback(self, *, invocation_context, model, response):
        self.sink.write({"evt":"after_model", "model": getattr(model, "name", None), "summary": str(response)[:500]})

    async def on_tool_error_callback(self, *, tool, tool_args, tool_context, error):
        self.sink.write({"evt":"tool_error", "tool": tool.name, "error": str(error)})
        # choose: rethrow vs fallback dict

    async def on_event_callback(self, *, invocation_context, event):
        # Optionally enrich streamed events with trace IDs, timing, etc.
        event.metadata = {**getattr(event, "metadata", {}), "trace_id": invocation_context.trace_id}
        return event
```

**Tips**

* Emit **correlation IDs** (run_id, trace_id, agent_id, tool_call_id).
* For high volume, sample tool payloads; always log metadata.

---

## 7) Recipe: Budgets & rate limiting (don’t get melted)

### When to use

* You need hard ceilings on tool calls, model calls, tokens, or wall-clock time.
* You want predictable costs / SLAs.

```python
from google.adk.plugins import BasePlugin

class BudgetPlugin(BasePlugin):
    def __init__(self, *, max_model_calls=50, max_tool_calls=200):
        super().__init__(name="budget")
        self.max_model_calls = max_model_calls
        self.max_tool_calls = max_tool_calls

    async def before_model_callback(self, *, invocation_context, model, messages):
        c = invocation_context.state.get("model_calls", 0) + 1
        invocation_context.state["model_calls"] = c
        if c > self.max_model_calls:
            raise RuntimeError("Model call budget exceeded")

    async def before_tool_callback(self, *, tool, tool_args, tool_context):
        c = tool_context.state.get("tool_calls", 0) + 1
        tool_context.state["tool_calls"] = c
        if c > self.max_tool_calls:
            raise RuntimeError("Tool call budget exceeded")
```

---

## 8) Recipe: Caching model calls (speed + cost)

### When to use

* Repeated prompts / deterministic model usage.
* You want to stabilize latency.

```python
from google.adk.plugins import BasePlugin

class ModelCachePlugin(BasePlugin):
    def __init__(self, *, cache):
        super().__init__(name="model_cache")
        self.cache = cache

    async def before_model_callback(self, *, invocation_context, model, messages):
        key = (getattr(model, "name", "model"), hash(str(messages)))
        hit = self.cache.get(key)
        if hit is not None:
            return hit  # if ADK allows short-circuiting by returning a response

    async def after_model_callback(self, *, invocation_context, model, response):
        key = (getattr(model, "name", "model"), invocation_context.state.get("last_messages_hash"))
        self.cache.set(key, response)
```

**Tips**

* Only cache when messages are normalized (strip timestamps, run IDs).
* Consider caching only for “planner” steps, not for stochastic generation.

---

## 9) Recipe: Redaction (PII / secrets in + out)

### When to use

* You operate on enterprise data, logs, or regulated content.
* You stream events to UIs and must prevent leaks.

```python
from google.adk.plugins import BasePlugin

class RedactionPlugin(BasePlugin):
    def __init__(self, *, redactor):
        super().__init__(name="redaction")
        self.redactor = redactor

    async def on_user_message_callback(self, *, invocation_context, user_message):
        return self.redactor.clean(user_message)

    async def before_model_callback(self, *, invocation_context, model, messages):
        return [self.redactor.clean(m) for m in messages]

    async def on_event_callback(self, *, invocation_context, event):
        event.text = self.redactor.clean(getattr(event, "text", ""))
        return event
```

---

## 10) Composition rules (keep plugins sane)

### Make plugins single-purpose

* Prefer 5 small plugins over 1 god plugin.
* One plugin = one axis: resiliency, observability, provenance, policy, budgets.

### Determinism boundaries

* Keep guardrails and budgets deterministic.
* Allow retries/fallback to be policy-driven but still bounded.

### Don’t mutate everything

* Only rewrite messages/events when you must; otherwise enrich metadata.

---

## 11) Production checklist

* [ ] Every plugin has an explicit **name** and versioned config
* [ ] Budgets (model/tool/time) are enforced early
* [ ] Tool failures are either retried boundedly or return structured fallback
* [ ] Large tool outputs are artifacted and only referenced in context
* [ ] Logs include correlation IDs and error details
* [ ] Redaction runs on inbound + outbound paths
* [ ] Unit tests cover: tool error suppression (return dict), circuit breaker, cache hits, event enrichment

---

## References

* ADK Plugins documentation: callbacks, registration, built-in plugin list, error suppression semantics.
* Prebuilt plugin implementations in `adk-python`: LoggingPlugin, ContextFilterPlugin, GlobalInstructionPlugin, SaveFilesAsArtifactsPlugin.
* Reflect-and-retry tool plugin concepts / examples.

```

If you want, tell me your **target architecture** (single Runner vs multi-runner, long-running “dev loop” vs short “bounded executor” runs), and I’ll tailor this cookbook into a **tight “production plugin set”** (like: 6 plugins you always register, their order, and the exact invariants each enforces).
::contentReference[oaicite:16]{index=16}
```

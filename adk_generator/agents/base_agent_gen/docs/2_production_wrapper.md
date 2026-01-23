# Production Wrapper (`App`)

Wraps the `root_agent` to enable production-grade runtime features that an `Agent` cannot handle alone.

```python
from google.adk.apps.app import App
from google.adk.agents.context_cache_config import ContextCacheConfig
from google.adk.apps.events_compaction_config import EventsCompactionConfig
from google.adk.apps.resumability_config import ResumabilityConfig

production_app = App(
    name="my_app",
    root_agent=my_agent,
    # 1. Reduce costs/latency for long contexts
    context_cache_config=ContextCacheConfig(min_tokens=2048, ttl_seconds=600),
    # 2. Allow resuming crashed workflows from last state
    resumability_config=ResumabilityConfig(is_resumable=True),
    # 3. Manage long conversation history automatically
    events_compaction_config=EventsCompactionConfig(compaction_interval=5, overlap_size=1)
)

# Usage: Pass 'app' instead of 'agent' to the Runner
# runner = Runner(app=production_app, ...)
```

## App Configuration Options

### Context Cache Config
Reduces costs and latency for long contexts by caching repeated prompt prefixes.

```python
context_cache_config=ContextCacheConfig(
    min_tokens=2048,    # Minimum tokens before caching kicks in
    ttl_seconds=600     # How long to keep the cache (10 minutes)
)
```

### Resumability Config
Allows workflows to resume from the last state if they crash or are interrupted.

```python
resumability_config=ResumabilityConfig(
    is_resumable=True   # Enable workflow resumption
)
```

### Events Compaction Config
Automatically manages long conversation history by summarizing older events.

```python
events_compaction_config=EventsCompactionConfig(
    compaction_interval=5,  # Compact every 5 invocations
    overlap_size=1          # Keep 1 previous invocation for context
)
```

## Build Agents without Code (Agent Config)

ADK allows you to define agents, tools, and even multi-agent workflows using a simple YAML format, eliminating the need to write Python code for orchestration. This is ideal for rapid prototyping and for non-programmers to configure agents.

### Getting Started with Agent Config

*   **Create a Config-based Agent**:
    ```bash
    adk create --type=config my_yaml_agent
    ```
    This generates a `my_yaml_agent/` folder with `root_agent.yaml` and `.env` files.

*   **Environment Setup** (in `.env` file):
    ```bash
    # For Google AI Studio (simpler setup)
    GOOGLE_GENAI_USE_VERTEXAI=0
    GOOGLE_API_KEY=<your-Google-Gemini-API-key>
    
    # For Google Cloud Vertex AI (production)
    GOOGLE_GENAI_USE_VERTEXAI=1
    GOOGLE_CLOUD_PROJECT=<your_gcp_project>
    GOOGLE_CLOUD_LOCATION=us-east1
    ```

### Core Agent Config Structure

*   **Basic Agent (`root_agent.yaml`)**:
    ```yaml
    # yaml-language-server: $schema=https://raw.githubusercontent.com/google/adk-python/refs/heads/main/src/google/adk/agents/config_schemas/AgentConfig.json
    name: assistant_agent
    model: gemini-2.5-flash
    description: A helper agent that can answer users' various questions.
    instruction: You are an agent to help answer users' various questions.
    ```

*   **Agent with Built-in Tools**:
    ```yaml
    name: search_agent
    model: gemini-2.0-flash
    description: 'an agent whose job it is to perform Google search queries and answer questions about the results.'
    instruction: You are an agent whose job is to perform Google search queries and answer questions about the results.
    tools:
      - name: google_search # Built-in ADK tool
    ```

### Loading Agent Config in Python

```python
from google.adk.agents import config_agent_utils
root_agent = config_agent_utils.from_config("{agent_folder}/root_agent.yaml")
```

### Running Agent Config Agents

From the agent directory, use any of these commands:
*   `adk web` - Launch web UI interface
*   `adk run` - Run in terminal without UI
*   `adk api_server` - Run as a service for other applications


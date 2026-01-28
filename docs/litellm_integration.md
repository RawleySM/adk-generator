# LiteLLM Model Integration

This document describes how to configure the RLM Agent to use different LLM providers via LiteLLM, and how the automatic fallback mechanism works for content-policy blocking errors.

## Overview

The RLM Agent supports two model provider modes:

1. **Native ADK Gemini** (default): Uses Google ADK's built-in Gemini connector
2. **LiteLLM**: Uses the [LiteLLM](https://docs.litellm.ai/) library to access 100+ LLM providers

Additionally, the agent includes an automatic **fallback mechanism** that switches providers when content-policy blocking errors occur.

## Configuration

Model selection is controlled via environment variables or Databricks job parameters.

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ADK_MODEL_PROVIDER` | `gemini` | Primary provider: `gemini` or `litellm` |
| `ADK_GEMINI_MODEL` | `gemini-3-pro-preview` | Model string for native Gemini |
| `ADK_LITELLM_MODEL` | `openai/gpt-4o` | Primary model string for LiteLLM |
| `ADK_LITELLM_FALLBACK_MODELS` | (empty) | Comma-separated fallback chain for LiteLLM |
| `ADK_FALLBACK_ON_BLOCKED` | `true` | Enable fallback on content-policy errors |
| `ADK_FALLBACK_GEMINI_TO_LITELLM` | `true` | Enable Gemini→LiteLLM fallback |

### API Keys

Different providers require different API keys:

| Provider | Secret Key | Environment Variable |
|----------|------------|---------------------|
| Native Gemini | `google-api-key` | `GOOGLE_API_KEY` |
| LiteLLM Gemini | `gemini-api-key` | `GEMINI_API_KEY` |
| LiteLLM OpenAI | `openai-api-key` | `OPENAI_API_KEY` |
| LiteLLM Anthropic | `anthropic-api-key` | `ANTHROPIC_API_KEY` |

Configure these in your Databricks secret scope (`adk-secrets` by default).

## Usage Examples

### Example 1: Default (Native Gemini)

No configuration needed. The agent uses native ADK Gemini by default:

```bash
# No environment variables needed - uses gemini-3-pro-preview
```

### Example 2: Use OpenAI GPT-4o via LiteLLM

```bash
export ADK_MODEL_PROVIDER=litellm
export ADK_LITELLM_MODEL=openai/gpt-4o
export OPENAI_API_KEY=sk-...
```

### Example 2b: OpenAI **Responses API** (e.g. `openai/o1-pro`)

Some newer OpenAI models are best used via the OpenAI **Responses API**. LiteLLM supports this via `litellm.responses()` (and via LiteLLM Proxy’s `/v1/responses` endpoint).

References:
- [LiteLLM `/responses` overview](https://docs.litellm.ai/docs/response_api)
- [LiteLLM OpenAI Responses API](https://docs.litellm.ai/docs/providers/openai/responses_api)

#### Option A: Direct LiteLLM Python SDK (Responses API)

Set:

```bash
export OPENAI_API_KEY=sk-...
```

Then call:

```python
import os
import litellm

response = litellm.responses(
    model=os.environ.get("ADK_LITELLM_MODEL", "openai/o1-pro"),
    input="Tell me a three sentence bedtime story about a unicorn.",
    max_output_tokens=200,
)

print(response)
```

#### Option B: LiteLLM Proxy (OpenAI SDK → `/responses`)

1) Create a proxy config (this repo includes `resources/litellm_responses_proxy.yaml`).

2) Start the proxy:

```bash
export OPENAI_API_KEY=sk-...
litellm --config resources/litellm_responses_proxy.yaml
```

If you want the **RLM agent** (which uses ADK’s `LiteLlm` wrapper) to route through the same proxy, set:

```bash
export ADK_MODEL_PROVIDER=litellm
export ADK_LITELLM_MODEL=openai/o1-pro
export ADK_LITELLM_API_BASE=http://localhost:4000
```

3) Call the proxy using the OpenAI SDK:

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:4000", api_key="your-proxy-api-key")
resp = client.responses.create(
    model="openai/o1-pro",
    input="Tell me a three sentence bedtime story about a unicorn.",
)
print(resp)
```

### Example 3: Use Anthropic Claude via LiteLLM

```bash
export ADK_MODEL_PROVIDER=litellm
export ADK_LITELLM_MODEL=anthropic/claude-3-opus-20240229
export ANTHROPIC_API_KEY=sk-ant-...
```

### Example 4: Use Gemini via LiteLLM (AI Studio)

```bash
export ADK_MODEL_PROVIDER=litellm
export ADK_LITELLM_MODEL=gemini/gemini-2.0-flash
export GEMINI_API_KEY=...
```

### Example 5: Configure Fallback Chain

```bash
# Primary: OpenAI GPT-4o
# Fallbacks: GPT-4o-mini → Claude Haiku
export ADK_MODEL_PROVIDER=litellm
export ADK_LITELLM_MODEL=openai/gpt-4o
export ADK_LITELLM_FALLBACK_MODELS=openai/gpt-4o-mini,anthropic/claude-3-haiku-20240307
export ADK_FALLBACK_ON_BLOCKED=true
```

### Example 6: Gemini with LiteLLM Fallback

```bash
# Primary: Native Gemini
# Fallback: OpenAI if Gemini blocks content
export ADK_MODEL_PROVIDER=gemini
export ADK_GEMINI_MODEL=gemini-3-pro-preview
export ADK_LITELLM_MODEL=openai/gpt-4o
export ADK_FALLBACK_ON_BLOCKED=true
export ADK_FALLBACK_GEMINI_TO_LITELLM=true
```

## Databricks Job Configuration

Add these parameters to your orchestrator job (`resources/rlm_orchestrator_job.yml`):

```yaml
parameters:
  # ... existing parameters ...
  - name: ADK_MODEL_PROVIDER
    default: "gemini"
  - name: ADK_GEMINI_MODEL
    default: "gemini-3-pro-preview"
  - name: ADK_LITELLM_MODEL
    default: "openai/gpt-4o"
  - name: ADK_LITELLM_FALLBACK_MODELS
    default: ""
  - name: ADK_FALLBACK_ON_BLOCKED
    default: "true"
  - name: ADK_FALLBACK_GEMINI_TO_LITELLM
    default: "true"
```

## Fallback Mechanism

The agent automatically detects content-policy blocking errors and switches to fallback models.

### How It Works

1. **Error Detection**: The agent monitors for blocking errors by checking:
   - LiteLLM's `ContentPolicyViolationError` exception
   - Error messages containing keywords like `blocked`, `SAFETY`, `content policy`, etc.
   - Exception type names like `BlockedPromptException`, `SafetyException`

2. **Fallback Chain**: When a blocking error is detected:
   - If using Gemini and `ADK_FALLBACK_GEMINI_TO_LITELLM=true`, switch to LiteLLM primary model
   - If using LiteLLM, advance to the next model in `ADK_LITELLM_FALLBACK_MODELS`
   - If no more fallbacks available, the error is raised

3. **Logging**: All fallback events are logged for observability:
   ```
   [FALLBACK_ROUTER] Content-policy block detected! Falling back: gemini/gemini-3-pro-preview → litellm/openai/gpt-4o
   ```

### Fallback Order (Gemini Primary)

```
gemini/gemini-3-pro-preview
    ↓ (on block)
litellm/openai/gpt-4o (ADK_LITELLM_MODEL)
    ↓ (on block)
litellm/openai/gpt-4o-mini (first in ADK_LITELLM_FALLBACK_MODELS)
    ↓ (on block)
litellm/anthropic/claude-3-haiku (second in ADK_LITELLM_FALLBACK_MODELS)
    ↓ (on block)
Error raised (no more fallbacks)
```

### Fallback Order (LiteLLM Primary)

```
litellm/openai/gpt-4o (ADK_LITELLM_MODEL)
    ↓ (on block)
litellm/openai/gpt-4o-mini (first in ADK_LITELLM_FALLBACK_MODELS)
    ↓ (on block)
litellm/anthropic/claude-3-haiku (second in ADK_LITELLM_FALLBACK_MODELS)
    ↓ (on block)
Error raised (no more fallbacks)
```

## LiteLLM Model Strings

LiteLLM uses a `provider/model` format for model strings. Common examples:

| Provider | Model String | Required Key |
|----------|--------------|--------------|
| OpenAI | `openai/gpt-4o` | `OPENAI_API_KEY` |
| OpenAI | `openai/gpt-4o-mini` | `OPENAI_API_KEY` |
| Anthropic | `anthropic/claude-3-opus-20240229` | `ANTHROPIC_API_KEY` |
| Anthropic | `anthropic/claude-3-haiku-20240307` | `ANTHROPIC_API_KEY` |
| Google AI Studio | `gemini/gemini-2.0-flash` | `GEMINI_API_KEY` |
| Vertex AI | `vertex_ai/gemini-2.0-flash` | GCP credentials |

See the [LiteLLM Providers Documentation](https://docs.litellm.ai/docs/providers) for the full list.

## Disabling Fallback

To disable automatic fallback:

```bash
export ADK_FALLBACK_ON_BLOCKED=false
```

To disable only Gemini→LiteLLM fallback (keep LiteLLM chain fallback):

```bash
export ADK_FALLBACK_GEMINI_TO_LITELLM=false
```

## Troubleshooting

### "Required secrets not found" Error

Ensure the appropriate API key is configured in your Databricks secret scope:

```python
# Check which secrets are loaded
from databricks_rlm_agent.secrets import validate_secrets
print(validate_secrets())
```

### Fallback Not Triggering

Check that:
1. `ADK_FALLBACK_ON_BLOCKED=true` is set
2. For Gemini→LiteLLM: `ADK_FALLBACK_GEMINI_TO_LITELLM=true`
3. The error is actually a content-policy block (check logs for `[FALLBACK]` messages)

### Model Not Found

Verify the model string format matches LiteLLM's expected format:
- OpenAI: `openai/gpt-4o` (not `gpt-4o`)
- Anthropic: `anthropic/claude-3-opus-20240229` (with date suffix)

## References

- [ADK LiteLLM Documentation](https://google.github.io/adk-docs/agents/models/litellm/)
- [LiteLLM Providers](https://docs.litellm.ai/docs/providers)
- [LiteLLM Exception Mapping](https://docs.litellm.ai/docs/exception_mapping)
- [LiteLLM Model Fallbacks](https://docs.litellm.ai/docs/tutorials/model_fallbacks)

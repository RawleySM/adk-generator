"""Smoke test for LiteLLM OpenAI Responses API.

Usage:
  export OPENAI_API_KEY=...
  python3 scripts/litellm_responses_smoke_test.py

Optional:
  export ADK_LITELLM_MODEL=openai/o1-pro
  export LITELLM_TEST_INPUT="Say hello in one sentence."
"""

from __future__ import annotations

import os


def _extract_text(response) -> str:
    """
    Best-effort extraction of text from a Responses API response object.
    Falls back to str(response) if structure is unexpected.
    """
    try:
        chunks = []
        for output_item in getattr(response, "output", []) or []:
            # OpenAI Responses API format: output[].content[].text
            for content_item in getattr(output_item, "content", []) or []:
                text = getattr(content_item, "text", None)
                if text:
                    chunks.append(text)
        if chunks:
            return "\n".join(chunks).strip()
    except Exception:
        pass
    return str(response)


def main() -> None:
    import litellm

    model = os.environ.get("ADK_LITELLM_MODEL", "openai/o1-pro")
    user_input = os.environ.get(
        "LITELLM_TEST_INPUT",
        "Tell me a three sentence bedtime story about a unicorn.",
    )

    response = litellm.responses(
        model=model,
        input=user_input,
        max_output_tokens=200,
    )

    print(_extract_text(response))


if __name__ == "__main__":
    main()


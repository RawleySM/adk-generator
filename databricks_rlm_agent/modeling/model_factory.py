"""Model factory for selecting between native ADK Gemini and LiteLLM.

This module provides configuration-driven model selection, supporting:
- Native ADK Gemini (default): Uses model string directly with ADK's Gemini connector
- LiteLLM: Uses the LiteLlm wrapper for OpenAI/Anthropic/local/self-hosted models

Configuration is read from environment variables or Databricks job parameters:
- ADK_MODEL_PROVIDER: "gemini" (default) or "litellm"
- ADK_GEMINI_MODEL: Model string for native Gemini (default: "gemini-3-pro-preview")
- ADK_LITELLM_MODEL: Model string for LiteLLM (e.g., "openai/gpt-4o")
- ADK_LITELLM_FALLBACK_MODELS: Comma-separated fallback chain for LiteLLM
- ADK_FALLBACK_ON_BLOCKED: Enable fallback on content-policy errors (default: "true")
- ADK_FALLBACK_GEMINI_TO_LITELLM: Enable Gemini→LiteLLM fallback (default: "true")
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from google.adk.models.lite_llm import LiteLlm

logger = logging.getLogger(__name__)

# Type alias for model parameter accepted by LlmAgent
ModelType = Union[str, "LiteLlm"]


@dataclass
class ModelConfig:
    """Configuration for model selection and fallback behavior.
    
    Attributes:
        provider: Primary provider ("gemini" or "litellm")
        gemini_model: Model string for native ADK Gemini
        litellm_model: Model string for LiteLLM primary model
        litellm_fallback_models: List of fallback model strings for LiteLLM
        fallback_on_blocked: Whether to fallback on content-policy errors
        fallback_gemini_to_litellm: Whether to fallback from Gemini to LiteLLM
    """
    provider: str = "gemini"
    gemini_model: str = "gemini-3-pro-preview"
    litellm_model: str = "openai/gpt-4o"
    litellm_fallback_models: list[str] = field(default_factory=list)
    fallback_on_blocked: bool = True
    fallback_gemini_to_litellm: bool = True
    
    def __post_init__(self):
        """Validate configuration values."""
        valid_providers = ("gemini", "litellm")
        if self.provider not in valid_providers:
            raise ValueError(
                f"Invalid ADK_MODEL_PROVIDER: '{self.provider}'. "
                f"Must be one of: {valid_providers}"
            )


def get_model_config() -> ModelConfig:
    """Build ModelConfig from environment variables.
    
    Environment Variables:
        ADK_MODEL_PROVIDER: "gemini" (default) or "litellm"
        ADK_GEMINI_MODEL: Model string for native Gemini
        ADK_LITELLM_MODEL: Model string for LiteLLM
        ADK_LITELLM_FALLBACK_MODELS: Comma-separated fallback chain
        ADK_FALLBACK_ON_BLOCKED: Enable fallback on content-policy errors
        ADK_FALLBACK_GEMINI_TO_LITELLM: Enable Gemini→LiteLLM fallback
    
    Returns:
        ModelConfig instance with parsed values.
    """
    # Parse fallback models from comma-separated string
    fallback_str = os.environ.get("ADK_LITELLM_FALLBACK_MODELS", "")
    fallback_models = [m.strip() for m in fallback_str.split(",") if m.strip()]
    
    # Parse boolean flags
    def parse_bool(key: str, default: bool) -> bool:
        val = os.environ.get(key, "").lower()
        if val in ("true", "1", "yes"):
            return True
        elif val in ("false", "0", "no"):
            return False
        return default
    
    config = ModelConfig(
        provider=os.environ.get("ADK_MODEL_PROVIDER", "gemini").lower(),
        gemini_model=os.environ.get("ADK_GEMINI_MODEL", "gemini-3-pro-preview"),
        litellm_model=os.environ.get("ADK_LITELLM_MODEL", "openai/gpt-4o"),
        litellm_fallback_models=fallback_models,
        fallback_on_blocked=parse_bool("ADK_FALLBACK_ON_BLOCKED", True),
        fallback_gemini_to_litellm=parse_bool("ADK_FALLBACK_GEMINI_TO_LITELLM", True),
    )
    
    logger.info(f"[MODEL_CONFIG] Provider: {config.provider}")
    logger.info(f"[MODEL_CONFIG] Gemini model: {config.gemini_model}")
    logger.info(f"[MODEL_CONFIG] LiteLLM model: {config.litellm_model}")
    if config.litellm_fallback_models:
        logger.info(f"[MODEL_CONFIG] LiteLLM fallbacks: {config.litellm_fallback_models}")
    logger.info(f"[MODEL_CONFIG] Fallback on blocked: {config.fallback_on_blocked}")
    logger.info(f"[MODEL_CONFIG] Gemini→LiteLLM fallback: {config.fallback_gemini_to_litellm}")
    
    return config


def build_litellm_model(model_string: str) -> "LiteLlm":
    """Build a LiteLlm wrapper for the given model string.
    
    Args:
        model_string: LiteLLM model identifier (e.g., "openai/gpt-4o")
    
    Returns:
        LiteLlm instance configured for the model.
    """
    from google.adk.models.lite_llm import LiteLlm
    
    logger.info(f"[MODEL_FACTORY] Creating LiteLlm wrapper for: {model_string}")
    return LiteLlm(model=model_string)


def build_agent_model(config: ModelConfig | None = None) -> ModelType:
    """Build the appropriate model for LlmAgent based on configuration.
    
    This is the main entry point for model selection. It returns either:
    - A native Gemini model string (for ADK's built-in Gemini connector)
    - A LiteLlm wrapper instance (for LiteLLM-based providers)
    
    Args:
        config: Optional ModelConfig. If None, reads from environment.
    
    Returns:
        Model suitable for passing to LlmAgent(model=...).
    """
    if config is None:
        config = get_model_config()
    
    if config.provider == "gemini":
        logger.info(f"[MODEL_FACTORY] Using native Gemini: {config.gemini_model}")
        return config.gemini_model
    else:
        # LiteLLM provider
        return build_litellm_model(config.litellm_model)


# Module-level cached config (lazy initialization)
_cached_config: ModelConfig | None = None


def get_cached_config() -> ModelConfig:
    """Get or create cached ModelConfig.
    
    This is useful for sharing config across multiple agent instantiations.
    """
    global _cached_config
    if _cached_config is None:
        _cached_config = get_model_config()
    return _cached_config


def clear_config_cache():
    """Clear the cached config (useful for testing)."""
    global _cached_config
    _cached_config = None

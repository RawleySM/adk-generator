"""Modeling package for model selection and fallback routing.

This package provides:
- ModelConfig: Configuration for model selection
- build_agent_model: Factory function to build the appropriate model
- FallbackRouter: Handles content-policy fallbacks between providers
"""

from .model_factory import (
    ModelConfig,
    build_agent_model,
    get_model_config,
)
from .fallback_router import (
    FallbackRouter,
    is_blocking_error,
    get_fallback_router,
)

__all__ = [
    "ModelConfig",
    "build_agent_model",
    "get_model_config",
    "FallbackRouter",
    "is_blocking_error",
    "get_fallback_router",
]

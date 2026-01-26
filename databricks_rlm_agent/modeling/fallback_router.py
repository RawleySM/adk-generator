"""Fallback router for handling content-policy blocking errors.

This module provides fallback routing when LLM providers block requests due to
content policy violations. It supports:
- Gemini → LiteLLM fallback (when using native ADK Gemini)
- LiteLLM → fallback chain (when using LiteLLM as primary)

The fallback router is implemented as an ADK plugin that intercepts errors
and retries with alternative models.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, Any

if TYPE_CHECKING:
    from google.adk.models.lite_llm import LiteLlm

from .model_factory import ModelConfig, build_litellm_model, get_cached_config

logger = logging.getLogger(__name__)


# =============================================================================
# Blocking Error Detection
# =============================================================================

# Patterns that indicate content-policy blocking (case-insensitive)
BLOCKING_PATTERNS = [
    r"content.?policy",
    r"blocked",
    r"safety",
    r"SAFETY",
    r"harm.?category",
    r"HARM_CATEGORY",
    r"finish.?reason.*SAFETY",
    r"BLOCK_REASON",
    r"content.?filter",
    r"responsible.?ai",
    r"violated",
    r"inappropriate",
    r"refused",
]

# Compiled regex for efficiency
_BLOCKING_REGEX = re.compile(
    "|".join(BLOCKING_PATTERNS),
    re.IGNORECASE
)


def is_blocking_error(error: Exception) -> bool:
    """Check if an exception indicates a content-policy blocking error.
    
    This function handles both:
    - LiteLLM's ContentPolicyViolationError
    - Native Gemini/Google API errors with blocking indicators
    
    Args:
        error: The exception to check.
    
    Returns:
        True if the error indicates content-policy blocking.
    """
    error_str = str(error)
    error_type = type(error).__name__
    
    # Check for LiteLLM's specific content policy error
    try:
        import litellm
        if isinstance(error, litellm.ContentPolicyViolationError):
            logger.debug(f"[FALLBACK] Detected LiteLLM ContentPolicyViolationError")
            return True
    except ImportError:
        pass
    
    # Check error type names that indicate blocking
    blocking_type_names = [
        "ContentPolicyViolationError",
        "BlockedPromptException", 
        "StopCandidateException",
        "SafetyException",
    ]
    if error_type in blocking_type_names:
        logger.debug(f"[FALLBACK] Detected blocking error type: {error_type}")
        return True
    
    # Check error message patterns
    if _BLOCKING_REGEX.search(error_str):
        logger.debug(f"[FALLBACK] Detected blocking pattern in error: {error_str[:200]}")
        return True
    
    # Check for Google GenAI specific attributes
    if hasattr(error, 'finish_reason'):
        finish_reason = str(getattr(error, 'finish_reason', '')).upper()
        if 'SAFETY' in finish_reason or 'BLOCKED' in finish_reason:
            logger.debug(f"[FALLBACK] Detected blocking finish_reason: {finish_reason}")
            return True
    
    return False


# =============================================================================
# Fallback Router
# =============================================================================

class FallbackRouter:
    """Routes model calls with fallback on content-policy errors.
    
    The router maintains a chain of fallback models and attempts them
    in sequence when the primary model encounters a blocking error.
    
    Usage:
        router = FallbackRouter(config)
        # Use router.get_current_model() for LlmAgent configuration
        # Call router.handle_error(error) when errors occur
    """
    
    def __init__(self, config: ModelConfig | None = None):
        """Initialize the fallback router.
        
        Args:
            config: Model configuration. If None, reads from environment.
        """
        self.config = config or get_cached_config()
        self._current_index = 0
        self._fallback_chain = self._build_fallback_chain()
        self._fallback_triggered = False
        
        logger.info(f"[FALLBACK_ROUTER] Initialized with {len(self._fallback_chain)} models in chain")
        for i, (provider, model) in enumerate(self._fallback_chain):
            logger.debug(f"[FALLBACK_ROUTER]   [{i}] {provider}: {model}")
    
    def _build_fallback_chain(self) -> list[tuple[str, str]]:
        """Build the ordered fallback chain based on configuration.
        
        Returns:
            List of (provider, model_string) tuples in fallback order.
        """
        chain = []
        
        if self.config.provider == "gemini":
            # Primary: Native Gemini
            chain.append(("gemini", self.config.gemini_model))
            
            # Fallback to LiteLLM if enabled
            if self.config.fallback_on_blocked and self.config.fallback_gemini_to_litellm:
                chain.append(("litellm", self.config.litellm_model))
                
                # Then LiteLLM fallback chain
                for fallback_model in self.config.litellm_fallback_models:
                    chain.append(("litellm", fallback_model))
        else:
            # Primary: LiteLLM
            chain.append(("litellm", self.config.litellm_model))
            
            # LiteLLM fallback chain
            if self.config.fallback_on_blocked:
                for fallback_model in self.config.litellm_fallback_models:
                    chain.append(("litellm", fallback_model))
        
        return chain
    
    @property
    def current_provider(self) -> str:
        """Get the current provider in the fallback chain."""
        if self._current_index < len(self._fallback_chain):
            return self._fallback_chain[self._current_index][0]
        return self._fallback_chain[-1][0] if self._fallback_chain else "gemini"
    
    @property
    def current_model_string(self) -> str:
        """Get the current model string in the fallback chain."""
        if self._current_index < len(self._fallback_chain):
            return self._fallback_chain[self._current_index][1]
        return self._fallback_chain[-1][1] if self._fallback_chain else self.config.gemini_model
    
    def get_current_model(self) -> str | "LiteLlm":
        """Get the current model suitable for LlmAgent(model=...).
        
        Returns:
            Either a Gemini model string or a LiteLlm wrapper.
        """
        provider = self.current_provider
        model_string = self.current_model_string
        
        if provider == "gemini":
            return model_string
        else:
            return build_litellm_model(model_string)
    
    def handle_error(self, error: Exception) -> bool:
        """Handle an error and potentially advance to the next fallback model.
        
        Args:
            error: The exception that occurred.
        
        Returns:
            True if fallback is available and was activated, False otherwise.
        """
        if not is_blocking_error(error):
            logger.debug(f"[FALLBACK_ROUTER] Error is not a blocking error, not triggering fallback")
            return False
        
        if not self.config.fallback_on_blocked:
            logger.info(f"[FALLBACK_ROUTER] Fallback disabled by config")
            return False
        
        # Check if we have more models in the chain
        if self._current_index + 1 >= len(self._fallback_chain):
            logger.warning(
                f"[FALLBACK_ROUTER] Blocking error with no more fallbacks available. "
                f"Current: {self.current_provider}/{self.current_model_string}"
            )
            return False
        
        # Advance to next model
        old_provider = self.current_provider
        old_model = self.current_model_string
        self._current_index += 1
        self._fallback_triggered = True
        
        logger.warning(
            f"[FALLBACK_ROUTER] Content-policy block detected! "
            f"Falling back: {old_provider}/{old_model} → {self.current_provider}/{self.current_model_string}"
        )
        
        return True
    
    def reset(self):
        """Reset the router to the primary model.
        
        Call this at the start of a new conversation/session to reset fallback state.
        """
        self._current_index = 0
        self._fallback_triggered = False
        logger.debug(f"[FALLBACK_ROUTER] Reset to primary model: {self.current_provider}/{self.current_model_string}")
    
    @property
    def fallback_triggered(self) -> bool:
        """Check if fallback has been triggered during this session."""
        return self._fallback_triggered
    
    def get_status(self) -> dict[str, Any]:
        """Get current router status for logging/telemetry.
        
        Returns:
            Dict with current state information.
        """
        return {
            "provider": self.current_provider,
            "model": self.current_model_string,
            "fallback_index": self._current_index,
            "fallback_triggered": self._fallback_triggered,
            "remaining_fallbacks": len(self._fallback_chain) - self._current_index - 1,
        }


# =============================================================================
# Module-level Router Instance
# =============================================================================

_router_instance: FallbackRouter | None = None


def get_fallback_router() -> FallbackRouter:
    """Get or create the module-level fallback router instance.
    
    This provides a shared router across all agent instances in the same process.
    """
    global _router_instance
    if _router_instance is None:
        _router_instance = FallbackRouter()
    return _router_instance


def reset_fallback_router():
    """Reset the module-level fallback router.
    
    Call this at the start of a new conversation/run to reset fallback state.
    """
    global _router_instance
    if _router_instance is not None:
        _router_instance.reset()


def clear_fallback_router():
    """Clear the module-level fallback router (useful for testing)."""
    global _router_instance
    _router_instance = None

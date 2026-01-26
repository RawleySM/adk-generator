"""Fallback router for handling LLM API errors with smart retry and fallback.

This module provides fallback routing with intelligent error handling:
- Rate limiting errors: Retry with exponential backoff (up to max_retries)
- Auth/API key errors: Immediate fallback (permanent errors)
- Content-policy blocking: Immediate fallback (different model may work)

It supports:
- Gemini → LiteLLM fallback (when using native ADK Gemini)
- LiteLLM → fallback chain (when using LiteLLM as primary)

The fallback router is implemented as an ADK plugin that intercepts errors
and retries with alternative models.
"""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Callable, Any

if TYPE_CHECKING:
    from google.adk.models.lite_llm import LiteLlm

from .model_factory import ModelConfig, build_litellm_model, get_cached_config

logger = logging.getLogger(__name__)


# =============================================================================
# Error Classification Types
# =============================================================================

class ErrorType:
    """Classification of API errors for fallback decision making."""
    RATE_LIMIT = "rate_limit"       # Transient - retry with backoff
    AUTH_ERROR = "auth_error"       # Permanent - fallback immediately
    BLOCKING = "blocking"           # Content policy - fallback immediately
    TRANSIENT = "transient"         # Other transient errors - retry
    UNKNOWN = "unknown"             # Unknown errors - don't fallback


# =============================================================================
# Error Detection Patterns
# =============================================================================

# Patterns that indicate rate limiting (transient - retry with backoff)
RATE_LIMIT_PATTERNS = [
    r"rate.?limit",
    r"too.?many.?requests",
    r"quota.?exceeded",
    r"throttl",
    r"429",
    r"resource.?exhausted",
    r"requests?.?per.?(second|minute|hour|day)",
    r"retry.?after",
    r"slow.?down",
    r"overloaded",
    r"capacity",
]

# Patterns that indicate authentication/API key errors (permanent - fallback immediately)
AUTH_ERROR_PATTERNS = [
    r"api.?key.?(invalid|expired|revoked|missing)",
    r"authentication.?(failed|error|invalid)",
    r"unauthorized",
    r"401",
    r"403",
    r"invalid.?(api.?key|credentials?|token)",
    r"expired.?(key|token|credentials?)",
    r"access.?denied",
    r"permission.?denied",
    r"invalid.?auth",
    r"auth.?(token|key).*(invalid|expired)",
    r"could.?not.?authenticate",
    r"incorrect.?api.?key",
]

# Patterns that indicate content-policy blocking (permanent for this prompt - fallback)
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

# Compiled regexes for efficiency
_RATE_LIMIT_REGEX = re.compile("|".join(RATE_LIMIT_PATTERNS), re.IGNORECASE)
_AUTH_ERROR_REGEX = re.compile("|".join(AUTH_ERROR_PATTERNS), re.IGNORECASE)
_BLOCKING_REGEX = re.compile("|".join(BLOCKING_PATTERNS), re.IGNORECASE)


def classify_error(error: Exception) -> str:
    """Classify an exception to determine the appropriate fallback strategy.
    
    This function handles errors from:
    - LiteLLM (RateLimitError, AuthenticationError, ContentPolicyViolationError)
    - Native Gemini/Google API errors
    - Generic HTTP errors with status codes
    
    Args:
        error: The exception to classify.
    
    Returns:
        ErrorType constant indicating the error classification.
    """
    error_str = str(error)
    error_type = type(error).__name__
    
    # Check for LiteLLM's specific error types first (most reliable)
    try:
        import litellm
        if isinstance(error, litellm.RateLimitError):
            logger.debug(f"[FALLBACK] Detected LiteLLM RateLimitError")
            return ErrorType.RATE_LIMIT
        if isinstance(error, litellm.AuthenticationError):
            logger.debug(f"[FALLBACK] Detected LiteLLM AuthenticationError")
            return ErrorType.AUTH_ERROR
        if isinstance(error, litellm.ContentPolicyViolationError):
            logger.debug(f"[FALLBACK] Detected LiteLLM ContentPolicyViolationError")
            return ErrorType.BLOCKING
        # Check for other transient errors that might be retried
        if isinstance(error, (litellm.ServiceUnavailableError, litellm.Timeout)):
            logger.debug(f"[FALLBACK] Detected LiteLLM transient error: {error_type}")
            return ErrorType.TRANSIENT
    except ImportError:
        pass
    
    # Check error type names
    rate_limit_type_names = ["RateLimitError", "TooManyRequestsError", "QuotaExceededError"]
    auth_error_type_names = ["AuthenticationError", "AuthError", "InvalidApiKeyError", "PermissionDeniedError"]
    blocking_type_names = ["ContentPolicyViolationError", "BlockedPromptException", "StopCandidateException", "SafetyException"]
    
    if error_type in rate_limit_type_names:
        logger.debug(f"[FALLBACK] Detected rate limit error type: {error_type}")
        return ErrorType.RATE_LIMIT
    if error_type in auth_error_type_names:
        logger.debug(f"[FALLBACK] Detected auth error type: {error_type}")
        return ErrorType.AUTH_ERROR
    if error_type in blocking_type_names:
        logger.debug(f"[FALLBACK] Detected blocking error type: {error_type}")
        return ErrorType.BLOCKING
    
    # Check HTTP status code if available
    status_code = getattr(error, 'status_code', None) or getattr(error, 'code', None)
    if status_code:
        if status_code == 429:
            logger.debug(f"[FALLBACK] Detected rate limit from status code 429")
            return ErrorType.RATE_LIMIT
        if status_code in (401, 403):
            logger.debug(f"[FALLBACK] Detected auth error from status code {status_code}")
            return ErrorType.AUTH_ERROR
    
    # Fall back to pattern matching on error message
    # Check auth patterns first (more specific)
    if _AUTH_ERROR_REGEX.search(error_str):
        logger.debug(f"[FALLBACK] Detected auth error pattern in: {error_str[:200]}")
        return ErrorType.AUTH_ERROR
    
    # Check rate limit patterns
    if _RATE_LIMIT_REGEX.search(error_str):
        logger.debug(f"[FALLBACK] Detected rate limit pattern in: {error_str[:200]}")
        return ErrorType.RATE_LIMIT
    
    # Check blocking patterns
    if _BLOCKING_REGEX.search(error_str):
        logger.debug(f"[FALLBACK] Detected blocking pattern in: {error_str[:200]}")
        return ErrorType.BLOCKING
    
    # Check for Google GenAI specific attributes
    if hasattr(error, 'finish_reason'):
        finish_reason = str(getattr(error, 'finish_reason', '')).upper()
        if 'SAFETY' in finish_reason or 'BLOCKED' in finish_reason:
            logger.debug(f"[FALLBACK] Detected blocking finish_reason: {finish_reason}")
            return ErrorType.BLOCKING
    
    logger.debug(f"[FALLBACK] Unknown error type: {error_type}, message: {error_str[:200]}")
    return ErrorType.UNKNOWN


def is_blocking_error(error: Exception) -> bool:
    """Check if an exception indicates a content-policy blocking error.
    
    This is a convenience wrapper around classify_error() for backward compatibility.
    
    Args:
        error: The exception to check.
    
    Returns:
        True if the error indicates content-policy blocking.
    """
    return classify_error(error) == ErrorType.BLOCKING


def is_rate_limit_error(error: Exception) -> bool:
    """Check if an exception indicates a rate limiting error.
    
    Args:
        error: The exception to check.
    
    Returns:
        True if the error indicates rate limiting.
    """
    return classify_error(error) == ErrorType.RATE_LIMIT


def is_auth_error(error: Exception) -> bool:
    """Check if an exception indicates an authentication/API key error.
    
    Args:
        error: The exception to check.
    
    Returns:
        True if the error indicates auth failure.
    """
    return classify_error(error) == ErrorType.AUTH_ERROR


def should_retry_error(error: Exception) -> bool:
    """Check if an error should be retried with backoff.
    
    Args:
        error: The exception to check.
    
    Returns:
        True if the error is transient and should be retried.
    """
    error_type = classify_error(error)
    return error_type in (ErrorType.RATE_LIMIT, ErrorType.TRANSIENT)


def should_fallback_immediately(error: Exception) -> bool:
    """Check if an error should trigger immediate fallback without retry.
    
    Args:
        error: The exception to check.
    
    Returns:
        True if fallback should happen immediately (auth errors, blocking).
    """
    error_type = classify_error(error)
    return error_type in (ErrorType.AUTH_ERROR, ErrorType.BLOCKING)


# =============================================================================
# Fallback Router
# =============================================================================

class FallbackRouter:
    """Routes model calls with fallback on API errors, with smart retry logic.
    
    The router maintains a chain of fallback models and implements:
    - Rate limiting: Retry with exponential backoff before falling back
    - Auth errors: Immediate fallback (API key expired/invalid)
    - Content blocking: Immediate fallback (different model may work)
    
    Usage:
        router = FallbackRouter(config)
        # Use router.get_current_model() for LlmAgent configuration
        # Call router.handle_error(error) when errors occur
    """
    
    # Default retry configuration
    DEFAULT_MAX_RETRIES = 2
    DEFAULT_BASE_BACKOFF_SECONDS = 1.0
    DEFAULT_MAX_BACKOFF_SECONDS = 30.0
    
    def __init__(
        self,
        config: ModelConfig | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    ):
        """Initialize the fallback router.
        
        Args:
            config: Model configuration. If None, reads from environment.
            max_retries: Maximum retries for rate limit errors before fallback.
            base_backoff_seconds: Initial backoff duration for retries.
            max_backoff_seconds: Maximum backoff duration cap.
        """
        self.config = config or get_cached_config()
        self._current_index = 0
        self._fallback_chain = self._build_fallback_chain()
        self._fallback_triggered = False
        
        # Retry configuration
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        
        # Per-model retry tracking
        self._retry_counts: dict[int, int] = {}  # model_index -> retry_count
        
        logger.info(
            f"[FALLBACK_ROUTER] Initialized with {len(self._fallback_chain)} models in chain "
            f"(max_retries={max_retries}, base_backoff={base_backoff_seconds}s)"
        )
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
    
    def handle_error(self, error: Exception) -> tuple[str, bool]:
        """Handle an error and determine the appropriate action.
        
        This method classifies the error and returns the recommended action:
        - "retry": Retry with the current model after backoff
        - "fallback": Switch to the next fallback model
        - "raise": Re-raise the error (no fallback available or unknown error)
        
        Args:
            error: The exception that occurred.
        
        Returns:
            Tuple of (action, success) where:
            - action: "retry", "fallback", or "raise"
            - success: True if the action was successfully prepared
        """
        error_type = classify_error(error)
        
        logger.debug(
            f"[FALLBACK_ROUTER] Handling error type={error_type}, "
            f"model={self.current_provider}/{self.current_model_string}"
        )
        
        # Unknown errors - don't attempt fallback
        if error_type == ErrorType.UNKNOWN:
            logger.debug(f"[FALLBACK_ROUTER] Unknown error type, not triggering fallback")
            return ("raise", False)
        
        # Rate limit or transient errors - try retry with backoff first
        if error_type in (ErrorType.RATE_LIMIT, ErrorType.TRANSIENT):
            retry_count = self._retry_counts.get(self._current_index, 0)
            
            if retry_count < self.max_retries:
                # Increment retry count and recommend retry
                self._retry_counts[self._current_index] = retry_count + 1
                backoff = self._calculate_backoff(retry_count)
                
                logger.info(
                    f"[FALLBACK_ROUTER] Rate limit hit (attempt {retry_count + 1}/{self.max_retries}). "
                    f"Recommending retry after {backoff:.1f}s backoff"
                )
                return ("retry", True)
            else:
                # Max retries exceeded - fall through to fallback logic
                logger.warning(
                    f"[FALLBACK_ROUTER] Max retries ({self.max_retries}) exceeded for rate limiting. "
                    f"Attempting fallback."
                )
        
        # Auth errors or blocking errors - immediate fallback
        # Also handles rate limit after max retries
        if not self.config.fallback_on_blocked:
            logger.info(f"[FALLBACK_ROUTER] Fallback disabled by config")
            return ("raise", False)
        
        # Check if we have more models in the chain
        if self._current_index + 1 >= len(self._fallback_chain):
            logger.warning(
                f"[FALLBACK_ROUTER] No more fallbacks available. "
                f"Current: {self.current_provider}/{self.current_model_string}, "
                f"Error type: {error_type}"
            )
            return ("raise", False)
        
        # Advance to next model
        old_provider = self.current_provider
        old_model = self.current_model_string
        self._current_index += 1
        self._fallback_triggered = True
        
        # Reset retry count for the new model
        self._retry_counts[self._current_index] = 0
        
        reason = {
            ErrorType.AUTH_ERROR: "API key expired/invalid",
            ErrorType.BLOCKING: "Content-policy block",
            ErrorType.RATE_LIMIT: "Rate limit (retries exhausted)",
            ErrorType.TRANSIENT: "Transient error (retries exhausted)",
        }.get(error_type, error_type)
        
        logger.warning(
            f"[FALLBACK_ROUTER] {reason}! "
            f"Falling back: {old_provider}/{old_model} → {self.current_provider}/{self.current_model_string}"
        )
        
        return ("fallback", True)
    
    def _calculate_backoff(self, retry_count: int) -> float:
        """Calculate exponential backoff duration.
        
        Args:
            retry_count: Number of retries already attempted.
        
        Returns:
            Backoff duration in seconds.
        """
        # Exponential backoff: base * 2^retry_count
        backoff = self.base_backoff_seconds * (2 ** retry_count)
        return min(backoff, self.max_backoff_seconds)
    
    def get_backoff_duration(self) -> float:
        """Get the current recommended backoff duration.
        
        Returns:
            Backoff duration in seconds based on current retry count.
        """
        retry_count = self._retry_counts.get(self._current_index, 0)
        return self._calculate_backoff(retry_count)
    
    def wait_for_backoff(self) -> None:
        """Sleep for the recommended backoff duration.
        
        Call this after handle_error returns ("retry", True).
        """
        duration = self.get_backoff_duration()
        logger.debug(f"[FALLBACK_ROUTER] Waiting {duration:.1f}s for backoff")
        time.sleep(duration)
    
    def handle_error_legacy(self, error: Exception) -> bool:
        """Legacy error handler for backward compatibility.
        
        This method provides the old behavior where it returns True/False
        for whether fallback was triggered.
        
        Args:
            error: The exception that occurred.
        
        Returns:
            True if fallback is available and was activated, False otherwise.
        """
        action, success = self.handle_error(error)
        return action == "fallback" and success
    
    def reset(self):
        """Reset the router to the primary model.
        
        Call this at the start of a new conversation/session to reset fallback state.
        """
        self._current_index = 0
        self._fallback_triggered = False
        self._retry_counts.clear()
        logger.debug(f"[FALLBACK_ROUTER] Reset to primary model: {self.current_provider}/{self.current_model_string}")
    
    def reset_retries(self):
        """Reset retry counts without changing the current model.
        
        Useful when starting a new request but wanting to stay on the current model.
        """
        self._retry_counts.clear()
        logger.debug(f"[FALLBACK_ROUTER] Reset retry counts for model: {self.current_provider}/{self.current_model_string}")
    
    @property
    def fallback_triggered(self) -> bool:
        """Check if fallback has been triggered during this session."""
        return self._fallback_triggered
    
    def get_status(self) -> dict[str, Any]:
        """Get current router status for logging/telemetry.
        
        Returns:
            Dict with current state information.
        """
        current_retries = self._retry_counts.get(self._current_index, 0)
        return {
            "provider": self.current_provider,
            "model": self.current_model_string,
            "fallback_index": self._current_index,
            "fallback_triggered": self._fallback_triggered,
            "remaining_fallbacks": len(self._fallback_chain) - self._current_index - 1,
            "current_retry_count": current_retries,
            "max_retries": self.max_retries,
            "retries_remaining": self.max_retries - current_retries,
        }
    
    def execute_with_fallback(
        self,
        func: Callable[[], Any],
        max_total_attempts: int | None = None,
    ) -> Any:
        """Execute a function with automatic retry and fallback handling.
        
        This is a convenience method that wraps the retry/backoff/fallback logic.
        It will:
        1. Try to execute the function
        2. On rate limit errors: retry with backoff up to max_retries
        3. On auth/blocking errors: fallback to next model immediately
        4. If all models exhausted: raise the last error
        
        Args:
            func: The function to execute (should use self.get_current_model()).
            max_total_attempts: Optional cap on total attempts across all models.
        
        Returns:
            The result of func() on success.
        
        Raises:
            The last exception if all retries and fallbacks are exhausted.
        """
        total_attempts = 0
        max_attempts = max_total_attempts or (
            (self.max_retries + 1) * len(self._fallback_chain)
        )
        
        last_error: Exception | None = None
        
        while total_attempts < max_attempts:
            total_attempts += 1
            
            try:
                return func()
            except Exception as e:
                last_error = e
                action, success = self.handle_error(e)
                
                if action == "retry" and success:
                    self.wait_for_backoff()
                    continue
                elif action == "fallback" and success:
                    # Model switched, try again with new model
                    continue
                else:
                    # No retry or fallback available
                    raise
        
        # Should not reach here, but just in case
        if last_error:
            raise last_error
        raise RuntimeError("execute_with_fallback exhausted attempts without result")


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

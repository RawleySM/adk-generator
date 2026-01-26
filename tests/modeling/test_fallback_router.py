"""Tests for fallback router and blocking error detection."""

import pytest
from unittest.mock import patch, MagicMock


class TestErrorClassification:
    """Tests for error classification functions."""

    def test_litellm_content_policy_violation(self):
        """Test detection of LiteLLM ContentPolicyViolationError."""
        from databricks_rlm_agent.modeling.fallback_router import is_blocking_error
        
        # Create a mock ContentPolicyViolationError
        mock_error = MagicMock()
        mock_error.__class__.__name__ = "ContentPolicyViolationError"
        
        # Mock litellm import
        with patch.dict("sys.modules", {"litellm": MagicMock()}):
            import sys
            sys.modules["litellm"].ContentPolicyViolationError = type(mock_error)
            
            # The error type check should match
            result = is_blocking_error(mock_error)
            assert result is True

    def test_blocking_error_messages(self):
        """Test detection of blocking error messages."""
        from databricks_rlm_agent.modeling.fallback_router import is_blocking_error
        
        blocking_messages = [
            "Request blocked due to content policy violation",
            "SAFETY filter triggered",
            "Content was blocked",
            "HARM_CATEGORY_DANGEROUS detected",
            "finish_reason: SAFETY",
            "BLOCK_REASON: SAFETY",
            "Content filter triggered",
            "Response refused due to safety concerns",
        ]
        
        for msg in blocking_messages:
            error = Exception(msg)
            assert is_blocking_error(error) is True, f"Failed to detect: {msg}"

    def test_rate_limit_error_messages(self):
        """Test detection of rate limit error messages."""
        from databricks_rlm_agent.modeling.fallback_router import is_rate_limit_error
        
        rate_limit_messages = [
            "Rate limit exceeded",
            "Too many requests",
            "Quota exceeded for the day",
            "You have been throttled",
            "HTTP 429: Too Many Requests",
            "Resource exhausted",
            "Requests per minute limit reached",
            "Please slow down",
        ]
        
        for msg in rate_limit_messages:
            error = Exception(msg)
            assert is_rate_limit_error(error) is True, f"Failed to detect: {msg}"

    def test_auth_error_messages(self):
        """Test detection of authentication error messages."""
        from databricks_rlm_agent.modeling.fallback_router import is_auth_error
        
        auth_error_messages = [
            "API key invalid",
            "API key expired",
            "Authentication failed",
            "Unauthorized request",
            "HTTP 401",
            "Invalid credentials",
            "Expired token",
            "Access denied",
            "Permission denied",
            "Could not authenticate",
            "Incorrect API key provided",
        ]
        
        for msg in auth_error_messages:
            error = Exception(msg)
            assert is_auth_error(error) is True, f"Failed to detect: {msg}"

    def test_unknown_error_messages(self):
        """Test that unknown errors are not misclassified."""
        from databricks_rlm_agent.modeling.fallback_router import (
            classify_error, ErrorType
        )
        
        unknown_messages = [
            "Connection timeout",
            "Model not found",
            "Internal server error",
            "Something went wrong",
        ]
        
        for msg in unknown_messages:
            error = Exception(msg)
            assert classify_error(error) == ErrorType.UNKNOWN, f"Misclassified: {msg}"

    def test_blocking_error_type_names(self):
        """Test detection of error types by name."""
        from databricks_rlm_agent.modeling.fallback_router import is_blocking_error
        
        blocking_type_names = [
            "ContentPolicyViolationError",
            "BlockedPromptException",
            "StopCandidateException",
            "SafetyException",
        ]
        
        for type_name in blocking_type_names:
            # Create a custom exception class with the blocking type name
            CustomError = type(type_name, (Exception,), {})
            error = CustomError("test error")
            assert is_blocking_error(error) is True, f"Failed for type: {type_name}"

    def test_rate_limit_error_type_names(self):
        """Test detection of rate limit error types by name."""
        from databricks_rlm_agent.modeling.fallback_router import is_rate_limit_error
        
        rate_limit_type_names = [
            "RateLimitError",
            "TooManyRequestsError",
            "QuotaExceededError",
        ]
        
        for type_name in rate_limit_type_names:
            CustomError = type(type_name, (Exception,), {})
            error = CustomError("test error")
            assert is_rate_limit_error(error) is True, f"Failed for type: {type_name}"

    def test_auth_error_type_names(self):
        """Test detection of auth error types by name."""
        from databricks_rlm_agent.modeling.fallback_router import is_auth_error
        
        auth_error_type_names = [
            "AuthenticationError",
            "AuthError",
            "InvalidApiKeyError",
            "PermissionDeniedError",
        ]
        
        for type_name in auth_error_type_names:
            CustomError = type(type_name, (Exception,), {})
            error = CustomError("test error")
            assert is_auth_error(error) is True, f"Failed for type: {type_name}"


class TestFallbackRouter:
    """Tests for FallbackRouter class."""

    def test_gemini_provider_fallback_chain(self):
        """Test fallback chain construction for Gemini provider."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            litellm_fallback_models=["anthropic/claude-3-haiku"],
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        
        # Check initial state
        assert router.current_provider == "gemini"
        assert router.current_model_string == "gemini-3-pro-preview"
        assert router.fallback_triggered is False

    def test_litellm_provider_fallback_chain(self):
        """Test fallback chain construction for LiteLLM provider."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="litellm",
            litellm_model="openai/gpt-4o",
            litellm_fallback_models=["openai/gpt-4o-mini", "anthropic/claude-3-haiku"],
            fallback_on_blocked=True,
        )
        
        router = FallbackRouter(config)
        
        # Check initial state
        assert router.current_provider == "litellm"
        assert router.current_model_string == "openai/gpt-4o"

    def test_handle_blocking_error_advances_fallback(self):
        """Test that blocking errors advance the fallback chain immediately."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            litellm_fallback_models=["anthropic/claude-3-haiku"],
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        
        # Simulate blocking error
        blocking_error = Exception("SAFETY filter triggered - content blocked")
        
        # First fallback: gemini -> litellm primary (immediate, no retry)
        action, success = router.handle_error(blocking_error)
        assert action == "fallback"
        assert success is True
        assert router.current_provider == "litellm"
        assert router.current_model_string == "openai/gpt-4o"
        assert router.fallback_triggered is True
        
        # Second fallback: litellm primary -> litellm fallback
        action, success = router.handle_error(blocking_error)
        assert action == "fallback"
        assert success is True
        assert router.current_provider == "litellm"
        assert router.current_model_string == "anthropic/claude-3-haiku"

    def test_handle_auth_error_advances_fallback_immediately(self):
        """Test that auth errors advance the fallback chain immediately."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            litellm_fallback_models=["anthropic/claude-3-haiku"],
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        
        # Simulate auth error (API key expired)
        auth_error = Exception("API key expired")
        
        # Auth errors should fallback immediately without retries
        action, success = router.handle_error(auth_error)
        assert action == "fallback"
        assert success is True
        assert router.current_provider == "litellm"
        assert router.current_model_string == "openai/gpt-4o"

    def test_handle_rate_limit_error_retries_before_fallback(self):
        """Test that rate limit errors retry before falling back."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config, max_retries=2)
        
        # Simulate rate limit error
        rate_limit_error = Exception("Rate limit exceeded")
        
        # First hit: should recommend retry
        action, success = router.handle_error(rate_limit_error)
        assert action == "retry"
        assert success is True
        assert router.current_provider == "gemini"  # Still on original model
        
        # Second hit: should still recommend retry
        action, success = router.handle_error(rate_limit_error)
        assert action == "retry"
        assert success is True
        assert router.current_provider == "gemini"
        
        # Third hit: max retries exceeded, should fallback
        action, success = router.handle_error(rate_limit_error)
        assert action == "fallback"
        assert success is True
        assert router.current_provider == "litellm"

    def test_handle_non_blocking_error_no_fallback(self):
        """Test that unknown errors don't trigger fallback."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        
        # Simulate unknown error (connection timeout)
        unknown_error = Exception("Connection timeout")
        
        action, success = router.handle_error(unknown_error)
        assert action == "raise"
        assert success is False
        assert router.current_provider == "gemini"
        assert router.fallback_triggered is False

    def test_fallback_disabled(self):
        """Test that fallback is not triggered when disabled."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            fallback_on_blocked=False,  # Disabled
        )
        
        router = FallbackRouter(config)
        
        # Simulate blocking error
        blocking_error = Exception("SAFETY filter triggered")
        
        action, success = router.handle_error(blocking_error)
        assert action == "raise"
        assert success is False
        assert router.current_provider == "gemini"

    def test_no_more_fallbacks_returns_raise(self):
        """Test behavior when no more fallbacks are available."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            litellm_fallback_models=[],  # No additional fallbacks
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        blocking_error = Exception("content blocked")
        
        # First fallback works
        action, success = router.handle_error(blocking_error)
        assert action == "fallback"
        assert success is True
        assert router.current_model_string == "openai/gpt-4o"
        
        # Second fallback fails (no more models)
        action, success = router.handle_error(blocking_error)
        assert action == "raise"
        assert success is False
        assert router.current_model_string == "openai/gpt-4o"

    def test_reset_returns_to_primary(self):
        """Test that reset() returns to primary model and clears retry counts."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        blocking_error = Exception("content blocked")
        
        # Trigger fallback
        router.handle_error(blocking_error)
        assert router.current_provider == "litellm"
        assert router.fallback_triggered is True
        
        # Reset
        router.reset()
        assert router.current_provider == "gemini"
        assert router.fallback_triggered is False
        assert router._retry_counts == {}  # Retry counts cleared

    def test_get_status(self):
        """Test get_status() returns correct information."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            litellm_fallback_models=["anthropic/claude-3-haiku"],
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config, max_retries=3)
        status = router.get_status()
        
        assert status["provider"] == "gemini"
        assert status["model"] == "gemini-3-pro-preview"
        assert status["fallback_index"] == 0
        assert status["fallback_triggered"] is False
        assert status["remaining_fallbacks"] == 2  # litellm primary + 1 fallback
        assert status["current_retry_count"] == 0
        assert status["max_retries"] == 3
        assert status["retries_remaining"] == 3

    def test_handle_error_legacy_compatibility(self):
        """Test that handle_error_legacy provides backward compatibility."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="gemini",
            gemini_model="gemini-3-pro-preview",
            litellm_model="openai/gpt-4o",
            fallback_on_blocked=True,
            fallback_gemini_to_litellm=True,
        )
        
        router = FallbackRouter(config)
        
        # Blocking error should return True (fallback triggered)
        blocking_error = Exception("content blocked")
        assert router.handle_error_legacy(blocking_error) is True
        
        # Reset and test unknown error
        router.reset()
        unknown_error = Exception("Something happened")
        assert router.handle_error_legacy(unknown_error) is False

    def test_backoff_calculation(self):
        """Test exponential backoff calculation."""
        from databricks_rlm_agent.modeling.fallback_router import FallbackRouter
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(provider="gemini", gemini_model="gemini-3-pro-preview")
        
        router = FallbackRouter(
            config,
            base_backoff_seconds=1.0,
            max_backoff_seconds=10.0,
        )
        
        # Verify exponential backoff
        assert router._calculate_backoff(0) == 1.0   # 1 * 2^0 = 1
        assert router._calculate_backoff(1) == 2.0   # 1 * 2^1 = 2
        assert router._calculate_backoff(2) == 4.0   # 1 * 2^2 = 4
        assert router._calculate_backoff(3) == 8.0   # 1 * 2^3 = 8
        assert router._calculate_backoff(4) == 10.0  # 1 * 2^4 = 16, capped at 10

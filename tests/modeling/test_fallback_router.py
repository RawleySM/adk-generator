"""Tests for fallback router and blocking error detection."""

import pytest
from unittest.mock import patch, MagicMock


class TestIsBlockingError:
    """Tests for is_blocking_error() function."""

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

    def test_non_blocking_error_messages(self):
        """Test that non-blocking errors are not flagged."""
        from databricks_rlm_agent.modeling.fallback_router import is_blocking_error
        
        non_blocking_messages = [
            "Connection timeout",
            "Rate limit exceeded",
            "Invalid API key",
            "Model not found",
            "Internal server error",
        ]
        
        for msg in non_blocking_messages:
            error = Exception(msg)
            assert is_blocking_error(error) is False, f"False positive for: {msg}"

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
        """Test that blocking errors advance the fallback chain."""
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
        
        # First fallback: gemini -> litellm primary
        result = router.handle_error(blocking_error)
        assert result is True
        assert router.current_provider == "litellm"
        assert router.current_model_string == "openai/gpt-4o"
        assert router.fallback_triggered is True
        
        # Second fallback: litellm primary -> litellm fallback
        result = router.handle_error(blocking_error)
        assert result is True
        assert router.current_provider == "litellm"
        assert router.current_model_string == "anthropic/claude-3-haiku"

    def test_handle_non_blocking_error_no_fallback(self):
        """Test that non-blocking errors don't trigger fallback."""
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
        
        # Simulate non-blocking error
        non_blocking_error = Exception("Connection timeout")
        
        result = router.handle_error(non_blocking_error)
        assert result is False
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
        
        result = router.handle_error(blocking_error)
        assert result is False
        assert router.current_provider == "gemini"

    def test_no_more_fallbacks_returns_false(self):
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
        result = router.handle_error(blocking_error)
        assert result is True
        assert router.current_model_string == "openai/gpt-4o"
        
        # Second fallback fails (no more models)
        result = router.handle_error(blocking_error)
        assert result is False
        assert router.current_model_string == "openai/gpt-4o"

    def test_reset_returns_to_primary(self):
        """Test that reset() returns to primary model."""
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
        
        router = FallbackRouter(config)
        status = router.get_status()
        
        assert status["provider"] == "gemini"
        assert status["model"] == "gemini-3-pro-preview"
        assert status["fallback_index"] == 0
        assert status["fallback_triggered"] is False
        assert status["remaining_fallbacks"] == 2  # litellm primary + 1 fallback

"""Tests for model factory configuration and selection."""

import os
import pytest
from unittest.mock import patch, MagicMock


class TestModelConfig:
    """Tests for ModelConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig()
        assert config.provider == "gemini"
        assert config.gemini_model == "gemini-3-pro-preview"
        assert config.litellm_model == "openai/gpt-4o"
        assert config.litellm_fallback_models == []
        assert config.fallback_on_blocked is True
        assert config.fallback_gemini_to_litellm is True

    def test_custom_config(self):
        """Test custom configuration values."""
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        config = ModelConfig(
            provider="litellm",
            gemini_model="gemini-2.0-flash",
            litellm_model="anthropic/claude-3-haiku-20240307",
            litellm_fallback_models=["openai/gpt-4o-mini"],
            fallback_on_blocked=False,
            fallback_gemini_to_litellm=False,
        )
        assert config.provider == "litellm"
        assert config.gemini_model == "gemini-2.0-flash"
        assert config.litellm_model == "anthropic/claude-3-haiku-20240307"
        assert config.litellm_fallback_models == ["openai/gpt-4o-mini"]
        assert config.fallback_on_blocked is False
        assert config.fallback_gemini_to_litellm is False

    def test_invalid_provider_raises_error(self):
        """Test that invalid provider raises ValueError."""
        from databricks_rlm_agent.modeling.model_factory import ModelConfig
        
        with pytest.raises(ValueError, match="Invalid ADK_MODEL_PROVIDER"):
            ModelConfig(provider="invalid_provider")


class TestGetModelConfig:
    """Tests for get_model_config() function."""

    def test_default_env_parsing(self):
        """Test config parsing with no environment variables set."""
        from databricks_rlm_agent.modeling.model_factory import get_model_config, clear_config_cache
        
        clear_config_cache()
        
        # Clear relevant env vars
        env_vars = [
            "ADK_MODEL_PROVIDER",
            "ADK_GEMINI_MODEL",
            "ADK_LITELLM_MODEL",
            "ADK_LITELLM_FALLBACK_MODELS",
            "ADK_FALLBACK_ON_BLOCKED",
            "ADK_FALLBACK_GEMINI_TO_LITELLM",
        ]
        with patch.dict(os.environ, {}, clear=True):
            for var in env_vars:
                os.environ.pop(var, None)
            
            config = get_model_config()
            assert config.provider == "gemini"
            assert config.gemini_model == "gemini-3-pro-preview"
            assert config.litellm_model == "openai/gpt-4o"

    def test_env_parsing_litellm_provider(self):
        """Test config parsing with LiteLLM provider."""
        from databricks_rlm_agent.modeling.model_factory import get_model_config, clear_config_cache
        
        clear_config_cache()
        
        env_patch = {
            "ADK_MODEL_PROVIDER": "litellm",
            "ADK_LITELLM_MODEL": "anthropic/claude-3-opus",
            "ADK_LITELLM_FALLBACK_MODELS": "openai/gpt-4o,anthropic/claude-3-haiku-20240307",
        }
        
        with patch.dict(os.environ, env_patch, clear=False):
            config = get_model_config()
            assert config.provider == "litellm"
            assert config.litellm_model == "anthropic/claude-3-opus"
            assert config.litellm_fallback_models == [
                "openai/gpt-4o",
                "anthropic/claude-3-haiku-20240307",
            ]

    def test_boolean_env_parsing(self):
        """Test parsing of boolean environment variables."""
        from databricks_rlm_agent.modeling.model_factory import get_model_config, clear_config_cache
        
        clear_config_cache()
        
        # Test false values
        env_patch = {
            "ADK_FALLBACK_ON_BLOCKED": "false",
            "ADK_FALLBACK_GEMINI_TO_LITELLM": "0",
        }
        
        with patch.dict(os.environ, env_patch, clear=False):
            config = get_model_config()
            assert config.fallback_on_blocked is False
            assert config.fallback_gemini_to_litellm is False
        
        clear_config_cache()
        
        # Test true values
        env_patch = {
            "ADK_FALLBACK_ON_BLOCKED": "yes",
            "ADK_FALLBACK_GEMINI_TO_LITELLM": "1",
        }
        
        with patch.dict(os.environ, env_patch, clear=False):
            config = get_model_config()
            assert config.fallback_on_blocked is True
            assert config.fallback_gemini_to_litellm is True


class TestBuildAgentModel:
    """Tests for build_agent_model() function."""

    def test_gemini_provider_returns_string(self):
        """Test that Gemini provider returns a model string."""
        from databricks_rlm_agent.modeling.model_factory import (
            build_agent_model,
            ModelConfig,
        )
        
        config = ModelConfig(provider="gemini", gemini_model="gemini-3-pro-preview")
        model = build_agent_model(config)
        
        assert isinstance(model, str)
        assert model == "gemini-3-pro-preview"

    def test_litellm_provider_returns_litellm_object(self):
        """Test that LiteLLM provider returns a LiteLlm instance."""
        from databricks_rlm_agent.modeling.model_factory import (
            build_agent_model,
            ModelConfig,
        )
        
        # Mock the LiteLlm class since we may not have google-adk installed in test env
        with patch("databricks_rlm_agent.modeling.model_factory.build_litellm_model") as mock_build:
            mock_litellm = MagicMock()
            mock_build.return_value = mock_litellm
            
            config = ModelConfig(provider="litellm", litellm_model="openai/gpt-4o")
            model = build_agent_model(config)
            
            mock_build.assert_called_once_with("openai/gpt-4o")
            assert model is mock_litellm

"""Secrets and configuration loader for Databricks RLM Agent.

This module provides a secure way to load API keys and credentials from
Databricks Secrets when running as a wheel/spark_python_task in Databricks Jobs.

Usage:
    # At application startup (in run.py):
    from .secrets import load_secrets
    load_secrets(spark)  # Loads secrets into os.environ

Configuration:
    Secrets are loaded via two methods (in order of precedence):
    1. Environment variables (injected by Databricks Jobs via secret references)
    2. dbutils.secrets.get() fallback (when env vars are not set)

    For dbutils fallback, configure these env vars:
    - ADK_SECRET_SCOPE: The Databricks secret scope name (default: "adk-secrets")

    Expected secrets in the scope:
    - google-api-key -> GOOGLE_API_KEY
    - openai-api-key -> OPENAI_API_KEY (optional)
    - anthropic-api-key -> ANTHROPIC_API_KEY (optional)
    - databricks-host -> DATABRICKS_HOST (optional, for Jobs API calls)
    - databricks-token -> DATABRICKS_TOKEN (optional, for Jobs API calls)

Databricks Jobs Configuration (Recommended):
    In your job/task definition, configure environment variables using secret references:

    "spark_python_task": {
        "python_file": "...",
        "parameters": [...],
    },
    "environment": {
        "env_vars": {
            "GOOGLE_API_KEY": "{{secrets/adk-secrets/google-api-key}}",
            "DATABRICKS_HOST": "{{secrets/adk-secrets/databricks-host}}",
            "DATABRICKS_TOKEN": "{{secrets/adk-secrets/databricks-token}}"
        }
    }
"""

import os
import logging
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Default secret scope name
DEFAULT_SECRET_SCOPE = "adk-secrets"

# Mapping of secret key names (in Databricks Secrets) to environment variable names
SECRET_KEY_MAPPING = {
    "google-api-key": "GOOGLE_API_KEY",
    "openai-api-key": "OPENAI_API_KEY",
    "anthropic-api-key": "ANTHROPIC_API_KEY",
    "databricks-host": "DATABRICKS_HOST",
    "databricks-token": "DATABRICKS_TOKEN",
}

# Required secrets (will raise error if not found)
REQUIRED_SECRETS = {"google-api-key"}

# Optional secrets (will log warning if not found)
OPTIONAL_SECRETS = {
    "openai-api-key",
    "anthropic-api-key",
    "databricks-host",
    "databricks-token",
}


def _get_dbutils(spark: "SparkSession"):
    """Get dbutils from SparkSession (Databricks Runtime only).

    Args:
        spark: Active SparkSession.

    Returns:
        dbutils object if available, None otherwise.
    """
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        logger.debug("pyspark.dbutils not available (not running on Databricks Runtime)")
        return None
    except Exception as e:
        logger.debug(f"Could not initialize DBUtils: {e}")
        return None


def _get_secret_from_dbutils(
    dbutils,
    scope: str,
    key: str,
) -> Optional[str]:
    """Retrieve a secret from Databricks Secrets using dbutils.

    Args:
        dbutils: The dbutils object.
        scope: Secret scope name.
        key: Secret key name within the scope.

    Returns:
        Secret value if found, None otherwise.
    """
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        logger.debug(f"Could not retrieve secret '{key}' from scope '{scope}': {e}")
        return None


def load_secrets(
    spark: Optional["SparkSession"] = None,
    scope: Optional[str] = None,
    required_keys: Optional[set] = None,
) -> dict[str, bool]:
    """Load secrets into environment variables.

    This function loads secrets from environment variables first (preferred),
    then falls back to dbutils.secrets.get() for any missing secrets.

    Args:
        spark: Optional SparkSession for dbutils access. If None, only env vars are used.
        scope: Secret scope name. Defaults to ADK_SECRET_SCOPE env var or "adk-secrets".
        required_keys: Set of secret keys that must be present. Defaults to REQUIRED_SECRETS.

    Returns:
        Dict mapping secret keys to whether they were successfully loaded.

    Raises:
        ValueError: If any required secret is not found.
    """
    scope = scope or os.environ.get("ADK_SECRET_SCOPE", DEFAULT_SECRET_SCOPE)
    required_keys = required_keys if required_keys is not None else REQUIRED_SECRETS

    results = {}
    missing_required = []

    # Try to get dbutils for fallback
    dbutils = None
    if spark is not None:
        dbutils = _get_dbutils(spark)
        if dbutils:
            logger.info(f"dbutils available for secrets fallback (scope: {scope})")

    for secret_key, env_var in SECRET_KEY_MAPPING.items():
        # Check if already set in environment (highest priority)
        if os.environ.get(env_var):
            logger.info(f"Secret '{secret_key}' loaded from environment variable {env_var}")
            results[secret_key] = True
            continue

        # Try dbutils fallback
        if dbutils:
            value = _get_secret_from_dbutils(dbutils, scope, secret_key)
            if value:
                os.environ[env_var] = value
                logger.info(f"Secret '{secret_key}' loaded from Databricks Secrets ({scope}/{secret_key})")
                results[secret_key] = True
                continue

        # Secret not found
        results[secret_key] = False
        if secret_key in required_keys:
            missing_required.append(secret_key)
        elif secret_key in OPTIONAL_SECRETS:
            logger.debug(f"Optional secret '{secret_key}' not found (env: {env_var})")

    # Raise error if required secrets are missing
    if missing_required:
        raise ValueError(
            f"Required secrets not found: {missing_required}. "
            f"Configure them via:\n"
            f"  1. Environment variables in Databricks Job config using secret references, e.g.:\n"
            f"     \"GOOGLE_API_KEY\": \"{{{{secrets/{scope}/google-api-key}}}}\"\n"
            f"  2. Or ensure dbutils.secrets.get(scope='{scope}', key='<key>') works\n"
            f"Scope: {scope}, Required keys: {required_keys}"
        )

    return results


def get_secret(
    key: str,
    spark: Optional["SparkSession"] = None,
    scope: Optional[str] = None,
) -> Optional[str]:
    """Get a single secret value.

    Checks environment variable first, then falls back to dbutils.

    Args:
        key: The secret key name (e.g., "google-api-key").
        spark: Optional SparkSession for dbutils access.
        scope: Secret scope name.

    Returns:
        Secret value if found, None otherwise.
    """
    env_var = SECRET_KEY_MAPPING.get(key)
    if env_var and os.environ.get(env_var):
        return os.environ[env_var]

    scope = scope or os.environ.get("ADK_SECRET_SCOPE", DEFAULT_SECRET_SCOPE)

    if spark:
        dbutils = _get_dbutils(spark)
        if dbutils:
            return _get_secret_from_dbutils(dbutils, scope, key)

    return None


def validate_secrets() -> dict[str, bool]:
    """Validate that required secrets are present in environment.

    Call this after load_secrets() to verify configuration.

    Returns:
        Dict mapping env var names to whether they are set.
    """
    return {
        env_var: bool(os.environ.get(env_var))
        for env_var in SECRET_KEY_MAPPING.values()
    }


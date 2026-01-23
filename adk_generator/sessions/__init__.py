"""Sessions package for ADK Generator.

This package provides custom session services for Google ADK,
including the DeltaSessionService for Databricks Unity Catalog.
"""

from .delta_session_service import DeltaSessionService

__all__ = ["DeltaSessionService"]

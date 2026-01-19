"""ADK Generator Application.

This module defines the ADK App that wraps the generator's root agent.
"""

from google.adk.apps import App
from .agent import root_agent

# The ADK Generator itself is an ADK App!
generator_app = App(
    name="adk_generator",
    root_agent=root_agent,
    plugins=[],  # Can add plugins for extended functionality
)

__all__ = ['generator_app']

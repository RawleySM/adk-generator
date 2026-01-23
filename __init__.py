"""ADK Generator - Build Google ADK agents with AI assistance.

This package provides a multi-agent system built with Google ADK that generates
other ADK agent projects. It uses specialist agents for design, code generation,
and review.
"""

from .adk_generator.agent import root_agent
from .adk_generator.agent import app as generator_app

__version__ = "0.1.0"

__all__ = ['generator_app', 'root_agent']

"""ADK Generator - Build Google ADK agents with AI assistance.

This package provides a multi-agent system built with Google ADK that generates
other ADK agent projects. It uses specialist agents for design, code generation,
and review.
"""

from .app import generator_app
from .agent import root_agent

__version__ = "0.1.0"

__all__ = ['generator_app', 'root_agent']

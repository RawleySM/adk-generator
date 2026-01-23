"""Pytest configuration for tests."""

import sys
from pathlib import Path

# Add adk_generator to path without importing root __init__.py
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

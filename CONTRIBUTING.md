# Contributing to ADK Generator

Thank you for your interest in contributing to the ADK Generator!

## Development Setup

1. **Clone the repository**:
```bash
git clone https://github.com/RawleySM/adk-builder-manus.git
cd adk-builder-manus
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
pip install -e ".[dev]"
```

3. **Run tests**:
```bash
pytest tests/ -v
```

## Project Structure

```
adk-generator/
├── agents/           # Specialist generator agents
├── tools/            # FunctionTools for file ops, templates, config
├── templates/        # Jinja2 templates for code generation
├── tests/            # Test suite
├── agent.py          # Root coordinator agent
├── app.py            # ADK App wrapper
└── cli.py            # Command-line interface
```

## Adding a New Generator Agent

To add a new specialist agent (e.g., for generating tests, documentation, etc.):

1. **Create the agent file** in `agents/`:
```python
# agents/test_generator.py
from google.adk.agents import LlmAgent
from ..tools import render_template_tool, write_file_tool

test_generator = LlmAgent(
    name="test_generator",
    model="gemini-2.5-flash",
    description="Generates test files for ADK agents",
    instruction="""
    You generate pytest test files for ADK agents...
    """,
    tools=[render_template_tool, write_file_tool]
)
```

2. **Create templates** in `templates/tests/`:
```jinja2
{# templates/tests/test_agent.py.jinja2 #}
"""Tests for {{ project_name }}."""

import pytest
from {{ project_name }} import root_agent

def test_agent_creation():
    assert root_agent.name == "{{ agent_name }}"
```

3. **Register the agent** in `agents/__init__.py`:
```python
from .test_generator import test_generator

__all__ = [..., 'test_generator']
```

4. **Add to root agent** in `agent.py`:
```python
from .agents import (..., test_generator)

root_agent = LlmAgent(
    ...
    sub_agents=[..., test_generator]
)
```

5. **Update root agent instruction** to delegate to the new agent when appropriate.

## Adding New Tools

To add new FunctionTools:

1. **Create the function** in the appropriate tools file:
```python
# tools/file_tools.py
def copy_file(source: str, destination: str) -> str:
    """Copy a file from source to destination."""
    try:
        shutil.copy2(source, destination)
        return f"✅ Copied {source} to {destination}"
    except Exception as e:
        return f"❌ Error copying file: {str(e)}"
```

2. **Wrap as FunctionTool** in `tools/__init__.py`:
```python
from .file_tools import copy_file

copy_file_tool = FunctionTool(copy_file)

__all__ = [..., 'copy_file_tool']
```

3. **Add to agent tools** where needed:
```python
from ..tools import copy_file_tool

agent = LlmAgent(
    ...
    tools=[..., copy_file_tool]
)
```

## Adding New Templates

1. **Create the template** in the appropriate directory:
```
templates/
└── new_feature/
    └── new_file.py.jinja2
```

2. **Use Jinja2 syntax** with proper variable interpolation:
```jinja2
"""{{ description }}"""

from google.adk.agents import LlmAgent

{% if has_feature %}
# Feature-specific code
{% endif %}
```

3. **Document required context** in the generator agent that uses it:
```python
instruction="""
...
Required Context for Template:
```json
{
  "project_name": "my_agent",
  "description": "Agent description",
  "has_feature": true
}
```
...
"""
```

## Code Style

- Follow PEP 8
- Use type hints for all function parameters and returns
- Add docstrings to all public functions
- Use meaningful variable names
- Keep functions focused (single responsibility)

### Formatting

```bash
# Format code
black adk_generator/

# Check types
mypy adk_generator/

# Lint
pylint adk_generator/
```

## Testing

- Write tests for all new tools
- Test templates with various contexts
- Test error handling
- Use pytest fixtures for common setup

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_tools.py -v

# Run with coverage
pytest tests/ --cov=adk_generator
```

## Documentation

- Update README.md if adding major features
- Add docstrings to all functions
- Include examples in docstrings
- Update CONTRIBUTING.md if changing development workflow

## Pull Request Process

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/my-feature`
3. **Make your changes**
4. **Run tests**: `pytest tests/ -v`
5. **Format code**: `black adk_generator/`
6. **Commit**: `git commit -m "Add my feature"`
7. **Push**: `git push origin feature/my-feature`
8. **Open a Pull Request**

### PR Guidelines

- Clear description of changes
- Reference any related issues
- Include tests for new features
- Update documentation as needed
- Ensure all tests pass
- Follow code style guidelines

## Questions?

Open an issue or start a discussion on GitHub!

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

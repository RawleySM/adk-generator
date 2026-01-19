# ADK Generator

**Build Google ADK agents with AI assistance**

A multi-agent system built with Google ADK that generates other ADK agent projects. The generator itself is an ADK application that uses specialist agents for design, code generation, and review.

## Features

- 🤖 **Multi-Agent Architecture**: Uses Google ADK's native multi-agent system
- 🎨 **Design-First Workflow**: Creates pseudocode and flowcharts before implementation
- ✅ **User Approval**: Requires explicit approval before generating code
- 🔧 **Modular Generation**: Separate agents for callbacks, tools, memory, etc.
- 📝 **Code Review**: Automated review for best practices and correctness
- 🚀 **Complete Projects**: Generates ready-to-run ADK applications

## Architecture

The generator is itself an ADK App with a root agent that coordinates specialist agents:

```
App: adk_generator
└── Root Agent (LlmAgent)
    ├── Design Agent - Creates architecture and pseudocode
    ├── Base Agent Generator - Generates core agent structure
    ├── Callbacks Generator - Generates callback implementations
    ├── Tools Generator - Generates tool definitions
    ├── Memory Generator - Generates session/memory configuration
    └── Review Agent - Reviews generated code
```

## Installation

```bash
# Clone the repository
git clone https://github.com/RawleySM/adk-builder-manus.git
cd adk-builder-manus

# Install dependencies
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

## Usage

### Interactive CLI

```bash
python -m adk_generator.cli

# Or if installed
adk-gen
```

The CLI will guide you through the generation process:

1. Ask about your requirements
2. Generate a design with pseudocode and flowcharts
3. Wait for your approval
4. Generate the complete project
5. Review the code

### Programmatic Usage

```python
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from adk_generator import generator_app

# Create a runner
runner = Runner(
    agent=generator_app.root_agent,
    app_name="adk_generator",
    session_service=InMemorySessionService()
)

# Generate an agent
result = runner.run(
    "Generate a customer support agent that searches a knowledge base "
    "and creates support tickets. Output to ./my_agent"
)

print(result)
```

### With Requirements File

```bash
# Create a requirements file
cat > requirements.txt << EOF
Build a customer support agent that:
- Searches an internal knowledge base via API
- Creates support tickets in Zendesk
- Logs all interactions
- Remembers conversation history
EOF

# Generate
adk-gen --requirements requirements.txt --output ./my_agent
```

## Generated Project Structure

The generator creates a complete, ready-to-run ADK project:

```
my_agent/
├── agent.py              # Main agent definition
├── app.py                # ADK App wrapper
├── __init__.py           # Package initialization
├── tools.py              # Tool definitions (if needed)
├── callbacks.py          # Callback implementations (if needed)
├── session_config.py     # Session/memory config (if needed)
├── requirements.txt      # Python dependencies
├── pyproject.toml        # Project configuration
└── README.md             # Usage instructions
```

## Example Workflow

```
$ adk-gen

Welcome to the ADK Generator!

I'll help you build a Google ADK agent by asking a few questions.

You: I need an agent that searches a knowledge base and creates support tickets

Generator: Great! Let me clarify a few things:
1. What knowledge base API will you use?
2. What ticketing system?
3. Do you need logging?
4. Should it remember conversation history?

You: Use a REST API for the KB, Zendesk for tickets, yes to logging, yes to memory

Generator: Perfect! Let me create a design...

[Design Agent creates pseudocode and flowchart]

Generator: Here's the proposed design. Please review and let me know if I should proceed.

You: yes

Generator: Starting implementation...
✅ Generated agent.py
✅ Generated app.py
✅ Generated tools.py
✅ Generated callbacks.py
✅ Generated session_config.py
✅ Generated requirements.txt
✅ Generated README.md

[Review Agent checks the code]

Generator: Code review complete! All checks passed. Your agent is ready at ./generated_agent
```

## How It Works

### 1. Design Phase

The **Design Agent** analyzes your requirements and creates:
- Pseudocode showing the agent structure
- Mermaid flowchart showing the data flow
- List of required features (callbacks, tools, memory, etc.)

### 2. Approval

The generator **waits for your explicit approval** before proceeding. This ensures you're happy with the design before any code is generated.

### 3. Generation Phase

Based on the approved design, specialist agents generate code:

- **Base Agent Generator**: Creates the core agent structure (agent.py, app.py, __init__.py)
- **Callbacks Generator**: Creates callback implementations for logging, validation, etc.
- **Tools Generator**: Creates tool definitions and implementations
- **Memory Generator**: Creates session and memory configuration

### 4. Review Phase

The **Review Agent** checks the generated code for:
- Proper ADK component usage
- Code quality (type hints, docstrings, PEP 8)
- Security issues
- Best practices

### 5. Final Output

You receive a complete, working ADK project with:
- All source files
- Dependencies listed
- README with usage instructions
- Ready to run!

## Core Tools

The generator uses ADK's `FunctionTool` to wrap Python functions:

- **File Tools**: write_file, read_file, create_directory
- **Template Tools**: render_template (Jinja2), render_template_to_file
- **Config Tools**: merge_config, merge_dependencies, create_pyproject_toml

## Templates

Templates are organized by feature:

```
templates/
├── base_agent/
│   ├── agent.py.jinja2
│   ├── app.py.jinja2
│   ├── __init__.py.jinja2
│   └── README.md.jinja2
├── callbacks/
│   └── callbacks.py.jinja2
├── tools/
│   └── tools.py.jinja2
└── memory/
    └── session_config.py.jinja2
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Quality

```bash
# Format
black adk_generator/

# Type checking
mypy adk_generator/

# Linting
pylint adk_generator/
```

### Adding New Features

To add a new generator feature:

1. Create a new agent in `agents/`
2. Add templates in `templates/`
3. Register the agent in `agent.py` as a sub-agent
4. Update the root agent's instruction

## Requirements

- Python 3.10+
- google-adk >= 0.1.0
- jinja2 >= 3.1.0
- deepmerge >= 1.1.0
- pydantic >= 2.0.0

## License

Apache 2.0

## Contributing

Contributions welcome! Please open an issue or PR.

## Credits

Built with [Google ADK](https://github.com/google/adk-python) - Agent Development Kit

## Repository

https://github.com/RawleySM/adk-builder-manus

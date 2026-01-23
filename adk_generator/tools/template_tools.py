"""Template rendering tools for the ADK generator."""

from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
import json
from typing import Dict, Any


# Initialize Jinja2 environment
TEMPLATE_DIR = Path(__file__).parent.parent / "templates"
jinja_env = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    trim_blocks=True,
    lstrip_blocks=True,
)


def render_template(template_name: str, context_json: str) -> str:
    """Render a Jinja2 template with the provided context.
    
    Args:
        template_name: Name of the template file relative to templates/ directory
                      (e.g., 'base_agent/agent.py.jinja2')
        context_json: JSON string of the context dictionary to pass to the template
        
    Returns:
        The rendered template content as a string
        
    Example:
        context = {"project_name": "my_agent", "model": "gemini-2.5-flash"}
        result = render_template("base_agent/agent.py.jinja2", json.dumps(context))
    """
    try:
        # Parse the context JSON
        context = json.loads(context_json)
        
        # Load and render the template
        template = jinja_env.get_template(template_name)
        rendered = template.render(**context)
        
        return rendered
    except json.JSONDecodeError as e:
        return f"❌ Invalid JSON context: {str(e)}"
    except TemplateNotFound:
        return f"❌ Template not found: {template_name}"
    except Exception as e:
        return f"❌ Error rendering template {template_name}: {str(e)}"


def list_templates(category: str = "") -> str:
    """List available templates, optionally filtered by category.
    
    Args:
        category: Optional category to filter templates (e.g., 'base_agent', 'callbacks')
        
    Returns:
        Formatted list of available templates
    """
    try:
        if category:
            search_path = TEMPLATE_DIR / category
        else:
            search_path = TEMPLATE_DIR
        
        if not search_path.exists():
            return f"❌ Template category not found: {category}"
        
        templates = sorted(search_path.rglob("*.jinja2"))
        if not templates:
            return f"No templates found in {category or 'templates/'}"
        
        template_list = "\n".join([
            f"  - {t.relative_to(TEMPLATE_DIR)}" for t in templates
        ])
        return f"Available templates:\n{template_list}"
    except Exception as e:
        return f"❌ Error listing templates: {str(e)}"


def render_template_to_file(
    template_name: str,
    output_path: str,
    context_json: str
) -> str:
    """Render a template and write it directly to a file.
    
    Args:
        template_name: Name of the template file
        output_path: Path where the rendered content should be written
        context_json: JSON string of the context dictionary
        
    Returns:
        Success message or error
    """
    try:
        # Render the template
        rendered = render_template(template_name, context_json)
        
        # Check if rendering failed
        if rendered.startswith("❌"):
            return rendered
        
        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(rendered, encoding='utf-8')
        
        return f"✅ Rendered {template_name} to {output_path}"
    except Exception as e:
        return f"❌ Error rendering template to file: {str(e)}"

"""Configuration management tools for the ADK generator."""

import json
from typing import Dict, Any
from deepmerge import always_merger


def merge_config(base_json: str, new_json: str) -> str:
    """Deep merge two configuration dictionaries.
    
    This function performs a deep merge where:
    - Dictionaries are recursively merged
    - Lists are concatenated and deduplicated
    - Scalar values from new_json override base_json
    
    Args:
        base_json: JSON string of the base configuration
        new_json: JSON string of the new configuration to merge in
        
    Returns:
        JSON string of the merged configuration
        
    Example:
        base = {"dependencies": ["google-adk"], "name": "my_app"}
        new = {"dependencies": ["pydantic"], "version": "0.1.0"}
        result = merge_config(json.dumps(base), json.dumps(new))
        # Result: {"dependencies": ["google-adk", "pydantic"], "name": "my_app", "version": "0.1.0"}
    """
    try:
        base = json.loads(base_json)
        new = json.loads(new_json)
        
        # Perform deep merge
        merged = always_merger.merge(base, new)
        
        return json.dumps(merged, indent=2)
    except json.JSONDecodeError as e:
        return f"❌ Invalid JSON: {str(e)}"
    except Exception as e:
        return f"❌ Error merging configurations: {str(e)}"


def merge_dependencies(base_deps: str, new_deps: str) -> str:
    """Merge two lists of Python dependencies, removing duplicates.
    
    Args:
        base_deps: JSON array string of base dependencies
        new_deps: JSON array string of new dependencies to add
        
    Returns:
        JSON array string of merged dependencies
        
    Example:
        base = ["google-adk>=0.1.0", "pydantic>=2.0"]
        new = ["pydantic>=2.0", "jinja2>=3.0"]
        result = merge_dependencies(json.dumps(base), json.dumps(new))
        # Result: ["google-adk>=0.1.0", "pydantic>=2.0", "jinja2>=3.0"]
    """
    try:
        base = json.loads(base_deps)
        new = json.loads(new_deps)
        
        if not isinstance(base, list) or not isinstance(new, list):
            return "❌ Both arguments must be JSON arrays"
        
        # Merge and deduplicate while preserving order
        seen = set()
        merged = []
        
        for dep in base + new:
            # Extract package name (before >=, ==, etc.)
            pkg_name = dep.split('>=')[0].split('==')[0].split('<')[0].strip()
            if pkg_name not in seen:
                seen.add(pkg_name)
                merged.append(dep)
        
        return json.dumps(merged, indent=2)
    except json.JSONDecodeError as e:
        return f"❌ Invalid JSON: {str(e)}"
    except Exception as e:
        return f"❌ Error merging dependencies: {str(e)}"


def create_pyproject_toml(config_json: str) -> str:
    """Create a pyproject.toml content from configuration.
    
    Args:
        config_json: JSON string with project configuration
                    Expected keys: name, version, description, dependencies, etc.
        
    Returns:
        The pyproject.toml content as a string
    """
    try:
        config = json.loads(config_json)
        
        name = config.get('name', 'my_agent')
        version = config.get('version', '0.1.0')
        description = config.get('description', 'An ADK agent')
        dependencies = config.get('dependencies', ['google-adk>=0.1.0'])
        
        toml_content = f'''[project]
name = "{name}"
version = "{version}"
description = "{description}"
requires-python = ">=3.10"
dependencies = [
'''
        
        for dep in dependencies:
            toml_content += f'    "{dep}",\n'
        
        toml_content += ''']

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"
'''
        
        return toml_content
    except json.JSONDecodeError as e:
        return f"❌ Invalid JSON: {str(e)}"
    except Exception as e:
        return f"❌ Error creating pyproject.toml: {str(e)}"


def create_requirements_txt(dependencies_json: str) -> str:
    """Create a requirements.txt content from a list of dependencies.
    
    Args:
        dependencies_json: JSON array string of dependencies
        
    Returns:
        The requirements.txt content as a string
    """
    try:
        dependencies = json.loads(dependencies_json)
        
        if not isinstance(dependencies, list):
            return "❌ Dependencies must be a JSON array"
        
        return "\n".join(dependencies) + "\n"
    except json.JSONDecodeError as e:
        return f"❌ Invalid JSON: {str(e)}"
    except Exception as e:
        return f"❌ Error creating requirements.txt: {str(e)}"

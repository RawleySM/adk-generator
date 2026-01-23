"""Tools package for the ADK generator.

This package provides FunctionTools for file operations, template rendering,
and configuration management.
"""

from google.adk.tools import FunctionTool

from .file_tools import (
    write_file,
    read_file,
    list_files,
    create_directory,
)

from .template_tools import (
    render_template,
    list_templates,
    render_template_to_file,
)

from .config_tools import (
    merge_config,
    merge_dependencies,
    create_pyproject_toml,
    create_requirements_txt,
)

# Wrap all functions as ADK FunctionTools
write_file_tool = FunctionTool(write_file)
read_file_tool = FunctionTool(read_file)
list_files_tool = FunctionTool(list_files)
create_directory_tool = FunctionTool(create_directory)

render_template_tool = FunctionTool(render_template)
list_templates_tool = FunctionTool(list_templates)
render_template_to_file_tool = FunctionTool(render_template_to_file)

merge_config_tool = FunctionTool(merge_config)
merge_dependencies_tool = FunctionTool(merge_dependencies)
create_pyproject_toml_tool = FunctionTool(create_pyproject_toml)
create_requirements_txt_tool = FunctionTool(create_requirements_txt)

__all__ = [
    # File tools
    'write_file_tool',
    'read_file_tool',
    'list_files_tool',
    'create_directory_tool',
    # Template tools
    'render_template_tool',
    'list_templates_tool',
    'render_template_to_file_tool',
    # Config tools
    'merge_config_tool',
    'merge_dependencies_tool',
    'create_pyproject_toml_tool',
    'create_requirements_txt_tool',
]

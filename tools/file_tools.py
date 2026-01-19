"""File operation tools for the ADK generator."""

from pathlib import Path
from typing import Optional


def write_file(path: str, content: str) -> str:
    """Write content to a file.
    
    Args:
        path: The file path to write to (relative or absolute)
        content: The content to write to the file
        
    Returns:
        Success message with the path
        
    Raises:
        IOError: If the file cannot be written
    """
    try:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding='utf-8')
        return f"✅ Successfully wrote {len(content)} characters to {path}"
    except Exception as e:
        return f"❌ Error writing to {path}: {str(e)}"


def read_file(path: str) -> str:
    """Read content from a file.
    
    Args:
        path: The file path to read from
        
    Returns:
        The file content as a string
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If the file cannot be read
    """
    try:
        file_path = Path(path)
        if not file_path.exists():
            return f"❌ File not found: {path}"
        content = file_path.read_text(encoding='utf-8')
        return content
    except Exception as e:
        return f"❌ Error reading {path}: {str(e)}"


def list_files(directory: str, pattern: str = "*") -> str:
    """List files in a directory matching a pattern.
    
    Args:
        directory: The directory to list files from
        pattern: Glob pattern to match files (default: "*")
        
    Returns:
        Formatted list of files
    """
    try:
        dir_path = Path(directory)
        if not dir_path.exists():
            return f"❌ Directory not found: {directory}"
        
        files = sorted(dir_path.glob(pattern))
        if not files:
            return f"No files found matching '{pattern}' in {directory}"
        
        file_list = "\n".join([f"  - {f.name}" for f in files])
        return f"Files in {directory}:\n{file_list}"
    except Exception as e:
        return f"❌ Error listing files: {str(e)}"


def create_directory(path: str) -> str:
    """Create a directory and all parent directories.
    
    Args:
        path: The directory path to create
        
    Returns:
        Success message
    """
    try:
        dir_path = Path(path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return f"✅ Created directory: {path}"
    except Exception as e:
        return f"❌ Error creating directory {path}: {str(e)}"

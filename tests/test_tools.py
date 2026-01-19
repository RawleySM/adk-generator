"""Tests for the generator tools."""

import json
import tempfile
from pathlib import Path

import pytest

from adk_generator.tools.file_tools import (
    write_file,
    read_file,
    create_directory,
    list_files,
)
from adk_generator.tools.config_tools import (
    merge_config,
    merge_dependencies,
    create_requirements_txt,
)


class TestFileTools:
    """Tests for file operation tools."""
    
    def test_write_and_read_file(self):
        """Test writing and reading a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.txt"
            content = "Hello, ADK Generator!"
            
            # Write file
            result = write_file(str(filepath), content)
            assert "✅" in result
            assert filepath.exists()
            
            # Read file
            read_content = read_file(str(filepath))
            assert read_content == content
    
    def test_create_directory(self):
        """Test creating a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dirpath = Path(tmpdir) / "subdir" / "nested"
            
            result = create_directory(str(dirpath))
            assert "✅" in result
            assert dirpath.exists()
            assert dirpath.is_dir()
    
    def test_list_files(self):
        """Test listing files in a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some files
            (Path(tmpdir) / "file1.txt").write_text("content1")
            (Path(tmpdir) / "file2.txt").write_text("content2")
            (Path(tmpdir) / "file3.py").write_text("content3")
            
            # List all files
            result = list_files(tmpdir, "*.txt")
            assert "file1.txt" in result
            assert "file2.txt" in result
            assert "file3.py" not in result


class TestConfigTools:
    """Tests for configuration management tools."""
    
    def test_merge_config(self):
        """Test merging two configurations."""
        base = {"name": "my_app", "dependencies": ["google-adk"]}
        new = {"version": "0.1.0", "dependencies": ["pydantic"]}
        
        result = merge_config(json.dumps(base), json.dumps(new))
        merged = json.loads(result)
        
        assert merged["name"] == "my_app"
        assert merged["version"] == "0.1.0"
        assert "google-adk" in merged["dependencies"]
        assert "pydantic" in merged["dependencies"]
    
    def test_merge_dependencies(self):
        """Test merging dependency lists."""
        base = ["google-adk>=0.1.0", "pydantic>=2.0"]
        new = ["pydantic>=2.0", "jinja2>=3.0"]
        
        result = merge_dependencies(json.dumps(base), json.dumps(new))
        merged = json.loads(result)
        
        assert "google-adk>=0.1.0" in merged
        assert "jinja2>=3.0" in merged
        # Should deduplicate pydantic
        assert merged.count("pydantic>=2.0") == 1
    
    def test_create_requirements_txt(self):
        """Test creating requirements.txt content."""
        deps = ["google-adk>=0.1.0", "jinja2>=3.1.0", "pydantic>=2.0.0"]
        
        result = create_requirements_txt(json.dumps(deps))
        
        assert "google-adk>=0.1.0" in result
        assert "jinja2>=3.1.0" in result
        assert "pydantic>=2.0.0" in result
        assert result.endswith("\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

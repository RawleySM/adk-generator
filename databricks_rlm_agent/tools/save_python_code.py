import os
from google.adk.tools import ToolContext

# Databricks Volumes paths - configurable via environment variables
AGENT_CODE_PATH = os.environ.get("ADK_AGENT_CODE_PATH", "/Volumes/silo_dev_rs/adk/agent_code/agent_code_raw.py")

def save_python_code(code: str, tool_context: ToolContext) -> dict:
    """
    Saves generated Python code to a specific Databricks Volume location.
    Target path: /Volumes/silo_dev_rs/adk/agent_code/agent_code_raw.py

    Args:
        code (str): The Python code to save.
        tool_context (ToolContext): The tool context.

    Returns:
        dict: Status of the save operation.
    """
    import os

    try:
        # Ensure the directory exists
        directory = os.path.dirname(AGENT_CODE_PATH)
        os.makedirs(directory, exist_ok=True)

        # Write the content to the file
        with open(AGENT_CODE_PATH, 'w') as f:
            f.write(code)

        print(f"Agent code saved successfully to: {AGENT_CODE_PATH}")
        return {
            "status": "success",
            "message": f"Code saved to {AGENT_CODE_PATH}",
            "file_path": AGENT_CODE_PATH
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

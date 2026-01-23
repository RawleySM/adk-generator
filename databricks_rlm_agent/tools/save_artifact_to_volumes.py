import os
from google.adk.tools import ToolContext

# Databricks Volumes paths - configurable via environment variables
ARTIFACTS_PATH = os.environ.get("ADK_ARTIFACTS_PATH", "/Volumes/silo_dev_rs/adk/artifacts")

def save_artifact_to_volumes(filename: str, content: str, tool_context: ToolContext) -> dict:
    """
    Saves a file artifact to Databricks Volumes.

    Args:
        filename (str): The name of the file to save.
        content (str): The content to write to the file.
        tool_context (ToolContext): The tool context.

    Returns:
        dict: Status of the save operation with the file path.
    """
    import os

    try:
        # Ensure the artifacts directory exists
        os.makedirs(ARTIFACTS_PATH, exist_ok=True)

        # Full path for the artifact
        file_path = os.path.join(ARTIFACTS_PATH, filename)

        # Write the content to the file
        with open(file_path, 'w') as f:
            f.write(content)

        print(f"Artifact saved successfully: {file_path}")
        return {
            "status": "success",
            "message": f"File saved to {file_path}",
            "file_path": file_path
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

import os
from google.adk.tools import ToolContext

# Databricks Volumes paths - configurable via environment variables
AGENT_CODE_PATH = os.environ.get("ADK_AGENT_CODE_PATH", "/Volumes/silo_dev_rs/adk/agent_code/agent_code_raw.py")

def llm_query(code: str, tool_context: ToolContext) -> dict:
    """
    Saves agent-generated code with embedded instructions for sub-LLM processing.

    This tool writes code to AGENT_CODE_PATH in the same manner as save_python_code(),
    allowing the agent to leverage sub-LLM capabilities for semantic analysis of data,
    code review, and contextual understanding tasks within the REPL environment.

    The code should contain:
    - Context data (table schemas, code snippets, data samples)
    - Embedded instructions for analysis
    - Specific questions or tasks for the sub-LLM

    Example usage from RLM_SYSTEM_PROMPT:
        analysis = llm_query(f'''Analyze this table schema for vendor enrichment potential:
        Table: {row.path}
        Columns: {row.column_array}

        Does this table contain columns useful for enriching healthcare vendor masterdata?
        Look for: company name, address, phone, website, industry codes.
        Rate viability: HIGH/MEDIUM/LOW with explanation.''')

    Args:
        code (str): The code string containing context and instructions for the sub-LLM.
        tool_context (ToolContext): The tool context for state management.

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

        print(f"[LLM_QUERY] Code saved successfully to: {AGENT_CODE_PATH}")
        return {
            "status": "success",
            "message": f"Code saved to {AGENT_CODE_PATH}",
            "file_path": AGENT_CODE_PATH
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

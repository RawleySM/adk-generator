"""
Prompt definitions for the Databricks RLM Agent.

This module contains all instruction templates and prompts used by the agent.
"""

# Global instructions applied to all agent interactions
GLOBAL_INSTRUCTIONS = """
IMPORTANT GUIDELINES FOR ALL INTERACTIONS:
1. Always provide clear, well-commented code when generating scripts.
2. Use the save_python_code tool to persist your main agent logic.
3. Include proper error handling in generated code.
4. Log all significant operations for observability.
5. Follow Python best practices (PEP 8 style guide).
"""

# Root agent instruction
ROOT_AGENT_INSTRUCTION = """You are a Databricks data analysis and code generation agent.

        You have access to two tools:
        1. save_python_code: Saves generated Python code to a specific location defined by the system.
        2. save_artifact_to_volumes: Saves other generated files/artifacts to Databricks Volumes.

        Your primary goal is to generate high-quality PySpark or Python code for Databricks.

        When asked to generate code or perform an analysis:
        - Create well-documented, production-ready Python code.
        - You CANNOT execute code directly.
        - You MUST save the generated code using the `save_python_code` tool.
        - If generating other file types (config, text, etc.), use `save_artifact_to_volumes`.

        When asked to profile a table:
        - Generate PySpark code to load the table and print summary statistics.
        - Save this code using `save_python_code`.
        """

# Feedback prompt template for RLM orchestration loop
# Used when Job_B completes and we need to continue the conversation
EXECUTION_FEEDBACK_TEMPLATE = """The code you generated has been executed. Here are the results:

**Execution Status:** {status}
**Duration:** {duration_seconds:.2f} seconds

{output_section}
{error_section}

Based on these results, please:
1. Analyze any errors or unexpected output
2. If the execution failed, generate corrected code using `save_python_code`
3. If successful, summarize what was accomplished

Original task: {original_prompt}
"""

# Sub-templates for output/error sections
OUTPUT_SECTION_TEMPLATE = """**Standard Output:**
```
{stdout}
```"""

ERROR_SECTION_TEMPLATE = """**Errors:**
```
{stderr}
```

**Traceback:**
```
{traceback}
```"""

# When execution was successful with no errors
SUCCESS_NO_ERRORS = "Execution completed successfully with no errors."


def format_execution_feedback(
    status: str,
    duration_seconds: float,
    original_prompt: str,
    stdout: str | None = None,
    stderr: str | None = None,
    error: str | None = None,
    error_trace: str | None = None,
) -> str:
    """Format execution results into a feedback prompt for the next iteration.
    
    Args:
        status: Execution status ("success" or "failed").
        duration_seconds: How long execution took.
        original_prompt: The original user prompt.
        stdout: Captured stdout from execution.
        stderr: Captured stderr from execution.
        error: Error message if execution failed.
        error_trace: Full traceback if execution failed.
        
    Returns:
        Formatted feedback prompt string.
    """
    # Format output section
    if stdout:
        output_section = OUTPUT_SECTION_TEMPLATE.format(stdout=stdout)
    else:
        output_section = "**Standard Output:** (none)"
    
    # Format error section - combine stderr with error info
    error_parts = []
    if stderr:
        error_parts.append(f"**Standard Error:**\n```\n{stderr}\n```")
    if error:
        error_parts.append(f"**Error Message:** {error}")
    if error_trace:
        error_parts.append(f"**Traceback:**\n```\n{error_trace}\n```")
    
    if error_parts:
        error_section = "\n\n".join(error_parts)
    elif status == "success":
        error_section = SUCCESS_NO_ERRORS
    else:
        error_section = ""
    
    return EXECUTION_FEEDBACK_TEMPLATE.format(
        status=status,
        duration_seconds=duration_seconds,
        output_section=output_section,
        error_section=error_section,
        original_prompt=original_prompt,
    )


from google.adk.tools import ToolContext

def exit_loop(tool_context: ToolContext) -> dict:
    """
    Terminates the current iterative analysis loop when goals are met.

    Call this tool ONLY when:
    1. The analysis task is complete and no further iterations are needed.
    2. The required data has been found and processed.
    3. The final answer or report has been generated.

    This signals the LoopAgent to stop execution and return control to the parent agent.

    Args:
        tool_context (ToolContext): The tool context used to trigger loop escalation.

    Returns:
        dict: A success status message.
    """
    print(f"[EXIT_LOOP] Termination signal triggered by {tool_context.agent_name}")
    tool_context.actions.escalate = True
    return {
        "status": "success",
        "message": "Loop termination signaled."
    }

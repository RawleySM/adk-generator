from google.adk.tools import ToolContext

# State key to distinguish exit_loop from delegate_code_results escalation
# Uses temp:rlm:* prefix for invocation-scoped state (auto-discarded after invocation)
STATE_EXIT_REQUESTED = "temp:rlm:exit_requested"


def exit_loop(tool_context: ToolContext) -> dict:
    """
    Terminates the current iterative analysis loop when goals are met.

    Call this tool ONLY when:
    1. The analysis task is complete and no further iterations are needed.
    2. The required data has been found and processed.
    3. The final answer or report has been generated.

    This signals the LoopAgent to stop execution and return control to the parent agent.

    Unlike delegate_code_results (which also escalates to advance to the next sub-agent),
    exit_loop sets temp:rlm:exit_requested=True to signal a full termination request.

    Args:
        tool_context (ToolContext): The tool context used to trigger loop escalation.

    Returns:
        dict: A success status message.
    """
    print(f"[EXIT_LOOP] Termination signal triggered by {tool_context.agent_name}")
    # Set state key to distinguish from delegate_code_results escalation
    # Uses temp:rlm:* for invocation-scoped state (won't leak to next invocation)
    tool_context.state[STATE_EXIT_REQUESTED] = True
    tool_context.actions.escalate = True
    return {
        "status": "success",
        "message": "Loop termination signaled."
    }

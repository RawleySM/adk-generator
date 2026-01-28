"""ADK tool for downloading Jira ticket attachments.

Re-exports the get_Jira_ticket_attachments function from utils/jira_attachments
for use as an ADK FunctionTool.
"""

from databricks_rlm_agent.utils.jira_attachments import get_Jira_ticket_attachments

__all__ = ["get_Jira_ticket_attachments"]

"""ADK tools for Databricks RLM Agent.

This package provides custom tools for the RLM workflow:

Code Generation Tools:
- save_artifact_to_volumes: Generic artifact storage to UC Volumes
- delegate_code_results: Delegates code execution and result processing

Query Tools:
- metadata_keyword_search: Searches Unity Catalog metadata
- repo_filename_search: Searches repository files

File Download Tools:
- get_repo_file: Downloads files from GitHub repos to UC Volumes
- get_Jira_ticket_attachments: DISABLED - functionality moved to ingestor.py download_task_attachments()

Control Flow Tools:
- exit_loop: Terminates the LoopAgent iteration
"""

from .save_artifact_to_volumes import save_artifact_to_volumes
from .metadata_keyword_search import metadata_keyword_search
from .repo_filename_search import repo_filename_search
from .get_repo_file import (
    get_repo_file,
    download_single_file_from_full_filepath,
    download_files_from_full_filepaths,
)
# NOTE: get_Jira_ticket_attachments disabled - functionality is being implemented
# in the task ingestor (ingestor.py) via download_task_attachments()
# from .get_Jira_ticket_attachments import get_Jira_ticket_attachments
from .exit_loop import exit_loop
from .delegate_code_results import delegate_code_results

__all__ = [
    # Code generation tools
    "save_artifact_to_volumes",
    "delegate_code_results",
    # Query tools
    "metadata_keyword_search",
    "repo_filename_search",
    # File download tools
    "get_repo_file",
    "download_single_file_from_full_filepath",
    "download_files_from_full_filepaths",
    # "get_Jira_ticket_attachments",  # DISABLED - see ingestor.py download_task_attachments()
    # Control flow tools
    "exit_loop",
]

"""Jira ticket attachment download utilities.

This module provides functions for downloading Jira attachments to Unity Catalog
Volumes. It can be used as:
  1. A standalone utility via `download_jira_attachments()` (no ToolContext required)
  2. An ADK FunctionTool via `get_Jira_ticket_attachments()` (requires ToolContext)

Expected credentials (Jira Cloud basic auth):
- USER_NAME: Jira username (typically email)
- JIRA_API_KEY: Jira API token

Default output location:
  /Volumes/silo_dev_rs/task/jira_attachment/<ISSUE_KEY>/<filename>
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import requests

# Import ToolContext at runtime (required for ADK FunctionTool introspection)
from google.adk.tools import ToolContext

logger = logging.getLogger(__name__)

# Default Jira Cloud domain (SpendMend)
DEFAULT_JIRA_DOMAIN = "spendmend.atlassian.net"

# Default target volume for Jira attachments
DEFAULT_TARGET_VOLUME = "/Volumes/silo_dev_rs/task/jira_attachment"

# Avoid runaway calls / rate limiting
MAX_TICKETS_PER_CALL = 25
MAX_ATTACHMENTS_PER_TICKET = 200
DOWNLOAD_DELAY_SECONDS = 0.25


def _normalize_volume_path(path: str) -> str:
    """Normalize Volumes path (allow missing leading '/')."""
    p = (path or "").strip()
    if not p:
        return DEFAULT_TARGET_VOLUME
    if not p.startswith("/"):
        p = "/" + p
    return p


def _parse_ticket_csv(tickets_csv: str) -> list[str]:
    """Parse comma-separated Jira ticket keys."""
    if not tickets_csv or not str(tickets_csv).strip():
        return []
    raw = str(tickets_csv)
    parts = [p.strip().upper() for p in raw.split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _get_jira_session(username: str, api_key: str) -> requests.Session:
    """Create an authenticated session for Jira API calls."""
    session = requests.Session()
    session.auth = (username, api_key)
    session.headers.update({"Accept": "application/json"})
    return session


def _maybe_get_jira_creds_from_databricks_secrets() -> tuple[Optional[str], Optional[str]]:
    """Best-effort fallback: try to read Jira creds from Databricks Secrets."""
    username = None
    api_key = None

    try:
        from databricks.sdk import WorkspaceClient

        profile = os.environ.get("DATABRICKS_PROFILE", "rstanhope")
        client = WorkspaceClient(profile=profile)

        scopes = ["adk-secrets", "jira", "secrets", "rlm"]
        username_keys = ["user-name", "jira-user-name", "jira-username", "USER_NAME"]
        api_keys = ["jira-api-key", "jira_api_key", "JIRA_API_KEY"]

        for scope in scopes:
            if username is None:
                for key in username_keys:
                    try:
                        secret = client.secrets.get_secret(scope=scope, key=key)
                        if secret and secret.value:
                            username = secret.value
                            break
                    except Exception:
                        continue

            if api_key is None:
                for key in api_keys:
                    try:
                        secret = client.secrets.get_secret(scope=scope, key=key)
                        if secret and secret.value:
                            api_key = secret.value
                            break
                    except Exception:
                        continue

            if username and api_key:
                break
    except Exception as e:
        logger.debug(f"Could not retrieve Jira creds from Databricks secrets: {e}")

    return username, api_key


def _get_issue_attachments(
    session: requests.Session, domain: str, issue_key: str
) -> tuple[str, list[dict[str, Any]]]:
    """Fetch attachment metadata for a Jira issue. Returns (summary, attachments)."""
    url = f"https://{domain}/rest/api/3/issue/{issue_key}?fields=attachment,summary"
    response = session.get(url, timeout=30)

    if response.status_code == 404:
        raise ValueError(f"Issue {issue_key} not found (404)")
    if response.status_code == 401:
        raise PermissionError("Authentication failed (401)")
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch {issue_key}: HTTP {response.status_code}")

    data = response.json()
    summary = data.get("fields", {}).get("summary", "No summary")
    attachments = data.get("fields", {}).get("attachment", []) or []
    return summary, attachments


def _dedupe_path(target: Path) -> Path:
    """Append _N suffix if target exists."""
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _download_attachment_to_path(
    session: requests.Session,
    attachment: dict[str, Any],
    issue_dir: Path,
) -> dict[str, Any]:
    """Download a single attachment into issue_dir. Returns a result dict."""
    filename = str(attachment.get("filename") or "attachment.bin")
    content_url = attachment.get("content")
    size = attachment.get("size")
    mime_type = attachment.get("mimeType")
    attachment_id = attachment.get("id")

    if not content_url:
        return {
            "success": False,
            "error": "Attachment missing content URL",
            "filename": filename,
            "attachment_id": attachment_id,
        }

    issue_dir.mkdir(parents=True, exist_ok=True)
    out_path = _dedupe_path(issue_dir / filename)

    try:
        # Jira returns 303 redirect to signed URL; follow redirects.
        with session.get(
            content_url,
            timeout=60,
            allow_redirects=True,
            headers={"Accept": "*/*"},
            stream=True,
        ) as resp:
            if resp.status_code != 200:
                return {
                    "success": False,
                    "error": f"Download failed: HTTP {resp.status_code}",
                    "filename": filename,
                    "attachment_id": attachment_id,
                }

            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        return {
            "success": True,
            "filename": filename,
            "output_path": str(out_path),
            "bytes": out_path.stat().st_size if out_path.exists() else size,
            "mime_type": mime_type,
            "attachment_id": attachment_id,
            "content_url": content_url,
        }
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "Timeout downloading attachment",
            "filename": filename,
            "attachment_id": attachment_id,
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {e}",
            "filename": filename,
            "attachment_id": attachment_id,
        }


def download_jira_attachments(
    tickets_csv: str,
    domain: str = DEFAULT_JIRA_DOMAIN,
    target_volume: str = DEFAULT_TARGET_VOLUME,
    session_id: Optional[str] = None,
    invocation_id: Optional[str] = None,
    iteration: int = 0,
    register_artifacts: bool = True,
) -> dict[str, Any]:
    """Download Jira ticket attachments to UC Volumes (standalone utility).

    This function can be called without a ToolContext, making it suitable
    for use by the ingestor or other non-ADK code paths.

    Args:
        tickets_csv: Comma-separated Jira ticket keys (e.g., "TRUL-1,TRUL-2").
        domain: Jira Cloud domain (default: "spendmend.atlassian.net").
        target_volume: UC Volumes directory to write downloads under.
                      Default: "/Volumes/silo_dev_rs/task/jira_attachment".
        session_id: Optional session ID for artifact registry metadata.
        invocation_id: Optional invocation ID for artifact registry metadata.
        iteration: Iteration number for artifact registry metadata.
        register_artifacts: Whether to register downloaded files in ArtifactRegistry.

    Returns:
        dict with:
          - status: "success" | "partial" | "error"
          - tickets: list of processed ticket keys
          - output_directory: base directory written to
          - attachments_total / attachments_downloaded
          - files: per-attachment results (including output_path on success)
          - registry_rows_created: count of ArtifactRegistry rows inserted
    """
    tickets = _parse_ticket_csv(tickets_csv)
    if not tickets:
        return {
            "status": "error",
            "message": "No Jira tickets provided. Pass a comma-separated string like 'TRUL-1,TRUL-2'.",
        }
    if len(tickets) > MAX_TICKETS_PER_CALL:
        return {
            "status": "error",
            "message": f"Too many tickets ({len(tickets)}). Max per call is {MAX_TICKETS_PER_CALL}.",
            "limit": MAX_TICKETS_PER_CALL,
            "requested": len(tickets),
        }

    # Credentials: prefer environment vars (Databricks Jobs best practice)
    username = os.environ.get("USER_NAME")
    api_key = os.environ.get("JIRA_API_KEY")
    if not username or not api_key:
        u2, k2 = _maybe_get_jira_creds_from_databricks_secrets()
        username = username or u2
        api_key = api_key or k2

    if not username:
        return {"status": "error", "message": "Missing USER_NAME for Jira authentication."}
    if not api_key:
        return {"status": "error", "message": "Missing JIRA_API_KEY for Jira authentication."}

    base_dir = Path(_normalize_volume_path(target_volume))
    base_dir.mkdir(parents=True, exist_ok=True)

    http_session = _get_jira_session(username, api_key)

    # Default session/invocation IDs if not provided
    session_id = session_id or f"ingestor_{uuid.uuid4().hex[:8]}"
    invocation_id = invocation_id or f"inv_{uuid.uuid4().hex[:8]}"

    results: dict[str, Any] = {
        "status": "success",
        "tickets": tickets,
        "domain": domain,
        "output_directory": str(base_dir),
        "attachments_total": 0,
        "attachments_downloaded": 0,
        "files": [],
        "registry_rows_created": 0,
        "ticket_summaries": {},
        "ticket_errors": {},
    }

    registry = None
    if register_artifacts:
        try:
            from pyspark.sql import SparkSession
            from databricks_rlm_agent.artifact_registry import get_artifact_registry

            spark = SparkSession.builder.getOrCreate()
            registry = get_artifact_registry(spark, ensure_exists=True)
        except Exception:
            registry = None

    for ti, ticket in enumerate(tickets):
        if ti > 0:
            time.sleep(DOWNLOAD_DELAY_SECONDS)

        try:
            summary, attachments = _get_issue_attachments(http_session, domain, ticket)
            results["ticket_summaries"][ticket] = summary
        except Exception as e:
            results["ticket_errors"][ticket] = str(e)
            continue

        # Limit attachment count per ticket for safety
        attachments = attachments[:MAX_ATTACHMENTS_PER_TICKET]
        results["attachments_total"] += len(attachments)

        issue_dir = base_dir / ticket
        for ai, attachment in enumerate(attachments):
            if ai > 0:
                time.sleep(DOWNLOAD_DELAY_SECONDS)

            file_result = _download_attachment_to_path(http_session, attachment, issue_dir)
            file_result["ticket"] = ticket
            results["files"].append(file_result)

            if file_result.get("success"):
                results["attachments_downloaded"] += 1

                # Create ArtifactRegistry row (best effort)
                if registry is not None:
                    try:
                        artifact_id = f"jira_{uuid.uuid4().hex[:12]}"
                        registry.create_artifact(
                            artifact_id=artifact_id,
                            session_id=str(session_id),
                            invocation_id=str(invocation_id),
                            iteration=iteration,
                            artifact_type="jira_attachment",
                            sublm_instruction=None,
                            code_artifact_key=None,
                            metadata={
                                "ticket": ticket,
                                "summary": results["ticket_summaries"].get(ticket),
                                "filename": file_result.get("filename"),
                                "output_path": file_result.get("output_path"),
                                "bytes": file_result.get("bytes"),
                                "mime_type": file_result.get("mime_type"),
                                "attachment_id": file_result.get("attachment_id"),
                                "content_url": file_result.get("content_url"),
                            },
                        )
                        results["registry_rows_created"] += 1
                        file_result["artifact_id"] = artifact_id
                    except Exception as e:
                        file_result["registry_error"] = str(e)

    # Determine status
    failed = results["attachments_total"] - results["attachments_downloaded"]
    if results["attachments_total"] == 0:
        if results["ticket_errors"]:
            results["status"] = "error"
            results["message"] = "No attachments downloaded; at least one ticket failed to fetch."
        else:
            results["status"] = "success"
            results["message"] = "No attachments found on the provided tickets."
    elif failed == 0:
        results["status"] = "success"
        results["message"] = f"Downloaded {results['attachments_downloaded']} attachments to {results['output_directory']}."
    elif results["attachments_downloaded"] == 0:
        results["status"] = "error"
        results["message"] = "Failed to download any attachments. Check ticket_errors/files for details."
    else:
        results["status"] = "partial"
        results["message"] = f"Downloaded {results['attachments_downloaded']} of {results['attachments_total']} attachments."

    return results


def get_Jira_ticket_attachments(
    tickets_csv: str,
    domain: str = DEFAULT_JIRA_DOMAIN,
    target_volume: str = DEFAULT_TARGET_VOLUME,
    *,
    tool_context: ToolContext,
) -> dict[str, Any]:
    """Download Jira ticket attachments to UC Volumes and register them as artifacts.

    This is the ADK FunctionTool wrapper around `download_jira_attachments()`.
    Provide one or more Jira ticket keys as a comma-separated string.

    Args:
        tickets_csv: Comma-separated Jira ticket keys (e.g., "TRUL-1,TRUL-2").
        domain: Jira Cloud domain (default: "spendmend.atlassian.net").
        target_volume: UC Volumes directory to write downloads under.
                      Default: "/Volumes/silo_dev_rs/task/jira_attachment".
        tool_context: Provided by ADK; used for state updates.

    Returns:
        dict with:
          - status: "success" | "partial" | "error"
          - tickets: list of processed ticket keys
          - output_directory: base directory written to
          - attachments_total / attachments_downloaded
          - files: per-attachment results (including output_path on success)
          - registry_rows_created: count of ArtifactRegistry rows inserted
    """
    # Resolve session/invocation from ToolContext
    session_id = getattr(tool_context, "session_id", None) or tool_context.state.get(
        "session_id", "unknown_session"
    )
    invocation_id = getattr(tool_context, "invocation_id", None) or tool_context.state.get(
        "invocation_id", f"inv_{uuid.uuid4().hex[:8]}"
    )
    iteration = int(tool_context.state.get("rlm:iteration", 0) or 0)

    # Delegate to standalone function
    results = download_jira_attachments(
        tickets_csv=tickets_csv,
        domain=domain,
        target_volume=target_volume,
        session_id=session_id,
        invocation_id=invocation_id,
        iteration=iteration,
        register_artifacts=True,
    )

    # Store quick pointers for downstream agent reasoning
    tool_context.state["last_jira_attachments_directory"] = results["output_directory"]
    tool_context.state["last_jira_attachments"] = [
        f.get("output_path") for f in results["files"] if f.get("success")
    ]

    return results


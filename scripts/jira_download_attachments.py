# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests",
#     "python-dotenv",
# ]
# ///
"""
Download attachments from a JIRA ticket.

Usage:
    uv run scripts/jira_download_attachments.py TRUL-1
    uv run scripts/jira_download_attachments.py TRUL-1 --output ./downloads
    uv run scripts/jira_download_attachments.py TRUL-1 TRUL-2 TRUL-3  # Multiple tickets
"""

import argparse
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv


def get_jira_session(username: str, api_key: str) -> requests.Session:
    """Create an authenticated session for JIRA API calls."""
    session = requests.Session()
    session.auth = (username, api_key)
    session.headers.update({"Accept": "application/json"})
    return session


def get_issue_attachments(
    session: requests.Session, domain: str, issue_key: str
) -> list[dict]:
    """Fetch attachment metadata for a JIRA issue."""
    url = f"https://{domain}/rest/api/3/issue/{issue_key}?fields=attachment,summary"
    
    response = session.get(url, timeout=30)
    
    if response.status_code == 404:
        print(f"  ✗ Issue {issue_key} not found")
        return []
    
    if response.status_code == 401:
        print(f"  ✗ Authentication failed")
        return []
    
    if response.status_code != 200:
        print(f"  ✗ Failed to fetch {issue_key}: HTTP {response.status_code}")
        return []
    
    data = response.json()
    summary = data.get("fields", {}).get("summary", "No summary")
    attachments = data.get("fields", {}).get("attachment", [])
    
    print(f"  Summary: {summary[:60]}")
    print(f"  Attachments: {len(attachments)}")
    
    return attachments


def download_attachment(
    session: requests.Session,
    attachment: dict,
    output_dir: Path,
    issue_key: str,
) -> bool:
    """Download a single attachment."""
    filename = attachment["filename"]
    content_url = attachment["content"]
    size = attachment["size"]
    
    # Create issue subdirectory
    issue_dir = output_dir / issue_key
    issue_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = issue_dir / filename
    
    # Handle duplicate filenames
    if output_path.exists():
        stem = output_path.stem
        suffix = output_path.suffix
        counter = 1
        while output_path.exists():
            output_path = issue_dir / f"{stem}_{counter}{suffix}"
            counter += 1
    
    print(f"    Downloading: {filename} ({size:,} bytes)...", end=" ", flush=True)
    
    try:
        # Follow redirects (Jira returns 303 to actual file URL)
        response = session.get(
            content_url,
            timeout=60,
            allow_redirects=True,
            headers={"Accept": "*/*"},
        )
        
        if response.status_code == 200:
            output_path.write_bytes(response.content)
            print(f"✓ Saved to {output_path}")
            return True
        else:
            print(f"✗ HTTP {response.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        print("✗ Timeout")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def download_ticket_attachments(
    session: requests.Session,
    domain: str,
    issue_key: str,
    output_dir: Path,
) -> tuple[int, int]:
    """Download all attachments for a ticket. Returns (success_count, total_count)."""
    print(f"\n[{issue_key}]")
    
    attachments = get_issue_attachments(session, domain, issue_key)
    
    if not attachments:
        return 0, 0
    
    success = 0
    for attachment in attachments:
        if download_attachment(session, attachment, output_dir, issue_key):
            success += 1
    
    return success, len(attachments)


def main():
    parser = argparse.ArgumentParser(
        description="Download attachments from JIRA tickets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run scripts/jira_download_attachments.py TRUL-1
    uv run scripts/jira_download_attachments.py TRUL-1 --output ./downloads
    uv run scripts/jira_download_attachments.py TRUL-1 TRUL-2 TRUL-3
        """,
    )
    parser.add_argument(
        "tickets",
        nargs="+",
        help="JIRA ticket key(s) to download attachments from (e.g., TRUL-1)",
    )
    parser.add_argument(
        "--output", "-o",
        default="./jira_attachments",
        help="Output directory (default: ./jira_attachments)",
    )
    parser.add_argument(
        "--domain",
        default="spendmend.atlassian.net",
        help="Jira Cloud domain (default: spendmend.atlassian.net)",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file (default: .env)",
    )
    args = parser.parse_args()
    
    # Find and load .env file
    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        script_dir = Path(__file__).parent.parent
        candidates = [script_dir / args.env_file, Path.cwd() / args.env_file]
        for candidate in candidates:
            if candidate.exists():
                env_path = candidate
                break
    
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded environment from: {env_path}")
    else:
        print(f"Warning: .env file not found, using environment variables")
    
    # Get credentials
    username = os.getenv("USER_NAME")
    api_key = os.getenv("JIRA_API_KEY")
    
    if not username:
        print("✗ USER_NAME not set in environment")
        sys.exit(1)
    
    if not api_key:
        print("✗ JIRA_API_KEY not set in environment")
        sys.exit(1)
    
    # Setup
    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Domain: {args.domain}")
    print(f"Output: {output_dir}")
    print(f"Tickets: {', '.join(args.tickets)}")
    
    # Create session and download
    session = get_jira_session(username, api_key)
    
    total_success = 0
    total_attachments = 0
    
    for ticket in args.tickets:
        success, total = download_ticket_attachments(
            session, args.domain, ticket.upper(), output_dir
        )
        total_success += success
        total_attachments += total
    
    # Summary
    print(f"\n{'='*50}")
    print(f"Downloaded {total_success}/{total_attachments} attachments")
    print(f"Output directory: {output_dir}")
    
    sys.exit(0 if total_success == total_attachments else 1)


if __name__ == "__main__":
    main()

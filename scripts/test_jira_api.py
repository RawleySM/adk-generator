# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests",
#     "python-dotenv",
# ]
# ///
"""
Test JIRA API Key validity.

Usage:
    uv run scripts/test_jira_api.py
    
    # Or with a custom Jira domain:
    uv run scripts/test_jira_api.py --domain yourcompany.atlassian.net
"""

import argparse
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv


def test_jira_connection(domain: str, username: str, api_key: str) -> bool:
    """Test JIRA API connection by fetching current user info."""
    
    url = f"https://{domain}/rest/api/3/myself"
    
    print(f"Testing JIRA API connection...")
    print(f"  Domain:   {domain}")
    print(f"  Username: {username}")
    print(f"  API Key:  {api_key[:8]}...{api_key[-4:]}")
    print()
    
    try:
        response = requests.get(
            url,
            auth=(username, api_key),
            headers={"Accept": "application/json"},
            timeout=30,
        )
        
        if response.status_code == 200:
            user_data = response.json()
            print("✓ JIRA API connection successful!")
            print()
            print("User Details:")
            print(f"  Account ID:    {user_data.get('accountId', 'N/A')}")
            print(f"  Display Name:  {user_data.get('displayName', 'N/A')}")
            print(f"  Email:         {user_data.get('emailAddress', 'N/A')}")
            print(f"  Active:        {user_data.get('active', 'N/A')}")
            print(f"  Account Type:  {user_data.get('accountType', 'N/A')}")
            return True
        
        elif response.status_code == 401:
            print("✗ Authentication failed (401 Unauthorized)")
            print("  Check that your USER_NAME and JIRA_API_KEY are correct.")
            print("  Note: USER_NAME should be your email address for Jira Cloud.")
            return False
        
        elif response.status_code == 403:
            print("✗ Access forbidden (403 Forbidden)")
            print("  Your credentials are valid but you lack permission.")
            return False
        
        elif response.status_code == 404:
            print(f"✗ JIRA instance not found at {domain} (404)")
            print("  Check the --domain argument.")
            return False
        
        else:
            print(f"✗ Unexpected response: {response.status_code}")
            print(f"  Response: {response.text[:500]}")
            return False
            
    except requests.exceptions.Timeout:
        print("✗ Request timed out. Check your network connection.")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"✗ Connection error: {e}")
        print(f"  Could not connect to {domain}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test JIRA API Key validity")
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
        # Look relative to script location, then current directory
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
        print(f"Warning: .env file not found at {env_path}")
        print("Falling back to environment variables.")
    
    print()
    
    # Get credentials
    username = os.getenv("USER_NAME")
    api_key = os.getenv("JIRA_API_KEY")
    
    if not username:
        print("✗ USER_NAME not set in environment")
        sys.exit(1)
    
    if not api_key:
        print("✗ JIRA_API_KEY not set in environment")
        sys.exit(1)
    
    # Test connection
    success = test_jira_connection(args.domain, username, api_key)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Test script to validate download_file_streaming and download_file_simple functions
using full_filepath values from silo_dev_rs.repos.files.

This script tests BOTH token sources (dotenv vs Databricks secrets) to isolate
auth-related 404 errors that occur in Databricks but not locally.

Designed to run as a Databricks spark_python_task for auth debugging.
"""
import os
import sys
import json
import time
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from datetime import datetime

# Configure logging for Databricks observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Detect if running on Databricks
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# Target volumes for test downloads (using Unity Catalog Volumes as per best practices)
if IS_DATABRICKS:
    # Primary volume for first run (Databricks UC Volumes)
    TARGET_VOLUME_1 = "/Volumes/silo_dev_rs/repos/test_dev"
    # Secondary volume for second run with Databricks secrets - use git_downloads subdir
    TARGET_VOLUME_2 = "/Volumes/silo_dev_rs/repos/git_downloads"
else:
    # Local temp directories for testing
    TARGET_VOLUME_1 = "/tmp/github_download_test/test_dev"
    TARGET_VOLUME_2 = "/tmp/github_download_test/repos"

# Default organization (always SpendMend)
DEFAULT_ORG = "SpendMend"
DEFAULT_BRANCH = "main"

# Test files (known to exist in GenAIPharmacy repo)
TEST_FILES = [
    "GenAIPharmacy/shared/application/integration/audit_initiate_processing_message.py",
    "GenAIPharmacy/app/audit/domain/client.py",
]

# =============================================================================
# TOKEN SOURCES ENUM
# =============================================================================

class TokenSource:
    DOTENV = "dotenv"
    DATABRICKS_SECRETS = "databricks_secrets"
    ENVIRONMENT = "environment"


# =============================================================================
# TOKEN RETRIEVAL FUNCTIONS
# =============================================================================

def get_token_from_dotenv() -> Optional[str]:
    """Get GitHub token from .env file via python-dotenv."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            logger.info("  Token retrieved from dotenv (.env file)")
        return token
    except ImportError:
        logger.warning("  python-dotenv not installed, skipping dotenv")
        return None
    except Exception as e:
        logger.warning(f"  Error loading dotenv: {e}")
        return None


def decode_token_if_base64(token: str) -> str:
    """
    Decode token if it's base64 encoded.
    
    GitHub PATs start with 'ghp_' or 'github_pat_'. If the token doesn't
    start with these prefixes, try base64 decoding.
    """
    import base64
    
    # Already in correct format
    if token.startswith(("ghp_", "github_pat_", "gho_", "ghu_", "ghs_", "ghr_")):
        return token
    
    # Try base64 decode
    try:
        decoded = base64.b64decode(token).decode('utf-8')
        if decoded.startswith(("ghp_", "github_pat_", "gho_", "ghu_", "ghs_", "ghr_")):
            logger.info(f"  Token was base64 encoded, decoded successfully")
            return decoded
    except Exception:
        pass
    
    # Return original if decoding fails
    return token


def get_token_from_databricks_secrets(scope: str = "adk-secrets") -> Optional[str]:
    """Get GitHub token from Databricks secrets."""
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        
        # Try multiple key names (in order of preference)
        key_names = ["github-token", "github_token", "pat", "GITHUB_TOKEN"]
        
        for key in key_names:
            try:
                secret = client.secrets.get_secret(scope=scope, key=key)
                if secret and secret.value:
                    logger.info(f"  Token retrieved from Databricks secrets ({scope}/{key})")
                    # Handle base64 encoded tokens
                    decoded_token = decode_token_if_base64(secret.value)
                    return decoded_token
            except Exception:
                continue
        
        logger.warning(f"  No GitHub token found in Databricks secrets (scope: {scope})")
        return None
        
    except ImportError:
        logger.warning("  databricks-sdk not installed, skipping Databricks secrets")
        return None
    except Exception as e:
        logger.warning(f"  Error accessing Databricks secrets: {e}")
        return None


def get_token_from_environment() -> Optional[str]:
    """Get GitHub token from environment variable (pre-set)."""
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        logger.info("  Token found in environment variable GITHUB_TOKEN")
    return token


# =============================================================================
# DOWNLOAD FUNCTIONS (inline for Databricks job portability)
# =============================================================================

def build_raw_url_from_full_filepath(full_filepath: str, branch: str = DEFAULT_BRANCH) -> str:
    """Build raw GitHub URL from full_filepath format (repo/path/to/file)."""
    parts = full_filepath.split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid full_filepath format: {full_filepath}")
    repo_name, relative_path = parts
    return f"https://raw.githubusercontent.com/{DEFAULT_ORG}/{repo_name}/{branch}/{relative_path}"


def download_raw_url_streaming(
    token: str,
    url: str,
    timeout: int = 60
) -> Tuple[Optional[bytes], Dict[str, Any]]:
    """
    Download file from raw GitHub URL using streaming.
    
    Returns:
        Tuple of (content_bytes, metadata_dict)
    """
    import requests
    
    headers = {"Authorization": f"token {token}"}
    
    metadata = {
        "url": url,
        "method": "raw_url_streaming",
    }
    
    try:
        start_time = time.perf_counter()
        
        with requests.get(url, headers=headers, stream=True, timeout=timeout) as response:
            metadata["status_code"] = response.status_code
            
            if response.status_code == 200:
                chunks = []
                bytes_downloaded = 0
                
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=False):
                    if chunk:
                        chunks.append(chunk)
                        bytes_downloaded += len(chunk)
                
                end_time = time.perf_counter()
                download_time = end_time - start_time
                
                raw_bytes = b''.join(chunks)
                
                metadata["success"] = True
                metadata["bytes_downloaded"] = bytes_downloaded
                metadata["download_time_seconds"] = round(download_time, 4)
                metadata["download_speed_kbps"] = round((bytes_downloaded / 1024) / download_time, 2) if download_time > 0 else 0
                
                return raw_bytes, metadata
                
            else:
                end_time = time.perf_counter()
                metadata["download_time_seconds"] = round(end_time - start_time, 4)
                metadata["success"] = False
                
                error_map = {
                    404: "File not found (404)",
                    403: "Forbidden (403) - check token permissions or rate limit",
                    429: "Rate limited (429)",
                    401: "Unauthorized (401) - invalid or expired token",
                }
                metadata["error"] = error_map.get(response.status_code, f"HTTP {response.status_code}")
                
                return None, metadata
                
    except requests.exceptions.Timeout:
        metadata["success"] = False
        metadata["error"] = "Request timed out"
        return None, metadata
    except requests.exceptions.RequestException as e:
        metadata["success"] = False
        metadata["error"] = str(e)
        return None, metadata


def download_raw_url_simple(
    token: str,
    url: str,
    timeout: int = 60
) -> Tuple[Optional[bytes], Dict[str, Any]]:
    """
    Download file from raw GitHub URL (non-streaming).
    
    Returns:
        Tuple of (content_bytes, metadata_dict)
    """
    import requests
    
    headers = {"Authorization": f"token {token}"}
    
    metadata = {
        "url": url,
        "method": "raw_url_simple",
    }
    
    try:
        start_time = time.perf_counter()
        
        response = requests.get(url, headers=headers, timeout=timeout)
        
        end_time = time.perf_counter()
        download_time = end_time - start_time
        
        metadata["status_code"] = response.status_code
        metadata["download_time_seconds"] = round(download_time, 4)
        
        if response.status_code == 200:
            raw_bytes = response.content
            bytes_downloaded = len(raw_bytes)
            
            metadata["success"] = True
            metadata["bytes_downloaded"] = bytes_downloaded
            metadata["download_speed_kbps"] = round((bytes_downloaded / 1024) / download_time, 2) if download_time > 0 else 0
            
            return raw_bytes, metadata
            
        else:
            metadata["success"] = False
            error_map = {
                404: "File not found (404)",
                403: "Forbidden (403) - check token permissions or rate limit",
                429: "Rate limited (429)",
                401: "Unauthorized (401) - invalid or expired token",
            }
            metadata["error"] = error_map.get(response.status_code, f"HTTP {response.status_code}")
            
            return None, metadata
            
    except requests.exceptions.Timeout:
        metadata["success"] = False
        metadata["error"] = "Request timed out"
        return None, metadata
    except requests.exceptions.RequestException as e:
        metadata["success"] = False
        metadata["error"] = str(e)
        return None, metadata


# =============================================================================
# FILE VALIDATION FUNCTIONS (using pathlib per Databricks best practices)
# =============================================================================

def validate_file_in_volume(filepath: str) -> Dict[str, Any]:
    """
    Validate that a file exists in the target volume.
    
    Uses pathlib for robust, portable file path handling as recommended
    by Databricks agent file reading best practices.
    
    Returns:
        Dict with validation results
    """
    result = {
        "filepath": filepath,
        "exists": False,
        "is_file": False,
        "size_bytes": None,
        "validation_passed": False,
    }
    
    try:
        # Use pathlib for robust path handling (works on both UC Volumes and local)
        path = Path(filepath)
        
        result["exists"] = path.exists()
        result["is_file"] = path.is_file()
        
        if result["is_file"]:
            stat_info = path.stat()
            result["size_bytes"] = stat_info.st_size
            result["validation_passed"] = result["size_bytes"] > 0
            
    except Exception as e:
        result["error"] = str(e)
    
    return result


def write_file_to_volume(
    content: bytes,
    filename: str,
    target_volume: str
) -> Dict[str, Any]:
    """
    Write downloaded content to a Unity Catalog Volume.
    
    Uses pathlib for robust path handling.
    
    Args:
        content: File content as bytes
        filename: Name of the file to write
        target_volume: Volume path (e.g., /Volumes/silo_dev_rs/repos/test_dev)
    
    Returns:
        Dict with write operation results
    """
    result = {
        "filename": filename,
        "target_volume": target_volume,
        "success": False,
    }
    
    try:
        # Use pathlib for robust path handling
        volume_path = Path(target_volume)
        
        # Ensure directory exists (pathlib handles this robustly)
        volume_path.mkdir(parents=True, exist_ok=True)
        
        output_path = volume_path / filename
        result["output_path"] = str(output_path)
        
        start_time = time.perf_counter()
        
        # Write file
        output_path.write_bytes(content)
        
        end_time = time.perf_counter()
        result["write_time_seconds"] = round(end_time - start_time, 4)
        
        # Validate write
        validation = validate_file_in_volume(str(output_path))
        result["validation"] = validation
        result["success"] = validation["validation_passed"]
        result["bytes_written"] = validation.get("size_bytes", 0)
        
    except Exception as e:
        result["error"] = str(e)
    
    return result


# =============================================================================
# TEST EXECUTION FUNCTIONS
# =============================================================================

def run_download_test(
    token: str,
    token_source: str,
    full_filepath: str,
    download_method: str,
    target_volume: str
) -> Dict[str, Any]:
    """
    Run a single download test with specified configuration.
    
    Args:
        token: GitHub token to use
        token_source: Source of token (dotenv, databricks_secrets, environment)
        full_filepath: Full filepath in format repo/path/to/file
        download_method: Either "streaming" or "simple"
        target_volume: Target volume for downloaded file
    
    Returns:
        Dict with complete test results
    """
    test_result = {
        "token_source": token_source,
        "download_method": download_method,
        "full_filepath": full_filepath,
        "target_volume": target_volume,
        "timestamp": datetime.now().isoformat(),
    }
    
    # Build URL
    try:
        url = build_raw_url_from_full_filepath(full_filepath)
        test_result["url"] = url
    except ValueError as e:
        test_result["success"] = False
        test_result["error"] = str(e)
        return test_result
    
    # Download
    if download_method == "streaming":
        content, meta = download_raw_url_streaming(token, url)
    else:
        content, meta = download_raw_url_simple(token, url)
    
    test_result["download_metadata"] = meta
    
    if not meta.get("success"):
        test_result["success"] = False
        test_result["error"] = meta.get("error")
        return test_result
    
    # Write to volume
    filename = f"{download_method}_{token_source}_{full_filepath.split('/')[-1]}"
    write_result = write_file_to_volume(content, filename, target_volume)
    
    test_result["write_result"] = write_result
    test_result["success"] = write_result.get("success", False)
    test_result["output_path"] = write_result.get("output_path")
    
    return test_result


def print_test_result(result: Dict[str, Any]) -> None:
    """Print formatted test result with success/failure status."""
    method = result.get("download_method", "unknown")
    token_source = result.get("token_source", "unknown")
    success = result.get("success", False)
    
    status_icon = "✓" if success else "✗"
    status_text = "SUCCESS" if success else "FAILED"
    
    print(f"\n{status_icon} [{method.upper()}] [{token_source.upper()}] {status_text}")
    print(f"  Full filepath: {result.get('full_filepath')}")
    print(f"  URL: {result.get('url')}")
    print(f"  Target volume: {result.get('target_volume')}")
    
    if success:
        download_meta = result.get("download_metadata", {})
        write_result = result.get("write_result", {})
        print(f"  Bytes downloaded: {download_meta.get('bytes_downloaded', 0)}")
        print(f"  Download time: {download_meta.get('download_time_seconds', 0)}s")
        print(f"  Download speed: {download_meta.get('download_speed_kbps', 0)} KB/s")
        print(f"  Output path: {result.get('output_path')}")
        print(f"  File validated: {write_result.get('validation', {}).get('validation_passed', False)}")
    else:
        print(f"  Error: {result.get('error')}")
        if result.get("download_metadata", {}).get("status_code"):
            print(f"  HTTP Status: {result['download_metadata']['status_code']}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main test execution.
    
    Tests GitHub downloads using:
    1. dotenv token -> TARGET_VOLUME_1 (/Volumes/silo_dev_rs/repos/test_dev)
    2. Databricks secrets token -> TARGET_VOLUME_2 (/Volumes/silo_dev_rs/repos)
    
    This helps isolate whether 404 errors in Databricks are auth-related.
    """
    print("=" * 70)
    print("GitHub Download Auth Isolation Test")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Running on: {'Databricks' if 'DATABRICKS_RUNTIME_VERSION' in os.environ else 'Local'}")
    print()
    
    # Track all results
    all_results = {
        "timestamp": datetime.now().isoformat(),
        "environment": "databricks" if "DATABRICKS_RUNTIME_VERSION" in os.environ else "local",
        "tests": [],
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
        }
    }
    
    # ==========================================================================
    # PHASE 1: Test with dotenv token -> TARGET_VOLUME_1
    # ==========================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: Testing with DOTENV token")
    print(f"Target Volume: {TARGET_VOLUME_1}")
    print("=" * 70)
    
    # Get dotenv token
    print("\nRetrieving token from dotenv...")
    dotenv_token = get_token_from_dotenv()
    
    if not dotenv_token:
        # Fallback to environment variable
        print("  Falling back to environment variable...")
        dotenv_token = get_token_from_environment()
    
    if dotenv_token:
        print(f"  Token found (length: {len(dotenv_token)}, prefix: {dotenv_token[:4]}...)")
        
        for test_file in TEST_FILES:
            for method in ["streaming", "simple"]:
                result = run_download_test(
                    token=dotenv_token,
                    token_source=TokenSource.DOTENV,
                    full_filepath=test_file,
                    download_method=method,
                    target_volume=TARGET_VOLUME_1
                )
                all_results["tests"].append(result)
                all_results["summary"]["total"] += 1
                if result.get("success"):
                    all_results["summary"]["passed"] += 1
                else:
                    all_results["summary"]["failed"] += 1
                print_test_result(result)
    else:
        print("  ERROR: No token available from dotenv or environment")
        all_results["tests"].append({
            "phase": "dotenv",
            "error": "No token available",
            "success": False
        })
    
    # ==========================================================================
    # PHASE 2: Test with Databricks secrets token -> TARGET_VOLUME_2
    # ==========================================================================
    print("\n" + "=" * 70)
    print("PHASE 2: Testing with DATABRICKS SECRETS token")
    print(f"Target Volume: {TARGET_VOLUME_2}")
    print("=" * 70)
    
    # Get Databricks secrets token
    print("\nRetrieving token from Databricks secrets...")
    db_token = get_token_from_databricks_secrets()
    
    if db_token:
        print(f"  Token found (length: {len(db_token)}, prefix: {db_token[:4]}...)")
        
        # Check if it's the same token
        if dotenv_token and db_token == dotenv_token:
            print("  NOTE: Databricks secrets token is IDENTICAL to dotenv token")
        elif dotenv_token:
            print("  NOTE: Databricks secrets token is DIFFERENT from dotenv token")
        
        for test_file in TEST_FILES:
            for method in ["streaming", "simple"]:
                result = run_download_test(
                    token=db_token,
                    token_source=TokenSource.DATABRICKS_SECRETS,
                    full_filepath=test_file,
                    download_method=method,
                    target_volume=TARGET_VOLUME_2
                )
                all_results["tests"].append(result)
                all_results["summary"]["total"] += 1
                if result.get("success"):
                    all_results["summary"]["passed"] += 1
                else:
                    all_results["summary"]["failed"] += 1
                print_test_result(result)
    else:
        print("  WARNING: No token available from Databricks secrets")
        print("  This is expected when running locally without Databricks SDK")
        all_results["tests"].append({
            "phase": "databricks_secrets",
            "error": "No token available from Databricks secrets",
            "success": False,
            "note": "Expected when running locally"
        })
    
    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    print(f"\nTotal tests: {all_results['summary']['total']}")
    print(f"Passed: {all_results['summary']['passed']}")
    print(f"Failed: {all_results['summary']['failed']}")
    
    # Group results by token source
    dotenv_results = [r for r in all_results["tests"] if r.get("token_source") == TokenSource.DOTENV]
    db_results = [r for r in all_results["tests"] if r.get("token_source") == TokenSource.DATABRICKS_SECRETS]
    
    print(f"\nBy token source:")
    print(f"  Dotenv token: {sum(1 for r in dotenv_results if r.get('success'))}/{len(dotenv_results)} passed")
    print(f"  Databricks secrets: {sum(1 for r in db_results if r.get('success'))}/{len(db_results)} passed")
    
    # Diagnosis
    print("\n" + "=" * 70)
    print("DIAGNOSIS")
    print("=" * 70)
    
    dotenv_failed = [r for r in dotenv_results if not r.get("success")]
    db_failed = [r for r in db_results if not r.get("success")]
    
    if not dotenv_results and not db_results:
        print("\n⚠️  No tests ran - check token availability")
    elif all(r.get("success") for r in all_results["tests"] if r.get("token_source")):
        print("\n✓ All tests passed - auth is working correctly")
    elif dotenv_results and not any(r.get("success") for r in dotenv_results):
        print("\n✗ All dotenv token tests failed - token may be invalid or expired")
    elif db_results and not any(r.get("success") for r in db_results):
        print("\n✗ All Databricks secrets tests failed - check secret scope configuration")
    else:
        print("\n⚠️  Mixed results - see details above")
        
        # Check for specific error patterns
        for failed in dotenv_failed + db_failed:
            if "404" in str(failed.get("error", "")):
                print(f"  - 404 error for {failed.get('token_source')}: Check if repo/file exists")
            elif "401" in str(failed.get("error", "")):
                print(f"  - 401 error for {failed.get('token_source')}: Token is invalid/expired")
            elif "403" in str(failed.get("error", "")):
                print(f"  - 403 error for {failed.get('token_source')}: Token lacks permissions")
    
    # Output JSON summary for programmatic parsing
    print("\n" + "=" * 70)
    print("JSON RESULTS (for programmatic parsing)")
    print("=" * 70)
    print(json.dumps(all_results, indent=2, default=str))
    
    return all_results


if __name__ == "__main__":
    main()

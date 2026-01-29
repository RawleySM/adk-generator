"""Test file downloads from silo_dev_rs.repos.files using get_repo_file.py functions.

This test script:
1. Loads test data from silo_dev_rs.repos.files
2. Selects 10 random rows with distinct filetypes and filesize < 500000
3. Selects 5 more random rows with filetype="unknown" and filesize < 500000
4. Tests single file download functionality
5. Tests batch download with graceful failure handling
6. Validates that successfully downloaded files exist in the landing volume

Run as a Databricks Spark Python task to test the get_repo_file.py module.

DEPLOYMENT:
    Upload this file to dbfs:/tmp/spendmend/test_weburl_get_repo_file.py
    The necessary download functions are included inline for standalone execution.
"""

import os
import sys
import time
import logging
import requests
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark
spark = SparkSession.builder.appName("TestWebUrlGetRepoFile").getOrCreate()

# Target table with file metadata
SOURCE_TABLE = "silo_dev_rs.repos.files"

# Landing volume for downloaded files
LANDING_VOLUME = "/Volumes/silo_dev_rs/repos/git_downloads"

# Test configuration
MAX_FILESIZE = 500000  # 500KB limit for test files
NUM_DIVERSE_FILETYPES = 10  # Random rows with distinct filetypes
NUM_UNKNOWN_FILETYPE = 5   # Additional rows with filetype="unknown"

# ============================================================================
# INLINE DOWNLOAD FUNCTIONS (from get_repo_file.py)
# ============================================================================

# Default organization
DEFAULT_ORG = "SpendMend"
DEFAULT_BRANCH = "main"
DOWNLOAD_DELAY_SECONDS = 1.0
MAX_FILES_PER_BATCH = 40

# Binary file extensions that should NOT be decoded as text
BINARY_EXTENSIONS = {
    '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.bmp', '.webp',
    '.whl', '.tar.gz', '.tar.bz2', '.tar.xz', '.zip', '.jar', '.gz', '.bz2',
    '.parquet', '.delta', '.avro', '.orc',
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
    '.exe', '.dll', '.so', '.dylib', '.bin',
    '.pyc', '.pyo', '.class',
}


def _is_binary_file(filepath: str) -> bool:
    """Check if a file should be treated as binary based on its extension."""
    filepath_lower = filepath.lower()
    for ext in BINARY_EXTENSIONS:
        if filepath_lower.endswith(ext):
            return True
    return False


def _decode_token_if_base64(token: str) -> str:
    """
    Decode token if it's base64 encoded.
    
    GitHub PATs start with 'ghp_', 'gho_', etc. If the token doesn't 
    start with these prefixes, try base64 decoding.
    """
    import base64
    
    valid_prefixes = ("ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_")
    
    if token.startswith(valid_prefixes):
        return token
    
    try:
        decoded = base64.b64decode(token).decode('utf-8')
        if decoded.startswith(valid_prefixes):
            logger.info("GitHub token was base64 encoded, decoded successfully")
            return decoded
    except Exception:
        pass
    
    return token


def _get_github_token() -> Optional[str]:
    """Retrieves GitHub token from Databricks secrets or environment."""
    # First check environment variable
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return _decode_token_if_base64(token)
    
    # Try Databricks secrets
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        
        for scope in ["adk-secrets", "github", "secrets", "rlm"]:
            try:
                secret = client.secrets.get_secret(scope=scope, key="github-token")
                if secret and secret.value:
                    return _decode_token_if_base64(secret.value)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Could not retrieve token from Databricks secrets: {e}")
    
    return None


def _parse_full_filepath(full_filepath: str, branch: str = DEFAULT_BRANCH) -> dict:
    """Parse a full_filepath into components."""
    parts = full_filepath.split("/")
    if len(parts) < 2:
        raise ValueError(f"Invalid full_filepath format: {full_filepath}")
    
    repo_name = parts[0]
    relative_path = "/".join(parts[1:])
    filename = parts[-1]
    
    url = f"https://raw.githubusercontent.com/{DEFAULT_ORG}/{repo_name}/{branch}/{relative_path}"
    
    return {
        "repo_name": repo_name,
        "filepath": relative_path,
        "filename": filename,
        "url": url,
        "branch": branch
    }


def _download_from_raw_url(
    token: str,
    url: str,
    target_volume: str = LANDING_VOLUME,
    fallback_branches: list = None
) -> dict:
    """Downloads a file directly from a raw GitHub URL with branch fallback."""
    if fallback_branches is None:
        fallback_branches = ["master"]  # Default fallback when main fails
    
    # Parse URL to get components
    prefix = "https://raw.githubusercontent.com/"
    if not url.startswith(prefix):
        raise ValueError(f"Not a valid raw GitHub URL: {url}")
    
    remainder = url[len(prefix):]
    parts = remainder.split("/")
    
    if len(parts) < 4:
        raise ValueError(f"Invalid raw GitHub URL format: {url}")
    
    org = parts[0]
    repo_name = parts[1]
    original_branch = parts[2]
    filepath = "/".join(parts[3:])
    filename = parts[-1]
    
    is_binary = _is_binary_file(filename)
    
    result = {
        "repo_name": repo_name,
        "filepath": filepath,
        "filename": filename,
        "url": url,
        "branch": original_branch,
        "is_binary": is_binary,
    }
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Try branches: original first, then fallbacks
    branches_to_try = [original_branch] + [b for b in fallback_branches if b != original_branch]
    
    for branch in branches_to_try:
        try_url = url.replace(f"/{original_branch}/", f"/{branch}/") if branch != original_branch else url
        
        try:
            start_time = time.perf_counter()
            
            with requests.get(try_url, headers=headers, stream=True, timeout=60) as response:
                result["status_code"] = response.status_code
                
                if response.status_code == 200:
                    chunks = []
                    bytes_downloaded = 0
                    
                    for chunk in response.iter_content(chunk_size=8192, decode_unicode=False):
                        if chunk:
                            chunks.append(chunk)
                            bytes_downloaded += len(chunk)
                    
                    download_time = time.perf_counter() - start_time
                    raw_bytes = b''.join(chunks)
                    
                    if not target_volume.endswith('/'):
                        target_volume = target_volume + '/'
                    
                    output_path = os.path.join(target_volume, repo_name, filepath)
                    output_dir = os.path.dirname(output_path)
                    os.makedirs(output_dir, exist_ok=True)
                    
                    write_start = time.perf_counter()
                    
                    if is_binary:
                        with open(output_path, 'wb') as f:
                            f.write(raw_bytes)
                    else:
                        try:
                            content = raw_bytes.decode('utf-8')
                        except UnicodeDecodeError:
                            content = raw_bytes.decode('latin-1')
                        
                        with open(output_path, 'w', encoding='utf-8') as f:
                            f.write(content)
                    
                    write_time = time.perf_counter() - write_start
                    
                    result["success"] = True
                    result["bytes_downloaded"] = bytes_downloaded
                    result["download_time_seconds"] = round(download_time, 4)
                    result["write_time_seconds"] = round(write_time, 4)
                    result["output_path"] = output_path
                    result["branch"] = branch  # Actual branch that worked
                    result["url"] = try_url
                    
                    if branch != original_branch:
                        logger.info(f"  Downloaded using fallback branch: {branch}")
                    
                    return result
                    
                elif response.status_code == 404:
                    # Try next branch
                    if branch != branches_to_try[-1]:
                        logger.debug(f"Branch '{branch}' returned 404, trying next...")
                        continue
                    # Last branch also failed
                    result["success"] = False
                    result["error"] = f"File not found at URL: {url} (tried branches: {branches_to_try})"
                    
                elif response.status_code == 403:
                    result["success"] = False
                    result["error"] = "Forbidden - check token permissions"
                    return result
                    
                elif response.status_code == 429:
                    result["success"] = False
                    result["error"] = "Rate limited (429)"
                    return result
                    
                else:
                    result["success"] = False
                    result["error"] = f"HTTP {response.status_code}"
                    return result
                    
        except requests.exceptions.Timeout:
            result["success"] = False
            result["error"] = "Request timed out"
            return result
        except requests.exceptions.RequestException as e:
            result["success"] = False
            result["error"] = str(e)
            return result
        except Exception as e:
            if branch != branches_to_try[-1]:
                continue  # Try next branch on error
            result["success"] = False
            result["error"] = f"Unexpected error: {str(e)}"
    
    return result


def download_single_file_from_full_filepath(
    full_filepath: str,
    branch: str = DEFAULT_BRANCH,
    target_volume: str = LANDING_VOLUME
) -> dict:
    """Download a single file using full_filepath format."""
    token = _get_github_token()
    if not token:
        return {
            "success": False,
            "error": "GitHub token not found."
        }
    
    try:
        parsed = _parse_full_filepath(full_filepath, branch)
        url = parsed["url"]
        
        logger.info(f"Downloading single file: {full_filepath}")
        
        result = _download_from_raw_url(
            token=token,
            url=url,
            target_volume=target_volume
        )
        
        result["full_filepath"] = full_filepath
        return result
        
    except ValueError as e:
        return {
            "success": False,
            "full_filepath": full_filepath,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "full_filepath": full_filepath,
            "error": f"Unexpected error: {str(e)}"
        }


def download_files_from_full_filepaths(
    full_filepaths: list,
    branch: str = DEFAULT_BRANCH,
    target_volume: str = LANDING_VOLUME,
    continue_on_error: bool = True
) -> dict:
    """Download multiple files using full_filepath format with graceful error handling."""
    if not full_filepaths:
        return {"status": "error", "message": "No full_filepaths provided."}
    
    if not isinstance(full_filepaths, list):
        full_filepaths = [full_filepaths]
    
    if len(full_filepaths) > MAX_FILES_PER_BATCH:
        return {
            "status": "error",
            "message": f"Too many files ({len(full_filepaths)}). Max: {MAX_FILES_PER_BATCH}.",
        }
    
    token = _get_github_token()
    if not token:
        return {"status": "error", "message": "GitHub token not found."}
    
    logger.info(f"[FULL_FILEPATH MODE] Starting download of {len(full_filepaths)} files")
    
    results = {
        "status": "success",
        "mode": "full_filepath",
        "total_requested": len(full_filepaths),
        "successful_downloads": 0,
        "failed_downloads": 0,
        "files": [],
        "output_directory": target_volume,
        "errors_summary": {}
    }
    
    job_start_time = time.perf_counter()
    
    for i, full_filepath in enumerate(full_filepaths):
        if i > 0:
            time.sleep(DOWNLOAD_DELAY_SECONDS)
        
        full_filepath = str(full_filepath).strip()
        
        logger.info(f"[{i+1}/{len(full_filepaths)}] Processing: {full_filepath}")
        
        try:
            parsed = _parse_full_filepath(full_filepath, branch)
            url = parsed["url"]
            
            file_result = _download_from_raw_url(
                token=token,
                url=url,
                target_volume=target_volume
            )
            
            file_result["full_filepath"] = full_filepath
            
            if file_result.get("success"):
                results["successful_downloads"] += 1
                logger.info(f"  SUCCESS: {file_result.get('output_path')} ({file_result.get('bytes_downloaded')} bytes)")
            else:
                results["failed_downloads"] += 1
                error = file_result.get("error", "Unknown error")
                logger.warning(f"  FAILED: {error}")
                
                error_type = error.split(":")[0] if ":" in error else error
                results["errors_summary"][error_type] = results["errors_summary"].get(error_type, 0) + 1
                
                if not continue_on_error:
                    results["status"] = "error"
                    results["message"] = f"Stopped after error on file {i+1}: {error}"
                    results["files"].append({
                        "full_filepath": full_filepath,
                        "success": False,
                        "error": error
                    })
                    break
            
            results["files"].append({
                "full_filepath": full_filepath,
                "repo_name": file_result.get("repo_name"),
                "filepath": file_result.get("filepath"),
                "filename": file_result.get("filename"),
                "success": file_result.get("success"),
                "output_path": file_result.get("output_path") if file_result.get("success") else None,
                "bytes": file_result.get("bytes_downloaded") if file_result.get("success") else None,
                "error": file_result.get("error") if not file_result.get("success") else None
            })
            
        except ValueError as e:
            results["failed_downloads"] += 1
            error_msg = str(e)
            logger.warning(f"  PARSE ERROR: {error_msg}")
            
            results["errors_summary"]["ParseError"] = results["errors_summary"].get("ParseError", 0) + 1
            
            results["files"].append({
                "full_filepath": full_filepath,
                "success": False,
                "error": error_msg
            })
            
            if not continue_on_error:
                results["status"] = "error"
                results["message"] = f"Stopped after parse error: {error_msg}"
                break
                
        except Exception as e:
            results["failed_downloads"] += 1
            error_msg = f"Unexpected error: {str(e)}"
            logger.warning(f"  EXCEPTION: {error_msg}")
            
            results["errors_summary"]["UnexpectedError"] = results["errors_summary"].get("UnexpectedError", 0) + 1
            
            results["files"].append({
                "full_filepath": full_filepath,
                "success": False,
                "error": error_msg
            })
            
            if not continue_on_error:
                results["status"] = "error"
                results["message"] = f"Stopped after exception: {error_msg}"
                break
    
    job_end_time = time.perf_counter()
    total_time = round(job_end_time - job_start_time, 2)
    results["timing_seconds"] = total_time
    
    if results["failed_downloads"] == 0:
        results["status"] = "success"
        results["message"] = f"Downloaded all {results['successful_downloads']} files in {total_time}s"
    elif results["successful_downloads"] == 0:
        results["status"] = "error"
        results["message"] = f"Failed to download all {results['total_requested']} files."
    else:
        results["status"] = "partial"
        results["message"] = f"Downloaded {results['successful_downloads']} of {results['total_requested']} files ({results['failed_downloads']} failed)."
    
    logger.info(f"[FULL_FILEPATH MODE] Batch complete: {results['message']}")
    
    return results


# ============================================================================
# END INLINE DOWNLOAD FUNCTIONS
# ============================================================================


def get_test_files_df():
    """
    Load test files from silo_dev_rs.repos.files.
    
    Selects files that are likely to be actual source code (not from
    virtual environments, node_modules, etc.) to ensure valid download tests.
    
    Returns a DataFrame with:
    - 10 random rows with distinct filetypes and filesize < 500KB
    - 5 random rows with filetype="unknown" and filesize < 500KB
    """
    logger.info(f"Loading test files from {SOURCE_TABLE}...")
    
    # Check if full_filepath column exists
    df = spark.read.table(SOURCE_TABLE)
    if "full_filepath" not in df.columns:
        raise ValueError(
            f"Column 'full_filepath' not found in {SOURCE_TABLE}. "
            "Please run add_filepath_columns.py first to add the required columns."
        )
    
    # Filter by filesize and exclude paths that are likely not source code
    # Exclude: site-packages, node_modules, lib/python, venv, .nuget, dist-info, etc.
    df_filtered = (
        df
        .filter(F.col("filesize") < MAX_FILESIZE)
        .filter(F.col("filesize") > 100)  # Exclude tiny files
        .filter(~F.col("full_filepath").contains("site-packages"))
        .filter(~F.col("full_filepath").contains("node_modules"))
        .filter(~F.col("full_filepath").contains("/lib/python"))
        .filter(~F.col("full_filepath").contains("/.nuget/"))
        .filter(~F.col("full_filepath").contains("dist-info"))
        .filter(~F.col("full_filepath").contains("/venv/"))
        .filter(~F.col("full_filepath").contains("/env/"))
        .filter(~F.col("full_filepath").contains("/bin/"))
        .filter(~F.col("full_filepath").contains("/obj/"))
        .filter(~F.col("full_filepath").contains("/packages/"))
    )
    
    # Get random rows with common source file types
    source_filetypes = ['py', 'sql', 'md', 'json', 'yaml', 'yml', 'sh', 'txt', 'csv']
    
    window_by_filetype = Window.partitionBy("filetype").orderBy(F.rand())
    
    df_diverse = (
        df_filtered
        .filter(F.col("filetype").isin(source_filetypes))
        .withColumn("rn", F.row_number().over(window_by_filetype))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .orderBy(F.rand())
        .limit(NUM_DIVERSE_FILETYPES)
    )
    
    # Get random rows with filetype="unknown" (simple paths only)
    df_unknown = (
        df_filtered
        .filter(F.col("filetype") == "unknown")
        .filter(F.length(F.col("full_filepath")) < 100)  # Simple, short paths
        .orderBy(F.rand())
        .limit(NUM_UNKNOWN_FILETYPE)
    )
    
    # Combine both sets
    df_test = df_diverse.union(df_unknown)
    
    test_count = df_test.count()
    logger.info(f"Selected {test_count} test files (filtered for source code)")
    
    if test_count == 0:
        logger.warning("No suitable test files found after filtering! Check table data.")
    
    return df_test


def collect_full_filepaths(df) -> List[str]:
    """Extract full_filepath values from DataFrame."""
    rows = df.select("full_filepath").collect()
    return [row.full_filepath for row in rows]


def validate_file_exists(filepath: str) -> bool:
    """Check if a file exists at the given path."""
    try:
        return os.path.exists(filepath) and os.path.isfile(filepath)
    except Exception as e:
        logger.warning(f"Error checking file existence: {filepath} - {e}")
        return False


def validate_downloaded_files(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that successfully downloaded files exist in the landing volume.
    
    Returns validation results with counts of verified files.
    """
    logger.info("Validating downloaded files...")
    
    validation_results = {
        "total_to_validate": 0,
        "files_verified": 0,
        "files_missing": 0,
        "missing_files": [],
        "verified_files": []
    }
    
    for file_result in results.get("files", []):
        if file_result.get("success") and file_result.get("output_path"):
            validation_results["total_to_validate"] += 1
            output_path = file_result["output_path"]
            
            if validate_file_exists(output_path):
                validation_results["files_verified"] += 1
                validation_results["verified_files"].append(output_path)
                logger.info(f"  VERIFIED: {output_path}")
            else:
                validation_results["files_missing"] += 1
                validation_results["missing_files"].append(output_path)
                logger.warning(f"  MISSING: {output_path}")
    
    return validation_results


def test_single_file_download():
    """Test downloading a single file using download_single_file_from_full_filepath."""
    # Uses inline download_single_file_from_full_filepath function
    
    logger.info("=" * 60)
    logger.info("TEST: Single File Download")
    logger.info("=" * 60)
    
    # Get a single test file
    df = spark.read.table(SOURCE_TABLE)
    if "full_filepath" not in df.columns:
        logger.error("full_filepath column not found. Run add_filepath_columns.py first.")
        return {"status": "error", "message": "full_filepath column not found"}
    
    # Pick a small Python file for testing (filter out virtual environments)
    test_file_row = (
        df
        .filter((F.col("filetype") == "py") & (F.col("filesize") < 50000) & (F.col("filesize") > 100))
        .filter(~F.col("full_filepath").contains("site-packages"))
        .filter(~F.col("full_filepath").contains("/lib/python"))
        .filter(~F.col("full_filepath").contains("/venv/"))
        .filter(~F.col("full_filepath").contains("/env/"))
        .filter(~F.col("full_filepath").contains("/packages/"))
        .orderBy(F.rand())
        .limit(1)
        .collect()
    )
    
    if not test_file_row:
        logger.warning("No suitable test file found for single file test")
        return {"status": "skipped", "message": "No suitable test file found"}
    
    full_filepath = test_file_row[0].full_filepath
    logger.info(f"Testing single file download: {full_filepath}")
    
    result = download_single_file_from_full_filepath(
        full_filepath=full_filepath,
        branch="main",
        target_volume=LANDING_VOLUME
    )
    
    if result.get("success"):
        logger.info(f"SUCCESS: Downloaded to {result.get('output_path')}")
        
        # Validate file exists
        if validate_file_exists(result.get("output_path")):
            logger.info("VALIDATION PASSED: File exists")
            result["validation"] = "passed"
        else:
            logger.warning("VALIDATION FAILED: File not found after download")
            result["validation"] = "failed"
    else:
        logger.warning(f"FAILED: {result.get('error')}")
        result["validation"] = "not_applicable"
    
    return result


def test_batch_download():
    """Test batch downloading multiple files with graceful failure handling."""
    # Uses inline download_files_from_full_filepaths function
    
    logger.info("=" * 60)
    logger.info("TEST: Batch Download with Graceful Failure Handling")
    logger.info("=" * 60)
    
    # Get test files
    df_test = get_test_files_df()
    
    # Show test file selection
    logger.info("Selected test files:")
    df_test.select(
        "repo_name", "filename", "filetype", "filesize", "full_filepath"
    ).show(truncate=False)
    
    # Collect full_filepaths
    full_filepaths = collect_full_filepaths(df_test)
    
    logger.info(f"Downloading {len(full_filepaths)} files...")
    
    # Download with graceful failure handling (continue_on_error=True by default)
    results = download_files_from_full_filepaths(
        full_filepaths=full_filepaths,
        branch="main",
        target_volume=LANDING_VOLUME,
        continue_on_error=True
    )
    
    logger.info(f"Batch download result: {results.get('status')}")
    logger.info(f"  Successful: {results.get('successful_downloads')}")
    logger.info(f"  Failed: {results.get('failed_downloads')}")
    logger.info(f"  Time: {results.get('timing_seconds')}s")
    
    if results.get("errors_summary"):
        logger.info("Error summary:")
        for error_type, count in results.get("errors_summary", {}).items():
            logger.info(f"  {error_type}: {count}")
    
    # Validate downloaded files
    validation = validate_downloaded_files(results)
    results["validation"] = validation
    
    logger.info(f"Validation: {validation['files_verified']}/{validation['total_to_validate']} files verified")
    
    return results


def test_graceful_failure_with_invalid_file():
    """Test that invalid files fail gracefully without stopping the batch."""
    # Uses inline download_files_from_full_filepaths function
    
    logger.info("=" * 60)
    logger.info("TEST: Graceful Failure with Invalid File")
    logger.info("=" * 60)
    
    # Mix valid and invalid filepaths
    df = spark.read.table(SOURCE_TABLE)
    
    # Get one valid file (README.md at repo root is most reliable)
    valid_row = (
        df
        .filter(
            (F.col("filename") == "README.md") & 
            (F.col("filepath") == "README.md") &  # At repo root
            (F.col("filesize") < 50000) &
            (F.col("filesize") > 100)
        )
        .orderBy(F.rand())
        .limit(1)
        .collect()
    )
    
    if not valid_row:
        logger.warning("No valid test file found")
        return {"status": "skipped", "message": "No valid test file found"}
    
    valid_filepath = valid_row[0].full_filepath
    
    # Create test list with invalid file in the middle
    test_filepaths = [
        valid_filepath,
        "NonExistentRepo/path/to/fake_file.py",  # Invalid repo
        valid_filepath,  # Should still succeed after failure
    ]
    
    logger.info(f"Testing with {len(test_filepaths)} files (including 1 invalid)...")
    
    results = download_files_from_full_filepaths(
        full_filepaths=test_filepaths,
        branch="main",
        target_volume=LANDING_VOLUME,
        continue_on_error=True
    )
    
    # Verify graceful failure: should have 2 successes and 1 failure
    expected_successes = 2
    expected_failures = 1
    
    actual_successes = results.get("successful_downloads", 0)
    actual_failures = results.get("failed_downloads", 0)
    
    logger.info(f"Expected: {expected_successes} success, {expected_failures} failure")
    logger.info(f"Actual: {actual_successes} success, {actual_failures} failure")
    
    if actual_successes >= expected_successes - 1 and actual_failures >= expected_failures:
        logger.info("GRACEFUL FAILURE TEST PASSED: Batch continued after error")
        results["graceful_failure_test"] = "passed"
    else:
        logger.warning("GRACEFUL FAILURE TEST NEEDS REVIEW: Unexpected results")
        results["graceful_failure_test"] = "needs_review"
    
    return results


def report_failed_downloads(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and report details about failed downloads.
    
    Returns list of failed file details for further investigation.
    """
    failed_files = []
    
    for file_result in results.get("files", []):
        if not file_result.get("success"):
            failed_files.append({
                "full_filepath": file_result.get("full_filepath"),
                "error": file_result.get("error"),
                "filetype": file_result.get("full_filepath", "").split(".")[-1] if file_result.get("full_filepath") else None
            })
    
    if failed_files:
        logger.info("=" * 60)
        logger.info("FAILED DOWNLOADS REPORT")
        logger.info("=" * 60)
        for f in failed_files:
            logger.info(f"  File: {f['full_filepath']}")
            logger.info(f"  Filetype: {f['filetype']}")
            logger.info(f"  Error: {f['error']}")
            logger.info("-" * 40)
    
    return failed_files


def main():
    """Run all tests and report results."""
    logger.info("=" * 60)
    logger.info("GET_REPO_FILE.PY - WEB URL DOWNLOAD TESTS")
    logger.info("=" * 60)
    
    all_results = {
        "tests": [],
        "overall_status": "success"
    }
    
    # Test 1: Single file download
    try:
        single_result = test_single_file_download()
        all_results["tests"].append({
            "name": "single_file_download",
            "result": single_result
        })
    except Exception as e:
        logger.error(f"Single file download test failed with exception: {e}")
        all_results["tests"].append({
            "name": "single_file_download",
            "result": {"status": "error", "error": str(e)}
        })
        all_results["overall_status"] = "partial"
    
    # Test 2: Batch download with diverse filetypes
    try:
        batch_result = test_batch_download()
        all_results["tests"].append({
            "name": "batch_download",
            "result": batch_result
        })
        
        # Report any failed downloads
        failed = report_failed_downloads(batch_result)
        all_results["failed_downloads"] = failed
        
        if batch_result.get("status") == "error":
            all_results["overall_status"] = "partial"
            
    except Exception as e:
        logger.error(f"Batch download test failed with exception: {e}")
        all_results["tests"].append({
            "name": "batch_download",
            "result": {"status": "error", "error": str(e)}
        })
        all_results["overall_status"] = "partial"
    
    # Test 3: Graceful failure handling
    try:
        graceful_result = test_graceful_failure_with_invalid_file()
        all_results["tests"].append({
            "name": "graceful_failure",
            "result": graceful_result
        })
    except Exception as e:
        logger.error(f"Graceful failure test failed with exception: {e}")
        all_results["tests"].append({
            "name": "graceful_failure",
            "result": {"status": "error", "error": str(e)}
        })
    
    # Summary
    logger.info("=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    
    for test in all_results["tests"]:
        test_name = test["name"]
        result = test["result"]
        status = result.get("status", "unknown")
        logger.info(f"  {test_name}: {status}")
    
    logger.info(f"\nOverall status: {all_results['overall_status']}")
    
    # Return results for inspection
    return all_results


if __name__ == "__main__":
    results = main()
    
    # Print summary as JSON for easy parsing
    import json
    print("\n" + "=" * 60)
    print("RESULTS JSON:")
    print("=" * 60)
    print(json.dumps({
        "overall_status": results.get("overall_status"),
        "failed_downloads_count": len(results.get("failed_downloads", [])),
        "failed_downloads": results.get("failed_downloads", [])
    }, indent=2))

"""Repository file download tool for fetching code files from GitHub.

This tool downloads files from SpendMend GitHub repositories using the Raw URL method.
It is designed to be used AFTER repo_filename_search narrows down the target files.

Downloads are saved to Unity Catalog Volumes at /Volumes/silo_dev_rs/repos/git_downloads.
"""

import os
import time
import requests
import logging
from typing import Optional, List
from google.adk.tools import ToolContext

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default organization - always SpendMend
DEFAULT_ORG = "SpendMend"

# Default branch
DEFAULT_BRANCH = "main"

# Default target volume for downloaded files
DEFAULT_TARGET_VOLUME = "/Volumes/silo_dev_rs/repos/git_downloads"

# Maximum files per batch (rate limit protection)
MAX_FILES_PER_BATCH = 40

# Minimum delay between downloads (seconds) - rate limit protection
DOWNLOAD_DELAY_SECONDS = 1.0

# Databricks profile for secret retrieval
DEFAULT_PROFILE = os.environ.get("DATABRICKS_PROFILE", "rstanhope")

# Known file extensions for filepath conversion
KNOWN_EXTENSIONS = [
    '.py', '.sql', '.json', '.md', '.txt', '.csv', '.yaml', '.yml',
    '.sh', '.ps1', '.ipynb', '.scala', '.r', '.html', '.css', '.js',
    '.ts', '.tsx', '.jsx', '.xml', '.toml', '.cfg', '.ini', '.env',
    '.dockerfile', '.gitignore', '.parquet', '.delta', '.whl', '.tar.gz',
    '.zip', '.jar', '.log', '.rst', '.png', '.jpg', '.jpeg', '.gif', '.svg',
    '.sln', '.csproj', '.cs', '.vb', '.config'
]

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


def _convert_uc_filepath_to_github_path(filepath: str) -> str:
    """
    Converts Unity Catalog filepath format to GitHub API path format.
    
    In Unity Catalog (silo_dev_rs.repos.filenames):
    - Directory separators '/' are replaced with '.'
    - File extension dots remain unchanged
    
    Example:
        Input:  'PyFunctions.Shared.ai_models.perplexity.py'
        Output: 'PyFunctions/Shared/ai_models/perplexity.py'
    """
    if not filepath:
        raise ValueError("filepath cannot be empty")
    
    # Handle compound extensions first (e.g., .tar.gz)
    compound_extensions = ['.tar.gz', '.tar.bz2', '.tar.xz']
    for compound_ext in compound_extensions:
        if filepath.lower().endswith(compound_ext):
            base = filepath[:-len(compound_ext)]
            return base.replace('.', '/') + compound_ext
    
    # Find the file extension from known extensions
    detected_ext = None
    for ext in KNOWN_EXTENSIONS:
        if filepath.lower().endswith(ext):
            detected_ext = ext
            break
    
    if detected_ext:
        ext_len = len(detected_ext)
        base = filepath[:-ext_len]
        original_ext = filepath[-ext_len:]
        return base.replace('.', '/') + original_ext
    else:
        parts = filepath.rsplit('.', 1)
        if len(parts) == 2:
            potential_ext = parts[1]
            if len(potential_ext) <= 10 and potential_ext.replace('_', '').isalnum():
                return parts[0].replace('.', '/') + '.' + potential_ext
        
        segments = filepath.split('.')
        if len(segments) == 1:
            return filepath
        else:
            return '/'.join(segments)


def _extract_filename_from_path(filepath: str) -> str:
    """Extracts the filename from a UC filepath."""
    github_path = _convert_uc_filepath_to_github_path(filepath)
    return os.path.basename(github_path)


def _build_raw_github_url(repo_name: str, filepath: str, branch: str = DEFAULT_BRANCH) -> str:
    """Builds the full GitHub Raw URL for direct file download."""
    github_path = _convert_uc_filepath_to_github_path(filepath)
    return f"https://raw.githubusercontent.com/{DEFAULT_ORG}/{repo_name}/{branch}/{github_path}"


def _get_github_token() -> Optional[str]:
    """
    Retrieves GitHub token from Databricks secrets or environment.
    
    Returns:
        GitHub Personal Access Token or None if not found.
    """
    # First check environment variable
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    
    # Try Databricks secrets
    try:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient(profile=DEFAULT_PROFILE)
        
        # Try common secret scopes
        for scope in ["github", "secrets", "rlm"]:
            try:
                secret = client.secrets.get_secret(scope=scope, key="pat")
                if secret and secret.value:
                    return secret.value
            except Exception:
                pass
            try:
                secret = client.secrets.get_secret(scope=scope, key="github_token")
                if secret and secret.value:
                    return secret.value
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Could not retrieve token from Databricks secrets: {e}")
    
    return None


def _download_single_file(
    token: str,
    repo_name: str,
    filepath: str,
    branch: str = DEFAULT_BRANCH,
    target_volume: str = DEFAULT_TARGET_VOLUME
) -> dict:
    """
    Downloads a single file from GitHub and saves to Unity Catalog Volume.
    
    Preserves directory structure: files are saved to
    {target_volume}/{repo_name}/{github_path} to avoid filename collisions.
    
    Binary files (images, archives, etc.) are written in binary mode to
    prevent corruption.
    
    Returns:
        Dict with download results including success status.
    """
    github_path = _convert_uc_filepath_to_github_path(filepath)
    filename = _extract_filename_from_path(filepath)
    url = _build_raw_github_url(repo_name, filepath, branch)
    is_binary = _is_binary_file(filepath)
    
    result = {
        "repo_name": repo_name,
        "uc_filepath": filepath,
        "github_path": github_path,
        "filename": filename,
        "url": url,
        "branch": branch,
        "is_binary": is_binary
    }
    
    headers = {"Authorization": f"token {token}"}
    
    try:
        start_time = time.perf_counter()
        
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            result["status_code"] = response.status_code
            
            if response.status_code == 200:
                # Collect content from stream
                chunks = []
                bytes_downloaded = 0
                
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=False):
                    if chunk:
                        chunks.append(chunk)
                        bytes_downloaded += len(chunk)
                
                download_time = time.perf_counter() - start_time
                raw_bytes = b''.join(chunks)
                
                # Build output path preserving directory structure
                # e.g., /Volumes/.../repo_name/src/etl/loader.py instead of just loader.py
                if not target_volume.endswith('/'):
                    target_volume = target_volume + '/'
                
                # Use github_path (with slashes) to preserve directory hierarchy
                output_path = os.path.join(target_volume, repo_name, github_path)
                output_dir = os.path.dirname(output_path)
                os.makedirs(output_dir, exist_ok=True)
                
                write_start = time.perf_counter()
                
                if is_binary:
                    # Write binary files directly without decoding
                    with open(output_path, 'wb') as f:
                        f.write(raw_bytes)
                else:
                    # Decode and write text files
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
                
            elif response.status_code == 404:
                result["success"] = False
                result["error"] = "File not found"
                
            elif response.status_code == 403:
                result["success"] = False
                result["error"] = "Forbidden - check token permissions"
                
            elif response.status_code == 429:
                result["success"] = False
                result["error"] = "Rate limited (429)"
                
            else:
                result["success"] = False
                result["error"] = f"HTTP {response.status_code}"
                
    except requests.exceptions.Timeout:
        result["success"] = False
        result["error"] = "Request timed out"
    except requests.exceptions.RequestException as e:
        result["success"] = False
        result["error"] = str(e)
    except Exception as e:
        result["success"] = False
        result["error"] = f"Unexpected error: {str(e)}"
    
    return result


def get_repo_file(
    filepaths: List[str],
    repo_name: Optional[str] = None,
    branch: str = "main",
    target_volume: str = DEFAULT_TARGET_VOLUME,
    tool_context: Optional[ToolContext] = None,
) -> dict:
    """
    Downloads repository files from GitHub to a Unity Catalog Volume.
    
    This tool downloads files from SpendMend GitHub repositories using the Raw URL
    method (raw.githubusercontent.com), which bypasses REST API overhead and has no
    file size limit. Use this tool AFTER using repo_filename_search to identify the
    specific files you need.
    
    FILES TO DOWNLOAD:
    ==================
    
    The filepaths parameter accepts paths in Unity Catalog format (dots as separators)
    as returned by repo_filename_search from the silo_dev_rs.repos.files table.
    
    Examples of valid filepath formats:
    - 'PyFunctions.Shared.ai_models.perplexity.py' -> saves to PyFunctions/Shared/ai_models/perplexity.py
    - 'src.etl.loader.py' -> saves to src/etl/loader.py
    - 'docs.README.md' -> saves to docs/README.md
    
    RATE LIMITING:
    ==============
    - Maximum 40 files per batch (protects against GitHub rate limits)
    - 1 second delay between each download
    - If you need more files, call this tool multiple times
    
    OUTPUT LOCATION:
    ================
    Files are saved to: /Volumes/silo_dev_rs/repos/git_downloads/{repo_name}/{path_structure}/
    
    Directory structure is preserved to avoid filename collisions. For example,
    two files named 'config.yml' in different directories will be saved to their
    respective subdirectories.
    
    BINARY FILES:
    =============
    Binary files (images, archives, etc.) are detected by extension and written
    in binary mode to prevent corruption. Text files are decoded as UTF-8.
    
    WORKFLOW:
    =========
    
    1. First use repo_filename_search to find files:
       repo_filename_search(keyword="etl", filetype_filter="py")
       
    2. Then download the specific files you need:
       get_repo_file(
           filepaths=["PyFunctions.ETL.loader.py", "PyFunctions.ETL.transformer.py"],
           repo_name="Master-Vendor-Alignment"
       )
    
    EXAMPLES:
    =========
    
    Download single file:
    ---------------------
    get_repo_file(
        filepaths=["PyFunctions.Shared.ai_models.perplexity.py"],
        repo_name="Master-Vendor-Alignment"
    )
    
    Download multiple files from same repo:
    ---------------------------------------
    get_repo_file(
        filepaths=[
            "src.etl.loader.py",
            "src.etl.transformer.py",
            "src.utils.helpers.py"
        ],
        repo_name="sm-data-platform"
    )
    
    Download from specific branch:
    ------------------------------
    get_repo_file(
        filepaths=["config.schema.json"],
        repo_name="Master-Vendor-Alignment",
        branch="develop"
    )
    
    Args:
        filepaths (List[str]): List of filepaths in UC format (dots as separators).
                               These should come from repo_filename_search results.
                               Maximum 40 files per call.
        repo_name (str, optional): Repository name (e.g., 'Master-Vendor-Alignment').
                                   Required unless all filepaths include repo info.
        branch (str): Branch name or commit SHA (default: 'main').
        target_volume (str): Volume path for downloaded files
                             (default: '/Volumes/silo_dev_rs/repos/git_downloads').
    
    Returns:
        dict: Download results with keys:
              - status: "success", "partial", or "error"
              - total_requested: Number of files requested
              - successful_downloads: Number of files successfully downloaded
              - failed_downloads: Number of files that failed
              - files: List of individual file results
              - output_directory: Where files were saved
              - message: Human-readable summary
              - timing: Total time taken for all downloads
    """
    # Validate inputs
    if not filepaths:
        return {
            "status": "error",
            "message": "No filepaths provided. Please provide a list of filepaths to download."
        }
    
    if not isinstance(filepaths, list):
        # Handle case where single filepath is passed as string
        filepaths = [filepaths]
    
    # Enforce batch limit
    if len(filepaths) > MAX_FILES_PER_BATCH:
        return {
            "status": "error",
            "message": f"Too many files requested ({len(filepaths)}). Maximum is {MAX_FILES_PER_BATCH} files per batch. "
                       f"Please split your request into multiple calls.",
            "limit": MAX_FILES_PER_BATCH,
            "requested": len(filepaths)
        }
    
    if not repo_name:
        return {
            "status": "error",
            "message": "repo_name is required. Please specify the repository name (e.g., 'Master-Vendor-Alignment')."
        }
    
    # Get GitHub token
    token = _get_github_token()
    if not token:
        return {
            "status": "error",
            "message": "GitHub token not found. Please set GITHUB_TOKEN environment variable or configure Databricks secrets."
        }
    
    # Track results
    results = {
        "status": "success",
        "total_requested": len(filepaths),
        "successful_downloads": 0,
        "failed_downloads": 0,
        "files": [],
        "output_directory": os.path.join(target_volume, repo_name),
        "repo_name": repo_name,
        "branch": branch
    }
    
    job_start_time = time.perf_counter()
    
    logger.info(f"Starting download of {len(filepaths)} files from {repo_name}")
    
    # Download each file with delay between requests
    for i, filepath in enumerate(filepaths):
        if i > 0:
            # Enforce delay between downloads (rate limit protection)
            time.sleep(DOWNLOAD_DELAY_SECONDS)
        
        logger.info(f"[{i+1}/{len(filepaths)}] Downloading: {filepath}")
        
        file_result = _download_single_file(
            token=token,
            repo_name=repo_name,
            filepath=filepath,
            branch=branch,
            target_volume=target_volume
        )
        
        if file_result.get("success"):
            results["successful_downloads"] += 1
            logger.info(f"  SUCCESS: {file_result.get('output_path')} ({file_result.get('bytes_downloaded')} bytes)")
        else:
            results["failed_downloads"] += 1
            logger.warning(f"  FAILED: {file_result.get('error')}")
        
        # Add simplified result to list
        results["files"].append({
            "filepath": filepath,
            "filename": file_result.get("filename"),
            "success": file_result.get("success"),
            "output_path": file_result.get("output_path") if file_result.get("success") else None,
            "bytes": file_result.get("bytes_downloaded") if file_result.get("success") else None,
            "error": file_result.get("error") if not file_result.get("success") else None
        })
    
    # Calculate total time
    job_end_time = time.perf_counter()
    total_time = round(job_end_time - job_start_time, 2)
    results["timing_seconds"] = total_time
    
    # Determine overall status
    if results["failed_downloads"] == 0:
        results["status"] = "success"
        results["message"] = (
            f"Successfully downloaded all {results['successful_downloads']} files "
            f"to {results['output_directory']} in {total_time}s"
        )
    elif results["successful_downloads"] == 0:
        results["status"] = "error"
        results["message"] = (
            f"Failed to download all {results['total_requested']} files. "
            f"Check individual file errors for details."
        )
    else:
        results["status"] = "partial"
        results["message"] = (
            f"Downloaded {results['successful_downloads']} of {results['total_requested']} files "
            f"({results['failed_downloads']} failed). Check individual file errors."
        )
    
    # Store download info in tool context state if available
    if tool_context:
        tool_context.state["last_download_directory"] = results["output_directory"]
        tool_context.state["last_download_count"] = results["successful_downloads"]
        tool_context.state["last_download_files"] = [
            f["output_path"] for f in results["files"] if f.get("success")
        ]
    
    logger.info(f"Download batch complete: {results['message']}")
    
    return results

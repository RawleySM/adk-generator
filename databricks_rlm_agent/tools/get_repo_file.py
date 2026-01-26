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


def _is_raw_github_url(filepath: str) -> bool:
    """Check if the filepath is already a raw GitHub URL."""
    return filepath.startswith("https://raw.githubusercontent.com/")


def _parse_raw_github_url(url: str) -> dict:
    """
    Parse a raw GitHub URL into components.
    
    Example URL: https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/main/src/etl/loader.py
    
    Returns:
        dict with keys: org, repo_name, branch, filepath, filename
    """
    # Remove the base URL prefix
    # Format: https://raw.githubusercontent.com/{org}/{repo}/{branch}/{filepath}
    prefix = "https://raw.githubusercontent.com/"
    if not url.startswith(prefix):
        raise ValueError(f"Not a valid raw GitHub URL: {url}")
    
    remainder = url[len(prefix):]  # e.g., "SpendMend/Master-Vendor-Alignment/main/src/etl/loader.py"
    parts = remainder.split("/")
    
    if len(parts) < 4:
        raise ValueError(f"Invalid raw GitHub URL format: {url}")
    
    org = parts[0]
    repo_name = parts[1]
    branch = parts[2]
    filepath = "/".join(parts[3:])  # Everything after branch is the filepath
    filename = parts[-1]  # Last part is the filename
    
    return {
        "org": org,
        "repo_name": repo_name,
        "branch": branch,
        "filepath": filepath,
        "filename": filename,
        "url": url
    }


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
        
        # Try common secret scopes (adk-secrets is the default scope used by deploy script)
        for scope in ["adk-secrets", "github", "secrets", "rlm"]:
            # Try github-token key (used by deploy script)
            try:
                secret = client.secrets.get_secret(scope=scope, key="github-token")
                if secret and secret.value:
                    return secret.value
            except Exception:
                pass
            # Try pat key
            try:
                secret = client.secrets.get_secret(scope=scope, key="pat")
                if secret and secret.value:
                    return secret.value
            except Exception:
                pass
            # Try github_token key (alternative naming)
            try:
                secret = client.secrets.get_secret(scope=scope, key="github_token")
                if secret and secret.value:
                    return secret.value
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Could not retrieve token from Databricks secrets: {e}")
    
    return None


def _download_from_raw_url(
    token: str,
    url: str,
    target_volume: str = DEFAULT_TARGET_VOLUME
) -> dict:
    """
    Downloads a file directly from a raw GitHub URL.
    
    This is the PREFERRED download method when the URL is already known,
    as it bypasses all filepath parsing ambiguity.
    
    Returns:
        Dict with download results including success status.
    """
    parsed = _parse_raw_github_url(url)
    repo_name = parsed["repo_name"]
    filepath = parsed["filepath"]
    filename = parsed["filename"]
    is_binary = _is_binary_file(filename)
    
    result = {
        "repo_name": repo_name,
        "filepath": filepath,
        "filename": filename,
        "url": url,
        "branch": parsed["branch"],
        "is_binary": is_binary,
        "url_mode": True  # Flag indicating URL was used directly
    }
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        start_time = time.perf_counter()
        
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
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
                
                # Build output path preserving directory structure
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
                
            elif response.status_code == 404:
                result["success"] = False
                result["error"] = f"File not found at URL: {url}"
                
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
    
    headers = {"Authorization": f"Bearer {token}"}
    
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
    *,
    tool_context: ToolContext,
) -> dict:
    """
    Downloads repository files from GitHub to a Unity Catalog Volume.
    
    This tool downloads files from SpendMend GitHub repositories using the Raw URL
    method (raw.githubusercontent.com), which bypasses REST API overhead and has no
    file size limit. Use this tool AFTER using repo_filename_search to identify the
    specific files you need.
    
    RECOMMENDED WORKFLOW (PREFERRED - uses raw_github_url):
    =======================================================
    
    1. Search for files:
       result = repo_filename_search(keyword="etl", filetype_filter="py")
       
    2. Get raw_github_url from results and pass directly:
       get_repo_file(filepaths=[
           "https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/main/src/etl/loader.py",
           "https://raw.githubusercontent.com/SpendMend/sm-data-platform/main/src/utils/helpers.py"
       ])
    
    This is the MOST RELIABLE method because:
    - URLs are complete and unambiguous
    - No repo inference or path parsing required
    - Works with files from MULTIPLE repos in a single call
    - Deterministic - no state dependency
    
    FILES TO DOWNLOAD:
    ==================
    
    The filepaths parameter accepts:
    
    1. RAW GITHUB URLs (PREFERRED - from repo_filename_search `raw_github_url` column):
       'https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/main/README.md'
       
    2. Unity Catalog "dotted" format (legacy, single-repo only):
       'PyFunctions.Shared.ai_models.perplexity.py' -> saves to PyFunctions/Shared/ai_models/perplexity.py
       'Master-Vendor-Alignment.src.etl.loader.py' -> repo inferred; saves to src/etl/loader.py
    
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
    
    EXAMPLES:
    =========
    
    Download using raw URLs (PREFERRED - works with multiple repos):
    ----------------------------------------------------------------
    get_repo_file(filepaths=[
        "https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/main/README.md",
        "https://raw.githubusercontent.com/SpendMend/sm-data-platform/main/src/etl/loader.py"
    ])
    
    Download using dotted format (legacy, single-repo only):
    --------------------------------------------------------
    get_repo_file(
        filepaths=["PyFunctions.Shared.ai_models.perplexity.py"],
        repo_name="Master-Vendor-Alignment"
    )
    
    Download from specific branch (use raw URL format):
    ---------------------------------------------------
    get_repo_file(filepaths=[
        "https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/develop/config.json"
    ])
    
    Args:
        filepaths (List[str]): List of file identifiers. Accepts TWO formats:
                               
                               1. RAW GITHUB URLs (PREFERRED):
                                  e.g., 'https://raw.githubusercontent.com/SpendMend/repo/main/path/file.py'
                                  Get these from repo_filename_search `raw_github_url` column.
                                  Works with multiple repos in a single call.
                                  
                               2. UC dotted format (legacy, single-repo only):
                                  e.g., 'PyFunctions.Shared.ai_models.perplexity.py'
                                  Requires repo_name parameter or repo prefix.
                               
                               Maximum 40 files per call.
        repo_name (str, optional): Repository name (e.g., 'Master-Vendor-Alignment').
                                   IGNORED when using raw GitHub URLs.
                                   Required for UC dotted format unless prefixed.
        branch (str): Branch name (default: 'main').
                      IGNORED when using raw GitHub URLs (branch is in the URL).
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
    
    # =======================================================================
    # RAW URL MODE: If ALL filepaths are raw GitHub URLs, use direct download
    # This is the PREFERRED path - deterministic, no repo inference needed
    # =======================================================================
    raw_url_filepaths = [fp for fp in filepaths if _is_raw_github_url(str(fp))]
    
    if raw_url_filepaths:
        # If ANY are raw URLs, ALL must be raw URLs (no mixing)
        if len(raw_url_filepaths) != len(filepaths):
            non_url_filepaths = [fp for fp in filepaths if not _is_raw_github_url(str(fp))]
            return {
                "status": "error",
                "message": (
                    f"Mixed filepath formats detected. When using raw GitHub URLs, ALL filepaths must be URLs. "
                    f"Found {len(raw_url_filepaths)} URLs and {len(non_url_filepaths)} non-URLs. "
                    f"Non-URL filepaths: {non_url_filepaths[:3]}..."
                )
            }
        
        # Get GitHub token
        token = _get_github_token()
        if not token:
            return {
                "status": "error",
                "message": "GitHub token not found. Please set GITHUB_TOKEN environment variable or configure Databricks secrets."
            }
        
        # Process all raw URLs - can handle multiple repos!
        logger.info(f"[RAW URL MODE] Starting download of {len(filepaths)} files via direct URLs")
        
        results = {
            "status": "success",
            "mode": "raw_url",
            "total_requested": len(filepaths),
            "successful_downloads": 0,
            "failed_downloads": 0,
            "files": [],
            "output_directory": target_volume,
        }
        
        job_start_time = time.perf_counter()
        
        for i, url in enumerate(filepaths):
            if i > 0:
                time.sleep(DOWNLOAD_DELAY_SECONDS)
            
            logger.info(f"[{i+1}/{len(filepaths)}] Downloading: {url}")
            
            file_result = _download_from_raw_url(
                token=token,
                url=str(url),
                target_volume=target_volume
            )
            
            if file_result.get("success"):
                results["successful_downloads"] += 1
                logger.info(f"  SUCCESS: {file_result.get('output_path')} ({file_result.get('bytes_downloaded')} bytes)")
            else:
                results["failed_downloads"] += 1
                logger.warning(f"  FAILED: {file_result.get('error')}")
            
            results["files"].append({
                "url": url,
                "filepath": file_result.get("filepath"),
                "repo_name": file_result.get("repo_name"),
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
                f"[RAW URL MODE] Successfully downloaded all {results['successful_downloads']} files "
                f"to {results['output_directory']} in {total_time}s"
            )
        elif results["successful_downloads"] == 0:
            results["status"] = "error"
            results["message"] = (
                f"[RAW URL MODE] Failed to download all {results['total_requested']} files. "
                f"Check individual file errors for details."
            )
        else:
            results["status"] = "partial"
            results["message"] = (
                f"[RAW URL MODE] Downloaded {results['successful_downloads']} of {results['total_requested']} files "
                f"({results['failed_downloads']} failed). Check individual file errors."
            )
        
        # Store download info in tool context state if available
        if tool_context:
            tool_context.state["last_download_directory"] = results["output_directory"]
            tool_context.state["last_download_count"] = results["successful_downloads"]
            tool_context.state["last_download_files"] = [
                f["output_path"] for f in results["files"] if f.get("success")
            ]
        
        logger.info(f"[RAW URL MODE] Download batch complete: {results['message']}")
        
        return results
    
    # =======================================================================
    # LEGACY MODE: UC dotted filepath format (repo inference required)
    # This path is more fragile - prefer raw URLs when possible
    # =======================================================================
    logger.info(f"[LEGACY MODE] Processing {len(filepaths)} filepaths with repo inference")
    
    # Resolve repo_name from repo-prefixed filepaths when possible.
    # This eliminates ambiguity where an agent must remember to merge repo_name + filepath.
    known_repos: set[str] = set()
    try:
        if tool_context and getattr(tool_context, "state", None):
            rows = tool_context.state.get("last_repo_search_rows") or []
            if isinstance(rows, list):
                for r in rows:
                    if isinstance(r, dict) and r.get("repo_name"):
                        known_repos.add(str(r["repo_name"]))
    except Exception:
        known_repos = set()

    def _maybe_split_repo_uc_filepath(fp: str) -> tuple[Optional[str], str]:
        """
        Parse '<repo_name>.<uc_filepath>' → (repo_name, uc_filepath).
        We only treat the prefix as a repo_name if it matches known repos from the most
        recent `repo_filename_search`, to avoid mis-parsing normal dotted paths like
        'src.etl.loader.py' as repo='src'.
        """
        if not fp or "." not in fp:
            return None, fp
        prefix, rest = fp.split(".", 1)
        # Best case: validate against repos returned by the most recent repo_filename_search.
        if known_repos and prefix in known_repos:
            return prefix, rest
        # Fallback heuristic: SpendMend repo names typically include '-' (e.g. Master-Vendor-Alignment).
        # Only apply this heuristic when we *cannot* validate against known repos.
        if not known_repos and "-" in prefix:
            return prefix, rest
        return None, fp

    inferred_repo: Optional[str] = repo_name
    normalized_filepaths: list[str] = []
    for fp in filepaths:
        fp = str(fp)
        if inferred_repo:
            # If user passed repo_uc_filepath while also providing repo_name, strip the prefix.
            prefix = inferred_repo + "."
            normalized_filepaths.append(fp[len(prefix) :] if fp.startswith(prefix) else fp)
            continue

        repo_from_fp, rest = _maybe_split_repo_uc_filepath(fp)
        if repo_from_fp:
            inferred_repo = repo_from_fp
            normalized_filepaths.append(rest)
        else:
            normalized_filepaths.append(fp)

    if not inferred_repo:
        if len(known_repos) == 1:
            inferred_repo = next(iter(known_repos))
        else:
            return {
                "status": "error",
                "message": (
                    "repo_name is required unless filepaths are prefixed with '<repo_name>.' "
                    "(repo_uc_filepath) OR the most recent repo_filename_search returned a single repo."
                ),
            }

    # If any filepaths include a repo prefix, enforce that it's the same repo (single-repo tool).
    # We detect mismatches only when we have known_repos to validate against.
    if known_repos:
        repos_seen = set()
        for original_fp in filepaths:
            repo_from_fp, _ = _maybe_split_repo_uc_filepath(str(original_fp))
            if repo_from_fp:
                repos_seen.add(repo_from_fp)
        if len(repos_seen) > 1:
            return {
                "status": "error",
                "message": (
                    f"Multiple repositories detected in filepaths ({sorted(repos_seen)}). "
                    "Please call get_repo_file once per repo."
                ),
            }
        if repos_seen and inferred_repo not in repos_seen:
            return {
                "status": "error",
                "message": (
                    f"repo_name mismatch: inferred '{inferred_repo}' but filepaths include prefixes {sorted(repos_seen)}. "
                    "Please call get_repo_file with consistent repo_uc_filepath values."
                ),
            }

    repo_name = inferred_repo
    filepaths = normalized_filepaths
    
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

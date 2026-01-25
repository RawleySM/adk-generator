"""
GitHub Raw URL File Download - Proof of Concept

Downloads a single file from GitHub using the Raw URL method.
This bypasses the JSON parsing and metadata overhead of the REST API,
treating GitHub as a high-performance file server.

Designed to be run as a Databricks job and potentially used as an agent tool.

Usage:
    python github_RAW_URL_api_download.py <token> <repo_name> <filepath> [branch] [target_volume]
    
    Where:
    - token: GitHub Personal Access Token
    - repo_name: Repository name as stored in Unity Catalog (e.g., 'Master-Vendor-Alignment')
    - filepath: Filepath in UC format with dots as separators (e.g., 'PyFunctions.Shared.ai_models.perplexity.py')
    - branch: (Optional) Branch or commit SHA (default: main)
    - target_volume: (Optional) Volume path to write downloaded file (default: /Volumes/silo_dev_rs/repos/codebases/tmp/)
    
    The org is always assumed to be 'SpendMend'.
"""
import sys
import os
import requests
import time
import logging
from typing import Optional, Tuple
from datetime import datetime

# Configure logging for Databricks observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default organization - always SpendMend per requirements
DEFAULT_ORG = "SpendMend"

# Default branch
DEFAULT_BRANCH = "main"

# Default target volume for downloaded files
DEFAULT_TARGET_VOLUME = "/Volumes/silo_dev_rs/repos/codebases/tmp/"

# Known file extensions for filepath conversion
KNOWN_EXTENSIONS = [
    '.py', '.sql', '.json', '.md', '.txt', '.csv', '.yaml', '.yml',
    '.sh', '.ps1', '.ipynb', '.scala', '.r', '.html', '.css', '.js',
    '.ts', '.tsx', '.jsx', '.xml', '.toml', '.cfg', '.ini', '.env',
    '.dockerfile', '.gitignore', '.parquet', '.delta', '.whl', '.tar.gz',
    '.zip', '.jar', '.log', '.rst', '.png', '.jpg', '.jpeg', '.gif', '.svg'
]


def convert_uc_filepath_to_github_path(filepath: str) -> str:
    """
    Converts Unity Catalog filepath format to GitHub API path format.
    
    In Unity Catalog (silo_dev_rs.repos.filenames):
    - Directory separators '/' are replaced with '.'
    - File extension dots remain unchanged
    
    Example:
        Input:  'PyFunctions.Shared.ai_models.perplexity.py'
        Output: 'PyFunctions/Shared/ai_models/perplexity.py'
        
    Args:
        filepath: Filepath in UC format (dots as directory separators)
        
    Returns:
        Filepath in GitHub API format (slashes as directory separators)
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
        # Remove extension, convert dots to slashes, re-attach extension
        ext_len = len(detected_ext)
        base = filepath[:-ext_len]
        # Preserve original extension case from the filepath
        original_ext = filepath[-ext_len:]
        return base.replace('.', '/') + original_ext
    else:
        # No known extension detected
        # Check if last segment looks like a file (short, no further dots)
        parts = filepath.rsplit('.', 1)
        if len(parts) == 2:
            potential_ext = parts[1]
            # Heuristic: if last part is short (<=10 chars) and alphanumeric, treat as extension
            if len(potential_ext) <= 10 and potential_ext.replace('_', '').isalnum():
                return parts[0].replace('.', '/') + '.' + potential_ext
        
        # Fallback: treat entire string as path (no extension, like 'Dockerfile' or 'README')
        # In this case we need to be careful - it might be a filename without extension
        # sitting in a directory. Let's assume the last segment is the filename.
        segments = filepath.split('.')
        if len(segments) == 1:
            # No dots at all, just return as-is
            return filepath
        else:
            # Assume last segment is filename, rest are directories
            return '/'.join(segments)


def build_raw_github_url(
    repo_name: str,
    filepath: str,
    branch: str = DEFAULT_BRANCH,
    org: str = DEFAULT_ORG
) -> str:
    """
    Builds the full GitHub Raw URL for direct file download.
    
    URL format: https://raw.githubusercontent.com/{owner}/{repo}/{branch_or_commit}/{path}
    
    Args:
        repo_name: Repository name (e.g., 'Master-Vendor-Alignment')
        filepath: Filepath in UC format (dots as separators)
        branch: Branch name or commit SHA (default: main)
        org: GitHub organization (default: SpendMend)
        
    Returns:
        Full GitHub Raw URL for direct file download
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    return f"https://raw.githubusercontent.com/{org}/{repo_name}/{branch}/{github_path}"


def download_file_streaming(
    token: str,
    repo_name: str,
    filepath: str,
    branch: str = DEFAULT_BRANCH,
    org: str = DEFAULT_ORG
) -> Tuple[Optional[str], dict]:
    """
    Downloads file content from GitHub using the Raw URL method with streaming.
    
    This bypasses JSON parsing and metadata overhead of the REST API.
    Uses stream=True for memory-efficient downloading of potentially large files.
    
    Args:
        token: GitHub Personal Access Token
        repo_name: Repository name (e.g., 'Master-Vendor-Alignment')
        filepath: Filepath in UC format (dots as separators)
        branch: Branch name or commit SHA (default: main)
        org: GitHub organization (default: SpendMend)
        
    Returns:
        Tuple of (content, metadata):
        - content: Raw file content as string, or None if error
        - metadata: Dict with download info including timing
        
    Note:
        Raw URL method has no file size limit like the REST API (1MB).
        However, for very large files, consider chunked processing.
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    url = build_raw_github_url(repo_name, filepath, branch, org)
    
    # Use Authorization header for private repos
    headers = {
        "Authorization": f"token {token}"
    }
    
    metadata = {
        "repo_name": repo_name,
        "uc_filepath": filepath,
        "github_path": github_path,
        "branch": branch,
        "org": org,
        "url": url,
        "method": "raw_url_streaming"
    }
    
    try:
        # Start timing the download
        start_time = time.perf_counter()
        
        # Request with stream=True for memory efficiency
        with requests.get(url, headers=headers, stream=True, timeout=60) as response:
            metadata["status_code"] = response.status_code
            
            if response.status_code == 200:
                # Collect content from stream
                chunks = []
                bytes_downloaded = 0
                
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=False):
                    if chunk:
                        chunks.append(chunk)
                        bytes_downloaded += len(chunk)
                
                # Calculate download time
                end_time = time.perf_counter()
                download_time_seconds = end_time - start_time
                
                # Decode content
                raw_bytes = b''.join(chunks)
                try:
                    content = raw_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    # Try with latin-1 as fallback
                    content = raw_bytes.decode('latin-1')
                
                # Calculate download speed
                download_speed_kbps = (bytes_downloaded / 1024) / download_time_seconds if download_time_seconds > 0 else 0
                
                metadata["content_length"] = len(content)
                metadata["bytes_downloaded"] = bytes_downloaded
                metadata["download_time_seconds"] = round(download_time_seconds, 4)
                metadata["download_speed_kbps"] = round(download_speed_kbps, 2)
                metadata["success"] = True
                
                return content, metadata
                
            elif response.status_code == 404:
                end_time = time.perf_counter()
                metadata["download_time_seconds"] = round(end_time - start_time, 4)
                metadata["error"] = "File not found"
                metadata["success"] = False
                return None, metadata
                
            elif response.status_code == 403:
                end_time = time.perf_counter()
                metadata["download_time_seconds"] = round(end_time - start_time, 4)
                metadata["error"] = "Forbidden - check token permissions or rate limit"
                metadata["success"] = False
                return None, metadata
                
            elif response.status_code == 429:
                end_time = time.perf_counter()
                metadata["download_time_seconds"] = round(end_time - start_time, 4)
                metadata["error"] = "Rate limited (429 Too Many Requests)"
                metadata["success"] = False
                return None, metadata
                
            else:
                end_time = time.perf_counter()
                metadata["download_time_seconds"] = round(end_time - start_time, 4)
                metadata["error"] = f"HTTP {response.status_code}"
                metadata["success"] = False
                return None, metadata
            
    except requests.exceptions.Timeout:
        metadata["error"] = "Request timed out"
        metadata["success"] = False
        return None, metadata
    except requests.exceptions.RequestException as e:
        metadata["error"] = str(e)
        metadata["success"] = False
        return None, metadata


def download_file_simple(
    token: str,
    repo_name: str,
    filepath: str,
    branch: str = DEFAULT_BRANCH,
    org: str = DEFAULT_ORG
) -> Tuple[Optional[str], dict]:
    """
    Downloads file content from GitHub using the Raw URL method (non-streaming).
    
    Simpler approach for smaller files. Loads entire file into memory at once.
    
    Args:
        token: GitHub Personal Access Token
        repo_name: Repository name (e.g., 'Master-Vendor-Alignment')
        filepath: Filepath in UC format (dots as separators)
        branch: Branch name or commit SHA (default: main)
        org: GitHub organization (default: SpendMend)
        
    Returns:
        Tuple of (content, metadata):
        - content: Raw file content as string, or None if error
        - metadata: Dict with download info including timing
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    url = build_raw_github_url(repo_name, filepath, branch, org)
    
    headers = {
        "Authorization": f"token {token}"
    }
    
    metadata = {
        "repo_name": repo_name,
        "uc_filepath": filepath,
        "github_path": github_path,
        "branch": branch,
        "org": org,
        "url": url,
        "method": "raw_url_simple"
    }
    
    try:
        # Start timing the download
        start_time = time.perf_counter()
        
        response = requests.get(url, headers=headers, timeout=60)
        
        # End timing
        end_time = time.perf_counter()
        download_time_seconds = end_time - start_time
        
        metadata["status_code"] = response.status_code
        metadata["download_time_seconds"] = round(download_time_seconds, 4)
        
        if response.status_code == 200:
            content = response.text
            bytes_downloaded = len(response.content)
            
            # Calculate download speed
            download_speed_kbps = (bytes_downloaded / 1024) / download_time_seconds if download_time_seconds > 0 else 0
            
            metadata["content_length"] = len(content)
            metadata["bytes_downloaded"] = bytes_downloaded
            metadata["download_speed_kbps"] = round(download_speed_kbps, 2)
            metadata["success"] = True
            
            return content, metadata
            
        elif response.status_code == 404:
            metadata["error"] = "File not found"
            metadata["success"] = False
            return None, metadata
            
        elif response.status_code == 403:
            metadata["error"] = "Forbidden - check token permissions or rate limit"
            metadata["success"] = False
            return None, metadata
            
        elif response.status_code == 429:
            metadata["error"] = "Rate limited (429 Too Many Requests)"
            metadata["success"] = False
            return None, metadata
            
        else:
            metadata["error"] = f"HTTP {response.status_code}"
            metadata["success"] = False
            return None, metadata
            
    except requests.exceptions.Timeout:
        metadata["error"] = "Request timed out"
        metadata["success"] = False
        return None, metadata
    except requests.exceptions.RequestException as e:
        metadata["error"] = str(e)
        metadata["success"] = False
        return None, metadata


def write_file_to_volume(
    content: str,
    filename: str,
    target_volume: str = DEFAULT_TARGET_VOLUME
) -> dict:
    """
    Writes downloaded file content to a Unity Catalog Volume.
    
    Args:
        content: File content as string
        filename: Name of the file to write
        target_volume: Volume path (default: /Volumes/silo_dev_rs/repos/codebases/tmp/)
        
    Returns:
        Dict with write operation results
    """
    # Ensure target_volume ends with /
    if not target_volume.endswith('/'):
        target_volume = target_volume + '/'
    
    output_path = os.path.join(target_volume, filename)
    
    result = {
        "filename": filename,
        "target_volume": target_volume,
        "output_path": output_path,
        "content_length": len(content)
    }
    
    try:
        # Start timing the write
        start_time = time.perf_counter()
        
        # Ensure directory exists
        os.makedirs(target_volume, exist_ok=True)
        
        # Write the file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # End timing
        end_time = time.perf_counter()
        write_time_seconds = end_time - start_time
        
        # Verify write
        if os.path.exists(output_path):
            result["success"] = True
            result["bytes_written"] = os.path.getsize(output_path)
            result["write_time_seconds"] = round(write_time_seconds, 4)
            logger.info(f"Successfully wrote {result['bytes_written']} bytes to {output_path}")
        else:
            result["success"] = False
            result["error"] = "File not found after write"
            
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"Failed to write file: {e}")
    
    return result


def extract_filename_from_path(filepath: str) -> str:
    """
    Extracts the filename from a UC filepath.
    
    Args:
        filepath: Filepath in UC format (e.g., 'PyFunctions.Shared.ai_models.perplexity.py')
        
    Returns:
        The filename (e.g., 'perplexity.py')
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    return os.path.basename(github_path)


def main():
    """
    Main entry point for Databricks job execution.
    
    Arguments:
        sys.argv[1]: GitHub token
        sys.argv[2]: Repository name (from UC repos.filenames.repo_name)
        sys.argv[3]: Filepath in UC format (from UC repos.filenames.filepath)
        sys.argv[4]: (Optional) Branch or commit SHA (default: main)
        sys.argv[5]: (Optional) Target volume path for output
    """
    # Log job start with context
    logger.info("=" * 60)
    logger.info("GitHub Raw URL File Download - Job Started")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Arguments received: {len(sys.argv) - 1}")
    logger.info("=" * 60)
    
    if len(sys.argv) < 4:
        print("Usage: python github_RAW_URL_api_download.py <token> <repo_name> <filepath> [branch] [target_volume]")
        print("")
        print("Arguments:")
        print("  token         - GitHub Personal Access Token")
        print("  repo_name     - Repository name (e.g., 'Master-Vendor-Alignment')")
        print("  filepath      - Filepath in UC format (e.g., 'PyFunctions.Shared.ai_models.perplexity.py')")
        print(f"  branch        - (Optional) Branch or commit SHA (default: {DEFAULT_BRANCH})")
        print(f"  target_volume - (Optional) Volume path to write file (default: {DEFAULT_TARGET_VOLUME})")
        print("")
        print("Example:")
        print("  python github_RAW_URL_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py")
        print("  python github_RAW_URL_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py main")
        sys.exit(1)
    
    token = sys.argv[1]
    repo_name = sys.argv[2]
    filepath = sys.argv[3]
    branch = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_BRANCH
    target_volume = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_TARGET_VOLUME
    
    # Handle case where branch is actually a volume path (backwards compat)
    if branch.startswith('/Volumes') or branch.startswith('/mnt'):
        target_volume = branch
        branch = DEFAULT_BRANCH
    
    github_path = convert_uc_filepath_to_github_path(filepath)
    filename = extract_filename_from_path(filepath)
    
    # Log input parameters (mask token)
    logger.info(f"Organization:  {DEFAULT_ORG}")
    logger.info(f"Repository:    {repo_name}")
    logger.info(f"Branch:        {branch}")
    logger.info(f"UC Filepath:   {filepath}")
    logger.info(f"GitHub Path:   {github_path}")
    logger.info(f"Filename:      {filename}")
    logger.info(f"Target Volume: {target_volume}")
    logger.info(f"Raw URL:       {build_raw_github_url(repo_name, filepath, branch)}")
    logger.info("-" * 60)
    
    # Track total job time
    job_start_time = time.perf_counter()
    
    # Step 1: Download content using streaming method
    logger.info("[1/2] Downloading file content via Raw URL (streaming)...")
    content, download_meta = download_file_streaming(token, repo_name, filepath, branch)
    
    if content is None:
        logger.error(f"Download failed: {download_meta.get('error')}")
        logger.error(f"URL attempted: {download_meta.get('url')}")
        sys.exit(1)
    
    logger.info(f"  Downloaded {download_meta.get('bytes_downloaded')} bytes")
    logger.info(f"  Download time: {download_meta.get('download_time_seconds')} seconds")
    logger.info(f"  Download speed: {download_meta.get('download_speed_kbps')} KB/s")
    
    # Step 2: Write to volume
    logger.info("[2/2] Writing file to volume...")
    write_result = write_file_to_volume(content, filename, target_volume)
    
    if not write_result.get("success"):
        logger.error(f"Write failed: {write_result.get('error')}")
        sys.exit(1)
    
    logger.info(f"  Output path: {write_result.get('output_path')}")
    logger.info(f"  Bytes written: {write_result.get('bytes_written')}")
    logger.info(f"  Write time: {write_result.get('write_time_seconds')} seconds")
    
    # Calculate total job time
    job_end_time = time.perf_counter()
    total_job_time = job_end_time - job_start_time
    
    # Display content preview
    logger.info("=" * 60)
    logger.info("FILE CONTENT PREVIEW (first 30 lines):")
    logger.info("=" * 60)
    
    lines = content.split('\n')
    preview_lines = lines[:30]
    for i, line in enumerate(preview_lines, 1):
        print(f"{i:4d} | {line}")
    
    if len(lines) > 30:
        print(f"... ({len(lines) - 30} more lines)")
    
    # Final summary with timing details
    logger.info("=" * 60)
    logger.info("JOB COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)
    logger.info(f"  File:           {filename}")
    logger.info(f"  Size:           {download_meta.get('bytes_downloaded')} bytes")
    logger.info(f"  Output:         {write_result.get('output_path')}")
    logger.info("-" * 60)
    logger.info("TIMING SUMMARY:")
    logger.info(f"  Download Time:  {download_meta.get('download_time_seconds')} seconds")
    logger.info(f"  Download Speed: {download_meta.get('download_speed_kbps')} KB/s")
    logger.info(f"  Write Time:     {write_result.get('write_time_seconds')} seconds")
    logger.info(f"  Total Job Time: {round(total_job_time, 4)} seconds")
    logger.info("=" * 60)
    
    # Return content for programmatic use
    return content


if __name__ == "__main__":
    main()

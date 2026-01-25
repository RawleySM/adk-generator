"""
GitHub REST API File Download - Proof of Concept

Downloads a single file from GitHub using the Repository Contents API.
Designed to be run as a Databricks job and potentially used as an agent tool.

Usage:
    python github_REST_api_download.py <token> <repo_name> <filepath> [target_volume]
    
    Where:
    - token: GitHub Personal Access Token
    - repo_name: Repository name as stored in Unity Catalog (e.g., 'Master-Vendor-Alignment')
    - filepath: Filepath in UC format with dots as separators (e.g., 'PyFunctions.Shared.ai_models.perplexity.py')
    - target_volume: (Optional) Volume path to write downloaded file (default: /Volumes/silo_dev_rs/repos/codebases/tmp/)
    
    The org is always assumed to be 'SpendMend'.
"""
import sys
import os
import requests
import json
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


def build_github_api_url(repo_name: str, filepath: str, org: str = DEFAULT_ORG) -> str:
    """
    Builds the full GitHub API URL for the Repository Contents endpoint.
    
    Args:
        repo_name: Repository name (e.g., 'Master-Vendor-Alignment')
        filepath: Filepath in UC format (dots as separators)
        org: GitHub organization (default: SpendMend)
        
    Returns:
        Full GitHub API URL for the contents endpoint
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    return f"https://api.github.com/repos/{org}/{repo_name}/contents/{github_path}"


def download_file_content(
    token: str,
    repo_name: str,
    filepath: str,
    org: str = DEFAULT_ORG
) -> Tuple[Optional[str], Optional[dict]]:
    """
    Downloads file content from GitHub using the Repository Contents API.
    
    Uses the 'application/vnd.github.v3.raw' media type to get raw file content
    directly without Base64 encoding.
    
    Args:
        token: GitHub Personal Access Token
        repo_name: Repository name (e.g., 'Master-Vendor-Alignment')
        filepath: Filepath in UC format (dots as separators)
        org: GitHub organization (default: SpendMend)
        
    Returns:
        Tuple of (content, metadata):
        - content: Raw file content as string, or None if error
        - metadata: Dict with file info or error details
        
    Note:
        Repository Contents API has a 1MB file size limit.
        For larger files, use the Git Blobs API instead.
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    url = f"https://api.github.com/repos/{org}/{repo_name}/contents/{github_path}"
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.raw",  # Request raw content directly
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    metadata = {
        "repo_name": repo_name,
        "uc_filepath": filepath,
        "github_path": github_path,
        "org": org,
        "url": url
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        metadata["status_code"] = response.status_code
        metadata["rate_limit_remaining"] = response.headers.get("X-RateLimit-Remaining")
        
        if response.status_code == 200:
            content = response.text
            metadata["content_length"] = len(content)
            metadata["success"] = True
            return content, metadata
        elif response.status_code == 404:
            metadata["error"] = "File not found"
            metadata["success"] = False
            return None, metadata
        elif response.status_code == 403:
            # Could be rate limit or file too large
            try:
                error_data = response.json()
                metadata["error"] = error_data.get("message", "Forbidden")
            except:
                metadata["error"] = "Forbidden - possibly rate limited or file > 1MB"
            metadata["success"] = False
            return None, metadata
        else:
            try:
                error_data = response.json()
                metadata["error"] = error_data.get("message", f"HTTP {response.status_code}")
            except:
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


def get_file_metadata(
    token: str,
    repo_name: str,
    filepath: str,
    org: str = DEFAULT_ORG
) -> dict:
    """
    Gets file metadata from GitHub without downloading content.
    
    Useful for checking file size before downloading, or for getting SHA
    to use with the Blobs API for large files.
    
    Args:
        token: GitHub Personal Access Token
        repo_name: Repository name
        filepath: Filepath in UC format (dots as separators)
        org: GitHub organization (default: SpendMend)
        
    Returns:
        Dict with file metadata (name, path, sha, size, type, etc.)
    """
    github_path = convert_uc_filepath_to_github_path(filepath)
    url = f"https://api.github.com/repos/{org}/{repo_name}/contents/{github_path}"
    
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",  # Request JSON metadata
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "success": True,
                "name": data.get("name"),
                "path": data.get("path"),
                "sha": data.get("sha"),
                "size": data.get("size"),
                "type": data.get("type"),
                "encoding": data.get("encoding"),
                "html_url": data.get("html_url"),
                "download_url": data.get("download_url")
            }
        else:
            try:
                error_data = response.json()
                return {
                    "success": False,
                    "error": error_data.get("message", f"HTTP {response.status_code}"),
                    "status_code": response.status_code
                }
            except:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}",
                    "status_code": response.status_code
                }
                
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


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
        # Ensure directory exists
        os.makedirs(target_volume, exist_ok=True)
        
        # Write the file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        # Verify write
        if os.path.exists(output_path):
            result["success"] = True
            result["bytes_written"] = os.path.getsize(output_path)
            logger.info(f"Successfully wrote {result['bytes_written']} bytes to {output_path}")
        else:
            result["success"] = False
            result["error"] = "File not found after write"
            
    except Exception as e:
        result["success"] = False
        result["error"] = str(e)
        logger.error(f"Failed to write file: {e}")
    
    return result


def main():
    """
    Main entry point for Databricks job execution.
    
    Arguments:
        sys.argv[1]: GitHub token
        sys.argv[2]: Repository name (from UC repos.filenames.repo_name)
        sys.argv[3]: Filepath in UC format (from UC repos.filenames.filepath)
        sys.argv[4]: (Optional) Target volume path for output
    """
    # Log job start with context
    logger.info("=" * 60)
    logger.info("GitHub REST API File Download - Job Started")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Arguments received: {len(sys.argv) - 1}")
    logger.info("=" * 60)
    
    if len(sys.argv) < 4:
        print("Usage: python github_REST_api_download.py <token> <repo_name> <filepath> [target_volume]")
        print("")
        print("Arguments:")
        print("  token         - GitHub Personal Access Token")
        print("  repo_name     - Repository name (e.g., 'Master-Vendor-Alignment')")
        print("  filepath      - Filepath in UC format (e.g., 'PyFunctions.Shared.ai_models.perplexity.py')")
        print(f"  target_volume - (Optional) Volume path to write file (default: {DEFAULT_TARGET_VOLUME})")
        print("")
        print("Example:")
        print("  python github_REST_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py")
        sys.exit(1)
    
    token = sys.argv[1]
    repo_name = sys.argv[2]
    filepath = sys.argv[3]
    target_volume = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_TARGET_VOLUME
    
    # Log input parameters (mask token)
    logger.info(f"Organization: {DEFAULT_ORG}")
    logger.info(f"Repository:   {repo_name}")
    logger.info(f"UC Filepath:  {filepath}")
    logger.info(f"GitHub Path:  {convert_uc_filepath_to_github_path(filepath)}")
    logger.info(f"Target Volume: {target_volume}")
    logger.info("-" * 60)
    
    # Step 1: Get metadata to check file size
    logger.info("[1/3] Fetching file metadata...")
    metadata = get_file_metadata(token, repo_name, filepath)
    
    if not metadata.get("success"):
        logger.error(f"Metadata fetch failed: {metadata.get('error')}")
        sys.exit(1)
    
    logger.info(f"  Name: {metadata.get('name')}")
    logger.info(f"  Size: {metadata.get('size')} bytes")
    logger.info(f"  SHA:  {metadata.get('sha')}")
    logger.info(f"  Type: {metadata.get('type')}")
    
    # Check size limit (1MB for Contents API)
    file_size = metadata.get("size", 0)
    if file_size > 1_000_000:
        logger.error(f"File size ({file_size} bytes) exceeds 1MB limit. Use Git Blobs API for large files.")
        sys.exit(1)
    
    # Step 2: Download content
    logger.info("[2/3] Downloading file content...")
    content, download_meta = download_file_content(token, repo_name, filepath)
    
    if content is None:
        logger.error(f"Download failed: {download_meta.get('error')}")
        sys.exit(1)
    
    logger.info(f"  Downloaded {download_meta.get('content_length')} characters")
    logger.info(f"  Rate limit remaining: {download_meta.get('rate_limit_remaining')}")
    
    # Step 3: Write to volume
    logger.info("[3/3] Writing file to volume...")
    filename = metadata.get('name')
    write_result = write_file_to_volume(content, filename, target_volume)
    
    if not write_result.get("success"):
        logger.error(f"Write failed: {write_result.get('error')}")
        sys.exit(1)
    
    logger.info(f"  Output path: {write_result.get('output_path')}")
    logger.info(f"  Bytes written: {write_result.get('bytes_written')}")
    
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
    
    # Final summary
    logger.info("=" * 60)
    logger.info("JOB COMPLETED SUCCESSFULLY")
    logger.info(f"  File: {filename}")
    logger.info(f"  Size: {file_size} bytes")
    logger.info(f"  Output: {write_result.get('output_path')}")
    logger.info("=" * 60)
    
    # Return content for programmatic use
    return content


if __name__ == "__main__":
    main()

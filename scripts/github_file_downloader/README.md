# GitHub File Downloader Scripts

This directory contains Python scripts for downloading individual files from GitHub repositories. Both scripts are designed to run as **Databricks jobs** and can be used as agent tools.

## Scripts Overview

| Script | Method | File Size Limit | Timing | Best For |
|--------|--------|-----------------|--------|----------|
| `github_REST_api_download.py` | GitHub REST API (Contents endpoint) | 1 MB | No | Metadata-rich downloads, small files |
| `github_RAW_URL_api_download.py` | Raw URL (raw.githubusercontent.com) | None | Yes | Large files, high-performance downloads |

---

## 1. github_REST_api_download.py

Downloads files using the **GitHub Repository Contents API**.

### Features
- Fetches file metadata (SHA, size, type) before download
- Uses `application/vnd.github.v3.raw` media type for direct content
- Reports rate limit remaining
- 1MB file size limit (API restriction)

### Usage

```bash
python github_REST_api_download.py <token> <repo_name> <filepath> [target_volume]
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `token` | Yes | GitHub Personal Access Token |
| `repo_name` | Yes | Repository name (e.g., `Master-Vendor-Alignment`) |
| `filepath` | Yes | Filepath in UC format with dots as separators |
| `target_volume` | No | Volume path for output (default: `/Volumes/silo_dev_rs/repos/codebases/tmp/`) |

### Example

```bash
python github_REST_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py
```

### Workflow Steps
1. **[1/3] Fetch metadata** - Gets file size, SHA, and type
2. **[2/3] Download content** - Retrieves raw file content
3. **[3/3] Write to volume** - Saves file to Unity Catalog Volume

---

## 2. github_RAW_URL_api_download.py

Downloads files using the **Raw URL method** (`raw.githubusercontent.com`), bypassing REST API overhead.

### Features
- No file size limit (unlike REST API's 1MB)
- Streaming download with chunked transfers (memory efficient)
- Detailed timing metrics (download time, speed, write time)
- Branch/commit SHA support for version pinning
- Higher throughput for large files

### Usage

```bash
python github_RAW_URL_api_download.py <token> <repo_name> <filepath> [branch] [target_volume]
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `token` | Yes | GitHub Personal Access Token |
| `repo_name` | Yes | Repository name (e.g., `Master-Vendor-Alignment`) |
| `filepath` | Yes | Filepath in UC format with dots as separators |
| `branch` | No | Branch name or commit SHA (default: `main`) |
| `target_volume` | No | Volume path for output (default: `/Volumes/silo_dev_rs/repos/codebases/tmp/`) |

### Examples

```bash
# Download from main branch (default)
python github_RAW_URL_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py

# Download from specific branch
python github_RAW_URL_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py develop

# Download from specific commit SHA
python github_RAW_URL_api_download.py ghp_xxx Master-Vendor-Alignment PyFunctions.Shared.ai_models.perplexity.py a1b2c3d4e5f6
```

### Workflow Steps
1. **[1/2] Download content** - Streams file via Raw URL
2. **[2/2] Write to volume** - Saves file to Unity Catalog Volume

### Timing Output

The Raw URL script provides detailed timing metrics:

```
TIMING SUMMARY:
  Download Time:  0.2345 seconds
  Download Speed: 125.67 KB/s
  Write Time:     0.0123 seconds
  Total Job Time: 0.2512 seconds
```

---

## Unity Catalog Filepath Format

Both scripts accept filepaths in **Unity Catalog format** where directory separators are replaced with dots:

| UC Format (Input) | GitHub Path (Output) |
|-------------------|---------------------|
| `PyFunctions.Shared.ai_models.perplexity.py` | `PyFunctions/Shared/ai_models/perplexity.py` |
| `src.utils.helper.py` | `src/utils/helper.py` |
| `docs.README.md` | `docs/README.md` |

This format matches the `filepath` column in the `silo_dev_rs.repos.filenames` Unity Catalog table.

---

## Databricks Job Configuration

### Task Type: **Python Script Task**

Both scripts are designed to run as Databricks **Python script tasks**. Configure your job as follows:

```yaml
tasks:
  - task_key: download_github_file
    python_script_task:
      python_file: /Workspace/path/to/github_RAW_URL_api_download.py
      parameters:
        - "{{secrets/scope/github_token}}"
        - "{{job.parameters.repo_name}}"
        - "{{job.parameters.filepath}}"
        - "main"
        - "/Volumes/silo_dev_rs/repos/codebases/tmp/"
    existing_cluster_id: "1115-120035-jyzgoasz"
```

### Job JSON Example

```json
{
  "name": "GitHub File Download",
  "tasks": [
    {
      "task_key": "download_file",
      "python_script_task": {
        "python_file": "/Workspace/Repos/scripts/github_file_downloader/github_RAW_URL_api_download.py",
        "parameters": [
          "{{secrets/github/pat}}",
          "Master-Vendor-Alignment",
          "PyFunctions.Shared.ai_models.perplexity.py",
          "main",
          "/Volumes/silo_dev_rs/repos/codebases/tmp/"
        ]
      },
      "existing_cluster_id": "1115-120035-jyzgoasz"
    }
  ],
  "parameters": [
    {"name": "repo_name", "default": "Master-Vendor-Alignment"},
    {"name": "filepath", "default": "README.md"}
  ]
}
```

### Using Databricks Secrets

Store your GitHub token in Databricks secrets:

```bash
databricks secrets put-secret github pat --string-value "ghp_xxxxxxxxxxxx"
```

Reference in job parameters: `{{secrets/github/pat}}`

---

## Output

Both scripts write downloaded files to a Unity Catalog Volume and display:

1. **Structured logging** - Timestamped INFO/ERROR messages
2. **Content preview** - First 30 lines of the downloaded file
3. **Job summary** - File name, size, and output path

### Sample Output (Raw URL)

```
============================================================
GitHub Raw URL File Download - Job Started
Timestamp: 2025-01-24T10:30:45.123456
Arguments received: 4
============================================================
Organization:  SpendMend
Repository:    Master-Vendor-Alignment
Branch:        main
UC Filepath:   PyFunctions.Shared.ai_models.perplexity.py
GitHub Path:   PyFunctions/Shared/ai_models/perplexity.py
Filename:      perplexity.py
Target Volume: /Volumes/silo_dev_rs/repos/codebases/tmp/
Raw URL:       https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/main/PyFunctions/Shared/ai_models/perplexity.py
------------------------------------------------------------
[1/2] Downloading file content via Raw URL (streaming)...
  Downloaded 4523 bytes
  Download time: 0.1234 seconds
  Download speed: 35.78 KB/s
[2/2] Writing file to volume...
  Output path: /Volumes/silo_dev_rs/repos/codebases/tmp/perplexity.py
  Bytes written: 4523
  Write time: 0.0045 seconds
============================================================
JOB COMPLETED SUCCESSFULLY
============================================================
  File:           perplexity.py
  Size:           4523 bytes
  Output:         /Volumes/silo_dev_rs/repos/codebases/tmp/perplexity.py
------------------------------------------------------------
TIMING SUMMARY:
  Download Time:  0.1234 seconds
  Download Speed: 35.78 KB/s
  Write Time:     0.0045 seconds
  Total Job Time: 0.1302 seconds
============================================================
```

---

## When to Use Which Script

| Use Case | Recommended Script |
|----------|-------------------|
| Files < 1MB with metadata needs | `github_REST_api_download.py` |
| Files > 1MB | `github_RAW_URL_api_download.py` |
| Performance benchmarking | `github_RAW_URL_api_download.py` |
| Pinning to specific commit | `github_RAW_URL_api_download.py` |
| Need file SHA for caching | `github_REST_api_download.py` |

---

## Dependencies

Both scripts require only standard Python libraries plus `requests`:

```
requests>=2.28.0
```

These are pre-installed on Databricks clusters.

---

## Organization Default

Both scripts default to the **SpendMend** organization. This is hardcoded as:

```python
DEFAULT_ORG = "SpendMend"
```

Modify this constant if targeting a different organization.

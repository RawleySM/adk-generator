While the **Raw URL** method is like taking a direct flight, the **REST API method** is like going through the airport terminal: you get more security, more information, and more control, but it involves a bit more "paperwork" (headers).

Here is how to use the **Repository Contents API** to stream a file.

---

### 1. The REST API Endpoint

The endpoint you’ll use is:
`GET /repos/{owner}/{repo}/contents/{path}`

By default, this returns **JSON** containing metadata and a Base64-encoded string. To stream the file as raw bytes, you must change the **Media Type** in your request header.

### 2. The "Magic" Header

To bypass the JSON wrapper and stream the raw file content directly from the REST endpoint, you must include:
`Accept: application/vnd.github.v3.raw`

This tells GitHub: *"Don't send me a JSON object; just give me the raw bytes of the file."*

---

### 3. Implementation Code (Python)

This script uses the REST API but forces a raw stream. This is the most "official" way to handle file inspection within the GitHub API ecosystem.

```python
import requests

def stream_rest_api_file(owner, repo, path, token):
    # 1. Standard REST API URL
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    
    # 2. Crucial Headers
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3.raw", # The 'Raw' media type
        "X-GitHub-Api-Version": "2022-11-28"      # Best practice to version your calls
    }

    # 3. Stream the request
    with requests.get(url, headers=headers, stream=True) as r:
        if r.status_code == 200:
            print(f"Streaming from REST API: {path}\n" + "-"*30)
            
            for line in r.iter_lines():
                if line:
                    # Inspection logic
                    decoded_line = line.decode('utf-8')
                    if "def " in decoded_line: # Example: look for Python functions
                        print(f"Found Function: {decoded_line.strip()}")
        else:
            print(f"Error: {r.status_code} - {r.json().get('message')}")

# Usage (Requires a GitHub Personal Access Token)
# stream_rest_api_file("owner", "repo", "path/to/script.py", "your_token_here")

```

---

### 4. Why use the REST API over the Raw URL?

If you can just use the Raw URL, why bother with the REST API? There are three main reasons:

1. **Unified Authentication:** If your app is already authenticated with the GitHub REST API (using an OAuth App or GitHub App), using this endpoint ensures you stay within the same authentication flow.
2. **Symlink Resolution:** The REST API is "smarter." If your path points to a symlink, the API knows how to resolve it, whereas the Raw URL might just give you the text of the link.
3. **Discovery:** If you aren't 100% sure the file exists, you can remove the `Accept` header first to check the metadata (size, type, SHA), then re-request with the `raw` header to stream the content.

### 5. Important Constraints

* **Size Limit:** The Repository Contents API only supports files up to **1MB**. If your Python file is larger than 1MB, this call will fail with a `403 Forbidden` or a "too large" error.
* **Large Files (>1MB):** For files larger than 1MB, you **must** use the **Git Database API** (Blobs) using the file's SHA:
`GET /repos/{owner}/{repo}/git/blobs/{file_sha}`
(You would still use the `Accept: application/vnd.github.v3.raw` header to stream it).

---

**Would you like me to show you how to fetch the file's SHA first so you can stream files larger than 1MB using the Blobs API?**
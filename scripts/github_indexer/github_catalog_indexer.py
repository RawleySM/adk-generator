import sys
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, ArrayType
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder.appName("GithubCatalogIndexer").getOrCreate()

def list_repos(token, org):
    """
    Lists all repositories in the organization.
    """
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    url = f"https://api.github.com/orgs/{org}/repos?per_page=100"
    repos = []
    
    print(f"Listing repos for {org}...")
    while url:
        try:
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            if not data: break
            
            for r in data:
                repos.append(r['name'])
            
            if 'next' in resp.links:
                url = resp.links['next']['url']
            else:
                url = None
        except Exception as e:
            print(f"Error listing repos: {e}")
            break
    return repos

def get_repo_files(repo_name, token, org):
    """
    Fetches the file tree for a single repo and returns a list of file objects.
    
    Note: 'last_modified_by' and 'timestamp' are approximated using the repo's 
    updated_at time to avoid excessive API calls (N+1 problem) on large trees.
    """
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    
    # 1. Get default branch and repo metadata (for fallback timestamp)
    repo_url = f"https://api.github.com/repos/{org}/{repo_name}"
    try:
        repo_resp = requests.get(repo_url, headers=headers)
        repo_resp.raise_for_status()
        repo_data = repo_resp.json()
        default_branch = repo_data.get("default_branch", "main")
        # Use repo last updated as a fallback for file timestamp to be efficient
        repo_updated_at = repo_data.get("updated_at")
        if repo_updated_at:
            # Convert ISO 8601 to timestamp
            repo_ts = datetime.strptime(repo_updated_at, "%Y-%m-%dT%H:%M:%SZ")
        else:
            repo_ts = datetime.now()
    except Exception as e:
        print(f"Skipping {repo_name}: Could not get metadata. {e}")
        return []

    # 2. Get the Tree (Recursive)
    tree_url = f"https://api.github.com/repos/{org}/{repo_name}/git/trees/{default_branch}?recursive=1"
    file_objects = []
    
    try:
        tree_resp = requests.get(tree_url, headers=headers)
        if tree_resp.status_code == 409: # Empty repo or git error
            print(f"Repo {repo_name} appears empty or inaccessible.")
            return []
        tree_resp.raise_for_status()
        tree_data = tree_resp.json()
        
        for item in tree_data.get("tree", []):
            if item["type"] == "blob": # It's a file
                raw_path = item["path"]
                # Filepath with folders separated by "."
                # e.g., "src/main/app.py" -> "src.main.app.py"
                # But typically 'filepath' implies the location. 
                # If the user wants the full path as dot-separated:
                dot_filepath = raw_path.replace("/", ".")
                
                # Extract filename
                filename = raw_path.split("/")[-1]
                
                # Determine filetype (extension)
                filetype = filename.split(".")[-1] if "." in filename else "unknown"
                
                f_obj = {
                    "repo_name": repo_name,
                    "filename": filename,
                    "filepath": dot_filepath,
                    "filetype": filetype,
                    "filesize": item.get("size", 0),
                    # cost-prohibitive to fetch per-file commit info for 1000s of files
                    "last_modified_by": "unknown", 
                    "last_modified_timestamp": repo_ts,
                    "dataTables": [] # Placeholder for future logic
                }
                file_objects.append(f_obj)
                
    except Exception as e:
        print(f"Error fetching tree for {repo_name}: {e}")
        
    return file_objects

def process_repos_to_df(repo_list, token, org):
    """
    Iterates through repo list, collects file data, and creates a Spark DataFrame.
    """
    all_files = []
    print(f"Processing {len(repo_list)} repositories...")
    
    for i, repo in enumerate(repo_list):
        print(f"[{i+1}/{len(repo_list)}] Scanning {repo}...")
        files = get_repo_files(repo, token, org)
        all_files.extend(files)
        
    print(f"Total files found: {len(all_files)}")
    
    # Define Schema
    schema = StructType([
        StructField("repo_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("filepath", StringType(), True),
        StructField("filetype", StringType(), True),
        StructField("filesize", LongType(), True),
        StructField("last_modified_by", StringType(), True),
        StructField("last_modified_timestamp", TimestampType(), True),
        StructField("dataTables", ArrayType(StringType()), True)
    ])
    
    if not all_files:
        print("No files found. Returning empty DataFrame.")
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(all_files, schema)

def write_to_delta(df, table_name):
    """
    Writes the DataFrame to a Delta table.
    """
    print(f"Writing to {table_name}...")
    # Using overwriteSchema in case schema evolves
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print("Write complete.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python github_catalog_indexer.py <token> <org> [test_repo_name]")
        sys.exit(1)
        
    token = sys.argv[1]
    org = sys.argv[2]
    
    # Optional 3rd arg for testing specific repo(s)
    target_repos = []
    if len(sys.argv) > 3:
        target_repos = [sys.argv[3]]
        print(f"Running in TEST mode for repo: {target_repos}")
    else:
        # Full run
        target_repos = list_repos(token, org)
        print(f"Running in FULL mode for all {len(target_repos)} repos.")

    if target_repos:
        df = process_repos_to_df(target_repos, token, org)
        
        # Write to table
        # If in test mode, maybe write to a temp table? 
        # The prompt implies writing to the final table, so we will do that.
        # But for safety in a "Test" run, I'll append or overwrite?
        # The user said "Test... If this works, rerun". 
        # I'll stick to the requested table name.
        write_to_delta(df, "silo_dev_rs.repos.files")
        
        # Show sample
        df.show(5, truncate=False)
    else:
        print("No repos to process.")

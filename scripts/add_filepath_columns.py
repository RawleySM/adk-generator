"""Add relative_backslash and full_filepath columns to silo_dev_rs.repos.files.

This PySpark script adds two new columns to support get_repo_file.py:
1. relative_backslash: Converts dot-separated filepath to slash-separated path
   - Replaces "." with "/" for folder delimiters
   - Preserves the "." before filetype extension
   - If filetype="unknown", the rightmost "." remains unchanged

2. full_filepath: Combined as "<repo_name>/<relative_backslash>"

Unity Catalog file paths use forward slashes (/) with no escaping issues.
See: https://docs.databricks.com/en/data-governance/unity-catalog/paths.html
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("AddFilepathColumns").getOrCreate()

# Source table
SOURCE_TABLE = "silo_dev_rs.repos.files"


def convert_filepath_to_slash(filepath: str, filetype: str, filename: str) -> str:
    """
    Convert dot-separated filepath to slash-separated path.
    
    Uses the filename to correctly identify where the folder path ends
    and the actual filename begins, avoiding incorrect conversions of
    dots within filenames (e.g., terraform.tfvars.example).
    
    Examples:
        filepath="src.etl.loader.py", filename="loader.py"
            -> "src/etl/loader.py"
        
        filepath="frontend.terraform.dev.terraform.tfvars.example", filename="terraform.tfvars.example"
            -> "frontend/terraform/dev/terraform.tfvars.example"
        
        filepath="README.md", filename="README.md"
            -> "README.md"
        
        filepath="data.tar.gz", filename="data.tar.gz"
            -> "data.tar.gz"
            
        filepath="path.to.unknown_file", filename="unknown_file" (filetype="unknown")
            -> "path/to/unknown_file"
    """
    if not filepath:
        return filepath
    
    if not filename:
        # Fallback: if no filename, return filepath as-is (shouldn't happen)
        return filepath
    
    # Check if filepath ends with the filename (it should)
    if filepath.endswith(filename):
        # Split into directory path and filename
        if len(filepath) > len(filename):
            # There's a directory part before the filename
            # The character before filename should be "." which we'll replace with "/"
            dir_path = filepath[:-len(filename) - 1]  # -1 for the separator dot
            # Convert all dots in directory path to slashes
            dir_path_slashed = dir_path.replace('.', '/')
            return dir_path_slashed + '/' + filename
        else:
            # filepath IS the filename (no directory)
            return filename
    
    # Fallback: filename not found at end of filepath (shouldn't happen)
    # Try the original algorithm as best effort
    
    # Handle filetype = "unknown" - preserve rightmost dot as folder separator
    if filetype == "unknown":
        return filepath.replace('.', '/')
    
    # Try to find extension and preserve it
    if filetype and filepath.lower().endswith(f".{filetype.lower()}"):
        ext_len = len(filetype) + 1
        base = filepath[:-ext_len]
        extension = filepath[-ext_len:]
        return base.replace('.', '/') + extension
    
    # Last resort: return filepath as-is
    return filepath


# Register the UDF (now takes 3 args: filepath, filetype, filename)
convert_filepath_udf = F.udf(convert_filepath_to_slash, StringType())


def main():
    print(f"Reading from {SOURCE_TABLE}...")
    
    # Read source table
    df = spark.read.table(SOURCE_TABLE)
    
    print(f"Total rows: {df.count()}")
    print("Schema:")
    df.printSchema()
    
    # Check if columns already exist
    existing_cols = df.columns
    if "relative_backslash" in existing_cols:
        print("WARNING: relative_backslash column already exists. It will be overwritten.")
        df = df.drop("relative_backslash")
    if "full_filepath" in existing_cols:
        print("WARNING: full_filepath column already exists. It will be overwritten.")
        df = df.drop("full_filepath")
    
    # Add relative_backslash column using UDF
    # Uses filename to correctly identify where folder path ends and filename begins
    print("Adding relative_backslash column...")
    df = df.withColumn(
        "relative_backslash",
        convert_filepath_udf(F.col("filepath"), F.col("filetype"), F.col("filename"))
    )
    
    # Add full_filepath column: <repo_name>/<relative_backslash>
    print("Adding full_filepath column...")
    df = df.withColumn(
        "full_filepath",
        F.concat(F.col("repo_name"), F.lit("/"), F.col("relative_backslash"))
    )
    
    # Show sample of transformations
    print("\nSample transformations:")
    df.select(
        "repo_name",
        "filepath", 
        "filetype",
        "relative_backslash",
        "full_filepath"
    ).show(10, truncate=False)
    
    # Write back to table with new columns
    print(f"\nWriting to {SOURCE_TABLE}...")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SOURCE_TABLE)
    
    print("Successfully added relative_backslash and full_filepath columns.")
    
    # Verify
    print("\nVerifying updated table:")
    result = spark.read.table(SOURCE_TABLE)
    result.select(
        "repo_name",
        "filepath",
        "filetype", 
        "relative_backslash",
        "full_filepath"
    ).show(5, truncate=False)
    
    # Show count by filetype for validation
    print("\nFiletype distribution:")
    result.groupBy("filetype").count().orderBy(F.desc("count")).show(20)


if __name__ == "__main__":
    main()

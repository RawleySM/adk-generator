from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("Starting ETL process...")

try:
    # Read source tables
    print("Reading jira.raw.data...")
    df_data = spark.read.table("jira.raw.data")
    
    print("Reading jira.raw.dev...")
    df_dev = spark.read.table("jira.raw.dev")

    # Write to target tables
    print("Writing to silo_dev_rs.task.jira_raw_data...")
    df_data.write.format("delta").mode("overwrite").saveAsTable("silo_dev_rs.task.jira_raw_data")
    
    print("Writing to silo_dev_rs.task.jira_raw_dev...")
    df_dev.write.format("delta").mode("overwrite").saveAsTable("silo_dev_rs.task.jira_raw_dev")

    print("ETL process completed successfully.")

except Exception as e:
    print(f"Error during ETL: {e}")
    raise e

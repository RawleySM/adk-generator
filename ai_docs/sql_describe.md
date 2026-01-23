# SQL Describe Output

## Table: silo_dev_rs.repos.files
### DESCRIBE EXTENDED
```
============================================================
Databricks SQL Executor
============================================================
Profile: rstanhope
Dry Run: False
============================================================
Executing SQL:
DESCRIBE EXTENDED silo_dev_rs.repos.files
------------------------------------------------------------
2026-01-23 16:48:06,622 - INFO - loading rstanhope profile from ~/.databrickscfg: host, cluster_id, auth_type
2026-01-23 16:48:08,502 - INFO - Using Databricks CLI authentication
2026-01-23 16:48:09,221 - INFO - Using SQL warehouse: spendmend-dev-sql-cluster (id: 57f6389fdcdefbc0)
✓ SQL executed successfully!
Columns: ['col_name', 'data_type', 'comment']
Results (24 rows):
  Row 0: ['repo_name', 'string', "The name of the repository to which the file belongs, providing context for the file's location and purpose within the project structure."]
  Row 1: ['filename', 'string', 'The name of the file, which is essential for identifying the specific file within the repository.']
  Row 2: ['filepath', 'string', 'The complete path to the file within the repository, indicating its location in the directory structure.']
  Row 3: ['filetype', 'string', 'The type of the file, which helps in understanding the format and potential usage of the file (e.g., .txt, .jpg, .pdf).']
  Row 4: ['filesize', 'bigint', 'The size of the file in bytes, which can be useful for assessing storage requirements and file transfer times.']
  Row 5: ['last_modified_by', 'string', 'The identifier of the person who last modified the file, which is important for tracking changes and accountability.']
  Row 6: ['last_modified_timestamp', 'timestamp', 'The timestamp indicating when the file was last modified, providing a timeline for file updates and changes.']
  Row 7: ['dataTables', 'array<string>', 'An array of related data tables that may provide additional context or information associated with the file.']
  Row 8: ['', '', '']
  Row 9: ['# Detailed Table Information', '', '']
  Row 10: ['Catalog', 'silo_dev_rs', '']
  Row 11: ['Database', 'repos', '']
  Row 12: ['Table', 'files', '']
  Row 13: ['Created Time', 'Sun Jan 18 13:06:37 UTC 2026', '']
  Row 14: ['Last Access', 'UNKNOWN', '']
  Row 15: ['Created By', 'Spark ', '']
  Row 16: ['Type', 'MANAGED', '']
  Row 17: ['Comment', 'The table contains information about files associated with various repositories. It includes details such as the repository name, file name, file path, file type, file size, and the last person who modified the file along with the timestamp of the last modification. This data can be used for tracking file changes, managing repository contents, and analyzing file usage patterns.', '']
  Row 18: ['Location', 'abfss://unity-catalog@smlake.dfs.core.windows.net/518dcbde-e27d-4f7e-9785-ec49eb06282e/tables/5b64ed31-e23b-481e-9f17-120f05407e44', '']
  Row 19: ['Provider', 'delta', '']
  Row 20: ['Owner', 'rstanhope@spendmend.com', '']
  Row 21: ['Is_managed_location', 'true', '']
  Row 22: ['Predictive Optimization', 'ENABLE (inherited from METASTORE sm-datastore)', '']
  Row 23: ['Table Properties', '[delta.enableDeletionVectors=true,delta.feature.deletionVectors=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]', '']
```

### DESCRIBE DETAIL
```
============================================================
Databricks SQL Executor
============================================================
Profile: rstanhope
Dry Run: False
============================================================
Executing SQL:
DESCRIBE DETAIL silo_dev_rs.repos.files
------------------------------------------------------------
2026-01-23 16:48:11,709 - INFO - loading rstanhope profile from ~/.databrickscfg: host, cluster_id, auth_type
2026-01-23 16:48:11,745 - INFO - Using Databricks CLI authentication
2026-01-23 16:48:12,287 - INFO - Using SQL warehouse: spendmend-dev-sql-cluster (id: 57f6389fdcdefbc0)
✓ SQL executed successfully!
Columns: ['format', 'id', 'name', 'description', 'location', 'createdAt', 'lastModified', 'partitionColumns', 'clusteringColumns', 'numFiles', 'sizeInBytes', 'properties', 'minReaderVersion', 'minWriterVersion', 'tableFeatures', 'statistics', 'clusterByAuto']
Results (1 rows):
  Row 0: ['delta', 'd4393e24-fddd-4d81-a25b-903520d442c0', 'silo_dev_rs.repos.files', 'The table contains information about files associated with various repositories. It includes details such as the repository name, file name, file path, file type, file size, and the last person who modified the file along with the timestamp of the last modification. This data can be used for tracking file changes, managing repository contents, and analyzing file usage patterns.', 'abfss://unity-catalog@smlake.dfs.core.windows.net/518dcbde-e27d-4f7e-9785-ec49eb06282e/tables/5b64ed31-e23b-481e-9f17-120f05407e44', '2026-01-18T13:06:16.065Z', '2026-01-23T21:19:20.000Z', '[]', '[]', '16', '1542641', '{"delta.enableDeletionVectors":"true"}', '3', '7', '["deletionVectors"]', '{"numRowsDeletedByDeletionVectors":"0","numDeletionVectors":"0"}', 'false']
```
---------------------------------------------------

## Table: silo_dev_rs.metadata.columnnames
### DESCRIBE EXTENDED
```
============================================================
Databricks SQL Executor
============================================================
Profile: rstanhope
Dry Run: False
============================================================
Executing SQL:
DESCRIBE EXTENDED silo_dev_rs.metadata.columnnames
------------------------------------------------------------
2026-01-23 16:48:11,617 - INFO - loading rstanhope profile from ~/.databrickscfg: host, cluster_id, auth_type
2026-01-23 16:48:11,649 - INFO - Using Databricks CLI authentication
2026-01-23 16:48:12,164 - INFO - Using SQL warehouse: spendmend-dev-sql-cluster (id: 57f6389fdcdefbc0)
✓ SQL executed successfully!
Columns: ['col_name', 'data_type', 'comment']
Results (18 rows):
  Row 0: ['path', 'string', None]
  Row 1: ['column_array', 'array<string>', None]
  Row 2: ['', '', '']
  Row 3: ['# Detailed Table Information', '', '']
  Row 4: ['Catalog', 'silo_dev_rs', '']
  Row 5: ['Database', 'metadata', '']
  Row 6: ['Table', 'columnnames', '']
  Row 7: ['Created Time', 'Tue Jan 06 19:32:46 UTC 2026', '']
  Row 8: ['Last Access', 'UNKNOWN', '']
  Row 9: ['Created By', 'Spark ', '']
  Row 10: ['Statistics', '2307066 bytes, 42057 rows', '']
  Row 11: ['Type', 'MANAGED', '']
  Row 12: ['Location', 'abfss://unity-catalog@smlake.dfs.core.windows.net/518dcbde-e27d-4f7e-9785-ec49eb06282e/tables/5393997e-aa50-4b8c-ab83-1e2527dec495', '']
  Row 13: ['Provider', 'delta', '']
  Row 14: ['Owner', 'rstanhope@spendmend.com', '']
  Row 15: ['Is_managed_location', 'true', '']
  Row 16: ['Predictive Optimization', 'ENABLE (inherited from METASTORE sm-datastore)', '']
  Row 17: ['Table Properties', '[delta.enableDeletionVectors=true,delta.feature.deletionVectors=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]', '']
```

### DESCRIBE DETAIL
```
============================================================
Databricks SQL Executor
============================================================
Profile: rstanhope
Dry Run: False
============================================================
Executing SQL:
DESCRIBE DETAIL silo_dev_rs.metadata.columnnames
------------------------------------------------------------
2026-01-23 16:48:14,055 - INFO - loading rstanhope profile from ~/.databrickscfg: host, cluster_id, auth_type
2026-01-23 16:48:14,078 - INFO - Using Databricks CLI authentication
2026-01-23 16:48:14,618 - INFO - Using SQL warehouse: spendmend-dev-sql-cluster (id: 57f6389fdcdefbc0)
✓ SQL executed successfully!
Columns: ['format', 'id', 'name', 'description', 'location', 'createdAt', 'lastModified', 'partitionColumns', 'clusteringColumns', 'numFiles', 'sizeInBytes', 'properties', 'minReaderVersion', 'minWriterVersion', 'tableFeatures', 'statistics', 'clusterByAuto']
Results (1 rows):
  Row 0: ['delta', '220b26bb-3a48-41b0-9383-35ea00f424e0', 'silo_dev_rs.metadata.columnnames', None, 'abfss://unity-catalog@smlake.dfs.core.windows.net/518dcbde-e27d-4f7e-9785-ec49eb06282e/tables/5393997e-aa50-4b8c-ab83-1e2527dec495', '2026-01-06T19:32:36.353Z', '2026-01-06T19:32:41.000Z', '[]', '[]', '7', '2307066', '{"delta.enableDeletionVectors":"true"}', '3', '7', '["deletionVectors"]', '{"numRowsDeletedByDeletionVectors":"0","numDeletionVectors":"0"}', 'false']
```
---------------------------------------------------

## Table: silo_dev_rs.metadata.uc_provenance_index
### DESCRIBE EXTENDED
```
============================================================
Databricks SQL Executor
============================================================
Profile: rstanhope
Dry Run: False
============================================================
Executing SQL:
DESCRIBE EXTENDED silo_dev_rs.metadata.uc_provenance_index
------------------------------------------------------------
2026-01-23 16:48:16,705 - INFO - loading rstanhope profile from ~/.databrickscfg: host, cluster_id, auth_type
2026-01-23 16:48:16,727 - INFO - Using Databricks CLI authentication
2026-01-23 16:48:17,289 - INFO - Using SQL warehouse: spendmend-dev-sql-cluster (id: 57f6389fdcdefbc0)
✓ SQL executed successfully!
Columns: ['col_name', 'data_type', 'comment']
Results (29 rows):
  Row 0: ['table_name', 'string', 'Represents the name of the table that the provenance information pertains to.']
  Row 1: ['version', 'bigint', 'Indicates the version number of the table at the time the event was recorded.']
  Row 2: ['event_time', 'timestamp', 'Records the timestamp of when the event occurred, providing a chronological context for the provenance data.']
  Row 3: ['user_id', 'string', 'Identifies the unique user who performed the operation related to the provenance entry.']
  Row 4: ['user_name', 'string', 'Displays the name of the user associated with the user ID, offering a more human-readable reference.']
  Row 5: ['operation', 'string', 'Describes the type of operation that was performed on the table, such as insert, update, or delete.']
  Row 6: ['job_id', 'string', 'Contains the unique identifier for the job that executed the operation, linking it to a specific process.']
  Row 7: ['job_run_id', 'string', 'Represents the unique identifier for the specific run of the job, allowing for tracking of individual executions.']
  Row 8: ['notebook_id', 'string', 'Identifies the notebook associated with the operation, useful for tracing back to the context in which the operation was executed.']
  Row 9: ['cluster_id', 'string', 'Specifies the identifier of the cluster where the operation was executed, providing insight into the environment used.']
  Row 10: ['read_version', 'bigint', 'Indicates the version of the table that was read during the operation, which is important for understanding data consistency.']
  Row 11: ['user_metadata', 'string', 'Holds additional metadata about the user, which can provide context or attributes relevant to the operation.']
  Row 12: ['operation_params_json', 'string', 'Contains a JSON representation of the parameters used during the operation, allowing for detailed analysis of the execution context.']
  Row 13: ['operation_metrics_json', 'string', 'Stores a JSON object with metrics related to the operation, which can be useful for performance analysis and monitoring.']
  Row 14: ['', '', '']
  Row 15: ['# Detailed Table Information', '', '']
  Row 16: ['Catalog', 'silo_dev_rs', '']
  Row 17: ['Database', 'metadata', '']
  Row 18: ['Table', 'uc_provenance_index', '']
  Row 19: ['Created Time', 'Sun Jan 18 14:48:54 UTC 2026', '']
  Row 20: ['Last Access', 'UNKNOWN', '']
  Row 21: ['Created By', 'Spark ', '']
  Row 22: ['Type', 'MANAGED', '']
  Row 23: ['Location', 'abfss://unity-catalog@smlake.dfs.core.windows.net/518dcbde-e27d-4f7e-9785-ec49eb06282e/tables/842e2809-f87f-4b87-9830-bd60c1a60fe0', '']
  Row 24: ['Provider', 'delta', '']
  Row 25: ['Owner', 'rstanhope@spendmend.com', '']
  Row 26: ['Is_managed_location', 'true', '']
  Row 27: ['Predictive Optimization', 'ENABLE (inherited from METASTORE sm-datastore)', '']
  Row 28: ['Table Properties', '[delta.enableDeletionVectors=true,delta.feature.deletionVectors=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]', '']
```

### DESCRIBE DETAIL
```
============================================================
Databricks SQL Executor
============================================================
Profile: rstanhope
Dry Run: False
============================================================
Executing SQL:
DESCRIBE DETAIL silo_dev_rs.metadata.uc_provenance_index
------------------------------------------------------------
2026-01-23 16:48:19,359 - INFO - loading rstanhope profile from ~/.databrickscfg: host, cluster_id, auth_type
2026-01-23 16:48:19,400 - INFO - Using Databricks CLI authentication
2026-01-23 16:48:20,211 - INFO - Using SQL warehouse: spendmend-dev-sql-cluster (id: 57f6389fdcdefbc0)
✓ SQL executed successfully!
Columns: ['format', 'id', 'name', 'description', 'location', 'createdAt', 'lastModified', 'partitionColumns', 'clusteringColumns', 'numFiles', 'sizeInBytes', 'properties', 'minReaderVersion', 'minWriterVersion', 'tableFeatures', 'statistics', 'clusterByAuto']
Results (1 rows):
  Row 0: ['delta', 'a2c86f35-2a26-4f83-a089-aa78ea48d841', 'silo_dev_rs.metadata.uc_provenance_index', None, 'abfss://unity-catalog@smlake.dfs.core.windows.net/518dcbde-e27d-4f7e-9785-ec49eb06282e/tables/842e2809-f87f-4b87-9830-bd60c1a60fe0', '2026-01-18T14:48:39.079Z', '2026-01-18T14:52:38.000Z', '[]', '[]', '22', '204274', '{"delta.enableDeletionVectors":"true"}', '3', '7', '["deletionVectors"]', '{"numRowsDeletedByDeletionVectors":"0","numDeletionVectors":"0"}', 'false']
```
---------------------------------------------------

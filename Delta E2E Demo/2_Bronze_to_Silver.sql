-- Databricks notebook source
-- MAGIC %md # Data Cleaning: Bronze to Silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

-- COMMAND ----------

-- MAGIC %md ## Setup

-- COMMAND ----------

USE <database_name>;

DROP TABLE IF EXISTS iot_stream_silver;

-- configurations for auto-optimize
SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

-- Creates Delta Lake Table and infers schema based on files
CREATE TABLE iot_stream_silver
USING DELTA 
AS SELECT * 
FROM json.`/mnt/<s3_bucket_name>/iot-stream/`
WHERE 2=1;

-- COMMAND ----------

-- MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) ACID Transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support: `DELETE`, `UPDATE`, `MERGE INTO`
-- MAGIC 
-- MAGIC Delta Lake brings ACID transactions and full DML support to data lakes.

-- COMMAND ----------

-- MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Support Change Data Capture Workflows & Other Ingest Use Cases via `MERGE INTO`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With a legacy data pipeline, to insert or update a table, you must:
-- MAGIC 1. Identify the new rows to be inserted
-- MAGIC 2. Identify the rows that will be replaced (i.e. updated)
-- MAGIC 3. Identify all of the rows that are not impacted by the insert or update
-- MAGIC 4. Create a new temp based on all three insert statements
-- MAGIC 5. Delete the original table (and all of those associated files)
-- MAGIC 6. "Rename" the temp table back to the original table name
-- MAGIC 7. Drop the temp table
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif" alt='Merge process' width=600/>
-- MAGIC 
-- MAGIC 
-- MAGIC #### INSERT or UPDATE with Delta Lake
-- MAGIC 
-- MAGIC 2-step process: 
-- MAGIC 1. Identify rows to insert or update
-- MAGIC 2. Use `MERGE`

-- COMMAND ----------

MERGE INTO iot_stream_silver
USING iot_stream_bronze
ON iot_stream_bronze.id = iot_stream_silver.id
WHEN MATCHED
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *;

-- COMMAND ----------

-- add year and month columns and partition on these columns
CREATE OR REPLACE TABLE iot_stream_silver
USING DELTA
PARTITIONED BY (year, month)
SELECT YEAR(timestamp) year, MONTH(timestamp) month, *
FROM iot_stream_silver;

-- COMMAND ----------

-- MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) `DELETE`: Handle GDPR or CCPA Requests on your Data Lake

-- COMMAND ----------

SELECT * FROM iot_stream_silver WHERE id = 300000;

-- COMMAND ----------

DELETE FROM iot_stream_silver WHERE id = 300000;

-- Confirm the data was deleted
SELECT * FROM iot_stream_silver WHERE id = 300000;

-- COMMAND ----------

-- MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Time Travel

-- COMMAND ----------

-- MAGIC %md Delta Lake’s time travel capabilities simplify building data pipelines for use cases including:
-- MAGIC 
-- MAGIC * Auditing Data Changes
-- MAGIC * Reproducing experiments & reports
-- MAGIC * Rollbacks
-- MAGIC 
-- MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
-- MAGIC 
-- MAGIC <img src="https://github.com/risan4841/img/blob/master/transactionallogs.png?raw=true" width=250/>
-- MAGIC 
-- MAGIC You can query snapshots of your tables by:
-- MAGIC 1. **Version number**, or
-- MAGIC 2. **Timestamp.**
-- MAGIC 
-- MAGIC using Python, Scala, and/or SQL syntax; for these examples we will use the SQL syntax.  
-- MAGIC 
-- MAGIC For more information, refer to the [docs](https://docs.delta.io/latest/delta-utility.html#history), or [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

-- COMMAND ----------

DESCRIBE HISTORY iot_stream_silver;

-- COMMAND ----------

SELECT * FROM iot_stream_silver 
VERSION AS OF 1 
WHERE id = 300000;

-- COMMAND ----------

RESTORE iot_stream_silver
VERSION AS OF 1;

-- COMMAND ----------

DESCRIBE HISTORY iot_stream_silver;

-- COMMAND ----------

-- MAGIC %md ### Table Optimization
-- MAGIC 
-- MAGIC Z-Ordering is a method used to co-locate related information to use for data skipping. This can dramatically reduce the amount of data that needs to be read-in.

-- COMMAND ----------

OPTIMIZE iot_stream_silver
ZORDER BY id;

-- COMMAND ----------

-- MAGIC %md Next, are going to create business-level aggregations: <a href="$./3_Silver_to_Gold">`Silver to Gold`</a>

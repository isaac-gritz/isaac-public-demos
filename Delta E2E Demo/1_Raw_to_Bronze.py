# Databricks notebook source
# MAGIC %md # Ingestion: Raw Data to Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

s3_bucket = '<s3_bucket_name>'
database = '<database_name>'

input_data_path = "/mnt/" + s3_bucket + "/iot_stream/"
chkpt_path = "/mnt/" + s3_bucket + "/iot_stream_chkpts/"
schema_location = "/mnt/" + s3_bucket + "/schema_location/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS <database_name>;
# MAGIC USE <database_name>;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS <database_name>.iot_stream_bronze;
# MAGIC DROP TABLE IF EXISTS <database_name>.iot_devices;

# COMMAND ----------

# clean up the workspace
dbutils.fs.rm(input_data_path, recurse=True)
dbutils.fs.rm(chkpt_path, recurse=True)
dbutils.fs.rm(schema_location, recurse=True)

# COMMAND ----------

# copy first JSON file from IoT dataset to S3 bucket to use for COPY INTO
dbutils.fs.cp("/databricks-datasets/iot-stream/data-device/part-00000.json.gz", "/mnt/isaac/iot-stream/part-00000.json.gz", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- configurations for schema inference and auto-optimize
# MAGIC SET spark.databricks.cloudFiles.schemaInference.enabled = true;
# MAGIC SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Getting your data into Delta Lake with Auto Loader and COPY INTO
# MAGIC Incrementally and efficiently load new data files into Delta Lake tables as soon as they arrive in your data lake (S3/Azure Data Lake/Google Cloud Storage).
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-data-ingestion.png" width=1000/>

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> SQL `COPY INTO` command
# MAGIC Retriable, idempotent, simple.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creates Delta Lake Table and infers schema based on files
# MAGIC CREATE TABLE iot_devices
# MAGIC USING DELTA AS SELECT * 
# MAGIC FROM json.`/mnt/<s3_bucket_name>/iot-stream/` 
# MAGIC WHERE 2=1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COPY INTO from JSON files into Delta Lake Table
# MAGIC COPY INTO iot_devices
# MAGIC FROM "/mnt/<s3_bucket_name>/iot-stream/"
# MAGIC FILEFORMAT = JSON

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM iot_devices

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/autoloader.png">
# MAGIC 
# MAGIC Background
# MAGIC **Common ETL Workload:** Incrementally processing new data as it lands on a cloud blob store and making it ready for analytics. Ex: IoT data streams
# MAGIC 
# MAGIC **Out-of-the-box Solution:** 
# MAGIC * The naive file-based streaming source (Azure | AWS) identifies new files by repeatedly listing the cloud directory and tracking what files have been seen. 
# MAGIC * Both **cost** and **latency** can add up quickly as more and more files get added to a directory due to repeated listing of files
# MAGIC 
# MAGIC <br />
# MAGIC 
# MAGIC **Typical Workarounds...** 
# MAGIC * **Schedule & Batch Process:** Though data is arriving every few minutes, you batch the data together in a directory and then process them in a schedule. Using day or hour based partition directories is a common technique. This lengthens the SLA for making the data available to downstream consumers. Leads to high end-to-end data latencies.
# MAGIC * **Manual DevOps Approach:** To keep the SLA low, you can alternatively leverage cloud notification service and message queue service to notify when new files arrive to a message queue and then process the new files. This approach not only involves a manual setup process of required cloud services, but can also quickly become complex to manage when there are multiple ETL jobs that need to load data. Furthermore, re-processing existing files in a directory involves manually listing the files and handling them in addition to the cloud notification setup thereby adding more complexity to the setup.  
# MAGIC 
# MAGIC <br />
# MAGIC   
# MAGIC **The SOLUTION = AutoLoader**
# MAGIC * An optimized file source that overcomes all the above limitations and provides a seamless way for data teams to load the raw data at low cost and latency with minimal DevOps effort. 
# MAGIC * Just provide a source directory path and start a streaming job. 
# MAGIC * The new structured streaming source, called “cloudFiles”, will automatically set up file notification services that subscribe file events from the input directory and process new files as they arrive, with the option of also processing existing files in that directory.
# MAGIC 
# MAGIC **Benefits**
# MAGIC * **No file state management:** The source incrementally processes new files as they land on cloud storage. You don’t need to manage any state information on what files arrived.
# MAGIC * **Scalable:** The source will efficiently track the new files arriving by leveraging cloud services and RocksDB without having to list all the files in a directory. This approach is scalable even with millions of files in a directory.
# MAGIC * **Easy to use:** The source will automatically set up notification and message queue services required for incrementally processing the files. No setup needed on your side.

# COMMAND ----------

# MAGIC %md Run <a href="$./0_Data_Generator">`O_Data_Generator`</a> notebook to kicks off writes to the target directory every several seconds that we will use to demonstrate Auto Loader.

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Enforcement to protect data quality
# MAGIC 
# MAGIC **Schema enforcement helps keep our tables clean and tidy so that we can trust the data we have stored in Delta Lake.** Writes to tables that do not match the schema of the table are blocked with schema enforcement. See more information about how it works [here](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html).

# COMMAND ----------

# MAGIC %md ##  ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Use Schema Evolution to add new columns to schema
# MAGIC 
# MAGIC If we *want* to update our Delta Lake table to match this data source's schema, we can do so using schema evolution. Simply add the following to the Spark write command: `.option("mergeSchema", "true")`

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", schema_location)
      .load(input_data_path))

(df.writeStream.format("delta")
 .option("checkpointLocation", chkpt_path)
 .option("mergeSchema", "true")
 .table("<database_name>.iot_stream_bronze"))

# COMMAND ----------

display(df.selectExpr("COUNT(*) AS record_count"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY iot_stream_bronze

# COMMAND ----------

# MAGIC %md ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Auto Loader with `triggerOnce`

# COMMAND ----------

# MAGIC %md
# MAGIC Autoloader still keeps tracks of files even when there is no active cluster running. Waits to process until code is run (manually or scheduled job)

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", schema_location)
      .load(input_data_path))

(df.writeStream.format("delta")
   .trigger(once=True)
   .option("checkpointLocation", chkpt_path)
   .option("mergeSchema", "true")
   .table("iot_stream_bronze"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*) FROM iot_stream_bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY iot_stream_bronze

# COMMAND ----------

# MAGIC %md Next, we merge only the new records and updates into a silver table: <a href="$./2_Bronze_to_Silver">`Bronze to Silver`</a>

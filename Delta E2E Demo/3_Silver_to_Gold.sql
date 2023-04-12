-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Business Level Aggregations: Silver to Gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup

-- COMMAND ----------

-- configurations for auto-optimize
SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

USE <database_name>;

CREATE OR REPLACE TABLE iot_stream_gold
USING DELTA
PARTITIONED BY (year, month)
SELECT sum(calories_burnt) calories_burnt, device_id, id, sum(miles_walked) miles_walked, sum(num_steps) num_steps, user_id, YEAR(timestamp) year, MONTH(timestamp) month
FROM iot_stream_silver
GROUP BY year, month, device_id, id, user_id

-- COMMAND ----------

SELECT * FROM iot_stream_gold

-- COMMAND ----------

-- MAGIC %md Lastly, we are going to walk you through how to perform <a href="$./Delta_Optimization">`Delta Lake optimizations`</a>, [`schedule ETL jobs`](/#joblist), and [`perform ad-hoc SQL analytics`](/sql)

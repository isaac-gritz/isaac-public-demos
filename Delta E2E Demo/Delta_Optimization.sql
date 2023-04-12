-- Databricks notebook source
-- MAGIC %md # Delta Lake Optimizations
-- MAGIC 
-- MAGIC Run these optimizations as a daily job to ensure optimal performance with your Delta Lake Tables

-- COMMAND ----------

USE <database_name>;

OPTIMIZE iot_stream_silver
ZORDER BY id;

-- COMMAND ----------

OPTIMIZE iot_stream_gold
ZORDER BY id;

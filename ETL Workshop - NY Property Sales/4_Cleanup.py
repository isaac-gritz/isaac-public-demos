# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup and wipe all data

# COMMAND ----------

# MAGIC %md ### Setup
# MAGIC 
# MAGIC Replace the below parameter with the database name you plan to use

# COMMAND ----------

database = 'isaac'

# COMMAND ----------

spark.sql('USE {0};'.format(database))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS ny_bronze;
# MAGIC DROP TABLE IF EXISTS ny_silver;
# MAGIC DROP TABLE IF EXISTS ny_gold;
# MAGIC DROP TABLE IF EXISTS irs;

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/tmp/realestate/bronze

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/tmp/realestate/silver

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/tmp/realestate/gold

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/tmp/realestate/irs

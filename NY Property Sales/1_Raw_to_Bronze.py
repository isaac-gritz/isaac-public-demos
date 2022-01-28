# Databricks notebook source
# MAGIC %md # Ingestion: Raw Data to Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

# COMMAND ----------

# MAGIC %md
# MAGIC ###The Data
# MAGIC We'll be using a public domain [dataset from Kaggle.](https://kaggle.com/new-york-city/nyc-property-sales)
# MAGIC 
# MAGIC It contains information on all property sales in New York City from September 2016 to September 2017.
# MAGIC 
# MAGIC We're going to process this data then join it to some tax data from the IRS.
# MAGIC 
# MAGIC Download this data and upload it by going to `Data` > `DBFS` > `Upload` and then specify a folder path in the FileStore folder.

# COMMAND ----------

# MAGIC %md ### Setup
# MAGIC 
# MAGIC Replace the below parameters with the database name you plan to use and the location you uploaded the dataset set

# COMMAND ----------

database = 'isaac'

input_data_path = "/FileStore/etl-workshop/nyc_rolling_sales.csv"

# COMMAND ----------

# Create database if it does not exist
spark.sql('CREATE DATABASE IF NOT EXISTS {0};'.format(database))

# Use the selected database
spark.sql('USE {0};'.format(database))

# COMMAND ----------

ny_property_sales = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(input_data_path) \
    .drop("_c0") # drop number row

display(ny_property_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC Clean up column names and save as a BRONZE delta table

# COMMAND ----------

from pyspark.sql.types import *

# update column names
for col in ny_property_sales.columns:
  ny_property_sales = ny_property_sales.withColumnRenamed(col, col.replace(" ", "_"))  
ny_property_sales = ny_property_sales.withColumnRenamed("EASE-MENT", "EASEMENT")

# change sale_price column type to int
ny_property_sales = ny_property_sales.withColumn("SALE_PRICE", ny_property_sales["SALE_PRICE"].cast(IntegerType()))

# save as a BRONZE delta lake table using overwrite mode
ny_property_sales.write.format("delta").mode("overwrite").save("dbfs:/tmp/realestate/bronze")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/tmp/realestate/bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ny_bronze 
# MAGIC USING DELTA LOCATION 'dbfs:/tmp/realestate/bronze'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM ny_bronze
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md Next, we are going to clean up the data into into a silver table: <a href="$./2_Bronze_to_Silver">`Bronze to Silver`</a>

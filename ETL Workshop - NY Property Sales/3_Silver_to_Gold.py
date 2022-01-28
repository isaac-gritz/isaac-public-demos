# Databricks notebook source
# MAGIC %md
# MAGIC # Business Level Aggregations: Silver to Gold

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Gold.png" >

# COMMAND ----------

# MAGIC %md ### Setup
# MAGIC 
# MAGIC Replace the below parameter with the database name you plan to use

# COMMAND ----------

database = '<database-name>'

# COMMAND ----------

spark.sql('USE {0};'.format(database))

# COMMAND ----------

# MAGIC %md
# MAGIC Read in the SILVER delta table as a Spark Dataframe

# COMMAND ----------

ny_silver = spark.table("ny_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining with Other Datasets - IRS Data
# MAGIC We have a tax dataset from the IRS that includes the number of tax returns submitted per income bracket per zip code. It would be interesting to compare incomes in a zip code to the property types and values from the NYC dataset.

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/data.gov/irs_zip_code_data/

# COMMAND ----------

irs_data = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/databricks-datasets/data.gov/irs_zip_code_data/data-001/2013_soi_zipcode_agi.csv")

display(irs_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC I'm going to filter the IRS data down to only rows that correspond to NYC zip codes, to shrink down the data volume a bit.
# MAGIC 
# MAGIC The last thing I'm going to do before joining this table to my main NYC table is to rename some coded columns and replace the codes in the agi_stub column with the actual values. From the README I know that N1 is the number of returns, and the income codes for agi_stub are:
# MAGIC * 1 = $1 under $25,000
# MAGIC * 2 = $25,000 under $50,000
# MAGIC * 3 = $50,000 under $75,000
# MAGIC * 4 = $75,000 under $100,000
# MAGIC * 5 = $100,000 under $200,000
# MAGIC * 6 = $200,000 or more

# COMMAND ----------

from pyspark.sql.functions import *
from itertools import chain

ny_zips = ny_silver.select("ZIP_CODE").distinct().rdd.flatMap(lambda x: x).collect()
irs_data = irs_data[(irs_data['zipcode'].isin(ny_zips)) & (irs_data['zipcode'] != 0)]

irs_data_cleaned = irs_data.withColumnRenamed("agi_stub", "ADJ_GROSS_INCOME").withColumnRenamed("N1", "NUM_RETURNS")

income_dict = {1:'1 to 25,000', 2:'25,000 to 50,000', 3:'50,000 to 75,000', 4:'75,000 to 100,000', 5:'100,000 to 200,000', 6:'200,000+'}

mapping_expr = create_map([lit(x) for x in chain(*income_dict.items())])

irs_data_cleaned = irs_data_cleaned.withColumn('ADJ_GROSS_INCOME', mapping_expr[irs_data_cleaned['ADJ_GROSS_INCOME']])
irs_data_cleaned.createOrReplaceTempView("irs_data")

display(irs_data_cleaned)

# COMMAND ----------

irs_data_cleaned.write.format("delta").mode("overwrite").save("dbfs:/tmp/realestate/irs")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS irs 
# MAGIC USING DELTA LOCATION 'dbfs:/tmp/realestate/irs'

# COMMAND ----------

# MAGIC %md
# MAGIC To finish things up, let's join the IRS income data to the NYC building sale data. First I'm going to use Delta's delete capabilities to remove rows with invalid zip codes.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ny_silver WHERE ZIP_CODE = 0;
# MAGIC SELECT * FROM ny_silver WHERE ZIP_CODE = 0

# COMMAND ----------

ny_gold = ny_silver.join(irs_data_cleaned, how = 'inner', on = ny_silver['ZIP_CODE'] == irs_data_cleaned['zipcode'])\
                   .drop("zipcode") \
                   .select("ZIP_CODE", "ADDRESS", "SALE_PRICE", "NUM_RETURNS", "ADJ_GROSS_INCOME")

# COMMAND ----------

ny_gold.write.format("delta").mode("overwrite").save("dbfs:/tmp/realestate/gold") 

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS ny_gold USING DELTA LOCATION 'dbfs:/tmp/realestate/gold'")
ny_gold = spark.table("ny_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Orchestration with Databricks Multi-Task Job Scheduler
# MAGIC <img src= https://databricks.com/wp-content/uploads/2021/07/task-orch-blog-img-1.png width="500">
# MAGIC 
# MAGIC Next, we are going to schedule these 3 notebooks as an ETL pipeline using the [`Multi-Task Job Scheduler`](/#joblist)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Databricks SQL
# MAGIC <img src = https://databricks.com/wp-content/uploads/2021/09/HH-SQL-graphicImage-first-class.png width = "1000">
# MAGIC 
# MAGIC Lastly, we are going to [`perform ad-hoc SQL analytics`](/sql) with Databricks SQL

# COMMAND ----------

# MAGIC %md Once we are done, we can clean up the datasets and tables using our <a href="$./4_Cleanup">`Cleanup Script`</a>

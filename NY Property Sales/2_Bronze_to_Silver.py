# Databricks notebook source
# MAGIC %md # Data Cleaning: Bronze to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Silver.png" >

# COMMAND ----------

# MAGIC %md ### Setup
# MAGIC 
# MAGIC Replace the below parameter with the database name you plan to use

# COMMAND ----------

database = 'isaac'

# COMMAND ----------

spark.sql('USE {0};'.format(database))

# COMMAND ----------

# MAGIC %md
# MAGIC Read in the BRONZE delta table as a Spark Dataframe

# COMMAND ----------

ny_bronze = spark.table("ny_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleansing
# MAGIC There's some empty columns and suspicious data, so let's fix up our raw dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC * The EASEMENT column is empty, so remove it entirely.
# MAGIC * The BOROUGH column is encoded with numerical values representing different boroughs of the city, so I'll replace them with the names.
# MAGIC  * Manhattan = 1
# MAGIC  * Bronx = 2 
# MAGIC  * Brooklyn = 3
# MAGIC  * Queens = 4
# MAGIC  * Staten Island = 5

# COMMAND ----------

from itertools import chain
from pyspark.sql.functions import create_map, lit

# drop easement column
ny_silver = ny_bronze.drop("EASEMENT")

# map borough column to name
borough_dict = {1:'Manhattan', 2:'Bronx', 3:'Brooklyn', 4:'Queens', 5:'Staten Island'}
mapping_expr = create_map([lit(x) for x in chain(*borough_dict.items())])

# replace borough column with the mapped column names
ny_silver = ny_silver.withColumn('BOROUGH', mapping_expr[ny_silver['BOROUGH']])

display(ny_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Click on the Data Profile tab. We can see that the SALE_PRICE column has a lot of null values.

# COMMAND ----------

# MAGIC %md
# MAGIC It turns out that sales with a SALE_PRICE of $0 are a deed transfer (e.g. parent to child). For the sake of the demo, let's remove those rows and all rows with a null sale price.

# COMMAND ----------

ny_silver = ny_silver.na.fill(0, subset=['SALE_PRICE'])\
                     .filter(ny_silver["SALE_PRICE"] > 0)

display(ny_silver)

# COMMAND ----------

# write SILVER Delta Lake table using overwrite mode
ny_silver.write.format("delta").mode("overwrite").save("dbfs:/tmp/realestate/silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ny_silver
# MAGIC USING DELTA LOCATION 'dbfs:/tmp/realestate/silver';

# COMMAND ----------

# MAGIC %md Next, are going to create business-level aggregations: <a href="$./3_Silver_to_Gold">`Silver to Gold`</a>

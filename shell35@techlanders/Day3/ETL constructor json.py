# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/constructors.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = df.withColumn ("IngestionDate",current_timestamp())

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.saveAsTable("jathin.constructors")

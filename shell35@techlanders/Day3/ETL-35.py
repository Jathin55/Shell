# Databricks notebook source
dbutils.fs.ls("dbfs:/mnt/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/

# COMMAND ----------

# CSV
Method 1: Using PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read the csv (Extract)

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/circuits.csv",header=True, inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2: Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame Functions
# MAGIC - select
# MAGIC - alias
# MAGIC - withColumnRenamed
# MAGIC
# MAGIC
# MAGIC ### Functions
# MAGIC - col

# COMMAND ----------

Spark is lazy evaluated: 

# COMMAND ----------

df.display()

# COMMAND ----------

select circuitId, location from df

# COMMAND ----------

df.select("circuitId", "location").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select circuitID as circuit_id, * from tablename

# COMMAND ----------

df1=df.select(col("circuitId").alias("circuit_id"),"*")

# COMMAND ----------

df.display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumnRenamed("circuitid","circuit_id").display()

# COMMAND ----------

df1 = df.withColumn ("IngestionDate",current_timestamp()).drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.parquet("dbfs:/mnt/adlsshelldatabricks/raw/processed/jathin/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database jathin

# COMMAND ----------

df1.write.saveAsTable("jathin.circuit")

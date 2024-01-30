# Databricks notebook source
inputfiles = "dbfs:/mnt/adlsshelldatabricks/raw/inputstreamfiles/json/"
output="dbfs:/mnt/adlsshelldatabricks/raw/outputstream"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation",f"{output}/jathin/json/schema")
 .option("cloudFiles.inferColumnTypes",True)
 .option("cloudFiles.schemaEvolutionMode","rescue")
 .load(f'{inputfiles}')
 .writeStream
 .option("checkpointLocation",f"{output}/jathin/json/checkpoint")
 .option("path",f"{output}/jathin/json/autoloader")
 .option("mergeSchema",True)
 .table("jathin.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jathin.autoloader

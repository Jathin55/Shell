# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

users_schema= StructType([StructField("Id", IntegerType()),
                          StructField("Name", StringType()),
                          StructField("Gender", StringType()),
                          StructField("Salary", IntegerType()),
                          StructField("Country", StringType()),
                          StructField("Date", StringType()),
                          ])

# COMMAND ----------

input="dbfs:/mnt/adlsshelldatabricks/raw/inputstreamfiles/csv/"

# COMMAND ----------

checkpoint="dbfs:/mnt/adlsshelldatabricks/raw/outputstream"

# COMMAND ----------

(spark.
 readStream
 .option("header",True)
 .schema(users_schema)
 .csv(f"{input}")
 .withColumn("ingestiondate",current_timestamp())
    .writeStream
    .option("checkpointLocation",f"{checkpoint}/jathin/checkpoint")
    .table("jathin.firststream"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jathin.firststream

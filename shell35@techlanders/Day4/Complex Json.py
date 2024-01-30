# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.option("multiline",True).json("dbfs:/mnt/adlsshelldatabricks/raw/jsonfiles/complexjson.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df1= df.withColumn("batters",explode("batters.batter"))\
    .withColumn("batters_id",col("batters.id"))\
        .withColumn("batters_type",col("batters.type"))\
            .drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
    .drop("topping")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.saveAsTable("jathin.complexjson")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jathin.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail jathin.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended jathin.complexjson

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history jathin.complexjson

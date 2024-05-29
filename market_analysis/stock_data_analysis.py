# Databricks notebook source
from pyspark.sql.functions import max

df = spark.read.format("csv") \
    .option("header", "true") \
    .load("/Volumes/datasets/default/datasets/tesla_data_full.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

import pyspark.sql.functions as f

df.agg(
    f.max_by("Date", "High").alias("Date"),
    f.max("High").alias("High"),
    f.max_by("Low", "High").alias("Low")
).display()


# COMMAND ----------



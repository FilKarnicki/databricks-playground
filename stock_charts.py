# Databricks notebook source
import plotly.express as px

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/datasets/default/datasets/tesla_data_2020.csv")

df.display()

# COMMAND ----------

dfp = df.toPandas()

px.line(dfp, x="Date", y="Volume", title="TSLA trading volumes").show()

# COMMAND ----------

px.line(dfp, x="Date", y="Adj Close", title="TSLA close prices").show()

# COMMAND ----------



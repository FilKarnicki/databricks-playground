# Databricks notebook source
# List parquet files for table tesla_data_full
parquet_files = spark.sql("SHOW TABLE EXTENDED LIKE 'databricks_workspace.default.tesla_data_full'")

parquet_files.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM default.tesla_data_full;
# MAGIC SHOW VOLUMES

# COMMAND ----------

tesla = spark.read.format("delta").load(f"/catalogs/63784dd4-daf6-4312-9d43-3a6a1d0070d9/tables/9ce9d8e9-da4a-4fac-a79a-a76fffd2c937")

# COMMAND ----------

# MAGIC %sql
# MAGIC select Date, "Adj Close", Volume
# MAGIC FROM tesla_data_full
# MAGIC ORDER BY Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe detail default.tesla_data_full

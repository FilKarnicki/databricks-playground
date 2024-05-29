# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS NutritionData;
# MAGIC copy into NutritionData
# MAGIC FROM (
# MAGIC   SELECT Category, Item, Serving_Size, Calories::int, Sugars::int, Protein::int
# MAGIC   FROM "/Volumes/datasets/default/datasets/menu_data_0*.csv"
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS("header" = "true")
# MAGIC COPY_OPTIONS("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from NutritionData

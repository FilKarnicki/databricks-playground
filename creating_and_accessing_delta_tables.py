# Databricks notebook source
menu_data = spark.read.format("csv").load(
    f"/Volumes/datasets/default/datasets/menu_data.csv",
    header="true",
    inferSchema=True,
)

# COMMAND ----------

menu_data.display()

# COMMAND ----------

menu_data.write.format('delta').saveAsTable('menu_nutrition_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM menu_nutrition_data

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE TABLE menu_nutrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC   DESCRIBE DETAIL menu_nutrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Category, Item, Serving_Size, Sugars, Protein
# MAGIC FROM menu_nutrition_data
# MAGIC WHERE Protein > 20
# MAGIC   AND Sugars < 10
# MAGIC ORDER BY Protein DESC

# COMMAND ----------

from pyspark.sql.functions import col

menu_data.select("Item", "Protein").where("Protein > 20").orderBy(col("Protein").desc()).display()

# COMMAND ----------

from pyspark.sql.functions import avg, round, col

menu_data.groupBy("Category").agg(round(avg("Protein"), 2).alias("avg_protein")).orderBy(col("avg_protein").desc()).display()

# COMMAND ----------



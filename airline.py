# Databricks notebook source
passengers = spark.read.format("csv").load(f"/Volumes/datasets/default/datasets/airline_passenger.csv", header=True, inferSchema=True)

# COMMAND ----------

colsMap = {col:col.replace(" ", "_") for col in passengers.columns}
print(colsMap)
passengers = passengers.withColumnRenamed("c0", "_c0").withColumnsRenamed(colsMap)

# COMMAND ----------

passengers.write.format("delta").saveAsTable("ap")

# COMMAND ----------

from pyspark.sql.functions import col

passengers.select("Gender", "Satisfaction").filter(col("Satisfaction") == "satisfied").display()

# COMMAND ----------

from pyspark.sql.functions import avg

passengers.select("Customer_Type", "Flight_Distance"). groupBy("Customer_Type").agg(avg(col("Flight_Distance"))).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ap

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC describe detail ap

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Class, round(avg(Age), 2) AS avg_age, percentile_approx(Flight_Distance, 0.5) as median_flight_distance
# MAGIC FROM ap
# MAGIC GROUP BY Class
# MAGIC HAVING median_flight_distance > 5
# MAGIC ORDER BY Class

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE ap
# MAGIC SET Class = "Business" 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY ap

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*), Class
# MAGIC from ap
# MAGIC version as of 0
# MAGIC group by Class

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES ap

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ap
# MAGIC SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 100 hours')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TBLPROPERTIES ap

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC VACUUM ap

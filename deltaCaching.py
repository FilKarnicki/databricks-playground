# Databricks notebook source
# MAGIC %fs
# MAGIC
# MAGIC ls /databricks-datasets/asa/airlines/

# COMMAND ----------

fd = spark.read.format("csv").load("/databricks-datasets/asa/airlines/2005.csv", inferSchema=True, header=True)

# COMMAND ----------

fd.display()

# COMMAND ----------

fd.write.format("delta").saveAsTable("fd")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL fd

# COMMAND ----------

spark.conf.get("spark.databricks.io.cache.enabled")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT Origin, Dest, DepTime, ArrTime, AirTime
# MAGIC FROM fd
# MAGIC WHERE UniqueCarrier = "UA"
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Origin
# MAGIC FROM fd
# MAGIC WHERE UniqueCarrier = "UA"
# MAGIC   AND AirTime >= 120

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select UniqueCarrier, Origin, Dest, FlightNum, ActualElapsedTime, ArrDelay, DepDelay
# MAGIC from fd
# MAGIC where Origin in ("JFK", "LGA", "EWR")

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select UniqueCarrier, Origin, Dest, FlightNum, ActualElapsedTime, ArrDelay, DepDelay
# MAGIC from fd
# MAGIC where Origin = "JFK"

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Origin, Dest, DepTime, ArrTime, AirTime
# MAGIC FROM fd
# MAGIC WHERE UniqueCarrier = "UA"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT UniqueCarrier, COUNT(*) AS FlightCount
# MAGIC FROM fd
# MAGIC GROUP BY UniqueCarrier
# MAGIC ORDER BY FlightCount DESC

# COMMAND ----------

fd.printSchema()

# COMMAND ----------

fd.write.format("delta").mode("overwrite").option("overrideSchema", "true").partitionBy("UniqueCarrier").saveAsTable("fd")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe detail fd

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE fd

# COMMAND ----------

fd.write.format("delta").saveAsTable("flights_delta_orig")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE flights_delta_orig ZORDER BY (UniqueCarrier)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT Origin, Dest, DepTime, ArrTime, AirTime
# MAGIC FROM flights_delta_orig
# MAGIC WHERE UniqueCarrier = "UA"

# COMMAND ----------



# Databricks notebook source
import urllib

user_creds = spark.read.csv("/Volumes/datasets/default/datasets/databricks-user_accessKeys.csv", header=True, inferSchema=True)
user_creds.display()

# COMMAND ----------

access_key = user_creds.select("Access key ID").collect()[0]["Access key ID"]
secret_key = user_creds.select("Secret access key").collect()[0]["Secret access key"]

# COMMAND ----------

print(access_key, secret_key)

# COMMAND ----------

code_secret_key = urllib.parse.quote(secret_key, "")

# COMMAND ----------

#s3://databricks-user-fil/databricks_files/
aws_bucket = "databricks-user-fil"
mount_name = "/mnt/bucket2"
sourceURI = "s3n://{0}:{1}@{2}".format(access_key, code_secret_key, aws_bucket)
print(sourceURI)
dbutils.fs.mount(sourceURI, mount_name)

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls "/mnt/bucket2/databricks_files"

# COMMAND ----------

menu_data = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
        .option("inferSchema", "true") \
            .option("cloudFiles.schemaLocation", "/Volumes/datasets/default/datasets/") \
                .load("/mnt/bucket2/databricks_files/*")

menu_data.display()

# COMMAND ----------

from pyspark.sql.functions import col
low_calorie_data = menu_data.filter(col("Calories") < 400)
low_calorie_data.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/delta/events/_checkpoints/low_cal") \
    .toTable("low_cal")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from low_cal

# COMMAND ----------



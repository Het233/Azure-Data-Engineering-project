# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./pipeline_config

# COMMAND ----------

# Bronze Container
bronze_path = BRONZE_ROOT

# Silver Container
silver_path = SILVER_ROOT

# COMMAND ----------

spark.conf.set("fs.azure.account.key.<storage-account>.dfs.core.windows.net", "<storage-account-key>")

files = dbutils.fs.ls(bronze_path)
for file in files:
    print(file.path)

# COMMAND ----------

latest_file = max(files, key=lambda x: x.modificationTime)
print(latest_file)

# COMMAND ----------

bronze_df = spark.read.parquet(latest_file.path)

# COMMAND ----------


silver_df = (
    bronze_df
    .dropDuplicates(["OrderID"])

    .filter(col("OrderID").isNotNull())

    .withColumn("OrderDate", to_date(col("OrderDate"), "M/d/yyyy"))

    .filter(col("OrderDate").isNotNull())

    .withColumn("CustomerName", trim(col("CustomerName")))

    .withColumn("CustomerEmail", trim(col("CustomerEmail")))

    .withColumn("Country", trim(col("Country")))

    .withColumn("ProductCategory", trim(col("ProductCategory")))

    .withColumn("ProductName", trim(col("ProductName")))

    .withColumn("SalesRegion", trim(col("SalesRegion")))

    .withColumn("CustomerEmail", lower(col("CustomerEmail")))

    .withColumn("Quantity", col("Quantity").cast("int"))

    .withColumn("UnitPrice", col("UnitPrice").cast("double"))

    .withColumn("TotalPrice", col("TotalPrice").cast("double"))

    .filter(col("Quantity") > 0)

    .filter(col("UnitPrice") > 0)

    .filter(col("TotalPrice") > 0)

    .fillna({
        "CustomerName": "Unknown",
        "Country": "Unknown",
        "ProductCategory": "Unknown",
        "ProductName": "Unknown",
        "SalesRegion": "Unknown"
    })
)

# COMMAND ----------

silver_df.write \
    .mode("append") \
    .parquet(silver_path)

# COMMAND ----------


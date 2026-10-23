# Databricks notebook source
from pyspark.sql.functions import col, current_date
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./pipeline_config

# COMMAND ----------

spark.conf.set("<storage-account>.dfs.core.windows.net", storage_account_key)

silver_path = SILVER_ROOT
gold_path = GOLD_SALES_REGION_PATH

# COMMAND ----------

all_files = dbutils.fs.ls(silver_path)

parquet_files = [
    f for f in all_files 
    if f.name.endswith(".parquet") and not f.name.startswith("_")
]

latest_file = max(parquet_files, key=lambda x: x.modificationTime)

latest_file_path = latest_file.path

print(latest_file_path)


# COMMAND ----------

silver_df = spark.read.parquet(latest_file_path)
incoming_regions_df = (
    silver_df
    .select("Country", "SalesRegion")
    .dropDuplicates(["Country", "SalesRegion"])
)

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_path):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{gold_path}` (
            RegionSK BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            Country STRING,
            SalesRegion STRING,
            InsertedAt DATE
        )
        USING DELTA
    """)
    (incoming_regions_df
     .withColumn("InsertedAt", current_date())
     .write
     .format("delta")
     .mode("append")
     .save(gold_path))

else:
    gold_table = DeltaTable.forPath(spark, gold_path)
    gold_table.alias("g").merge(
        incoming_regions_df.alias("s"),
        "g.Country = s.Country AND g.SalesRegion = s.SalesRegion"
    ).whenNotMatchedInsert(
        values = {
            "Country": "s.Country",
            "SalesRegion": "s.SalesRegion",
            "InsertedAt": "current_date()"
        }
    ).execute()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


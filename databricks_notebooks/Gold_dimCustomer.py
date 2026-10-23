# Databricks notebook source
from pyspark.sql.functions import col,lit,expr,current_date
from delta.tables import DeltaTable,delta

# COMMAND ----------

# MAGIC %run ./pipeline_config

# COMMAND ----------

spark.conf.set("<storage-account>.dfs.core.windows.net", storage_account_key)

silver_path = SILVER_ROOT
gold_path = GOLD_CUSTOMER_PATH

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

silver_df = (
    spark.read.parquet(latest_file_path)
    .select("CustomerID", "CustomerName", "CustomerEmail")
    .dropDuplicates(["CustomerID"])
)

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_path):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{gold_path}` (
            CustomerSK BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            CustomerID INT,
            CustomerName STRING,
            CustomerEmail STRING,
            StartDate DATE,
            EndDate DATE,
            IsCurrent BOOLEAN
        )
        USING DELTA
    """)
    # Inserting Day 1 data
    (silver_df
     .withColumn("StartDate", current_date())
     .withColumn("EndDate", lit(None).cast("date"))
     .withColumn("IsCurrent", lit(True))
     .write
     .format("delta")
     .mode("append")
     .save(gold_path))
    
else:
    gold_table = DeltaTable.forPath(spark, GOLD_CUSTOMER_PATH)
    updates_df = silver_df.join(
        gold_table.toDF().filter("IsCurrent = true"), 
        "CustomerID"
    ).filter(
        (silver_df.CustomerName != gold_table.CustomerName) | 
        (silver_df.CustomerEmail != gold_table.CustomerEmail)
    ).select(silver_df["*"])
    staged_updates = updates_df.select(expr("NULL as mergeKey"), "*").union(
        silver_df.select(col("CustomerID").alias("mergeKey"), "*")
    )
    gold_table.alias("g").merge(
        staged_updates.alias("s"),
        "g.CustomerID = s.mergeKey AND g.IsCurrent = true"
    ).whenMatchedUpdate(
        condition = "(g.CustomerName != s.CustomerName) OR (g.CustomerEmail != s.CustomerEmail)",
        set = {
            "IsCurrent": "false",
            "EndDate": "current_date()"
        }
    ).whenNotMatchedInsert(
        values = {
            "CustomerID": "s.CustomerID",
            "CustomerName": "s.CustomerName",
            "CustomerEmail": "s.CustomerEmail",
            "StartDate": "current_date()", 
            "EndDate": "NULL",            
            "IsCurrent": "true"
        }
    ).execute()

# COMMAND ----------

# df = spark.read.format("delta").load(gold_path)
# df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


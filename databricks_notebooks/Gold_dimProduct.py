# Databricks notebook source
from pyspark.sql.functions import col, current_date
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./pipeline_config

# COMMAND ----------

spark.conf.set("<storage-account>.dfs.core.windows.net", storage_account_key)

silver_path = SILVER_ROOT
gold_path = GOLD_PRODUCT_PATH

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
incoming_products_df = (
    silver_df
    .select("ProductID", "ProductCategory", "ProductName", "UnitPrice","Quantity")
    .dropDuplicates(["ProductID"])
)

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_path):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{gold_path}` (
            ProductSK BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            ProductID INT,
            ProductCategory STRING,
            ProductName STRING,
            UnitPrice DOUBLE,
            Quantity INT,
            UpdatedAt DATE
        )
        USING DELTA
    """)
    (incoming_products_df
     .withColumn("UpdatedAt", current_date())
     .write
     .format("delta")
     .mode("append")
     .save(gold_path))

else:
    gold_table = DeltaTable.forPath(spark, GOLD_PRODUCT_PATH)
    gold_table.alias("g").merge(
        incoming_products_df.alias("s"),
        "g.ProductID = s.ProductID"
    ).whenMatchedUpdate(
        condition = "(g.ProductName != s.ProductName) OR (g.ProductCategory != s.ProductCategory) OR (g.UnitPrice != s.UnitPrice) OR (g.Quantity != s.Quantity)",
        set = {
            "g.ProductCategory": "s.ProductCategory",
            "g.ProductName": "s.ProductName",
            "g.UnitPrice": "s.UnitPrice",
            "g.Quantity": "s.Quantity",
            "g.UpdatedAt": "current_date()"
        }
    ).whenNotMatchedInsert(
        values = {
            "ProductID": "s.ProductID",
            "ProductCategory": "s.ProductCategory",
            "ProductName": "s.ProductName",
            "UnitPrice": "s.UnitPrice",
            "Quantity": "s.Quantity",
            "UpdatedAt": "current_date()"
        }
    ).execute()

# COMMAND ----------

# df = spark.read.format("delta").load(gold_path)
# display(df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


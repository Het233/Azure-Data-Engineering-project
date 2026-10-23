# Databricks notebook source
from pyspark.sql.functions import col, current_date, expr
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./pipeline_config

# COMMAND ----------

spark.conf.set("fs.azure.account.key.<storage-account>.dfs.core.windows.net", storage_account_key)

silver_path = SILVER_ROOT
gold_path = GOLD_FACT_SALES_PATH

# COMMAND ----------

all_files = dbutils.fs.ls(silver_path)

parquet_files = [
    f for f in all_files 
    if f.name.endswith(".parquet") and not f.name.startswith("_")
]

latest_file = max(parquet_files, key=lambda x: x.modificationTime)

latest_file_path = latest_file.path

print(latest_file_path)
raw_sales_df = spark.read.parquet(latest_file_path)
incoming_sales_df = raw_sales_df.withColumn(
    "CleanOrderDate",
    expr("""
        try_to_date(OrderDate, 'M/d/yyyy')
    """)
)

# COMMAND ----------

dim_customer_lookup = spark.read.format("delta").load(GOLD_CUSTOMER_PATH).filter("IsCurrent = true").select("CustomerSK", "CustomerID")
dim_product_lookup = spark.read.format("delta").load(GOLD_PRODUCT_PATH).select("ProductSK", "ProductID")
dim_region_lookup = spark.read.format("delta").load(GOLD_SALES_REGION_PATH).select("RegionSK", "Country", "SalesRegion")
dim_date_lookup = spark.read.format("delta").load(GOLD_DATE_PATH).select("DateKey","OrderDate","OrderDateString")

staged_fact_df = (
    incoming_sales_df.alias("s")
    .join(dim_date_lookup.alias("d"), col("s.CleanOrderDate") == col("d.OrderDate"), "left")
    .join(dim_customer_lookup.alias("c"), col("s.CustomerID") == col("c.CustomerID"), "left")
    
    .join(dim_product_lookup.alias("p"), col("s.ProductID") == col("p.ProductID"), "left")
    .join(dim_region_lookup.alias("r"), (col("s.Country") == col("r.Country")) & (col("s.SalesRegion") == col("r.SalesRegion")), "left")
    .select(
        col("s.OrderID").cast("int"),
        col("d.DateKey"),
        col("c.CustomerSK"),
        col("p.ProductSK"),
        col("r.RegionSK"),         
        col("s.Quantity").cast("int"),
        col("s.UnitPrice").cast("double"),
        col("s.TotalPrice").cast("double"),
        current_date().alias("IngestedAt")
    )
)

# COMMAND ----------

if not DeltaTable.isDeltaTable(spark, gold_path):
    (staged_fact_df.write
     .format("delta")
     .mode("append")
     .save(gold_path))
     
    print("FactSales initialized successfully with the first batch!")

else:
    fact_table = DeltaTable.forPath(spark, gold_path)
    fact_table.alias("f").merge(
        staged_fact_df.alias("s"),
        "f.OrderID = s.OrderID"
    ).whenNotMatchedInsertAll().execute()
    
    print("Today's incremental sales successfully appended to FactSales!")

# COMMAND ----------

staged_fact_df.display()
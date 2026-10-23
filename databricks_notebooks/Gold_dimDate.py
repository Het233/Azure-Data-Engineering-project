# Databricks notebook source
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ./pipeline_config

# COMMAND ----------

spark.conf.set("<storage-account>.dfs.core.windows.net", storage_account_key)

# COMMAND ----------

gold_path = GOLD_DATE_PATH

# COMMAND ----------


start_date = "2020-01-01"
end_date = "2030-12-31"
date_range_df = spark.range(0, 1).select(
    expr(f"sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)").alias("GeneratedDate")
).select(expr("explode(GeneratedDate)").alias("OrderDate"))


final_date_df = (
    date_range_df
    .withColumn("DateKey", expr("cast(date_format(OrderDate, 'yyyyMMdd') as int)"))

    .withColumn("OrderDateString", expr("date_format(OrderDate, 'M/d/yyyy')"))
    
    .withColumn("Day", expr("dayofmonth(OrderDate)"))
    .withColumn("Month", expr("month(OrderDate)"))
    .withColumn("MonthName", expr("date_format(OrderDate, 'MMMM')"))
    .withColumn("Year", expr("year(OrderDate)"))
    .withColumn("DayOfWeek", expr("dayofweek(OrderDate)"))
    .withColumn("DayName", expr("date_format(OrderDate, 'EEEE')"))
    .withColumn("Quarter", expr("quarter(OrderDate)"))
    .select(
        "DateKey", "OrderDate", "OrderDateString", "Day", "Month", "MonthName", "Year", "DayOfWeek", "DayName", "Quarter"
    )
)

final_date_df.write.format("delta").mode("overwrite").save(gold_path)

# COMMAND ----------

# final_date_df.display()

# COMMAND ----------


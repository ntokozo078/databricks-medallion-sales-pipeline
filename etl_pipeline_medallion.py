# Databricks notebook source
# Read the table
raw_df = spark.read.table("superstore_dataset_2011_2015")

# Rename columns to remove spaces and invalid characters
new_columns = [
    col.replace(" ", "_").replace("-", "_")
    for col in raw_df.columns
]
raw_df = raw_df.toDF(*new_columns)

# Save as Delta table
raw_df.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")

print("Success! Bronze Table created from your existing UI upload.")

# COMMAND ----------

from pyspark.sql.functions import col, try_to_date

# 1. Read from Bronze
bronze_df = spark.read.table("bronze_sales")

# 2. Clean the Data
silver_df = (
    bronze_df
    .withColumn(
        "Order_Date_Clean",
        try_to_date(col("Order_Date"), "d/M/yyyy")
    )
    .filter(col("Order_ID").isNotNull())
    .dropDuplicates(["Order_ID"])
)

# 3. Quality Check
invalid_sales = silver_df.filter(col("Sales") < 0).count()
if invalid_sales > 0:
    print(f"WARNING: Found {invalid_sales} rows with negative sales!")

# 4. Write to Silver Table
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_sales")

print(f"Silver Table Created. Cleaned Rows: {silver_df.count()}")

# COMMAND ----------

from pyspark.sql.functions import sum, desc, round

# 1. Read from Silver
silver_df = spark.read.table("silver_sales")

# 2. Business Logic: Profit by Country
# We use "Total_Sales" and "Total_Profit" for the report
gold_df = silver_df.groupBy("Country", "Segment") \
  .agg(
      round(sum("Sales"), 2).alias("Total_Sales"),
      round(sum("Profit"), 2).alias("Total_Profit")
  ) \
  .orderBy(desc("Total_Profit"))

# 3. Write to Gold Table
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_sales_report")

# 4. Display the result
display(gold_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Delete data accidently (Simulating a mistake)
# MAGIC DELETE FROM silver_sales WHERE Country = 'United States';
# MAGIC
# MAGIC -- 2. Prove it's gone
# MAGIC SELECT count(*) as US_Rows_After_Delete FROM silver_sales WHERE Country = 'United States';
# MAGIC
# MAGIC -- 3. RESTORE (Time Travel) - Undoing the mistake
# MAGIC -- If you get a ConcurrentWriteException, wait for a few seconds and retry this command.
# MAGIC RESTORE TABLE silver_sales TO VERSION AS OF 0;
# MAGIC
# MAGIC -- 4. Prove it's back
# MAGIC SELECT count(*) as US_Rows_Restored FROM silver_sales WHERE Country = 'United States';
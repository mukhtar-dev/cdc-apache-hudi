import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HudiAggTable") \
    .getOrCreate()

# Define paths
agg_path = "/hudi_data/aggregated_orders"
cdc_path = "/hudi_data/retails_table"

# Define schema for aggregation table
schema = """
store_city STRING,
no_of_orders BIGINT
"""

# Define Hudi write options
hudi_options = {
    "hoodie.table.name": "hudi_aggregated_orders",
    "hoodie.datasource.write.recordkey.field": "store_city",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.precombine.field": "store_city",
    "hoodie.datasource.write.operation": "insert_overwrite",
}

# Create the aggregation table if it doesn't exist
if not os.path.exists(agg_path):
    agg_df = spark.createDataFrame([], schema)
    agg_df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(agg_path)

# Read CDC Data
cdc_df = spark.read.format("hudi").load(cdc_path)
cdc_df = cdc_df.filter(col("op").isNotNull()).sort("_hoodie_commit_time")

# Automatically determine last processed commit time
try:
    existing_agg = spark.read.format("hudi").load(agg_path)
    latest_commit_time = existing_agg.selectExpr("max(_hoodie_commit_time) as max_commit").first()["max_commit"]
except AnalysisException:
    latest_commit_time = None  # Table doesn't exist yet

# Filter only new changes if previous commit exists
if latest_commit_time:
    cdc_df = cdc_df.filter(col("_hoodie_commit_time") > lit(latest_commit_time))

cdc_df.show()

# Handle each type of op:
changes_df = cdc_df.withColumn(
    "store_city",
    when(col("op") == "d", col("before.store_city"))  # Use `before` for deletes
    .otherwise(col("after.store_city"))  # Use `after` for inserts/updates
).withColumn(
    "net_change",
    when((col("op") == "c") | (col("op") == "r"), 1)  # Insert: add 1
    .when(col("op") == "d", -1)  # Delete: subtract 1
    .when(
        (col("op") == "u") & (col("before.store_city") != col("after.store_city")),
        lit(0)  # No net change, but must handle this later
    )
    .otherwise(0)
)

changes_df.show()

# Handle updates where store_city changed
updated_rows_df = cdc_df.filter((col("op") == "u") & (col("before.store_city") != col("after.store_city")))

# Decrease count from old city
decrease_df = updated_rows_df.select(
    col("before.store_city").alias("store_city"),
    lit(-1).alias("net_change")
)

# Increase count in new city
increase_df = updated_rows_df.select(
    col("after.store_city").alias("store_city"),
    lit(1).alias("net_change")
)

# Combine all changes
final_changes_df = changes_df.select("store_city", "net_change") \
    .union(decrease_df) \
    .union(increase_df)

# Aggregate net changes per city
agg_updates = final_changes_df.groupBy("store_city") \
    .sum("net_change") \
    .withColumnRenamed("sum(net_change)", "delta_orders")

# Read the aggregated Hudi table
existing_agg = spark.read.format("hudi") \
    .load(agg_path) \
    .select("store_city", "no_of_orders")

# Join updates with current state
updated_agg = existing_agg.alias("agg") \
    .join(agg_updates.alias("delta"), on="store_city", how="outer") \
    .select(
        col("store_city"),
        (coalesce(col("agg.no_of_orders"), lit(0)) + coalesce(col("delta.delta_orders"), lit(0))).alias("no_of_orders")
    )

# Write back to Hudi
updated_agg.write.format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save(agg_path)

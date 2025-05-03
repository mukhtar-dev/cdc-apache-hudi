from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HudiReadExample") \
    .getOrCreate()

# Define paths
tableName = "aggregated_orders"
basePath = "file:///hudi_data/aggregated_orders"

# Read the Hudi table
retailsDF = spark.read.format("hudi").load(basePath)

# Show the contents
retailsDF.show()

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HudiReadExample") \
    .getOrCreate()

# Define paths
tableName = "retails_table"
basePath = "file:///hudi_data/retails_table"

# Read the Hudi table
retailsDF = spark.read.format("hudi").load(basePath)

# Show the contents
retailsDF.show()

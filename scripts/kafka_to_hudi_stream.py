import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_date
from pyspark.sql.avro.functions import from_avro

# Spark session with Hudi, Kafka, and Avro support
spark = SparkSession.builder \
    .appName("KafkaToHudiStreaming") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .getOrCreate()

# Configuration variables
topic = "test1.v1.retail_transactions"
schemaregistry = "http://schema-registry:8081"
kafka_brokers = "kafka:9092"

# Get Avro schema from Schema Registry
response = requests.get(f"{schemaregistry}/subjects/{topic}-value/versions/latest/schema")
response.raise_for_status()
schema = response.text

# Read Kafka stream
query = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Decode Avro and transform
df = query.selectExpr("substring(value, 6) as avro_value") \
    .select(from_avro(col("avro_value"), schema).alias("data"))

output = df.select(col("data.before"), col("data.after"), col("data.op")) \
    .withColumn("insert_date", date_format(current_date(), "yyyy-MM-dd"))

# Hudi options
tableName = "retails_table"
baseStreamingPath = "file:///hudi_data/retails_table"
checkpointLocation = "file:///hudi_data/checkpoint"

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'insert_date',
    'hoodie.datasource.write.precombine.field': 'insert_date',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.datasource.write.operation': 'upsert'
}

# Write to Hudi
output.writeStream.format("hudi") \
    .options(**hudi_options) \
    .outputMode("append") \
    .option("path", baseStreamingPath) \
    .option("checkpointLocation", checkpointLocation) \
    .trigger(processingTime="10 seconds") \
    .start() \
    .awaitTermination()

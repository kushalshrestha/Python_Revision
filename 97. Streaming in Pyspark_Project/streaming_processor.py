from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("RealTimeKafkaStream") \
    .getOrCreate()

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "real_time_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value as String (optional, depending on your event format)
df = df.selectExpr("CAST(value AS STRING)")

# Define the schema for the data (assuming JSON format)
schema = StructType([
    StructField("user_id", LongType()),
    StructField("event_type", StringType()),
    StructField("timestamp", LongType())
])

df_parsed = df.select(from_json(col("value"), schema).alias("data"))

# Write to a sink (for testing, we write to the console)
query = df_parsed.select("data.*") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination of the query
query.awaitTermination()

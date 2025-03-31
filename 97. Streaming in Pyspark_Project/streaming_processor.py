# streaming_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Initialize Spark Session with Kafka dependency
    
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.1.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.1.jar") \
    .getOrCreate()

spark.streams.awaitAnyTermination()

# # Define schema for incoming JSON data
# schema = StructType() \
#     .add("user_id", IntegerType()) \
#     .add("event_type", StringType()) \
#     .add("timestamp", DoubleType())

# # Read from Kafka topic as a streaming DataFrame
# df_stream = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "real_time_events") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Deserialize JSON data
# df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Perform Aggregation - Count Events per Window
# df_aggregated = df_parsed \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy(
#         window(col("timestamp").cast("timestamp"), "5 minutes"),  # 5-minute window
#         col("event_type")
#     ).count()

# # Perform Aggregation - Count Events per Window
# df_aggregated = df_parsed \
#     .withWatermark("timestamp", "10 minutes") \
#     .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "event_type") \
#     .groupBy(
#         window(col("timestamp"), "5 minutes"),  # 5-minute window
#         col("event_type")
#     ).count()



# # Write output to PostgreSQL
# query = df_aggregated.writeStream \
#     .outputMode("update") \
#     .foreachBatch(lambda batch_df, batch_id: batch_df.write
#         .format("jdbc")
#         .option("url", "jdbc:postgresql://postgres:5432/yourdb")
#         .option("dbtable", "event_counts")
#         .option("user", "user")
#         .option("password", "password")
#         .option("driver", "org.postgresql.Driver")
#         .mode("append")
#         .save()
#     ) \
#     .start()

# query.awaitTermination()

from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder.appName("TestPySpark").getOrCreate()

# Create DataFrame
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# Show Data
df.show()

time.sleep(300)  # Keep UI accessible for 5 minutes; else the spark executes quickly and stops the Spark session

# Stop Spark session
spark.stop()

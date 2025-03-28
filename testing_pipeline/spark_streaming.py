from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create Spark session
spark = SparkSession.builder \
    .appName("Kafka to HDFS Streaming") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

# Kafka connection parameters
kafka_brokers = "kafka-broker-1:29092,kafka-broker-2:29093"  # Adjust based on your setup
kafka_topic = "kol_user"

# Create a DataFrame representing the stream of input lines from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

# Data processing: Convert Kafka's value (binary) to string and split by commas (CSV format)
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Split the value (message) into separate columns based on the CSV format
df = df.selectExpr("split(value, ',')[0] AS user_id", "split(value, ',')[1] AS interaction")

# Define the output path for HDFS (Make sure the directory exists)
output_path = "hdfs://namenode:9000/user/root/kol_user_output"

# Write the stream to HDFS in CSV format in append mode
query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .option("path", output_path) \
    .start()

query.awaitTermination()

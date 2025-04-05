from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Kafka to HDFS Streaming CSV") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("userid", StringType(), True),
    StructField("id", StringType(), True),
    StructField("like", IntegerType(), True),
    StructField("comment", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Kafka config
kafka_brokers = "kafka-broker-1:29092,kafka-broker-2:29093"
kafka_topic = "kol-posts"

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse dữ liệu Kafka từ JSON
df_string = df.selectExpr("CAST(value AS STRING) as json_string")
df_parsed = df_string.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

# Ghi dữ liệu dạng CSV
output_path = "hdfs://namenode:9000/user/root/csv_data/kol_posts_output"
checkpoint_path = "/tmp/kol_posts_checkpoints_csv"

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .option("header", True) \
    .trigger(processingTime="5 minutes") \
    .start()

# Giữ ứng dụng chạy liên tục
query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Kafka to HDFS Streaming JSON") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

schema = StructType([
    StructField("userid", StringType(), True),
    StructField("id", StringType(), True),
    StructField("like", IntegerType(), True),
    StructField("comment", IntegerType(), True),
    StructField("content", StringType(), True)
])

kafka_brokers = "kafka-broker-1:29092,kafka-broker-2:29093"  # Điều chỉnh theo setup của bạn
kafka_topic = "kol-posts"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

df_string = df.selectExpr("CAST(value AS STRING) as json_string")

df_parsed = df_string.select(from_json(col("json_string"), schema).alias("data")).select("data.*")

output_path = "hdfs://namenode:9000/user/root/kol_posts_output"

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/kol_posts_checkpoints") \
    .option("path", output_path) \
    .start()

query.awaitTermination()

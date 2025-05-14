from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, regexp_extract, regexp_replace, lower, when, lit,
    to_timestamp, current_timestamp, rand, floor, date_sub
)
from pyspark.sql.types import *

# === Helper function ===
def parse_count_with_suffix(col_expr):
    num = regexp_replace(regexp_extract(col_expr, r"([\d,\.]+)", 1), ",", ".").cast("double")
    text = lower(col_expr)
    multiplier = when(text.like("%triệu%"), 1e6) \
                 .when(text.like("%k%"), 1e3) \
                 .otherwise(1)
    return when(col_expr.rlike(r"\d"), (num * multiplier).cast("long")).otherwise(lit(0))

# === Spark session ===
spark = (SparkSession.builder
    .appName("kol-comment-stream")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
    .getOrCreate())

# =================== kol-comment ======================
comment_schema = StructType([
    StructField("user_url", StringType()),
    StructField("comment_id", StringType()),
    StructField("page_id", StringType()),
    StructField("post_id", StringType()),
    StructField("user_name", StringType()),
    StructField("comment_text", StringType()),
    StructField("emote_count", StringType()),
    StructField("post_type", StringType())
])

comment_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:29092,kafka-broker-2:29093") \
    .option("subscribe", "kol-comment-topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

comment_json = comment_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), comment_schema).alias("data")).select("data.*")

comment_clean = comment_json.select(
    col("user_url"),
    col("comment_id"),
    col("page_id"),
    col("post_id"),
    col("user_name"),
    col("comment_text"),
    when((col("emote_count") == "") | col("emote_count").isNull(), lit(0))
        .otherwise(parse_count_with_suffix(col("emote_count"))).alias("emote_count"),
    when((col("post_type") == "") | col("post_type").isNull(), lit("unknown"))
        .otherwise(col("post_type")).alias("post_type")
)

comment_clean.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/user/stream/kol-comment/") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoint/kol-comment/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()

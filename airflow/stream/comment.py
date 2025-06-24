from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, regexp_extract, regexp_replace, lower, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType


def parse_count_with_suffix(col_expr):
    num = (regexp_replace(regexp_extract(col_expr, r"([\d,\.]+)", 1), ",", ".").cast("double"))
    text = lower(col_expr)
    multiplier = when(text.like("%triệu%"), 1e6) \
                 .when(text.like("%k%"), 1e3) \
                 .otherwise(1)
    return when(col_expr.rlike(r"\d"), (num * multiplier).cast("long")).otherwise(lit(0))

spark = (SparkSession.builder
    .appName("kol-comment-stream")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"  
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,"  
        "org.apache.iceberg:iceberg-hive-runtime:1.7.2")  
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://namenode:9000/user/hive/warehouse")
    .getOrCreate())

# 3. Define schema for comments
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

# 4. Read from Kafka
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:29092,kafka-broker-2:29093") \
    .option("subscribe", "kol-comment-topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df = (raw
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .select(from_json(col("json_str"), comment_schema).alias("d"), col("kafka_ts"))
    .select(
        col("d.user_url"),
        col("d.comment_id"),
        col("d.page_id"),
        col("d.post_id"),
        col("d.user_name"),
        col("d.comment_text"),
        when(col("d.emote_count").isNull() | (col("d.emote_count") == ""), lit(0))
            .otherwise(parse_count_with_suffix(col("d.emote_count"))).alias("emote_count"),
        when(col("d.post_type").isNull() | (col("d.post_type") == ""), lit("unknown"))
            .otherwise(col("d.post_type")).alias("post_type"),
        col("kafka_ts").alias("record_ts")
    ))

(
    df.writeStream
      .format("iceberg")
      .outputMode("append")
      .option("checkpointLocation", "hdfs://namenode:9000/iceberg/checkpoints/kol_comment_stream_raw/")
      .trigger(processingTime="1 minute")
      .toTable("hive_catalog.db1.kol_comment_stream")
)

spark.streams.awaitAnyTermination()

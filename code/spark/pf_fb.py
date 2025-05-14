from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, regexp_extract, when, lower, to_date, lit
from pyspark.sql.types import *

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("kol-profile-stream") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()


raw_schema = StructType([
    StructField("url", StringType()),
    StructField("name", StringType()),
    StructField("followers_count", StringType()),
    StructField("following_count", StringType()),
    StructField("verified_account", BooleanType()),
    StructField("category", StringType()),
    StructField("contact", StructType([
        StructField("Email", StringType()),
        StructField("Di động", StringType()),
        StructField("Trang web", StringType()),
    ])),
    StructField("social_links", StructType([
        StructField("Trang web", StringType()),
        StructField("Instagram", StringType()),
        StructField("TikTok", StringType()),
        StructField("YouTube", StringType()),
        StructField("Threads", StringType())
    ])),
    StructField("page_id", StringType()),
    StructField("create_date", StringType())
])

df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:29092,kafka-broker-2:29093") \
    .option("subscribe", "kol-profile-topic") \
    .option("startingOffsets", "earliest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), raw_schema).alias("data")) \
    .select("data.*")

followers_count_cleaned = when(
    col("followers_count").rlike(r"(?i)\d"), 
    (
        regexp_replace(
            regexp_extract(col("followers_count"), r"([\d,\.]+)", 1),
            ",", "."
        ).cast("double") *
        when(lower(col("followers_count")).like("%triệu%"), 1000000)
        .when(lower(col("followers_count")).like("%k%"), 1000)
        .otherwise(1)
    ).cast("long")
).otherwise(lit(0))

create_date_cleaned = when(
    col("create_date").rlike(r"\d{1,2} tháng \d{1,2}, \d{4}"),
    to_date(
        regexp_extract(col("create_date"), r"(\d{1,2}) tháng (\d{1,2}), (\d{4})", 0),
        "d 'tháng' M, yyyy"
    )
).otherwise(lit("2010-01-01").cast("date"))

df_cleaned = df_json.select(
    col("url"),
    col("name"),
    followers_count_cleaned.alias("followers_count"),
    col("category"),
    col("contact.Email").alias("email_contact"),
    col("contact.Di động").alias("phone_contact"),
    col("contact.Trang web").alias("web_contact"),
    col("social_links.Trang web").alias("webpage"),
    col("social_links.TikTok").alias("tiktok"),
    col("social_links.YouTube").alias("youtube"),
    col("social_links.Instagram").alias("instagram"),
    col("social_links.Threads").alias("threads"),
    col("page_id"),
    create_date_cleaned.alias("create_date")
)

# Ghi dữ liệu ra HDFS
df_cleaned.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/user/stream/kol-profile/") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoint/kol-profile/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start() \
    .awaitTermination()

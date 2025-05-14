from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, regexp_extract, regexp_replace, lower, when, lit,
    to_timestamp, expr, current_timestamp, rand, floor, date_sub, to_date
)
from pyspark.sql.types import *

def parse_count_with_suffix(col_expr):
    num = regexp_replace(regexp_extract(col_expr, r"([\d,\.]+)", 1), ",", ".").cast("double")
    text = lower(col_expr)
    return when(col_expr.rlike(r"\d"),
                (num *
                 when(text.like("%triệu%"), 1e6)
                 .when(text.like("%k%"), 1e3)
                 .otherwise(1)).cast("long")
           ).otherwise(lit(0))

def parse_plain_number(col_expr):
    cleaned = regexp_replace(col_expr, "\.", "")
    return when(cleaned.rlike(r"^\d+$"), cleaned.cast("long")).otherwise(lit(0))

spark = (SparkSession.builder
    .appName("kol-all-stream")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
    .getOrCreate())



raw_schema = StructType([
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
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
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

df_cleaned.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/user/stream/kol-profile/") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoint/kol-profile/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start() \

post_schema = StructType([
    StructField("url", StringType()),
    StructField("type", StringType()),
    StructField("base58_id", StringType()),
    StructField("post_id", StringType()),
    StructField("page_id", StringType()),
    StructField("post_time", StringType()),
    StructField("content", StringType()),
    StructField("total_comment", StringType()),
    StructField("total_share", StringType()),
    StructField("emotes", StructType([
        StructField("Tất cả", StringType()),
        StructField("Thích", StringType()),
        StructField("Yêu thích", StringType()),
        StructField("Thương thương", StringType()),
        StructField("Haha", StringType()),
        StructField("Wow", StringType()),
        StructField("Buồn", StringType()),
        StructField("Phẫn nộ", StringType())
    ]))
])

post_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:29092,kafka-broker-2:29093") \
    .option("subscribe", "kol-post-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

post_df = post_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), post_schema).alias("d")).select("d.*")

post_clean = post_df.select(
    col("url"),
    when(col("type").isNull(), lit("post")).otherwise(col("type")).alias("type"),
    col("base58_id"),
    col("post_id"),
    col("page_id"),
    when(
        col("post_time").rlike(r"\d{1,2} Tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{2}"),
        to_timestamp(regexp_extract(col("post_time"), r"(\d{1,2} Tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{2})", 1),
                     "d 'Tháng' M, yyyy 'lúc' H:mm")
    ).otherwise(date_sub(current_timestamp(), floor(rand() * 3 + 5).cast("int"))).alias("post_time"),
    col("content"),
    parse_count_with_suffix(col("total_comment")).alias("total_comment"),
    parse_count_with_suffix(col("total_share")).alias("total_share"),
    parse_plain_number(col("emotes.`Tất cả`"))
        .alias("total_emotes"),
    parse_count_with_suffix(col("emotes.`Thích`")).alias("likes"),
    parse_count_with_suffix(col("emotes.`Yêu thích`")).alias("loves"),
    parse_count_with_suffix(col("emotes.`Thương thương`")).alias("care"),
    parse_count_with_suffix(col("emotes.Haha")).alias("haha"),
    parse_count_with_suffix(col("emotes.Wow")).alias("wow"),
    parse_count_with_suffix(col("emotes.Buồn")).alias("sad"),
    parse_count_with_suffix(col("emotes.`Phẫn nộ`"))
    .alias("angry")
)

post_clean.writeStream.format("parquet") \
    .option("path", "hdfs://namenode:9000/user/stream/kol-post/") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoint/kol-post/") \
    .outputMode("append").trigger(processingTime="1 minute").start()

reel_schema = StructType([
    StructField("url", StringType()),
    StructField("page_id", StringType()),
    StructField("reel_id", StringType()),
    StructField("views", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("likes", StringType()),
    StructField("comments_count", StringType()),
    StructField("shares", StringType()),
    StructField("post_time", StringType()),
    StructField("type", StringType())
])

reel_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:29092,kafka-broker-2:29093") \
    .option("subscribe", "kol-reel-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

reel_df = reel_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), reel_schema).alias("d")).select("d.*")

reel_clean = reel_df.select(
    col("url"),
    col("page_id"),
    col("reel_id"),
    parse_count_with_suffix(col("views")).alias("views"),
    col("content"),
    col("hashtags"),
    parse_count_with_suffix(col("likes")).alias("likes"),
    parse_count_with_suffix(col("comments_count")).alias("comments"),
    parse_count_with_suffix(col("shares")).alias("share"),
    when(
        col("post_time").rlike(r"\d{1,2} Tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{2}"),
        to_timestamp(regexp_extract(col("post_time"), r"(\d{1,2} Tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{2})", 1),
                     "d 'Tháng' M, yyyy 'lúc' H:mm")
    ).otherwise(date_sub(current_timestamp(), floor(rand() * 3 + 5).cast("int"))).alias("post_time"),
    when(col("type").isNull(), lit("reel")).otherwise(col("type")).alias("type")
)

reel_clean.writeStream.format("parquet") \
    .option("path", "hdfs://namenode:9000/user/stream/kol-reel/") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/spark/checkpoint/kol-reel/") \
    .outputMode("append").trigger(processingTime="1 minute").start()

spark.streams.awaitAnyTermination()

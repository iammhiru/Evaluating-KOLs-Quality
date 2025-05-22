from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, regexp_extract, regexp_replace, lower, when, lit,
    to_timestamp, expr, rand, floor, date_sub, to_date, current_timestamp, row_number
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

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
    cleaned = regexp_replace(col_expr, "\\.", "")
    return when(cleaned.rlike(r"^\d+$"), cleaned.cast("long")).otherwise(lit(0))

spark = (SparkSession.builder
    .appName("kol-all-stream-iceberg")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1," 
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2," 
        "org.apache.iceberg:iceberg-hive-runtime:1.7.2") 
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://namenode:9000/user/hive/warehouse")
    .getOrCreate())


# ==================== STREAM: PROFILE ====================
raw_schema = StructType([
    StructField("name", StringType()),
    StructField("url", StringType()),
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

df_raw = df_raw.selectExpr(
    "CAST(value AS STRING) as json_str",
    "timestamp AS kafka_ts"
)

df_json = df_raw.select(
    from_json(col("json_str"), raw_schema).alias("data"),
    col("kafka_ts")
).select("data.*", "kafka_ts")

df_json = df_json.withColumn("record_ts", col("kafka_ts"))

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

profile_clean = df_json.select(
    col("name"),
    col("url"),
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
    create_date_cleaned.alias("create_date"),
    col("record_ts"),
    col("kafka_ts"),
)

def upsert_profiles(batch_df, batch_id):
    w = Window.partitionBy("page_id").orderBy(batch_df.record_ts.desc())
    dedup = (batch_df
             .withColumn("_rn", row_number().over(w))
             .filter(col("_rn") == 1)
             .drop("_rn"))
    dedup.createOrReplaceGlobalTempView("updates_profiles")

    spark.sql("""
        MERGE INTO hive_catalog.db1.kol_profile_stream AS target
        USING global_temp.updates_profiles AS source
          ON target.page_id = source.page_id
        WHEN MATCHED AND source.record_ts > target.record_ts
          THEN UPDATE SET *
        WHEN NOT MATCHED
          THEN INSERT *
    """)

profile_clean.writeStream \
    .trigger(processingTime="1 minute") \
    .foreachBatch(upsert_profiles) \
    .option("checkpointLocation", "hdfs://namenode:9000/iceberg/checkpoints/kol_profile_stream_upsert/") \
    .start()

# ==================== STREAM: POST ====================
post_schema = StructType([
    StructField("url", StringType()),
    StructField("type", StringType()),
    StructField("base58_id", StringType()),
    StructField("post_id", StringType()),
    StructField("page_id", StringType()),
    StructField("post_time", StringType()),
    StructField("content", StringType()),
    StructField("total_comment", StringType()),
    StructField("total_view", StringType()),
    StructField("total_share", StringType()),
    StructField("type", StringType()),
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

post_raw = post_raw.selectExpr(
    "CAST(value AS STRING) as json_str",
    "timestamp AS kafka_ts"
)

post_df = post_raw.select(
    from_json(col("json_str"), post_schema).alias("d"),
    col("kafka_ts")
).select("d.*", "kafka_ts")

post_clean = post_df.select(
    col("url"),
    when(col("type").isNull(), lit("post")).otherwise(col("type")).alias("type"),
    col("base58_id"),
    col("post_id"),
    col("page_id"),
    col("type"),
    when(
        col("post_time").rlike(r"\d{1,2} Tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{2}"),
        to_timestamp(regexp_extract(col("post_time"), r"(\d{1,2} Tháng \d{1,2}, \d{4} lúc \d{1,2}:\d{2})", 1),
                     "d 'Tháng' M, yyyy 'lúc' H:mm")
    ).otherwise(date_sub(current_timestamp(), floor(rand() * 3 + 5).cast("int"))).alias("post_time"),
    col("content"),
    parse_count_with_suffix(col("total_comment")).alias("total_comment"),
    parse_count_with_suffix(col("total_share")).alias("total_share"),
    parse_count_with_suffix(col("total_view")).alias("total_view"),
    parse_plain_number(col("emotes.`Tất cả`")).alias("total_emotes"),
    parse_count_with_suffix(col("emotes.`Thích`")).alias("likes"),
    parse_count_with_suffix(col("emotes.`Yêu thích`")).alias("loves"),
    parse_count_with_suffix(col("emotes.`Thương thương`")).alias("care"),
    parse_count_with_suffix(col("emotes.Haha")).alias("haha"),
    parse_count_with_suffix(col("emotes.Wow")).alias("wow"),
    parse_count_with_suffix(col("emotes.Buồn")).alias("sad"),
    parse_count_with_suffix(col("emotes.`Phẫn nộ`")).alias("angry"),
    col("kafka_ts")
)

post_clean = post_clean.withColumn("record_ts", col("kafka_ts"))

post_clean.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://namenode:9000/iceberg/checkpoints/kol_post_stream/") \
    .trigger(processingTime="1 minute") \
    .toTable("hive_catalog.db1.kol_post_stream")

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

reel_raw = reel_raw.selectExpr(
    "CAST(value AS STRING) as json_str",
    "timestamp AS kafka_ts"
)

reel_df = reel_raw.select(
    from_json(col("json_str"), reel_schema).alias("d"),
    col("kafka_ts")
).select("d.*", "kafka_ts")

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
    when(col("type").isNull(), lit("reel")).otherwise(col("type")).alias("type"),
    col("kafka_ts")
)

reel_clean = reel_clean.withColumn("record_ts", col("kafka_ts"))

reel_clean.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://namenode:9000/iceberg/checkpoints/kol_reel_stream/") \
    .trigger(processingTime="1 minute") \
    .toTable("hive_catalog.db1.kol_reel_stream")

spark.streams.awaitAnyTermination()

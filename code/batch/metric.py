from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, date_trunc, sum, avg, lit, when, row_number,
    sequence, explode, to_date, month, year, expr
)

spark = (SparkSession.builder
    .appName("kol-metrics-monthly-batch")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,"
        "org.apache.iceberg:iceberg-hive-runtime:1.7.2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
    .config("spark.sql.catalog.hive_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.hive_catalog.warehouse", "hdfs://namenode:9000/user/hive/warehouse")
    .getOrCreate())

raw_profiles = spark.read.format("iceberg").load("hive_catalog.db1.kol_profile_stream")
raw_posts = spark.read.format("iceberg").load("hive_catalog.db1.kol_post_stream")
raw_reels = spark.read.format("iceberg").load("hive_catalog.db1.kol_reel_stream")


months_df = spark.createDataFrame(
    [("2023-01-01","2025-04-01")], ["start","end"]
).select(
    explode(
        sequence(to_date(col("start")), to_date(col("end")), expr("interval 1 month"))
    ).alias("month_date")
).withColumn("month", month(col("month_date"))) \
 .withColumn("year", year(col("month_date"))) \
 .withColumn("month_of_year", month(col("month_date"))) \
 .select("month","year","month_of_year") \

profiles = raw_profiles.select(
    col("page_id"),
    col("name"),
    col("category"),
    col("followers_count"),
).crossJoin(months_df)

posts = (
    raw_posts
    .withColumn("rn", row_number().over(
        Window.partitionBy("page_id", "post_id").orderBy(col("record_ts").desc())
    ))
    .filter(col("rn") == 1)
    .drop("rn")
)

reels = (
    raw_reels
    .withColumn("rn", row_number().over(
        Window.partitionBy("page_id", "reel_id").orderBy(col("record_ts").desc())
    ))
    .filter(col("rn") == 1)
    .drop("rn")
)

text_posts = posts.filter(col("type") == "post").select(
    col("page_id"),
    (col("total_comment") + col("total_share") + col("total_emotes")).alias("engagement"),
    (col("likes") + col("loves") + col("wow") + col("care") + col("haha")).alias("applause"),
    col("total_comment").alias("comments"),
    col("total_share").alias("shares"),
    lit(1).alias("posts_count"),
    month(col("post_time")).alias("month"),
    year(col("post_time")).alias("year"),
)

post_metric = text_posts.groupBy("page_id", "month", "year").agg(
    sum("posts_count").alias("post_count"),
    avg("engagement").alias("post_engagement"),
    avg("applause").alias("post_applause"),
    avg("comments").alias("post_comments"),
    avg("shares").alias("post_shares_avg"),
)

video_posts = posts.filter(col("type") == "video").select(
    col("post_id").alias("post_id"),
    col("page_id"),
    (col("total_comment") + col("total_view") + col("total_emotes")).alias("engagement"),
    (col("likes") + col("loves") + col("wow") + col("care") + col("haha")).alias("applause"),
    col("total_comment").alias("comments"),
    col("total_share").alias("shares"),
    lit(1).alias("video_count"),
    lit(0).alias("reel_count"),
    ((col("total_emotes") + col("total_comment"))/col("total_view")*100).alias("engagement_rate"),
    month(col("post_time")).alias("month"),
    year(col("post_time")).alias("year")
)

reel_posts = reels.select(
    col("reel_id").alias("post_id"),
    col("page_id"),
    (col("comments") + col("views") + col("likes") + col("share")).alias("engagement"),
    col("likes").alias("applause"),
    col("comments").alias("comments"),
    col("share").alias("shares"),
    lit(0).alias("video_count"),
    lit(1).alias("reel_count"),
    ((col("comments") + col("likes") + col("share"))/col("views")*100).alias("engagement_rate"),
    month(col("post_time")).alias("month"),
    year(col("post_time")).alias("year")
)

vid_posts = video_posts.union(reel_posts)

vid_metrics = vid_posts.groupBy("page_id", "month", "year").agg(
    sum(col("reel_count") + col("video_count")).alias("vid_content_count"),
    avg("engagement").alias("vid_engagement"),
    avg("applause").alias("vid_applause"),
    avg("comments").alias("vid_comments"),
    sum("shares").alias("total_shares"),
    sum("video_count").alias("vid_count"),
    sum("reel_count").alias("reel_count"),
    avg("engagement_rate").alias("vid_engagement_rate"),
)

vid_metrics = vid_metrics.withColumn("vid_shares_avg",
    when(col("vid_count") > 0, col("total_shares") / col("vid_count")).otherwise(lit(0))                                     
)

results = profiles.join(post_metric, ["page_id", "month", "year"], "left") \
    .join(vid_metrics, ["page_id", "month", "year"], "left") \
    .withColumn("post_engagement_rate", 
        when(col("followers_count").isNotNull(), col("post_engagement") / col("followers_count") * 100).otherwise(lit(0))
    ) \
    .withColumn("vid_engagement_rate", 
        when(col("followers_count").isNotNull(), col("vid_engagement") / col("followers_count") * 100).otherwise(lit(0)))

results = results.fillna(0, subset=["post_count", "post_engagement", "post_applause", "post_comments", "post_shares_avg", "post_engagement_rate", "vid_content_count", "vid_engagement", "vid_applause", "vid_comments", "total_shares", "vid_count", "reel_count", "vid_engagement_rate", "vid_shares_avg"])

results.write.format("iceberg").mode("overwrite").option("overwrite-mode","dynamic") \
    .saveAsTable("hive_catalog.db1.kol_metrics_monthly")

spark.stop()
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, sum, avg, lit, row_number, min as _min, max as _max,
    month, year, when
)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute KOL metrics for a given month/year"
    )
    parser.add_argument("--month", type=int, required=True,
                        help="Tháng cần xử lý (1-12)")
    parser.add_argument("--year", type=int, required=True,
                        help="Năm cần xử lý, ví dụ 2025")
    return parser.parse_args()

def main():
    args = parse_args()
    month_param = args.month
    year_param  = args.year

    spark = (SparkSession.builder
        .appName("kol-metrics-monthly-batch")
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
    comments = spark.read.format("iceberg").load("hive_catalog.db1.kol_comment_sentiment_summary").drop("total_comment")

    profiles = raw_profiles.select(
        col("page_id"),
        col("name"),
        col("category"),
        col("followers_count"),
    ).withColumn("month", lit(month_param)) \
    .withColumn("year", lit(year_param))

    posts = (
        raw_posts
        .withColumn("rn", row_number().over(
            Window.partitionBy("page_id", "post_id").orderBy(col("record_ts").desc())
        ))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    posts = posts.join(
        comments,
        on="post_id",
        how="left" 
    ).fillna(0)

    text_posts = posts.filter(col("type") == "post").select(
        col("page_id"),

        (col("total_comment") + col("total_share") + col("total_emotes")).alias("engagement"),
        (col("likes") + col("loves") + col("wow") + col("care") + col("haha")).alias("applause"),
        
        col("total_comment").alias("comments"),
        col("total_share").alias("shares"),

        col("total_positive_rate").alias("positive_rate"),
        col("total_negative_rate").alias("negative_rate"),
        col("total_neutral_rate").alias("neutral_rate"),
        
        lit(1).alias("posts_count"),
        month(col("post_time")).alias("month"),
        year(col("post_time")).alias("year"),
    )

    post_metric = text_posts.groupBy("page_id", "month", "year").agg(
        sum("posts_count").alias("total_post_count"),
        avg("engagement").alias("engagement_per_post"),
        avg("applause").alias("applause_per_post"),
        avg("comments").alias("comment_per_post"),
        avg("shares").alias("amplification_avg"),
        (sum("positive_rate") - sum("negative_rate")).alias("post_sentiment"),
        (sum("positive_rate") + sum("negative_rate") + sum("neutral_rate")).alias("post_sentiment_total"),
    ).withColumn(
        "post_sentiment_score",
        when(
            col("post_sentiment_total") != 0,
            (col("post_sentiment") / col("post_sentiment_total")) * 100
        ).otherwise(lit(0.0))
    )

    video_posts = posts.filter(col("type") == "video").select(
        col("page_id"),
        col("total_view"),
        (col("total_comment") + col("total_emotes") + col("total_share")).alias("engagement"),
        (col("likes") + col("loves") + col("wow") + col("care") + col("haha") + col("sad") + col("angry")).alias("emotes"),
        col("total_comment").alias("comments"),
        lit(1).alias("video_count"),
        month(col("post_time")).alias("month"),
        year(col("post_time")).alias("year"),
        col("total_positive_rate").alias("positive_rate"),
        col("total_negative_rate").alias("negative_rate"),
        col("total_neutral_rate").alias("neutral_rate"),
    )

    video_metrics = video_posts.groupBy("page_id", "month", "year").agg(
        sum("video_count").alias("total_video_count"),
        avg("total_view").alias("views_per_vid"),
        avg("engagement").alias("engagement_per_vid"),
        avg(col("emotes") / col("total_view") * 100).alias("video_applause_rate"),
        avg(col("comments") / col("total_view") * 100).alias("video_conversation_rate"),
        avg(col("engagement") / col("total_view") * 100).alias("engagement_to_view_rate"),
        (sum("positive_rate") - sum("negative_rate")).alias("video_sentiment"),
        (sum("positive_rate") + sum("negative_rate") + sum("neutral_rate")).alias("video_sentiment_total"),
    ).withColumn(
        "video_sentiment_score",
        when(
            col("video_sentiment_total") != 0,
            (col("video_sentiment") / col("video_sentiment_total")) * 100
        ).otherwise(lit(0.0))
    )

    reels = (
        raw_reels
        .withColumn("rn", row_number().over(
            Window.partitionBy("page_id", "reel_id").orderBy(col("record_ts").desc())
        ))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    reels = reels.join(
        comments,
        reels.reel_id == comments.post_id,
        how="left"
    ).fillna(0)

    reel_posts = reels.select(
        col("page_id"),
        col("views"),
        (col("comments") + col("likes") + col("share")).alias("engagement"),
        col("likes"),
        col("comments").alias("comments"),
        col("share").alias("shares"),
        lit(1).alias("reel_count"),
        month(col("post_time")).alias("month"),
        year(col("post_time")).alias("year"),
        col("total_positive_rate").alias("positive_rate"),
        col("total_negative_rate").alias("negative_rate"),
        col("total_neutral_rate").alias("neutral_rate"),
    )

    reel_metric = reel_posts.groupBy("page_id", "month", "year").agg(
        sum("reel_count").alias("total_reel_count"),
        avg("views").alias("views_per_reel"),
        avg("engagement").alias("engagement_per_reel"),
        avg(col("likes") / col("views") * 100).alias("reel_applause_rate"),
        avg(col("comments") / col("views") * 100).alias("reel_conversation_rate"),
        avg(col("shares") / col("views") * 100).alias("reel_amplification_rate"),
        avg((col("engagement") / col("views")) * 100).alias("reel_engagement_rate"),
        (sum("positive_rate") - sum("negative_rate")).alias("reel_sentiment"),
        (sum("positive_rate") + sum("negative_rate") + sum("neutral_rate")).alias("reel_sentiment_total"),
    ).withColumn(
        "reel_sentiment_score",
        when(
            col("reel_sentiment_total") != 0,
            (col("reel_sentiment") / col("reel_sentiment_total")) * 100
        ).otherwise(lit(0.0))
    )



    results = profiles.join(post_metric, ["page_id", "month", "year"], "left") \
        .join(video_metrics, ["page_id", "month", "year"], "left") \
        .join(reel_metric, ["page_id", "month", "year"], "left") \
        
    results = results.fillna(0)
        
    w = Window.partitionBy("month", "year")

    results = results.select(
        col("page_id"),
        col("name"),
        col("category"),
        col("followers_count"),
        col("month"),
        col("year"),

        col("total_post_count").alias("post_count"),
        col("engagement_per_post"),
        (col("engagement_per_post") / col("followers_count") * 100).alias("post_engagement_rate"),
        (col("applause_per_post") / col("followers_count") * 100).alias("post_applause_rate"),
        (col("comment_per_post") / col("followers_count") * 100).alias("post_conversation_rate"),
        (col("amplification_avg") / col("followers_count") * 100).alias("post_amplification_rate"),
        col("post_sentiment_score"),

        col("total_video_count").alias("video_count"),
        col("engagement_per_vid").alias("video_engagement"),
        col("engagement_to_view_rate").alias("video_engagement_rate"),
        col("video_applause_rate"),
        col("video_conversation_rate"),
        col("video_sentiment_score"),

        col("total_reel_count").alias("reel_count"),
        col("engagement_per_reel").alias("reel_engagement"),
        col("reel_engagement_rate"),
        col("reel_applause_rate"),
        col("reel_conversation_rate"),
        col("reel_amplification_rate"),
        col("reel_sentiment_score"),

        (col("total_post_count") + col("total_video_count") + col("total_reel_count")).alias("total_content_count"),
        when(
            (col("total_post_count") + col("total_video_count") + col("total_reel_count")) != 0,
            (col("engagement_per_post") * col("total_post_count") +
            col("engagement_per_vid") * col("total_video_count") +
            col("engagement_per_reel") * col("total_reel_count")) / ((col("total_post_count") + col("total_video_count") + col("total_reel_count")) * col("followers_count")) * 100
        ).otherwise(lit(0.0)).alias("content_engagement_rate"),
        when(
            (col("post_sentiment_total") + col("video_sentiment_total") + col("reel_sentiment_total")) != 0,
            ((col("post_sentiment") + col("video_sentiment") + col("reel_sentiment")) /
            (col("post_sentiment_total") + col("video_sentiment_total") + col("reel_sentiment_total")) * 100),
        ).otherwise(lit(0.0)).alias("content_sentiment_score")
    ).withColumn("minF", _min("followers_count").over(w)) \
        .withColumn("maxF", _max("followers_count").over(w)) \
        .withColumn("minE", _min("content_engagement_rate").over(w)) \
        .withColumn("maxE", _max("content_engagement_rate").over(w)) \
        .withColumn(
            "F_norm",
            when(col("maxF") != col("minF"),
                (col("followers_count") - col("minF")) / (col("maxF") - col("minF"))
            ).otherwise(lit(0.0))
        ) \
        .withColumn(
            "E_norm",
            when(col("maxE") != col("minE"),
                (col("content_engagement_rate") - col("minE")) / (col("maxE") - col("minE"))
            ).otherwise(lit(0.0))
        ) \
        .withColumn(
            "S_norm",
            (col("content_sentiment_score") + lit(100)) / lit(200)
        ) \
        .withColumn(
            "kol_score",
            (col("F_norm") + col("E_norm") + col("S_norm")) / lit(3) * 100
        ).drop("minF", "maxF", "minE", "maxE", "F_norm", "E_norm", "S_norm") 
    
    results.filter(
        (col("month") == month_param) & (col("year") == year_param)
    )
                                                                            
    results.createOrReplaceTempView("src_df")

    spark.sql(f"""
    MERGE INTO hive_catalog.db1.kol_metrics_monthly_dev AS tgt
    USING src_df AS src
      ON tgt.page_id = src.page_id
     AND tgt.month   = src.month
     AND tgt.year    = src.year
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """)
    spark.stop()

if __name__ == "__main__":
    main()
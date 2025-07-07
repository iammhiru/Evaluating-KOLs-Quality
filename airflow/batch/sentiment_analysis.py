#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import torch
if not hasattr(torch, "get_default_device"):
    torch.get_default_device = lambda: torch.device("cpu")

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, pandas_udf, current_timestamp, date_trunc, count, sum as _sum
)
from pyspark.sql.types import StructType, StructField, FloatType
import pandas as pd

LABEL_MAP = {"NEGATIVE": -1, "NEUTRAL": 0, "POSITIVE": 1}
MODEL_PATH = "/opt/models/phobert-base-vi-sentiment-analysis"

spark = (
    SparkSession.builder
        .appName("kol-comment-sentiment-batch")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,"
                "org.apache.iceberg:iceberg-hive-runtime:1.7.2")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hive_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive_catalog.catalog-impl",
                "org.apache.iceberg.hive.HiveCatalog")
        .config("spark.sql.catalog.hive_catalog.uri",
                "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hive_catalog.warehouse",
                "hdfs://namenode:9000/user/hive/warehouse")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
)

tokenizer = None
model = None

score_schema = StructType([
    StructField("positive_rate", FloatType()),
    StructField("neutral_rate",  FloatType()),
    StructField("negative_rate", FloatType()),
])

@pandas_udf(score_schema)
def classify_sentiment_scores(texts: pd.Series) -> pd.DataFrame:
    import torch
    if not hasattr(torch, "get_default_device"):
        torch.get_default_device = lambda: torch.device("cpu")

    global tokenizer, model
    if tokenizer is None:
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    if model is None:
        from transformers import AutoModelForSequenceClassification
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
        model.eval()

    records = []
    BATCH_SIZE = 64

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts.iloc[i:i + BATCH_SIZE].fillna("").astype(str).tolist()
        enc = tokenizer(batch, truncation=True, padding=True, return_tensors="pt")
        with torch.no_grad():
            logits = model(**enc).logits
            probs = torch.softmax(logits, dim=-1).tolist()

        for p in probs:
            records.append({
                "positive_rate": float(p[1]),
                "neutral_rate":  float(p[2]),
                "negative_rate": float(p[0]),
            })

    return pd.DataFrame(records, index=texts.index)

raw = (
    spark.read.format("iceberg")
         .load("hive_catalog.db1.kol_comment_stream")
         .select("post_id", "comment_id", "comment_text", "record_ts")
)

latest = (
    raw
      .withColumn("rn", row_number().over(
            Window.partitionBy("post_id", "comment_id")
                  .orderBy(col("record_ts").desc())
      ))
      .filter(col("rn") == 1)
      .select("post_id", "comment_text")
)

scored = (
    latest
      .withColumn("scores", classify_sentiment_scores(col("comment_text")))
      .select(
          "post_id",
          col("scores.positive_rate"),
          col("scores.neutral_rate"),
          col("scores.negative_rate")
      )
)

ts         = current_timestamp()
month_ts   = date_trunc("month", ts)
year_ts    = date_trunc("year",  ts)

summary = (
    scored
      .groupBy("post_id")
      .agg(
          count("*").alias("total_comment"),
          _sum("positive_rate").alias("total_positive_rate"),
          _sum("negative_rate").alias("total_negative_rate"),
          _sum("neutral_rate").alias("total_neutral_rate")
      )
      .withColumn("record_ts",      ts)
      .withColumn("record_month",   month_ts)
      .withColumn("record_year",    year_ts)
)

(
    summary
      .write.format("iceberg")
      .mode("append")
      .option("overwrite-mode", "dynamic")
      .saveAsTable("hive_catalog.db1.kol_comment_sentiment_summary_demo")
)

spark.stop()

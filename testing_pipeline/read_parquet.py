from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Parquet from HDFS") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/user/root/batch_data/user_summary")
df.show()

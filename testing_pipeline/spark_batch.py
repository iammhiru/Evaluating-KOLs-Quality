from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Batch User Aggregation") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS (đã được stream lưu trước đó)
input_path = "hdfs://namenode:9000/user/root/csv_data/kol_posts_output"

# Nếu bạn dùng Parquet trước đó, hãy đổi .csv thành .parquet
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

# Nếu có cột 'share', thêm vào đây
agg_df = df.groupBy("userid").agg(
    sum("like").alias("total_like"),
    sum("comment").alias("total_comment")
    # sum("share").alias("total_share")  # uncomment nếu có cột share
)

# Ghi kết quả ra HDFS (dạng Parquet là gợi ý tốt cho analytics)
output_path = "hdfs://namenode:9000/user/root/batch_data/user_summary"

agg_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("✅ Aggregation complete. Output written to:", output_path)

spark.stop()

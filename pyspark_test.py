from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark Test with HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Create a sample DataFrame
data = [("Alice", 30), ("Bob", 25), ("Cathy", 27)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Write the DataFrame to HDFS
df.write.format("parquet").save("hdfs://namenode:9000/user/root/test_output.parquet")

# Stop the Spark session
spark.stop()

spark-submit   --master spark://spark-master:7077   --deploy-mode client   --driver-memory 2g   --driver-cores 1   --executor-memory 2g   --total-executor-cores 1   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,org.apache.iceberg:iceberg-hive-runtime:1.7.2   /opt/bitnami/spark/ss/post.py

spark-submit --master spark://spark-master:7077 --deploy-mode client --name kol-metrics-monthly-batch --driver-cores 1 --executor-cores 1 --num-executors 1   --total-executor-cores 2 --driver-memory 2g --executor-memory 2g --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,org.apache.iceberg:iceberg-hive-runtime:1.7.2 /opt/bitnami/spark/ss/metric.py


spark-submit --master spark://spark-master:7077 --deploy-mode client --name kol-coms-monthly-batch --driver-cores 1 --executor-cores 1 --num-executors 1   --total-executor-cores 2 --driver-memory 2g --executor-memory 2g  --conf spark.jars.ivy=/tmp/.ivy2 \ --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,org.apache.iceberg:iceberg-hive-runtime:1.7.2 /opt/bitnami/spark/ss/sentiment_analysis.py

spark-submit --master spark://spark-master:7077 --driver-cores 1 --executor-cores 3 --num-executors 1   --total-executor-cores 4 --driver-memory 2g --executor-memory 3g /opt/bitnami/spark/ss/sentiment_analysis.py

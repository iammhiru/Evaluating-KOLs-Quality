#!/bin/bash
set -e

echo "⏳ Waiting for Spark master to be ready..."
# (nếu cần có thể thêm sleep/chờ cổng 7077 mở)
sleep 5

# echo "🚀 Submitting streaming jobs..."
# /opt/bitnami/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --deploy-mode client \
#   --driver-memory 1G --executor-memory 1G --executor-cores 1 \
#   /opt/bitnami/spark/ss/comment.py &

# /opt/bitnami/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --deploy-mode client \
#   --driver-memory 1G --executor-memory 1G --executor-cores 1 \
#   /opt/bitnami/spark/ss/post.py &

# echo "✅ Streaming jobs launched, now starting Spark master..."
# cuối cùng gọi entrypoint gốc để khởi master
exec /opt/bitnami/scripts/spark/run.sh "$@"

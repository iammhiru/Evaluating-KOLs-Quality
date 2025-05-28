#!/usr/bin/env bash
set -e

if [ "$(id -u)" = '0' ]; then
  chown -R 1001:1001 "$AIRFLOW_HOME/dags" \
                    "$AIRFLOW_HOME/logs" \
                    "$AIRFLOW_HOME/plugins"
  exec su-exec 1001:1001 "$0" "$@"
fi

# 1) Start Spark master & worker
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077

# 2) Init Airflow metadata DB & default user (nếu dùng SQLite) 
#    hoặc với Postgres thì db init thôi, user tạo qua UI / CLI sau
airflow db init
airflow users create -r Admin -u admin -p admin \
    -e admin@example.com -f Admin -l User || true

# 3) Start scheduler in background, then webserver in foreground
airflow scheduler &
exec airflow webserver

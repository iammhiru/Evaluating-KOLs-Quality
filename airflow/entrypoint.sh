#!/usr/bin/env bash
set -e

# 1) Nếu là root thì fix quyền và re-exec dưới user 1001
if [ "$(id -u)" = '0' ]; then
  chown -R 1001:1001 "$AIRFLOW_HOME/dags" \
                    "$AIRFLOW_HOME/logs" \
                    "$AIRFLOW_HOME/plugins"
  exec su-exec 1001:1001 "$0" "$@"
fi

# 2) Switch logic dựa trên command đầu vào ($1)
case "$1" in
  scheduler)
    echo ">>> Starting Spark Master & Worker..."
    $SPARK_HOME/sbin/start-master.sh
    $SPARK_HOME/sbin/start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT

    echo ">>> Initializing Airflow DB & Users (idempotent)..."
    airflow db init
    airflow users create \
      --role Admin \
      --username admin \
      --password admin \
      --email admin@example.com \
      --firstname Admin \
      --lastname User || true

    echo ">>> Launching Airflow Scheduler..."
    exec airflow scheduler
    ;;

  webserver)
    echo ">>> Initializing Airflow DB & Users (idempotent)..."
    airflow db init
    airflow users create \
      --role Admin \
      --username admin \
      --password admin \
      --email admin@example.com \
      --firstname Admin \
      --lastname User || true

    echo ">>> Launching Airflow Webserver..."
    exec airflow webserver
    ;;

  *)
    # Cho phép override nếu bạn chạy bất cứ lệnh nào khác
    exec "$@"
    ;;
esac

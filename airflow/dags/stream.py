from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.http_sensor import HttpSensor

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_streaming_control",
    default_args=default_args,
    description="Start & health-check Spark Streaming for posts & comments",
    schedule_interval=None,
    start_date=datetime(2025, 5, 29),
    catchup=False,
) as dag:

    start_post = SparkSubmitOperator(
        task_id="start_post_stream",
        application="$SPARK_HOME/sstream/post.py",
        conn_id="spark_default",           # tạo connection spark_default ⇒ spark-master:7077
        driver_memory="2g",
        executor_memory="2g",
        name="kol-post-stream",
        conf={
            "spark.submit.deployMode": "cluster",
            "spark.driver.cores": "1",
            "spark.executor.cores": "1",
        },
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,"
            "org.apache.iceberg:iceberg-hive-runtime:1.7.2"
        ),
    )

    health_post = HttpSensor(
        task_id="healthcheck_post_stream",
        http_conn_id="spark_ui",          # Host=spark-master, Port=8080
        endpoint="/api/v1/applications",
        response_check=lambda r: "kol-post-stream" in r.text,
        poke_interval=60,
        timeout=600,
    )

    start_comment = SparkSubmitOperator(
        task_id="start_comment_stream",
        application="$SPARK_HOME/sstream/comment.py",
        conn_id="spark_default",
        driver_memory="2g",
        executor_memory="2g",
        name="kol-comment-stream",
        conf={
            "spark.submit.deployMode": "cluster",
            "spark.driver.cores": "1",
            "spark.executor.cores": "1",
        },
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.2,"
            "org.apache.iceberg:iceberg-hive-runtime:1.7.2"
        ),
    )

    health_comment = HttpSensor(
        task_id="healthcheck_comment_stream",
        http_conn_id="spark_ui",
        endpoint="/api/v1/applications",
        response_check=lambda r: "kol-comment-stream" in r.text,
        poke_interval=60,
        timeout=600,
    )

    start_post >> health_post
    start_comment >> health_comment

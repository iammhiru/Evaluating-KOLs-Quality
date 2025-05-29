from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='spark_batch_month_flow',
    default_args=default_args,
    description='Chạy các Spark batch job trong thư mục spark/ss hàng tháng',
    schedule_interval='0 2 1 * *',   
    start_date=datetime(2025, 4, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    sentiment = BashOperator(
        task_id='run_sentiment_analysis',
        bash_command=(
            '$SPARK_HOME/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '--deploy-mode client '
            '--conf spark.driver.cores=1 '
            '--conf spark.driver.memory=2g '
            '--conf spark.executor.cores=3 '
            '--conf spark.executor.memory=3g '
            '$SPARK_HOME/ss/sentiment_analysis.py'
        ),
    )

    metric = BashOperator(
        task_id='run_metric_computation',
        bash_command=(
            '$SPARK_HOME/bin/spark-submit '
            '--master spark://spark-master:7077 '
            '--deploy-mode client '
            '--conf spark.driver.cores=1 '
            '--conf spark.driver.memory=2g '
            '--conf spark.executor.cores=3 '
            '--conf spark.executor.memory=3g '
            '$SPARK_HOME/ss/metric.py'
        ),
    )

    clear_post = BashOperator(
        task_id='clear_kol_post_stream',
        bash_command=(
        'cd /opt/trino && '
        './trino --server http://trino:8080 '
        '--catalog iceberg --schema db1 '
        '--user admin '
        '--execute "TRUNCATE TABLE kol_v_monthly"'
        )
    )

    clear_reel = BashOperator(
        task_id='clear_kol_reel_stream',
        bash_command=(
        'cd /opt/trino && '
        './trino --server http://trino:8080 '
        '--catalog iceberg --schema db1 '
        '--user admin '
        '--execute "TRUNCATE TABLE kol_v_monthly"'
        )
    )

    clear_comment = BashOperator(
        task_id='clear_kol_comment_stream',
        bash_command=(
        'cd /opt/trino && '
        './trino --server http://trino:8080 '
        '--catalog iceberg --schema db1 '
        '--user admin '
        '--execute "TRUNCATE TABLE kol_v_monthly"'
        )
    )

    sentiment >> metric >> [ clear_post, clear_reel, clear_comment ]
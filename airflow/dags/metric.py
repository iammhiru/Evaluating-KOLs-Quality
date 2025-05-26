from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kol_metrics_monthly',
    default_args=default_args,
    start_date=datetime(2025,5,1),
    schedule_interval='@monthly',
    catchup=False,
) as dag:

    submit_metrics = SparkSubmitOperator(
        task_id='run_monthly_metrics',
        application='/opt/airflow/dags/spark/ss/batch/metric.py',     
        conn_id='spark_default',                    
        total_executor_cores=1,
        executor_cores=1,
        executor_memory='3g',
        driver_memory='3g',
        name='kol-metrics-monthly',
    )

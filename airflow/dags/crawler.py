from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'kol-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def call_flask_api():
    url = "http://crawler:5000/crawl"
    payload = {
        "post_limit": 5,
        "reel_limit": 2
    }

    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status() 

        data = response.json()
        print("✅ API response:", data)

    except requests.exceptions.RequestException as e:
        raise Exception(f"❌ API call failed: {str(e)}")

with DAG(
    dag_id='trigger_kol_crawler_api',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    start_date=datetime(2025, 6, 10),
    catchup=False,
    tags=['kol', 'crawler']
) as dag:

    trigger_crawler = PythonOperator(
        task_id='call_crawler_api',
        python_callable=call_flask_api
    )

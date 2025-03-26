from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

# ดึง API Key จาก Airflow Variables
api_key = Variable.get("air_quality_api_key")

def print_api_key():
    print(f"My API Key: {api_key}")  # อย่าใช้ใน Production เพราะอาจรั่วไหล

# ตั้งค่า DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 1
}

with DAG(
    dag_id="test_api_key",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_print_api_key = PythonOperator(
        task_id="print_api_key",
        python_callable=print_api_key
    )

    task_print_api_key
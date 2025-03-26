from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
import os
from airflow.models import Variable
from datetime import datetime

# ตั้งค่าพื้นฐาน
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

# ฟังก์ชันดึงข้อมูลจาก API
def _get_air_quality_data():
    API_KEY = Variable.get("air_quality_api_key")
    url = f"http://api.airvisual.com/v2/city?city=Bang Bon&state=Bangkok&country=Thailand&key={API_KEY}"
    print(f"Requesting URL: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()  # ตรวจสอบ status code และ raise exception หากมี error
        print("Response Headers:", response.headers)
        data = response.json()
        print("API Response Data: ", data)

        if response.status_code != 200 or data.get("status") != "success":
            raise ValueError(f"API request failed: {data.get('data', {}).get('message', 'Unknown error')}")

        with open("/tmp/air_quality_data.json", "w") as f:
            json.dump(data, f)

        return data

    except requests.exceptions.RequestException as e:
        print(f"API Request Error: {e}")
        raise ValueError(f"API request failed: {e}")

# ฟังก์ชันตรวจสอบข้อมูล
def _validate_data():
    try:
        with open(f"{DAG_FOLDER}/air_quality_data.json", "r") as f:
            data = json.load(f)

        # ตรวจสอบว่า key 'data' มีอยู่ในข้อมูลที่ได้รับ
        if data.get("data") is None:
            raise ValueError("No 'data' key found in API response")

        # ตรวจสอบว่า key 'current' มีอยู่ใน 'data'
        if data["data"].get("current") is None:
            raise ValueError("No 'current' key found in 'data'")

        print("Data validated successfully.")
    except Exception as e:
        print(f"Validation failed: {e}")
        print("Received data:", data)  # แสดงข้อมูลที่ได้รับจาก API
        raise e

# ตั้งค่าพารามิเตอร์ของ DAG
default_args = {
    "email": ["your-email@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    'aqi_data_pipeline',
    default_args=default_args,
    description='DAG for AQI data',
    schedule_interval='@daily',
    start_date=days_ago(1),
) as dag:

    # Task สำหรับการดึงข้อมูลจาก API
    get_air_quality_data = PythonOperator(
        task_id='get_air_quality_data',
        python_callable=_get_air_quality_data,  # ฟังก์ชันดึงข้อมูล
        provide_context=True,
    )

    # Task สำหรับการตรวจสอบข้อมูล
    validate_data_task = PythonOperator(
        task_id='validate_data_task',
        python_callable=_validate_data,  # ฟังก์ชันตรวจสอบข้อมูล
        provide_context=True,
    )

    # กำหนดลำดับการทำงาน
    get_air_quality_data >> validate_data_task  # Task validate_data_task จะทำงานหลังจาก get_air_quality_data
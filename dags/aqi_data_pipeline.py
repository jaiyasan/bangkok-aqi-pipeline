from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
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
    url = f"http://api.airvisual.com/v2/city?city=Chatuchak&state=Bangkok&country=Thailand&key={API_KEY}"
    print(f"Requesting URL: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if response.status_code != 200 or data.get("status") != "success":
            raise ValueError(f"API request failed: {data.get('data', {}).get('message', 'Unknown error')}")

        # Save JSON data
        with open(f"{DAG_FOLDER}/air_quality_data.json", "w") as f:
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

        if data.get("data") is None or data["data"].get("current") is None:
            raise ValueError("Invalid API data structure")

        print("Data validated successfully.")

    except Exception as e:
        print(f"Validation failed: {e}")
        raise e

# ฟังก์ชันสร้างตารางใน PostgreSQL
def _create_air_quality_table():
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS air_quality_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(50),
        state VARCHAR(50),
        country VARCHAR(50),
        aqi INTEGER,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
    print("Table created successfully.")


def _load_data_to_postgres():
    try:
        # อ่านข้อมูลที่แปลงแล้วจากไฟล์ที่ถูกต้อง
        temp_file_path = f"{DAG_FOLDER}/air_quality_data.json"  # ใช้ไฟล์ที่ถูกต้อง
        with open(temp_file_path, "r") as f:
            data = json.load(f)

        # ตรวจสอบข้อมูลที่ถูกแปลง
        print(f"Data to insert: {json.dumps(data, indent=2)}")

        # เชื่อมต่อกับฐานข้อมูล PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn", schema="postgres")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        # แทรกข้อมูลลงในตาราง
        sql = """
        INSERT INTO air_quality_data (city, state , country, aqi, timestamp)
        VALUES (%s, %s, %s, %s,%s)
        """
        # แทรกข้อมูลจากข้อมูลที่ได้รับจาก API
        cursor.execute(sql, (
            data["data"]["city"],  # ดึงชื่อเมืองจาก API
            data["data"]["state"],  # ดึงชื่อรัฐจาก API
            data["data"]["country"],  # ดึงชื่อประเทศจาก API
            data["data"]["current"]["pollution"]["aqius"],  # AQI จากข้อมูล
            datetime.strptime(data["data"]["current"]["pollution"]["ts"], "%Y-%m-%d %H:%M:%S.%f")  # Timestamp
        ))

        connection.commit()
        print("Data loaded into PostgreSQL successfully.")
        
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
        connection.rollback()
    
    finally:
        cursor.close()
        connection.close()

default_args = {
    "email": ["apologize.bow@gmail.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    'aqi_data_pipeline',
    default_args=default_args,
    description='DAG for AQI data',
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    get_air_quality_data = PythonOperator(
        task_id='get_air_quality_data',
        python_callable=_get_air_quality_data,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data_task',
        python_callable=_validate_data,
    )

    create_air_quality_table = PythonOperator(
        task_id="create_air_quality_table",
        python_callable=_create_air_quality_table,
    )


    # Task สำหรับการโหลดข้อมูลไปยัง PostgreSQL
    load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=_load_data_to_postgres,  # ฟังก์ชันโหลดข้อมูล
        provide_context=True,
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["kan@odds.team"],
        subject="Finished getting open weather data",
        html_content="Done",
    )

    end = EmptyOperator(task_id="end")

    # ลำดับการทำงาน
    start >> get_air_quality_data >> validate_data_task >> create_air_quality_table  >>  load_data_to_postgres >> send_email >> end
# type: ignore
import os
import json
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3

load_dotenv("/opt/airflow/dags/.env")

 
AWS_LOCAL_DIR = os.getenv("AWS_LOCAL_DIR", "/tmp/aws_downloads")
DEFAULT_S3_BUCKET = os.getenv("DEFAULT_S3_BUCKET")
PROCESSED_FILE_TRACKER = os.path.join(AWS_LOCAL_DIR, "processed_files.json")

 
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

FILENAME_TO_TABLE = {
    "patient": "patient",
    "doctor": "doctor",
    "hospital": "hospital",
    "appointment_events": "appointment_events"
}

def load_new_s3_files(**kwargs):
    """
    Poll S3, find new parquet files in raw/* folders, and load them incrementally into Snowflake.
    """
    os.makedirs(AWS_LOCAL_DIR, exist_ok=True)
    s3_client = boto3.client("s3")

     
    if os.path.exists(PROCESSED_FILE_TRACKER):
        with open(PROCESSED_FILE_TRACKER, "r") as f:
            processed_files = set(json.load(f))
    else:
        processed_files = set()

     
    paginator = s3_client.get_paginator("list_objects_v2")
    all_files = []
    for page in paginator.paginate(Bucket=DEFAULT_S3_BUCKET, Prefix="raw/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and key not in processed_files:
                all_files.append(key)

    if not all_files:
        print("No new parquet files found.")
        return

     
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    for key in all_files:
        filename = os.path.basename(key)
        local_file = os.path.join(AWS_LOCAL_DIR, filename)

         
        print(f"Downloading {key}")
        s3_client.download_file(DEFAULT_S3_BUCKET, key, local_file)

         
        folder = key.split("/")[1]   
        table_name = FILENAME_TO_TABLE.get(folder)
        if not table_name:
            print(f"Skipping {key}: no matching table")
            os.remove(local_file)
            continue

         
        cur.execute(f"PUT file://{local_file} @%{table_name}")
        cur.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT=(TYPE=PARQUET)
            ON_ERROR='CONTINUE'
        """)
        print(f"Loaded {filename} into {table_name}")

         
        os.remove(local_file)
        processed_files.add(key)

     
    with open(PROCESSED_FILE_TRACKER, "w") as f:
        json.dump(list(processed_files), f)

    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="s3_to_snowflake_incremental_polling",
    default_args=default_args,
    description="Incremental load of new parquet files from S3 to Snowflake",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval="*/1 * * * *",  
) as dag:

    load_task = PythonOperator(
        task_id="load_s3_to_snowflake",
        python_callable=load_new_s3_files,
    )

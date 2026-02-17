# type: ignore
import os
import snowflake.connector
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

 
load_dotenv("/opt/airflow/dags/.env")

 
AWS_LOCAL_DIR = os.getenv("AWS_LOCAL_DIR", "/tmp/aws_downloads")
DEFAULT_S3_BUCKET = os.getenv("DEFAULT_S3_BUCKET")

 
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

 
FILENAME_TO_TABLE = {
    "patient.parquet": "patient",
    "doctor.parquet": "doctor",
    "hospital.parquet": "hospital",
    "appointment_events.parquet": "appointment_events"
}

def load_new_s3_files(**kwargs):
    """
    Download new files from S3 and load into Snowflake.
    """
    s3 = S3Hook(aws_conn_id="aws_default")
    os.makedirs(AWS_LOCAL_DIR, exist_ok=True)

     
    files = s3.list_keys(bucket_name=DEFAULT_S3_BUCKET, prefix="raw/")
    if not files:
        print("No new files found.")
        return

    parquet_files = [f for f in files if f.endswith(".parquet")]
    if not parquet_files:
        print("No parquet files to process.")
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

    for key in parquet_files:
        filename = os.path.basename(key)
        local_file = os.path.join(AWS_LOCAL_DIR, filename)

         
        print(f"Downloading {key} from S3")
        s3_client = s3.get_conn()
        s3_client.download_file(
            Bucket=DEFAULT_S3_BUCKET,
            Key=key,
            Filename=local_file
        )

         
        table_name = None
        for prefix, tbl in FILENAME_TO_TABLE.items():
            if filename.startswith(prefix.replace(".parquet", "")):
                table_name = tbl
                break
        if not table_name:
            print(f"Skipping {filename}: no matching table")
            continue

         
        cur.execute(f"PUT file://{local_file} @%{table_name}")
        print(f"Uploaded {filename} -> @{table_name}")
        cur.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT=(TYPE=PARQUET)
            ON_ERROR='CONTINUE'
        """)
        print(f"Loaded {filename} into {table_name}")

         
        os.remove(local_file)

    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="s3_to_snowflake_incremental",
    default_args=default_args,
    description="Automatically load new S3 Parquet files into Snowflake",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None,  
) as dag:

    
    wait_for_new_file = S3KeySensor(
        task_id="wait_for_new_file",
        bucket_name=DEFAULT_S3_BUCKET,
        bucket_key="raw/*.parquet",
        aws_conn_id="aws_default",
        poke_interval=30,        
        timeout=60 * 30,          
        wildcard_match=True,
    )

    load_task = PythonOperator(
        task_id="load_s3_to_snowflake",
        python_callable=load_new_s3_files,
    )

    wait_for_new_file >> load_task

# import os
# import boto3
# import snowflake.connector
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv("/opt/airflow/dags/.env")

# # -------- AWS / S3 Config --------
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
# AWS_LOCAL_DIR = os.getenv("AWS_LOCAL_DIR", "/tmp/aws_downloads")

# # -------- Snowflake Config --------
# SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
# SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
# SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
# SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
# SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# # -------- Tables List --------
# TABLES = ["patient", "doctor", "hospital", "appointment_events"]

# # -------- Python Callables --------
# def download_from_s3(**kwargs):
#     """
#     Download the specific S3 file triggered by Lambda.
#     Lambda passes s3_bucket and s3_key in DAG run conf.
#     """
#     dag_run = kwargs.get("dag_run")
#     if not dag_run or not dag_run.conf:
#         raise ValueError("No DAG run conf found. Lambda must pass s3_bucket and s3_key.")

#     s3_bucket = dag_run.conf.get("s3_bucket")
#     s3_key = dag_run.conf.get("s3_key")

#     if not s3_bucket or not s3_key:
#         raise ValueError("s3_bucket or s3_key missing in DAG run conf")

#     os.makedirs(AWS_LOCAL_DIR, exist_ok=True)
#     local_file = os.path.join(AWS_LOCAL_DIR, os.path.basename(s3_key))

#     s3 = boto3.client(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#         region_name=AWS_REGION
#     )
#     s3.download_file(s3_bucket, s3_key, local_file)
#     print(f"Downloaded s3://{s3_bucket}/{s3_key} -> {local_file}")

#     # Push local file path to XCom for next task
#     return local_file


# def load_to_snowflake(**kwargs):
#     """
#     Load the downloaded file into Snowflake.
#     """
#     ti = kwargs['ti']
#     local_file = ti.xcom_pull(task_ids="download_s3")

#     if not local_file or not os.path.exists(local_file):
#         raise ValueError(f"Local file not found: {local_file}")

#     table_name = os.path.splitext(os.path.basename(local_file))[0]  # infer table from file name

#     conn = snowflake.connector.connect(
#         user=SNOWFLAKE_USER,
#         password=SNOWFLAKE_PASSWORD,
#         account=SNOWFLAKE_ACCOUNT,
#         warehouse=SNOWFLAKE_WAREHOUSE,
#         database=SNOWFLAKE_DB,
#         schema=SNOWFLAKE_SCHEMA,
#     )
#     cur = conn.cursor()

#     # Upload to Snowflake stage
#     cur.execute(f"PUT file://{local_file} @%{table_name}")
#     print(f"Uploaded {local_file} -> @{table_name} stage")

#     # Copy into table
#     copy_sql = f"""
#         COPY INTO {table_name}
#         FROM @%{table_name}
#         FILE_FORMAT=(TYPE=PARQUET)
#         ON_ERROR='CONTINUE'
#     """
#     cur.execute(copy_sql)
#     print(f"Data loaded into {table_name}")

#     cur.close()
#     conn.close()

#     # Optional: remove local file
#     os.remove(local_file)


# # -------- Airflow DAG --------
# default_args = {
#     "owner": "airflow",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1),
# }

# with DAG(
#     dag_id="s3_to_snowflake_lambda_trigger",
#     default_args=default_args,
#     description="Load S3 parquet to Snowflake when Lambda triggers DAG",
#     start_date=datetime(2025, 1, 1),
#     catchup=False,
#     schedule_interval=None,  # event-driven
# ) as dag:

#     task1 = PythonOperator(
#         task_id="download_s3",
#         python_callable=download_from_s3,
#         provide_context=True
#     )

#     task2 = PythonOperator(
#         task_id="load_snowflake",
#         python_callable=load_to_snowflake,
#         provide_context=True
#     )

#     task1 >> task2



# type: ignore
import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv("/opt/airflow/dags/.env")

# -------- AWS / S3 Config --------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_LOCAL_DIR = os.getenv("AWS_LOCAL_DIR", "/tmp/aws_downloads")
DEFAULT_S3_BUCKET = os.getenv("DEFAULT_S3_BUCKET")  # required for manual trigger

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# -------- File-to-Table Mapping --------
FILENAME_TO_TABLE = {
    "patient.parquet": "patient",
    "doctor.parquet": "doctor",
    "hospital.parquet": "hospital",
    "appointment_events.parquet": "appointment_events"
}

# -------- Python Callables --------
def download_from_s3(**kwargs):
    """
    Download files from S3.
    - Lambda trigger: single file from dag_run.conf
    - Manual trigger: all Parquet files in DEFAULT_S3_BUCKET
    Returns: list of local file paths
    """
    dag_run = kwargs.get("dag_run")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    os.makedirs(AWS_LOCAL_DIR, exist_ok=True)
    files_to_download = []

    if dag_run and dag_run.conf:
        # Lambda-triggered: single file
        s3_bucket = dag_run.conf.get("s3_bucket")
        s3_key = dag_run.conf.get("s3_key")
        if not s3_bucket or not s3_key:
            raise ValueError("s3_bucket or s3_key missing in DAG run conf")
        if not s3_key.endswith(".parquet"):
            print(f"Skipping non-Parquet file: {s3_key}")
            return []
        local_file = os.path.join(AWS_LOCAL_DIR, os.path.basename(s3_key))
        s3.download_file(s3_bucket, s3_key, local_file)
        print(f"Downloaded s3://{s3_bucket}/{s3_key} -> {local_file}")
        files_to_download.append(local_file)

    else:
        # Manual trigger: download all Parquet files
        if not DEFAULT_S3_BUCKET:
            raise ValueError("Manual trigger requires DEFAULT_S3_BUCKET in .env")
        print(f"No DAG run conf found. Scanning bucket {DEFAULT_S3_BUCKET} for Parquet files.")

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=DEFAULT_S3_BUCKET):
            for obj in page.get("Contents", []):
                s3_key = obj['Key']
                if not s3_key.endswith(".parquet"):
                    print(f"Skipping non-Parquet file: {s3_key}")
                    continue
                local_file = os.path.join(AWS_LOCAL_DIR, os.path.basename(s3_key))
                s3.download_file(DEFAULT_S3_BUCKET, s3_key, local_file)
                print(f"Downloaded s3://{DEFAULT_S3_BUCKET}/{s3_key} -> {local_file}")
                files_to_download.append(local_file)

    return files_to_download


def load_to_snowflake(**kwargs):
    """
    Load downloaded Parquet files into Snowflake.
    """
    ti = kwargs['ti']
    local_files = ti.xcom_pull(task_ids="download_s3")

    if not local_files:
        print("No Parquet files to load into Snowflake. Exiting.")
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

    for local_file in local_files:
        if not os.path.exists(local_file):
            print(f"Skipping missing file: {local_file}")
            continue

        filename = os.path.basename(local_file)    
        table_name = None
        for prefix, tbl in FILENAME_TO_TABLE.items():
            if filename.startswith(prefix.replace(".parquet", "")):
                table_name = tbl
                break

        if not table_name:
            print(f"Skipping {filename}: no matching table")
            continue

        # Upload to Snowflake stage
        cur.execute(f"PUT file://{local_file} @%{table_name}")
        print(f"Uploaded {local_file} -> @{table_name} stage")

        # Copy into table
        copy_sql = f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT=(TYPE=PARQUET)
            ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table_name}")

        # Optional: remove local file
        os.remove(local_file)

    cur.close()
    conn.close()


# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="s3_to_snowflake_lambda_trigger",
    default_args=default_args,
    description="Load S3 Parquet to Snowflake (Lambda or manual)",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval=None,
) as dag:

    task1 = PythonOperator(
        task_id="download_s3",
        python_callable=download_from_s3,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True
    )

    task1 >> task2

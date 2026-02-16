from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import snowflake.connector
from dotenv import load_dotenv

def test_snowflake_connection():
    # Load .env file
    load_dotenv("/opt/airflow/dags/.env")  

    print("Testing Snowflake connection...")

    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    # Check missing vars
    missing = [k for k,v in {
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_ACCOUNT": account,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_DB": database,
        "SNOWFLAKE_SCHEMA": schema
    }.items() if not v]

    if missing:
        raise ValueError(f"Missing environment variables: {missing}")

    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()
        print("Snowflake Version:", version)
        cur.close()
        conn.close()
        print("Connection successful!")

    except Exception as e:
        print("Connection failed:", e)
        raise

# DAG definition
default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="test_snowflake_connection",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_conn_task = PythonOperator(
        task_id="test_connection",
        python_callable=test_snowflake_connection
    )

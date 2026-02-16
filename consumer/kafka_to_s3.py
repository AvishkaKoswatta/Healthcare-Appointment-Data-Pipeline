import os
import json
import boto3
import io
from kafka import KafkaConsumer
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import atexit
import traceback
import posixpath

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

print("===== ENVIRONMENT CHECK =====")
print("KAFKA_BOOTSTRAP:", os.getenv("KAFKA_BOOTSTRAP"))
print("KAFKA_GROUP:", os.getenv("KAFKA_GROUP"))
print("S3_BUCKET:", os.getenv("S3_BUCKET"))
print("AWS_REGION:", os.getenv("AWS_REGION"))
print("S3_PREFIX:", os.getenv("S3_PREFIX"))
print("=============================")

# -----------------------------
# Normalize S3 Prefix
# -----------------------------
raw_prefix = os.getenv("S3_PREFIX", "").strip()

if raw_prefix:
    prefix = raw_prefix.strip("/")  # remove leading/trailing slashes
else:
    prefix = ""

# -----------------------------
# Kafka Consumer
# -----------------------------
topics = [
    'healthcare_server.public.patient',
    'healthcare_server.public.doctor',
    'healthcare_server.public.hospital',
    'healthcare_server.public.appointment_events'
]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    group_id=os.getenv("KAFKA_GROUP"),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Connected to Kafka.")

# -----------------------------
# AWS S3 Client
# -----------------------------
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

bucket = os.getenv("S3_BUCKET")

# -----------------------------
# Test S3 Connection
# -----------------------------
print("Testing S3 connection...")
try:
    s3.put_object(Bucket=bucket, Key="debug_test.txt", Body="hello")
    print("S3 connection successful")
except Exception:
    print("S3 connection FAILED:")
    traceback.print_exc()

# -----------------------------
# Batch Configuration
# -----------------------------
batch_size = 1

buffer = {topic: [] for topic in topics}

print("Streaming CDC events...")

# -----------------------------
# Upload Function
# -----------------------------
def upload_to_s3(table_name, records):
    print(f"Uploading {len(records)} records for {table_name}...")

    try:
        df = pd.DataFrame(records)

        if df.empty:
            print("⚠ DataFrame empty. Skipping upload.")
            return

        date_str = datetime.now().strftime('%Y-%m-%d')
        timestamp_str = datetime.now().strftime('%H%M%S%f')

        # Build S3 key safely
        if prefix:
            s3_key = posixpath.join(
                prefix,
                table_name,
                f"date={date_str}",
                f"{table_name}_{timestamp_str}.parquet"
            )
        else:
            s3_key = posixpath.join(
                table_name,
                f"date={date_str}",
                f"{table_name}_{timestamp_str}.parquet"
            )

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="fastparquet", index=False)
        parquet_buffer.seek(0)

        s3.upload_fileobj(parquet_buffer, bucket, s3_key)

        print(f"SUCCESS: Uploaded to s3://{bucket}/{s3_key}")

    except Exception:
        print("ERROR during upload:")
        traceback.print_exc()

# -----------------------------
# Flush Remaining Records on Exit
# -----------------------------
def flush_all():
    print("Flushing remaining records...")
    for topic, records in buffer.items():
        if records:
            table_name = topic.split('.')[-1]
            upload_to_s3(table_name, records)

atexit.register(flush_all)

# -----------------------------
# Consume CDC Events
# -----------------------------
try:
    for message in consumer:

        print("\n---- NEW MESSAGE ----")
        print("Topic:", message.topic)
        print("Offset:", message.offset)

        event = message.value
        payload = event.get("payload")

        if not payload:
            print("No payload found.")
            continue

        op = payload.get("op")
        after = payload.get("after")
        before = payload.get("before")

        topic = message.topic

        if topic not in buffer:
            print(f"Unknown topic {topic}. Skipping.")
            continue

        # Handle CDC operations
        if op in ("c", "u", "r") and after:
            buffer[topic].append(after)

        elif op == "d" and before:
            before["_deleted"] = True
            buffer[topic].append(before)

        else:
            print("Invalid CDC operation. Skipping.")
            continue

        print(f"Buffer size for {topic}: {len(buffer[topic])}")

        # Trigger batch upload
        if len(buffer[topic]) >= batch_size:
            print("Batch size reached. Uploading...")
            table_name = topic.split('.')[-1]
            upload_to_s3(table_name, buffer[topic])
            buffer[topic] = []

except Exception:
    print("Consumer crashed:")
    traceback.print_exc()

# Automatically registers a Debezium PostgreSQL CDC connector in Kafka Connect 
# using REST API.

import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "healthcare_server",
        "table.include.list": "public.patient,public.doctor,public.hospital, public.appointment_events",
        "plugin.name": "pgoutput",
        "slot.name": "healthcare_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("Connector created successfully!")
elif response.status_code == 409:
    print("Connector already exists.")
else:
    print(f"Failed to create connector ({response.status_code}): {response.text}")
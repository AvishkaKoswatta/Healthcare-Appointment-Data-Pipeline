# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user
USER airflow

# Install dbt packages with pinned versions
RUN pip install --no-cache-dir dbt-core==1.11.5 dbt-snowflake==1.11.2

# Install Amazon provider for Airflow with pinned version
RUN pip install --no-cache-dir apache-airflow-providers-amazon==7.4.0

FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt lives in its OWN virtualenv so its dependencies can never clash with
# Airflow's. The dbt_transform DAG invokes /opt/airflow/dbt-venv/bin/dbt.
RUN python -m venv /opt/airflow/dbt-venv && \
    /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.8.2

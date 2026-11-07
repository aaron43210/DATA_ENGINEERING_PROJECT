# Python 3.11: dbt 1.11 and PyIceberg need >= 3.10; great-expectations 0.18 needs <= 3.11.
FROM apache/airflow:2.9.1-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt and PyIceberg get their own virtualenvs so their dependencies cannot clash
# with Airflow's. dbt-core is pinned too: dbt-postgres alone lets it float.
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.17 dbt-postgres==1.11.0
RUN python -m venv /opt/airflow/iceberg-venv \
 && /opt/airflow/iceberg-venv/bin/pip install --no-cache-dir "pyiceberg[pyarrow]==0.10.0" boto3==1.34.69

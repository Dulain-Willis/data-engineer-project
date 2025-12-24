FROM apache/airflow:2.10.3

USER airflow
COPY infra/docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

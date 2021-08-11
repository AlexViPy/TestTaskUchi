FROM apache/airflow:2.0.2-python3.8

USER airflow
RUN pip install --no-cache-dir \
  --upgrade-strategy only-if-needed \
  --user -r /airflow/requirements.txt
RUN pip install --no-cache-dir airflow-clickhouse-plugin
import airflow
from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


ch_hook = ClickHouseHook()

def json_data_to_raw_table():
    ch_hook.run('CREATE DATABASE IF NOT EXISTS USERS')
    ch_hook.run('DROP TABLE IF EXISTS USERS.sessions_raw')
    with open('/usr/local/airflow/scripts/create_table_raw_json_data.sql', 'r') as file:
        create_table_raw_json_data = file.read().replace('\n', '')
        file.close()
    ch_hook.run(create_table_raw_json_data)
    with open('/usr/local/airflow/scripts/load_raw_data.sql', 'r') as file:
        load_raw_data = file.read().replace('\n', '')
        file.close()
    ch_hook.run(load_raw_data)


def json_data_to_target_table():
    ch_hook.run('DROP TABLE IF EXISTS USERS.sessions')  # this is useful only for debugging
    with open('/usr/local/airflow/scripts/create_target_table.sql', 'r') as file:
        create_target_table = file.read().replace('\n', '')
        file.close()
    ch_hook.run(create_target_table)
    ch_hook.run('INSERT INTO USERS.sessions SELECT * FROM USERS.sessions_raw')


default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
        dag_id='load_json_file',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False
) as dag:
    json_data_to_raw_table = PythonOperator(
        task_id='json_data_to_raw_table',
        python_callable=json_data_to_raw_table,
    )

    json_data_to_target_table = PythonOperator(
        task_id='json_data_to_target_table',
        python_callable=json_data_to_target_table,
    )

    json_data_to_raw_table >> json_data_to_target_table

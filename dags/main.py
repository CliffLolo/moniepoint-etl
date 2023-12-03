from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.etl import run_script

default_args = {
    'owner': 'Clifford Frempong',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moniepoint-etl-dag',
    default_args=default_args,
    description='This Airflow DAG orchestrates the execution of an ETL process. The ETL process extracts data from a ClickHouse database, performs necessary transformations, and loads the results into a SQLite database.',
     schedule_interval=None,
)

run_etl = PythonOperator(
    task_id='execute_script',
    python_callable=run_script,
    dag=dag,
)

run_etl
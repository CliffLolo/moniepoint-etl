from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.etl import extract_data, insert_data

default_args = {
    'owner': 'Clifford Frempong',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('moniepoint-etl-dag', start_date=datetime(2022, 1, 1), default_args=default_args, description='This Airflow DAG orchestrates the execution of an ETL process. The ETL process extracts data from a ClickHouse database, performs necessary transformations, and loads the results into a SQLite database.',
        schedule_interval=None, catchup=False) as dag:

    # Task 1: Extract data
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    # Task 2: Load data
    load = PythonOperator(
        task_id='load',
        python_callable=insert_data, 
        op_args=[extract.output],  
        provide_context=True,
    )

    extract >> load

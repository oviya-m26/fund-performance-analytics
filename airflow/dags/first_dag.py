from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

def run_etl():
    print("ETL process would run here")
    return "ETL completed successfully"

with DAG(
    dag_id='efs_to_oracle_eod_etl',
    default_args=default_args,
    description='Load files from EFS to Oracle based on MIG_PROCESS_SETUP',
    schedule=None,  
    catchup=False,
    tags=['oracle', 'etl'],
) as dag:

    etl_task = PythonOperator(
        task_id='run_eod_etl',
        python_callable=run_etl,
    )

    etl_task

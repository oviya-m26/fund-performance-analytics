from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime

with DAG(
    dag_id="my_dag2",
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
) as dag:
    task1=BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello World!'"
    )
    task2 = BashOperator(
        task_id="list_files",
        bash_command="ls -l"
    )
    task1>>task2

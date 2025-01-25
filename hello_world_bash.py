from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator


with DAG(
    dag_id = 'HelloWorld_Bash',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *'
) as dag:
    hello = BashOperator(
        task_id='t1',
        bash_command='echo "hello!"'
    )

    goodbye = BashOperator(
        task_id='t2',
        bash_command='echo "goodbye!"'
    )

    hello >> goodbye

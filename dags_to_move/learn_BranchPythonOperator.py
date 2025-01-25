from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime


dag = DAG(
    dag_id='Learn_BranchPythonOperator',
    schedule='@daily',
    start_date=datetime(2025, 1, 19),
    catchup=False
)


def decide_branch(**context):
    current_hour = datetime.now().hour
    print(f"current_hour: {current_hour}")
    if current_hour < 12:
        return 'morning_task'
    else:
        return 'afternoon_task'


branching_operator = BranchPythonOperator(
    task_id='branching_task',
    python_callable=decide_branch,
    dag=dag
)


morning_task = EmptyOperator(
    task_id='morning_task',
    dag=dag
)


afternoon_task = EmptyOperator(
    task_id='afternoon_task',
    dag=dag
)

branching_operator >> morning_task
branching_operator >> afternoon_task

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'TargetDag',
    schedule='@once',  # 스케줄을 세팅하지 않음
    catchup=False,
    start_date=datetime(2025, 1, 19),
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="""echo '{{ ds }}, {{ dag_run.conf.get("path", "none") }}' """
    )

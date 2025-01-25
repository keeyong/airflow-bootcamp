from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG 정의
dag = DAG(
    'Learn_Jinja',
    schedule='0 0 * * *',  # 매일 실행
    start_date=datetime(2025, 1, 19),
    catchup=False
)

# BashOperator를 사용하여 템플릿 작업 정의
task1 = BashOperator(
    task_id='task1',
    bash_command='echo "{{ ds }}"',
    dag=dag
)

# 동적 매개변수가 있는 다른 템플릿 작업 정의
task2 = BashOperator(
    task_id='task2',
    bash_command='echo "안녕하세요, {{ params.name }}!"',
    params={'name': 'John'},  # 사용자 정의 가능한 매개변수
    dag=dag
)

task3 = BashOperator(
    task_id='task3',
    bash_command="""echo "{{ dag }}, {{ task }}, {{ var.value.get('country_capital_url') }}" """,
    dag=dag
)

task1 >> task2 >> task3

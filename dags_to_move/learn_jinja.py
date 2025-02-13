from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# with 문법을 사용한 DAG 정의
with DAG(
    'Learn_Jinja',
    schedule='@daily',  # 매일 실행
    start_date=datetime(2025, 1, 19),
    catchup=False
) as dag:

    # logical_date 출력
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "{{ ds }}"'
    )

    # 동적 매개변수를 출력
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "안녕하세요, {{ params.name }}!"',
        params={'name': 'John'}  # 사용자 정의 가능한 매개변수
    )

    # airflow 객체들을 출력
    task3 = BashOperator(
        task_id='task3',
        bash_command="""echo "{{ dag }}, {{ task }}, {{ var.value.get('country_capital_url') }}" """
    )

    # 태스크 의존성 설정
    task1 >> task2 >> task3

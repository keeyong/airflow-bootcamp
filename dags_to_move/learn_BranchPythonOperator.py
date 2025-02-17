from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime


def check_weekend(**context):
    # 실행 날짜의 요일을 확인 (0:월요일 ~ 6:일요일)
    execution_date = context['execution_date']
    weekday = execution_date.weekday()
    
    # 요일 이름 매핑
    day_names = {
        0: '월요일',
        1: '화요일',
        2: '수요일',
        3: '목요일',
        4: '금요일',
        5: '토요일',
        6: '일요일'
    }
    
    # 현재 요일 출력
    print(day_names[weekday])
    
    # 주말(토,일)이면 weekend_task, 평일이면 weekday_task 반환
    if weekday in [5, 6]:  # 토요일(5) 또는 일요일(6)
        return 'weekend_task'
    return 'weekday_task'


with DAG(
    dag_id='Learn_BranchPythonOperator',
    schedule='@daily',
    start_date=datetime(2025, 1, 19),
    catchup=False
) as dag:
    branching_operator = BranchPythonOperator(
        task_id='branching_task',
        python_callable=check_weekend,
    )

    weekend_task = EmptyOperator(
        task_id='weekend_task'
    )

    weekday_task = EmptyOperator(
        task_id='weekday_task'
    )

branching_operator >> weekend_task
branching_operator >> weekday_task

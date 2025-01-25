from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime
from datetime import timedelta

with DAG(
   dag_id='Learn_LatestOnlyOperator',
   schedule=timedelta(hours=48),    # 매 48시간마다 실행되는 DAG로 설정
   start_date=datetime(2025, 1, 19),
   catchup=True) as dag:

   t1 = EmptyOperator(task_id='task1')
   t2 = LatestOnlyOperator(task_id='latest_only')
   t3 = EmptyOperator(task_id='task3')
   t4 = EmptyOperator(task_id='task4')

   t1 >> t2 >> [t3, t4]

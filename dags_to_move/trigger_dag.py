from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='TriggerDag',
    start_date=datetime(2025, 1, 19),
    catchup=False,
    schedule='@daily'
) as dag:
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_dag_id='TargetDag',
        conf={'path': '/opt/ml/conf'},
        logical_date='{{ ds }}',
        reset_dag_run=True
    )

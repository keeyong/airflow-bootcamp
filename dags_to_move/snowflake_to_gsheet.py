from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from helpers import gsheet
from datetime import datetime


@task
def update_gsheet(sheet_filename, sheetgid, sql):
    gsheet.update_sheet("google_sheet_access_token", sheet_filename, sheetgid, sql, "snowflake_conn")


with DAG(
    dag_id = 'Snowflake_to_Gsheet',
    start_date = datetime(2025,3,14),
    catchup=False,
    tags=['example'],
    schedule = '@once'
) as dag:
    update_gsheet(
        "구글스프레드시트-테스팅",
        "SnowflakeToSheet",
        """
SELECT 
  LEFT(created, 7) month,   
  AVG(score) nps
FROM dev.raw_data.nps
GROUP BY 1
ORDER BY 1;"""
    )

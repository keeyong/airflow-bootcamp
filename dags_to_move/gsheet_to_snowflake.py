"""
 - 아래 2개의 모듈 설치가 별도로 필요합니다.
  - pip3 install oauth2client
  - pip3 install gspread
"""
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from datetime import datetime
from helpers import gsheet
from helpers import util

import requests
import logging
import psycopg2
import json


@task
def download_tab_in_gsheet(url, tab, schema, table):
    tmp_dir = Variable.get("data_dir")  # ends with "/"
    sheet_api_credential_key = "google_sheet_access_token"
    file_path = util.get_file_path(tmp_dir, table, get_current_context())

    gsheet.get_google_sheet_to_csv(
        url,
        tab,
        file_path,
        sheet_api_credential_key
    )

    try:
        cur = util.return_snowflake_conn("snowflake_conn")
        # 이미 database는dev로 연결되어 있음
        cur.execute(f"USE SCHEMA {schema};")

        cur.execute(f"""
          CREATE TABLE IF NOT EXISTS {schema}.{table} (
            col1 int,
            col2 int,
            col3 int,
            col4 int
          );
        """)

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table}")
        # Stage와 COPY INTO를 사용하여 앞서 구글 시트로부터 다운로드 받은 파일을 테이블로 적재
        util.populate_table_via_stage(cur, table, file_path)     
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise


sheets = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1wogD6LSwMrNZHa5e0KP4X3mYY10viW8Sr_HBwxGrufc/",
        "tab": "SheetToSnowflake",
        "schema": "raw_data",
        "table": "gsheet_copy"
    }
]

with DAG(
    dag_id = 'Gsheet_to_Snowflake',
    start_date = datetime(2025,3,14),
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    max_active_tasks = 1,
    catchup = False
) as dag:

    for sheet in sheets:
        download_tab_in_gsheet(sheet["url"], sheet["tab"], sheet["schema"], sheet["table"])

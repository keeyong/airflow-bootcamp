from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta
from helpers import util

import pandas as pd
import pytz
import requests
import yfinance as yf


@task
def extract(symbol, debug=True):
    data = yf.download(symbol, start="2024-01-01")
    data.reset_index(inplace=True, names=['Date'])
    # symbol 하나만 다루기에 ticker 레벨 제거
    data.columns = data.columns.droplevel(1)
    if debug:
        print(data.head())

    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, symbol, get_current_context())
    data.to_csv(file_path, index=False)  # 데이터를 CSV로 저장
    return file_path  # 파일 경로만 반환


@task
def load(file_path, symbol, schema, table):
    tmp_dir = Variables.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, symbol, get_current_context())

    cur = util.return_snowflake_conn("snowflake_conn")

    try:
        cur.execute(f"USE SCHEMA {schema};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar
        )""")

        cur.execute("BEGIN;")
        delete_sql = f"DELETE FROM {table}"
        logging.info(delete_sql)
        cur.execute(delete_sql)

        # Internal Table Stage로 파일을 업로드
        util.populate_table_via_stage(cur, table, file_path)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        # 연결 닫기
        cur.close()


with DAG(
    dag_id='YfinanceToSnowflake_fullrefresh',
    description="Business Owner: xyz, Copy Apple stock info to Snowflake",
    start_date=datetime(2025,1,14),
    catchup=False,
    tags=['ETL', 'fullrefresh'],
    schedule = '30 1 * * *'
) as dag:

    schema = "raw_data"
    table = "stock_price"
    symbol = "AAPL"

    file_path = extract(symbol)
    load(file_path, symbol, schema, table)

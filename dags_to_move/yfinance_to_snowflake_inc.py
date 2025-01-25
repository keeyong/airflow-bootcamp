# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
from helpers import util

import logging
import pandas as pd
import yfinance as yf


@task
def extract(symbol, debug=True):
    today = datetime.today().strftime('%Y-%m-%d')  # 먼저 오늘 날짜를 계산
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # 어제 날짜를 계산

    # yesterday의 값을 읽어와서 data 데이터프레임에 저장
    if debug:
        print(yesterday, today)
    data = yf.download(symbol, start=yesterday, end=today)

    """
    data 데이터프레임 내용 정리
    """
    data.reset_index(inplace=True, names=['Date'])
    data.columns = data.columns.droplevel(1) # symbol 하나만 다루기에 ticker 레벨 제거
    if debug:
        print(data.head())

    """
    data 데이터 프레임을 파일로 저장
    """
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = f"{tmp_dir}{symbol}_{yesterday}.csv"
    data.to_csv(file_path, index=False)  # 데이터를 CSV로 저장
    return file_path


@task
def load(symbol, schema, table):
    cur = util.return_snowflake_conn("snowflake_conn")

    # 먼저 오늘 날짜를 계산
    today = datetime.today().strftime('%Y-%m-%d')
    # 어제 날짜를 계산
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # extract에서 만들어진 파일 경로 생성
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = f"{tmp_dir}{symbol}_{yesterday}.csv"

    try:
        cur.execute(f"USE SCHEMA {schema};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar 
        )""")

        cur.execute("BEGIN;")
        delete_sql = f"DELETE FROM {table} WHERE date='{yesterday}'"
        logging.info(delete_sql)
        cur.execute(delete_sql)

        # 루프를 돌기는 하지만 사실 하나의 레코드 혹은 레코드가 없는 것을 예상
        # date_to_process 날짜가 휴일인 경우에는 아무런 레코드도 존재하지 않음
        df = pd.read_csv(file_path)
        for index, row in df.iterrows():
            sql = f"""INSERT INTO {table} (date, open, close, high, low, volume, symbol) VALUES (
            '{row["Date"]}', {row['Open']}, {row['Close']}, {row['High']}, {row['Low']}, {row['Volume']}, '{symbol}')"""
            logging.info(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id='YfinanceToSnowflake_inc',
    description="Business Owner: xyz, Copy Apple stock info to Snowflake",
    start_date=datetime(2025,1,14),
    catchup=False,
    tags=['ETL', 'incremental'],
    schedule = '30 1 * * *'
) as dag:

    schema = "raw_data"
    table = "stock_price"
    symbol = "AAPL"

    extract(symbol) >> load(symbol, schema, table)

# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
from helpers import util

import pandas as pd
import yfinance as yf


@task
def extract(symbol, debug=True):
    date_to_process = str(util.get_logical_date(get_current_context()))[:10]  # Airflow에게 어느 날짜의 데이터를 읽을지 문의
    following_day = util.get_next_day(date_to_process)   # 그 다음날 계산

    # date_to_process의 값을 읽어와서 data 데이터프레임에 저장
    if debug:
        print(date_to_process, following_day)
    data = yf.download(symbol, start=date_to_process, end=following_day)

    """
    data 데이터프레임 내용 클린업
    """
    data.reset_index(inplace=True, names=['Date'])
    data.columns = data.columns.droplevel(1)  # symbol 하나만 다루기에 ticker 레벨 제거
    if debug:
        print(data.head())

    """
    data 데이터프레임 내용을 파일로 저장
    """
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, symbol, get_current_context())
    data.to_csv(file_path, index=False)  # 데이터를 CSV로 저장

    return file_path  # 파일 경로만 반환


@task
def load(symbol, schema, table):
    cur = util.return_snowflake_conn("snowflake_conn")

    date_to_process = str(util.get_logical_date(get_current_context()))[:10]
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, symbol, get_current_context())

    """ Airflow의 읽어올 데이터의 날짜와 시간 관리를 위해 몇 개의 DAG RUN 변수 출력 """
    context = get_current_context()
    print("logical_date", context["logical_date"])
    print("data_interval_start", context["data_interval_start"])
    print("data_interval_end", context["data_interval_end"])

    try:
        cur.execute(f"USE SCHEMA {schema};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar 
        )""")

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {table} WHERE date='{date_to_process}'")

        # 루프를 돌기는 하지만 사실 하나의 레코드 혹은 레코드가 없는 것을 예상
        # date_to_process 날짜가 휴일인 경우에는 아무런 레코드도 존재하지 않음
        df = pd.read_csv(file_path)
        for index, row in df.iterrows():
            sql = f"""INSERT INTO {table} (date, open, close, high, low, volume, symbol) VALUES (
            '{row["Date"]}', {row['Open']}, {row['Close']}, {row['High']}, {row['Low']}, {row['Volume']}, '{symbol}')"""
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id='YfinanceToSnowflake_inc_v2',
    description="Business Owner: xyz, Copy Apple stock info to Snowflake",
    start_date=datetime(2025,1,14),
    catchup=False,
    tags=['ETL', 'incremental'],
    schedule='30 1 * * *'
) as dag:

    schema = "raw_data"
    table = "stock_price"
    symbol = "AAPL"

    extract(symbol) >> load(symbol, schema, table)

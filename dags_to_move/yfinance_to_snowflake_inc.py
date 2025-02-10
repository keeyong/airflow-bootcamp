from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta
from helpers import util

import logging
import os
import pandas as pd
import yfinance as yf


def get_today_yesterday():
    """
    - 지금 실행 시간을 기준으로 어제 날짜를 계산
    """
    today = datetime.today().strftime('%Y-%m-%d')  # 먼저 오늘 날짜를 계산
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')  # 어제 날짜를 계산

    return today, yesterday


@task
def extract(symbol, debug=True):

    (today, yesterday) = get_today_yesterday()

    # yesterday의 값을 읽어와서 data 데이터프레임에 저장
    if debug:
        print(yesterday, today)
    data = yf.download(symbol, start=yesterday, end=today)
    # 'symbol' 컬럼을 추가하고 모든 행에 symbol 값 할당
    data['symbol'] = symbol

    """
    data 데이터프레임 내용 정리
    """
    data.columns = data.columns.droplevel(1) # symbol 하나만 다루기에 ticker 레벨 제거
    if debug:
        print(data.head())

    """
    data 데이터 프레임을 파일로 저장
    """
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = f"{tmp_dir}{symbol}_{yesterday}.csv"
    data.to_csv(file_path)  # 데이터를 CSV로 저장

    return file_path


@task
def load(symbol, schema, table):
    cur = util.return_snowflake_conn("snowflake_conn")

    (today, yesterday) = get_today_yesterday()

    # extract에서 만들어진 파일 경로 생성
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = f"{tmp_dir}{symbol}_{yesterday}.csv"

    try:
        df = pd.read_csv(file_path)
        if len(df) == 0:
            logging.info("No record to process")
            return

        cur.execute(f"USE SCHEMA {schema};")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            date date, open float, close float, high float, low float, volume int, symbol varchar,
            PRIMARY KEY (date, symbol)
        );""")

        cur.execute("BEGIN;")
        delete_sql = f"DELETE FROM {table} WHERE date='{yesterday}'"
        logging.info(delete_sql)
        cur.execute(delete_sql)

        # 루프를 돌기는 하지만 사실 하나의 레코드 혹은 레코드가 없는 것을 예상
        # date_to_process 날짜가 휴일인 경우에는 아무런 레코드도 존재하지 않음
        for index, row in df.iterrows():
            print(row)
            sql = f"""INSERT INTO {table} (date, open, close, high, low, volume, symbol) VALUES (
            '{row["Date"]}', {row['Open']}, {row['Close']}, {row['High']}, {row['Low']}, {row['Volume']}, '{symbol}')"""
            logging.info(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
    finally: 
        # file_path에서 파일 이름만 추출
        file_name = os.path.basename(file_path)
        # 스테이지에 올린 파일을 삭제
        table_stage = f"@%{table}"
        cur.execute(f"REMOVE {table_stage}/{file_name}")
        # 연결 닫기
        cur.close()


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
    symbol = "NVDA"

    extract(symbol) >> load(symbol, schema, table)

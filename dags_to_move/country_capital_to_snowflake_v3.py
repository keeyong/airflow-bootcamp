from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime

import requests
import os
import snowflake.connector


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()


def get_file_path(context):

    tmp_dir = "/tmp"   # 이 정보도 Variables에 저장하는 곳이 편리
    date = context['logical_date']

    # 현재 시간을 파일명에 포함하여 unique한 파일명 생성
    timestamp = date.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(tmp_dir, f"country_capital_{timestamp}.csv")
    
    return file_path


@task
def extract():

    # API 호출 및 파일 저장
    url = Variable.get("country_capital_url")
    f = requests.get(url)
 
    file_path = get_file_path(get_current_context())
    with open(file_path, 'w') as file:
        file.write(f.text)
    
    return file_path


@task
def transform_load():
    target_table = "dev.raw_data.country_capital"
    cur = return_snowflake_conn()

    file_path = get_file_path(get_current_context())
    # 파일 읽기
    with open(file_path, 'r') as file:
        text = file.read()

    lines = text.strip().split("\n")
    records = []
    for l in lines[1:]:  # remove the first row
        (country, capital) = l.split(",")
        records.append([country, capital])
    
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (country varchar primary key, capital varchar);")
        cur.execute(f"DELETE FROM {target_table};")
        for r in records:
            country = r[0].replace("'", "''")
            capital = r[1].replace("'", "''")
            print(country, "-", capital)

            sql = f"INSERT INTO {target_table} (country, capital) VALUES ('{country}', '{capital}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'CountryCaptial_v3',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL'],
    schedule = '30 3 * * *'
) as dag:

    extract() >> transform_load()

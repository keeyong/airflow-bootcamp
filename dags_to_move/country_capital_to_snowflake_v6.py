from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from datetime import datetime
from datetime import timedelta
from helpers import util

import os
import requests
import snowflake.connector


@task
def extract(table):

    # API 호출 및 파일 저장
    url = Variable.get("country_capital_url")
    f = requests.get(url)
 
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, table, get_current_context())
    with open(file_path, 'w') as file:
        file.write(f.text)
    
    return file_path


@task
def transform_load(target_schema, target_table):

    # STAGE를 사용해 복사시 DB와 Schema를 테이블 이름 앞에 지정불가
    staging_table = f"temp_{target_table}"
    # extract에서 저장한 파일 읽기
    tmp_dir = Variable.get("data_dir", "/tmp/")

    file_path = util.get_file_path(tmp_dir, target_table, get_current_context())

    try:
        cur = util.return_snowflake_conn("snowflake_conn")
        # 이미 database는dev로 연결되어 있음
        cur.execute(f"USE SCHEMA {target_schema};")

        cur.execute(f"""
          CREATE TABLE IF NOT EXISTS {target_table} (
            country varchar primary key, capital varchar
          );
        """)

        # staging table을 target_table과 동일한 스키마로 생성
        # 여기서 중요한 포인트는 CREATE OR REPLACE가 사용되어야 한다는 점
        cur.execute(f"""
          CREATE OR REPLACE TABLE {staging_table} LIKE {target_table};
        """)

        # 먼저 staging 테이블의 내용을 채운다. COPY INTO 사용
        util.populate_table_via_stage(cur, staging_table, file_path)

        # UPSERT 수행
        upsert_sql = f"""
            -- Performing the UPSERT operation
            MERGE INTO {target_table} AS target
            USING {staging_table} AS stage
            ON target.country = stage.country
            WHEN MATCHED THEN
                UPDATE SET
                    target.country = stage.country,
                    target.capital = stage.capital
            WHEN NOT MATCHED THEN
                INSERT (country, capital)
                VALUES (stage.country, stage.capital);
        """
        cur.execute(upsert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()


with DAG(
    dag_id = 'CountryCaptial_v6',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL', "incremental"],
    schedule = '30 4 * * *'
) as dag:

    table = "country_capital"
    schema = "raw_data"
    extract(table) >> transform_load(schema, table)

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta

import os
import requests
import snowflake.connector


def return_snowflake_conn():

    # SnowflakeHook을 통해 Snowflake 커넥션 생성하고 커서 리턴
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()


def get_file_path(tmp_dir, context):

    # DAG가 실행된 시점의 날짜를 읽어옴. 정확히는 그전날이나 그전시간임
    # logical_date에 대해서는 뒤에서 별도로 상세 설명
    date = context['logical_date']

    # 현재 시간을 파일명에 포함하여 unique한 파일명 생성
    timestamp = date.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(tmp_dir, f"country_capital_{timestamp}.csv")
    
    return file_path


def populate_table_via_stage(cur, table, file_path):
    """
    Only supports CSV file for now
    """

    table_stage = f"@%{table}"  # 테이블 스테이지 사용

    # Internal table stage에 파일을 복사
    # 보통 이때 파일은 압축이 됨 (GZIP 등)
    cur.execute(f"PUT file://{file_path} {table_stage};")

    # Stage로부터 해당 테이블로 벌크 업데이트
    copy_query = f"""
        COPY INTO {table}
        FROM {table_stage}  -- Internal table stage를 사용하는 경우 이 라인은 스킵 가능
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        )
        FORCE = TRUE;
    """
    cur.execute(copy_query)


@task
def extract():

    # API 호출 및 파일 저장
    url = Variable.get("country_capital_url")
    f = requests.get(url)
 
    # 이 정보도 Variables에 저장하는 곳이 편리
    tmp_dir = "/tmp"

    file_path = get_file_path(tmp_dir, get_current_context())
    with open(file_path, 'w') as file:
        file.write(f.text)
    
    return file_path


@task
def transform_load(target_schema, target_table):

    # STAGE를 사용해 복사시 DB와 Schema를 테이블 이름 앞에 지정불가
    staging_table = f"temp_{target_table}"
    # extract에서 저장한 파일 읽기
    tmp_dir = "/tmp"

    file_path = get_file_path(tmp_dir, get_current_context())

    try:
        cur = return_snowflake_conn()
        # 이미 database는dev로 연결되어 있음
        cur.execute(f"USE SCHEMA {target_schema};")

        cur.execute(f"""
          CREATE TABLE IF NOT EXISTS {target_table} (
            country varchar primary key, capital varchar
          );
        """)

        # staging table을 target_table과 동일한 스키마로 생성
        # 여기서 중요한 포인트는 CREATE TEMPORARY가 사용되어야 한다는 점
        cur.execute(f"""
          CREATE TEMPORARY TABLE {staging_table} LIKE {target_table};
        """)

        # 먼저 staging 테이블의 내용을 채운다. COPY INTO 사용
        populate_table_via_stage(cur, staging_table, file_path)

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
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()


with DAG(
    dag_id = 'CountryCaptial_v5',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL'],
    schedule = '0 4 * * *'
) as dag:
    target_table = "country_capital"
    target_schema = "raw_data"
    extract() >> transform_load(target_schema, target_table)

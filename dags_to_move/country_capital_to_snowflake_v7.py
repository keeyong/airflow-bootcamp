from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta

import os
import requests
import snowflake.connector
from helpers import util


def load_to_s3(aws_conn_id, file_path, s3_bucket_name, s3_key):
    """
     - aws_conn_id 커넥션의 액세스 권한을 바탕으로 S3://s3_bucket_name/s3_key 위치에 file_path가 가리키는 파일을 업로드
     - 예를 들어 이렇게 호출 가능:
       load_to_S3(
         "aws_connection",
         "/tmp/country_capital_20250207.csv",
         "airflow-bootcamp-test-bucket",
         "country_capital_20250207.csv"
       )
    """

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
    # S3로 파일 업로드
    s3_hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=s3_bucket_name,
        replace=True
    )


def populate_table_via_s3_stage(cur, table, aws_conn_id, s3_bucket_name, s3_key):
    """
     - s3_bucket_name을 바탕으로 External Stage를 생성하고 (이때 액세스 권한 정보는 aws_conn_id를 통해 취득)
     - COPY INTO를 external stage 밑의 s3_key 대상으로 실행하여 table으로 레코드 업로드
    """
    
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    creds = s3_hook.get_credentials()

    s3_stage_sql = f"""CREATE OR REPLACE STAGE my_s3_stage
        URL = 's3://{s3_bucket_name}'
        CREDENTIALS = (AWS_KEY_ID='{creds.access_key}' AWS_SECRET_KEY='{creds.secret_key}');
    """
    cur.execute(s3_stage_sql)

    copy_sql = f"""COPY INTO {table}
        FROM @my_s3_stage/{s3_key}
        FILE_FORMAT = (type='CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
    """
    cur.execute(copy_sql)


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
def transform_load(target_schema, target_table, s3_bucket_name):

    staging_table = f"temp_{target_table}"
    # extract에서 저장한 파일 읽기
    tmp_dir = Variable.get("data_dir", "/tmp/")

    file_path = util.get_file_path(tmp_dir, target_table, get_current_context())
    # file_path에서 파일 이름만 추출
    file_name = os.path.basename(file_path)

    load_to_s3("aws_connection", file_path, s3_bucket_name, file_name)

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
          CREATE TEMPORARY TABLE {staging_table} LIKE {target_table};
        """)

        # 먼저 staging 테이블의 내용을 External Stage를 통해 채운다
        populate_table_via_s3_stage(
            cur,
            staging_table,
            "aws_connection",
            s3_bucket_name,
            file_name       # s3 버킷내 파일의 위치
        )

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
        raise e
    finally:
        cur.close()


with DAG(
    dag_id = 'CountryCaptial_v7',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL'],
    schedule = '0 5 * * *'
) as dag:

    table = "country_capital"
    schema = "raw_data"
    s3_bucket_name = "airflow-bootcamp-test-bucket"

    extract(table) >> transform_load(schema, table, s3_bucket_name)

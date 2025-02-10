from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

import os

def get_file_path(tmp_dir, filename, context):

    # DAG가 실행된 시점의 날짜를 읽어옴. 정확히는 그전날이나 그전시간임
    # logical_date에 대해서는 뒤에서 별도로 상세 설명
    date = context['logical_date']

    # 현재 시간을 파일명에 포함하여 unique한 파일명 생성
    timestamp = date.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(tmp_dir, f"{filename}_{timestamp}.csv")

    return file_path


def return_snowflake_conn(snowflake_conn_id):

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    conn = hook.get_conn()
    return conn.cursor()


def populate_table_via_stage(cur, table, file_path):
    """
    Only supports CSV file for now
    """

    table_stage = f"@%{table}"  # 테이블 스테이지 사용
    file_name = os.path.basename(file_path)

    # Internal table stage에 파일을 복사
    # 보통 이때 파일은 압축이 됨 (GZIP 등)
    cur.execute(f"PUT file://{file_path} {table_stage};")

    # Stage로부터 해당 테이블로 벌크 업데이트
    copy_query = f"""
        COPY INTO {table}
        FROM {table_stage}/{file_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        )
    """
    cur.execute(copy_query)


def get_next_day(date_str):
    """
    'YYYY-MM-DD' 형식의 날짜 문자열이 주어지면, 동일한 형식의 문자열로 다음 날짜를 반환
    """
    # 먼저 date_str을 datetime 객체로 변환
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # 다음날 날짜를 계산
    return (date_obj + timedelta(days=1)).strftime('%Y-%m-%d')


def get_logical_date(context):
    return context['logical_date']

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


def get_next_day(date_str):
    """
    Given a date string in 'YYYY-MM-DD' format, returns the next day as a string in the same format.
    """
    # Convert the string date to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Add one day using timedelta
    next_day = date_obj + timedelta(days=1)

    # Convert back to string in "YYYY-MM-DD" format
    return next_day.strftime("%Y-%m-%d")


def get_logical_date(context):
    return context['logical_date']

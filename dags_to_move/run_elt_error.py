from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from helpers import util
from helpers import slack

import logging


TABLES = [
    {
        'schema' : 'analytics',
        'table': 'mau_summary',
        'business owner': 'John Doe',
        'sql' : """
            SSELECT 
                TO_VARCHAR(A.timestamp, 'YYYY-MM') AS month,
                COUNT(DISTINCT B.userid) AS mau
            FROM raw_data.session_timestamp A
            JOIN raw_data.user_session_channel B ON A.sessionid = B.sessionid
            GROUP BY 1;"""
    }
]


@task
def runCTAS(table_params):

    schema = table_params['schema'] 
    table = table_params['table']
    select_sql = table_params['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = util.return_snowflake_conn("snowflake_conn")
    try:
        # 임시 테이블로 CTAS 적용
        ctas_sql = f"""CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"""
        cur.execute(ctas_sql)

        # 임시 테이블에 레코드가 없다면 에러 발생 
        # 여기에 사실 더 많은 검증을 수행해야함 - 중복 레코드 체크, 기본키 유일성 체크 등등
        cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
        count = cur.fetchone()[0]
        if count == 0:
            raise AirflowException(f"{schema}.{table} didn't have any record")

        # 테이블 SWAP 명령을 사용할 예정이라 타겟 테이블이 없는 경우를 대비해서 없으면 하나 만들어둠
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} AS
            SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
        cur.execute(main_table_creation_if_not_exists_sql)

        # SWAP 명령 실행. DDL이라 transaction 여부와 관계없이 항상 바로 커밋이 됨
        swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
        logging.info(swap_sql)
        cur.execute(swap_sql)
    except Exception as e:
        raise e

with DAG(
    dag_id="RunELT_Alert",
    start_date=datetime(2025,1,10),
    description="Build ELT tables using SQL CTAS with Slack failure alert",
    schedule='@daily',
    tags=["ELT"],
    catchup=False,
    default_args = {
        'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    # 테이블을 일렬로 하나씩 빌드
    prev_task = None
    for table in TABLES:
        task = runCTAS(table)
        if prev_task is not None:
            prev_task >> task
        prev_task = task

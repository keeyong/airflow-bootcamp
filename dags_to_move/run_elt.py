from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta
from helpers import util

import logging


TABLES = [
    {
        'schema' : 'analytics',
        'table': 'mau_summary',
        'business owner': 'John Doe',
        'primary_key': 'month',
        'sql' : """
            SELECT 
                TO_VARCHAR(A.timestamp, 'YYYY-MM') AS month,
                COUNT(DISTINCT B.userid) AS mau
            FROM raw_data.session_timestamp A
            JOIN raw_data.user_session_channel B ON A.sessionid = B.sessionid
            GROUP BY 1;"""
    }
]


def count_table(cur, table):
    """ 주어진 테이블의 레코드 수를 리턴 """
    cur.execute(f"""SELECT COUNT(1) FROM {table}""")

    return cur.fetchone()[0]


def is_primary_key_uniquenss(cur, table, primary_key):
    """ 주어진 테이블의 기본키가 지켜지는지 리턴 """
    sql = f"""SELECT {primary_key}, COUNT(1) AS cnt
FROM {table} 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 1"""
    cur.execute(sql)
    return cur.fetchone()[1] == 1


@task
def runCTAS(table_params):
    """ 
    1. 임시 테이블을 주어진 SELECT 문('sql')으로 생성 (CTAS)
    2. 2개의 품질 체크 수행 (레코드 수가 0보다 큰가? 기본키가 지켜지는가?)
    3. 타겟 테이블이 존재하지 않는다면 생성
    4. 임시 테이블과 타겟 테이블을 교환 (SWAP)
    """
    schema = table_params['schema'] 
    table = table_params['table']
    select_sql = table_params['sql']
    primary_key = table_params['primary_key']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = util.return_snowflake_conn("snowflake_conn")

    # 임시 테이블로 CTAS 적용
    ctas_sql = f"""CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"""
    cur.execute(ctas_sql)

    # 임시 테이블에 레코드가 없다면 에러 발생 
    # 여기에 사실 더 많은 검증을 수행해야함 - 중복 레코드 체크, 기본키 유일성 체크 등등
    if count_table(cur, f"{schema}.temp_{table}") == 0:
        raise AirflowException(f"{schema}.{table} doesn't have any record")

    # 기본키가 유일한지 체크
    if not is_primary_key_uniquenss(cur, f"{schema}.temp_{table}", primary_key):
        raise AirflowException(f"{schema}.{table} doesn't guarantee primary key uniqueness")

    # 테이블 SWAP 명령을 사용할 예정이라 타겟 테이블이 없는 경우를 대비해서 없으면 하나 만들어둠
    # WHERE 1=0을 항상 거짓이기에 아무런 레코드 없이 뼈대만 동일한 타겟 테이블이 만들어짐
    main_table_creation_if_not_exists_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} AS
        SELECT * FROM {schema}.temp_{table} WHERE 1=0;"""
    cur.execute(main_table_creation_if_not_exists_sql)

    # SWAP 명령 실행. DDL이라 transaction 여부와 관계없이 항상 바로 커밋이 됨
    swap_sql = f"""ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};"""
    logging.info(swap_sql)
    cur.execute(swap_sql)


with DAG(
    dag_id="RunELT",
    start_date=datetime(2025,1,10),
    description="Build ELT tables using SQL CTAS",
    schedule='@daily',
    tags=["ELT"],
    catchup=False
) as dag:

    # 테이블을 일렬로 하나씩 빌드
    prev_task = None
    for table in TABLES:
        task = runCTAS(table)
        if prev_task is not None:
            prev_task >> task
        prev_task = task

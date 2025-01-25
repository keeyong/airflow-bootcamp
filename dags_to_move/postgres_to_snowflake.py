from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta

from helpers import util


# Table configurations
TABLES = {
    'user_session_channel': {
        'schema': """
            CREATE TABLE IF NOT EXISTS user_session_channel (
                userid INTEGER,
                sessionid VARCHAR(32) PRIMARY KEY,
                channel VARCHAR(32)
            )
        """
    },
    'session_timestamp': {
        'schema': """
            CREATE TABLE IF NOT EXISTS session_timestamp (
                sessionid VARCHAR(32) PRIMARY KEY,
                timestamp TIMESTAMP
            )
        """
    }
}


@task
def extract_from_postgres(schema, table):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # PostgreSQL에서 데이터 추출
    df = pg_hook.get_pandas_df(
        f"SELECT * FROM {schema}.{table};"
    )
    
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, table, get_current_context())
    df.to_csv(file_path, index=False)


@task
def load_to_snowflake(schema, table):
    tmp_dir = Variable.get("data_dir", "/tmp/")
    file_path = util.get_file_path(tmp_dir, table, get_current_context())    

    cur = util.return_snowflake_conn("snowflake_conn")
    try: 
        cur.execute(f"USE SCHEMA {schema};")

        # Snowflake 테이블 생성
        cur.execute(TABLES[table]["schema"])

        cur.execute("BEGIN;")
        delete_sql = f"DELETE FROM {table}" 
        cur.execute(delete_sql)  

        # Internal Table Stage로 파일을 업로드
        util.populate_table_via_stage(cur, table, file_path)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        # 연결 닫기
        cur.close()


with DAG(
    dag_id='PostgresToSnowflake',
    description='Transfer data from PostgreSQL to Snowflake',
    tags=["ETL", "fullrefresh"],
    start_date=datetime(2025,1,14),
    schedule='@daily',
    catchup=False
) as dag:

    postgres_schema = "production"
    snowflake_schema = "raw_data"
    for table_name in TABLES.keys():
        extract_from_postgres(postgres_schema, table_name) >> load_to_snowflake(snowflake_schema, table_name)

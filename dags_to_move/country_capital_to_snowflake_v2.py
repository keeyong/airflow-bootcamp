from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(url):
    f = requests.get(url)
    return (f.text)


@task
def transform(text):
    lines = text.strip().split("\n")
    records = []
    for l in lines:  # remove the first row
        (country, capital) = l.split(",")
        records.append([country, capital])
    return records[1:]

@task
def load(records, target_table):
    cur = return_snowflake_conn()
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
    dag_id = 'CountryCaptial_v2',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "dev.raw_data.country_capital"
    url = Variable.get("country_capital_url")

    data = extract(url)
    lines = transform(data)
    load(lines, target_table)

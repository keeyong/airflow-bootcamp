from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests


def return_snowflake_conn(warehouse='compute_wh', database='dev'):

    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  # Example: 'xyz12345.us-east-1'
        warehouse=warehouse,
        database=database
    )
    # Create a cursor object
    return conn.cursor()


def extract(url):
    f = requests.get(url)
    return (f.text)


def transform(text):
    lines = text.strip().split("\n")
    records = []
    for l in lines:  # remove the first row
        (country, capital) = l.split(",")
        records.append([country, capital])
    return records[1:]

def load(cur, records, target_table):
    try:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (country varchar primary key, capital varchar);")
        cur.execute("BEGIN;")
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

@task
def etl():
    csv_url = Variable.get("country_capital_url")
    conn = return_snowflake_conn()

    data = extract(csv_url)
    lines = transform(data)
    load(conn, lines, target_table)


with DAG(
    dag_id = 'CountryCaptial',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL'],
    schedule = '0 2 * * *'
) as dag:
    target_table = "dev.raw_data.country_capital"

    etl()

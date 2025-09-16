from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime

import requests
import os
import snowflake.connector


def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()


def get_file_path(tmp_dir, context):

    date = context['logical_date']

    timestamp = date.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(tmp_dir, f"country_capital_{timestamp}.csv")
    
    return file_path

def populate_table_via_stage(cur, table, file_path):
    """
    Only supports CSV file for now
    """

    table_stage = f"@%{table}"  # 테이블 스테이지 사용
    # file_path에서 파일 이름만 추출
    file_name = os.path.basename(file_path)

    # Internal table stage에 파일을 복사
    # 보통 이때 파일은 압축이 됨
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

@task
def extract():

    tmp_dir = "/tmp"   
    url = Variable.get("country_capital_url")
    f = requests.get(url)
 
    file_path = get_file_path(tmp_dir, get_current_context())
    with open(file_path, 'w') as file:
        file.write(f.text)
    
    return file_path


@task
def transform_load(target_schema, target_table):

    # STAGE를 사용해 복사시 DB와 Schema를 테이블 이름 앞에 지정불가
    staging_table = f"temp_{target_table}"
    tmp_dir = "/tmp"

    # extract에서 저장한 파일 읽기
    file_path = get_file_path(tmp_dir, get_current_context())
    
    try:
        cur = return_snowflake_conn()
        cur.execute(f"USE SCHEMA {target_schema};")

        cur.execute(f"""
          CREATE TABLE IF NOT EXISTS {target_table} (
            country varchar primary key, capital varchar
          );
        """) 

        cur.execute(f"""
          CREATE TEMPORARY TABLE {staging_table} LIKE {target_table};
        """)

        populate_table_via_stage(cur, staging_table, file_path)

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

        # 제대로 복사되었는지 레코드수 계산
        cur.execute(f"SELECT COUNT(1) FROM {target_table}")
        row = cur.fetchone()
        if row[0] <= 0:
            raise Exception("The number of records is ZERO")
        else:
            print(row[0])

    except Exception as e:
        print(e)
        raise e
    finally:
        # file_path에서 파일 이름만 추출
        file_name = os.path.basename(file_path)
        # 스테이지에 올린 파일을 삭제
        table_stage = f"@%{target_table}"
        cur.execute(f"REMOVE {table_stage}/{file_name}")
        cur.close()


with DAG(
    dag_id = 'CountryCaptial_v5',
    start_date = datetime(2025,9,16),
    catchup=False,
    tags=['ETL'],
    max_active_runs=1,
    schedule = '0 4 * * *'
) as dag:

    target_table = "country_capital"
    target_schema = "raw_data"
    extract() >> transform_load(target_schema, target_table)

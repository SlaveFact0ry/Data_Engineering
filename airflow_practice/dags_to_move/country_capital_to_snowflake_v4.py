from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta

import os
import requests
import snowflake.connector


def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()


def get_file_path(context):

    tmp_dir = "/tmp"   
    date = context['logical_date']

    timestamp = date.strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(tmp_dir, f"country_capital_{timestamp}.csv")
    
    return file_path


@task
def extract():

    url = Variable.get("country_capital_url")
    f = requests.get(url)
 
    file_path = get_file_path(get_current_context())
    with open(file_path, 'w') as file:
        file.write(f.text)
    
    return file_path


@task
def transform_load():

     # STAGE를 사용해 복사시 DB와 Schema를 테이블 이름 앞에 지정불가
    target_table = "country_capital"
    # 테이블 스테이지 사용
    target_stage = f"@%{target_table}"
    # extract에서 저장한 파일 읽기
    file_path = get_file_path(get_current_context())
    # file_path에서 파일 이름만 추출
    file_name = os.path.basename(file_path)

    try:
        cur = return_snowflake_conn()

        cur.execute("USE SCHEMA raw_data;")

        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table};")

        # Internal table stage에 파일을 복사   
        # 보통 이때 파일은 압축이 됨 (GZIP 등) 
        cur.execute(f"PUT file://{file_path} {target_stage};")

        # cur.execute(f"LIST {target_stage};")
        # print(cur.fetchall())

        # Stage로부터 해당 테이블로 벌크 업데이트
        copy_query = f"""
            COPY INTO {target_table}
            FROM {target_stage}/{file_name}  -- Internal table stage를 사용하는 경우 이 라인은 스킵 가능
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
            )
        """
        cur.execute(copy_query)

        # 제대로 복사되었는지 레코드수 계산
        cur.execute(f"SELECT COUNT(1) FROM {target_table}")
        row = cur.fetchone()
        if row[0] <= 0:
            raise Exception("The number of records is ZERO")
        else:
            print(row[0])

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
    finally:
        # 스테이지에 올린 파일을 삭제
        cur.execute(f"REMOVE {target_stage}/{file_name}")
        cur.close()


with DAG(
    dag_id = 'CountryCaptial_v4',
    start_date = datetime(2025,1,10),
    catchup=False,
    tags=['ETL'],
    schedule = '30 3 * * *'
) as dag:

    extract() >> transform_load()

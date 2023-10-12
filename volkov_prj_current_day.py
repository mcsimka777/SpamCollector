from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import requests

DAG_NAME = 'volkov_prj_day_table'
GP_CONN_ID = 'volkovms'

SQL_DROP_TABLE = 'drop materialized view volkov_prj_hcalls_mv;'
SQL_CREATE_TABLE = '''
        create materialized view volkov_prj_hcalls_mv as
        (
        select
                substring(ta, 1,4) as tag,
                count(ta) as calls,
                extract(hour from stime::time) as hours,
                extract (day from to_date(sdate,'DD.MM.YYYY')) as day,
                to_date(sdate,'DD.MM.YYYY') as sdate,
                tg,
                count(case when duration>0 then 1 end) as full_call, count(case when duration=0 then 1 end) as empty_call,
                100*count(case when duration=0 then 1 end)/count(ta) as percent_unsuccessfull
        from
                volkov_prj_hcalls
        where
                tg<>1000 and to_date(sdate,'DD.MM.YYYY') = to_date('23.09.2023','DD.MM.YYYY')
        group by
                tag,
                hours,
                day,
                sdate,
                tg
        having
        count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
        )
        distributed randomly;
'''

args = {
        'owner': 'volkovms',
        'start_date': datetime(2023,10,10),
        'retries': 3,
        'retry_delay': timedelta(seconds = 600)
        }

with DAG(DAG_NAME,
        description='volkovms',
        schedule_interval='* 4 * * *',
        catchup=False,
        max_active_runs=1,
        default_args = args,
        params={}
        ) as dag:

    sql_drop = PostgresOperator(
            task_id = 'volkov_day_table_drop',
            sql = SQL_DROP_TABLE,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_create = PostgresOperator(
            task_id = 'volkov_day_table_create',
            sql = SQL_CREATE_TABLE,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_drop >> sql_create

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import requests

DAG_NAME = 'volkov_prj_week_chart'
GP_CONN_ID = 'volkovms'

SQL_DROP_TABLE = 'drop materialized view if exists volkov_prj_week_chart_mv;'
SQL_CREATE_TABLE = '''
        create materialized view volkov_prj_week_chart_mv as
        (
                select
                  substring(ta, 1,4) as tag,
                  count(ta) as calls,
                  count(case when duration>0 then 1 end) as full_call,
                  count(case when duration=0 then 1 end) as empty_call,
                  100*count(case when duration=0 then 1 end)/count(ta) as percent_unsuccessful
                from volkov_hcalls
                where tg<>1000 and to_date(sdate, 'DD.MM.YYYY') > to_date('21.09.2023', 'DD.MM.YYYY') - interval '7 day'
                group by
                  tag
                having
                  count(ta)>500 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
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
        schedule_interval='* * * * *',
        catchup=False,
        max_active_runs=1,
        default_args = args,
        params={}
        ) as dag:

    sql_drop = PostgresOperator(
            task_id = 'week_chart_drop',
            sql = SQL_DROP_TABLE,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_create = PostgresOperator(
            task_id = 'week_chart_create',
            sql = SQL_CREATE_TABLE,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )


    sql_drop >> sql_create


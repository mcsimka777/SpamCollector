from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import requests

DAG_NAME = 'volkov_prj_day_refresh'
GP_CONN_ID = 'volkovms'

SQL_REQ1 = 'refresh materialized view volkov_prj_hcalls_mv;'
SQL_REQ2 = 'refresh materialized view volkov_prj_week_chart_mv;'
SQL_REQ3 = 'refresh materialized view volkov_prj_new_spam_mv;'


args = {
        'owner': 'volkovms',
        'start_date': datetime(2023,10,10),
        'retries': 3,
        'retry_delay': timedelta(seconds = 600)
        }

with DAG(DAG_NAME,
        description='volkovms',
        schedule_interval='*/10 * * * *',
        catchup=False,
        max_active_runs=1,
        default_args = args,
        params={}
        ) as dag:

    sql_req1 = PostgresOperator(
            task_id = 'volkov_day_table',
            sql = SQL_REQ1,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_req2 = PostgresOperator(
            task_id = 'volkov_week_chart',
            sql = SQL_REQ2,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_req3 = PostgresOperator(
            task_id = 'volkov_new_spam',
            sql = SQL_REQ3,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )


    sql_req1 >> sql_req2 >> sql_req3


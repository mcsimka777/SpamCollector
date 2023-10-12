from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow import configuration
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import requests

DAG_NAME = 'volkov_prj_update_spammers'
GP_CONN_ID = 'volkovms'

SQL_UPDATE = '''
        update volkov_prj_spammers vps set
                days_spam = vps.days_spam + 1
        from
        (
                select distinct substring(ta, 1,4) as tag, tg
                from volkov_hcalls
                where tg<>1000
                and to_date(sdate,'DD.MM.YYYY') = to_date('22.09.2023','DD.MM.YYYY')
                group by tag, tg, extract(hour from stime::time)
                having count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
        ) as subquery
        where vps.prefix = subquery.tag and vps.tg = subquery.tg;
'''

SQL_INSERT = '''
        insert into volkov_prj_spammers (prefix, tg, days_spam)
                select tag, tg, '1' as days_spam from
                (
                        select distinct substring(ta, 1,4) as tag, tg
                        from volkov_hcalls
                        where tg<>1000 and to_date(sdate,'DD.MM.YYYY') = to_date('22.09.2023','DD.MM.YYYY')
                        group by tag, tg, extract(hour from stime::time)
                        having count(ta)>30 and (100*count(case when duration=0 then 1 end)/count(ta)) > 80
                ) as subquery
                where not exists (select 1 from volkov_prj_spammers sp where sp.prefix = subquery.tag and sp.tg = subquery.tg)
        );
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

    sql_update = PostgresOperator(
            task_id = 'update_spammers',
            sql = SQL_UPDATE,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_insert = PostgresOperator(
            task_id = 'insert_spammers',
            sql = SQL_INSERT,
            postgres_conn_id = GP_CONN_ID,
            autocommit = True
            )

    sql_update >> sql_insert



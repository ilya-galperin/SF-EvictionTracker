"""
echo "" > working2.py
nano working2.py
"""


# The DAG object; we'll need to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# Instantiate a DAG
dag = DAG(
	'ilyatestdag2',
	default_args=default_args,
	description='ilya test dag2',
	schedule_interval=None
)

create_table = PostgresOperator(
	task_id='create_table',
	sql="""CREATE TABLE new_table(custom_id integer NOT NULL, timestamp timestamp NOT NULL, user_id varchar(50) NOT NULL);""",
	postgres_conn_id='_postgres_rds_evictions',
	dag=dag
) 

create_table

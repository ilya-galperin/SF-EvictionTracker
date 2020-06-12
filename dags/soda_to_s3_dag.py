# The DAG object; we'll need to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from operators.soda_to_s3_operator import SodaToS3Operator
from airflow.utils.dates import days_ago
from datetime import timedelta


soda_params = """$query=SELECT:*,* WHERE :created_at > '2020-04-04' OR :updated_at > '2020-04-04'
                           ORDER BY :id LIMIT 100"""
soda_headers = {
    'keyId':'########################',
    'keySecret':'########################################',
	'Accept':'application/json'
}

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

with DAG('SodaToS3',
		default_args=default_args,
		description='soda to s3 dag',
		max_active_runs=3,
		schedule_interval=None) as dag:
 
	op1 = SodaToS3Operator(
		task_id='taskA',
		http_conn_id='_soda',
		headers=soda_headers,
		data=soda_params,
		dag=dag
		)

	op1

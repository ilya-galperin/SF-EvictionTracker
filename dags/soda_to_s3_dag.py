# The DAG object; we'll need to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from operators.soda_to_s3_operator import SodaToS3Operator
from airflow.utils.dates import days_ago
from datetime import timedelta

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
		max_active_runs=1,
		schedule_interval=None) as dag:
 
	op1 = SodaToS3Operator(
		task_id='soda_to_s3',
		http_conn_id='_soda',
		headers=soda_headers,
		dag=dag
		)

	op1

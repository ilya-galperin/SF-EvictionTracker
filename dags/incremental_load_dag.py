from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from operators.soda_to_s3_operator import SodaToS3Operator
from operators.s3_to_postgres_operator import S3ToPostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

soda_headers = {
    'keyId':'############',
    'keySecret':'#################',
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

with DAG('eviction-tracker-incremental_load',
		default_args=default_args,
		description='Executes full load from SODA API to Production DW.',
		max_active_runs=1,
		schedule_interval=None) as dag:
 
	op1 = SodaToS3Operator(
		task_id='get_evictions_data',
		http_conn_id='API_Evictions',
		headers=soda_headers,
		s3_conn_id='S3_Evictions',
		s3_bucket='sf-evictionmeter',
		s3_directory='soda_jsons',
		size_check=True,
		max_bytes=500000000,
		dag=dag
	)
	
	op2 = PostgresOperator(
		task_id='initialize_target_db',
		postgres_conn_id='RDS_Evictions',
		sql='sql/init_db_schema.sql',
		dag=dag
	)
	
	op3 = S3ToPostgresOperator(
		task_id='load_evictions_data',
		s3_conn_id='S3_Evictions',
		s3_bucket='sf-evictionmeter',
		s3_prefix='soda_jsons/soda_evictions_import',
		source_data_type='json',
		postgres_conn_id='RDS_Evictions',
		schema='raw',
		table='soda_evictions',
		get_latest=True,
		dag=dag
	)
	
	op4 = S3ToPostgresOperator(
		task_id='load_neighborhood_data',
		s3_conn_id='S3_Evictions',
		s3_bucket='sf-evictionmeter',
		s3_prefix='census_csv/sf_by_neighborhood',
		source_data_type='csv',
		header=True,
		postgres_conn_id='RDS_Evictions',
		schema='raw',
		table='neighborhood_data',
		get_latest=True,
		dag=dag
	)
	
	op5 = S3ToPostgresOperator(
		task_id='load_district_data',
		s3_conn_id='S3_Evictions',
		s3_bucket='sf-evictionmeter',
		s3_prefix='census_csv/sf_by_district',
		source_data_type='csv',
		header=True,
		postgres_conn_id='RDS_Evictions',
		schema='raw',
		table='district_data',
		get_latest=True,
		dag=dag
	)
	
	op6 = PostgresOperator(
		task_id='execute_full_load',
		postgres_conn_id='RDS_Evictions',
		sql='sql/full_load.sql',
		dag=dag
	)
	
	op1 >> op2 >> (op3, op4, op5) >> op6

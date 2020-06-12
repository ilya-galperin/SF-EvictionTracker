# echo "" > /home/airflow/airflow/dags/operators/soda_to_s3_operator.py
# nano /home/airflow/airflow/dags/operators/soda_to_s3_operator.py

import json

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook


class SodaToS3Operator(BaseOperator):

	"""
	**kwargs) # **kwargs = keyword args, accepts keyword or NAMED argument call(a='Real',b='Python',c='Is')  def call(**kwargs) - returns a dictionary
	-> None:  # -> just a hint that the function returns nada
	"""
	
	@apply_defaults
	def __init__(self,
		endpoint=None,
		data=None,
		headers=None,	
		method='GET',
		http_conn_id='http_default',
		*args, 
		**kwargs) -> None: 
		
		super().__init__(*args, **kwargs)
		
		self.endpoint = endpoint
		self.data = data
		self.headers = headers
		self.method = method
		self.http_conn_id = http_conn_id
	
	
	def execute(self, context):

		soda = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
		
		if self.data:
			soql_filter = self.data
		else:
			soql_filter = """$query=SELECT:*,* WHERE :created_at > '2020-04-04' OR :updated_at > '2020-04-04'
							   ORDER BY :id LIMIT 100"""
							   
		response = soda.run(endpoint=self.endpoint, data=soql_filter, headers=self.headers)
			
		print('=================== start ===================')
		print(soql_filter)
		print(response.headers)
		# print(response.json())
		print('=================== done ===================')

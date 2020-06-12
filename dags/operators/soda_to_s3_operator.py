"""
You can create any operator you want by extending the airflow.models.baseoperator.BaseOperator.

There are two methods that you need to override in a derived class:

Constructor - Define the parameters required for the operator. You only need to specify the arguments specific to your operator. Use @apply_defaults decorator function to fill unspecified arguments with default_args. You can specify the default_args in the dag file. See Default args for more details.

Execute - The code to execute when the runner calls the operator. The method contains the airflow context as a parameter that can be used to read config values.
"""
# echo "" > /home/airflow/airflow/dags/operators/soda_to_s3_operator.py
# nano /home/airflow/airflow/dags/operators/soda_to_s3_operator.py

import json

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.http_hook import HttpHook


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
		
		self.method = method 
		self.http_conn_id = http_conn_id
		self.endpoint = endpoint
		self.data = data
		self.headers = headers
	
	
	def execute(self, context):
		soda = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
		response = soda.run(endpoint=self.endpoint, data=self.data, headers=self.headers)
		
		print('=================== start ===================')
		print(response.headers)
		print(response.json())
		print('=================== done ===================')

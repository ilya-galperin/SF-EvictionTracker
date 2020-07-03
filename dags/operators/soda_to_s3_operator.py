# echo "" > /home/airflow/airflow/dags/operators/soda_to_s3_operator.py
# nano /home/airflow/airflow/dags/operators/soda_to_s3_operator.py

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook

from datetime import datetime, timedelta
import json
import sys


class SizeExceededError(Exception):
	"""Raised when max file size is exceeded"""
	def __init__(self):
		self.message = 'Max file size exceeded'

	def __str__(self):
		return f'SizeExceededError, {self.message}'


class SodaToS3Operator(BaseOperator):
	""" 
	Queries the Socrata Open Data API using a SoQL string and uploads the results to an S3 bucket.
	
	:param endpoint:		Optional API connection endpoint
	:param data:			Custom Socrata SoQL string used to query API, overrides default get request
	:param days_ago:		Restricts get request to updated/created records from specified date onward
	:param headers:			Dictionary containing optional API connection keys (keyId, keySecret, Accept)
	:param s3_conn_id:		S3 Connection ID
	:param s3_bucket:		S3 Bucket Destination
	:param s3_directory:		S3 Directory Destination
	:param method:			Request type for API
	:param http_conn_id:		SODA API Connection ID
	:param size_check:		Boolean indicating whether to run a size check prior to upload to S3
	:param max_bytes:		Maximum number of bytes to allow for a single S3 upload		
	"""
	
	@apply_defaults
	def __init__(self,
		endpoint=None,
		data=None,
		days_ago=None,
		headers=None,
		s3_conn_id=None,
		s3_bucket=None,
		s3_directory='',
		method='GET',
		http_conn_id='http_default',
		size_check=False,
		max_bytes=5000000000,
		*args, 
		**kwargs) -> None: 
		
		super().__init__(*args, **kwargs)
		
		self.endpoint = endpoint
		self.data = data
		self.days_ago = days_ago
		self.s3_conn_id = s3_conn_id
		self.s3_bucket = s3_bucket
		self.s3_directory = s3_directory
		self.headers = headers
		self.method = method
		self.http_conn_id = http_conn_id
		self.size_check = size_check
		self.max_bytes = max_bytes
	
	
	def get_size(self, obj, seen=None):
		"""
		Recursively finds size of object.
		"""
		
		size = sys.getsizeof(obj)
		if seen is None:
			seen = set()
		obj_id = id(obj)
		if obj_id in seen:
			return 0
		seen.add(obj_id)
		if isinstance(obj, dict):
			size += sum([self.get_size(v, seen) for v in obj.values()])
			size += sum([self.get_size(k, seen) for k in obj.keys()])
		elif hasattr(obj, '__dict__'):
			size += self.get_size(obj.__dict__, seen)
		elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
			size += sum([self.get_size(i, seen) for i in obj])
		return size
	
	
	def parse_metadata(self, header):
		"""
		Parses metadata from API response.
		"""
		
		try:
			metadata = {
				'api-call-date': header['Date'],
				'content-type': header['Content-Type'],
				'source-last-modified': header['X-SODA2-Truth-Last-Modified'],
				'fields': header['X-SODA2-Fields'],
				'types': header['X-SODA2-Types']
			}
		except KeyError:
			metadata = {'KeyError': 'Metadata missing from header, see error log.'}
    
		return metadata

	
	def execute(self, context):
		"""
		Executes the operator, including running a max filesize check if enabled. 
		
		The SODA API maxes out the # of returned results so we use paging to query
		the endpoint multiple times and continuously move the offset. 
		
		Metadata is parsed and saved separately in a /logs/ subfolder along with 
		the JSON results from API call.
		"""
		
		soda = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
		
		if self.data:
			soql_filter = self.data
		elif self.days_ago:
			current_dt = datetime.now()
			target_dt = current_dt - timedelta(self.days_ago)
			format_dt = target_dt.strftime("%Y-%m-%d")
			soql_filter = f"""$query=SELECT:*,* WHERE :created_at > '{format_dt}' OR :updated_at > '{format_dt}'
							   ORDER BY :id LIMIT 10000"""
		else:
			soql_filter = """$query=SELECT:*,* ORDER BY :id LIMIT 10000"""
		
		print('getting... ' + soql_filter)
		
		#soql_filter = f"""$query=SELECT:*,* WHERE :created_at < '2020-04-01' ORDER BY :id LIMIT 10000"""
		
		offset, counter = 0, 1
		combined = []
		while True:
			soql_filter_offset = soql_filter + f' OFFSET {offset}'
			response = soda.run(endpoint=self.endpoint, data=soql_filter_offset, headers=self.headers)
			if response.status_code != 200:
				break
			captured = response.json()
			if len(captured) == 0:
				break
			combined.extend(captured)
			offset = 10000 * counter
			counter += 1

		if self.size_check == True:
			print('actual size... ' + str(self.get_size(combined)))
			print('max size... ' + str(self.max_bytes))
			if self.get_size(combined) > self.max_bytes:
				raise SizeExceededError
		
		dest_s3 = S3Hook(self.s3_conn_id)
		
		body_obj = 'soda_evictions_import_' + datetime.now().strftime("%Y-%m-%dT%H%M%S") + '.json'
		
		metadata = self.parse_metadata(response.headers)
		meta_obj = 'logs/soda_evictions_import_log_' + datetime.now().strftime("%Y-%m-%dT%H%M%S")
		
		dest_s3.load_string(json.dumps(combined), key=self.s3_directory+'/'+body_obj, bucket_name=self.s3_bucket)
		dest_s3.load_string(json.dumps(metadata), key=self.s3_directory+'/'+meta_obj, bucket_name=self.s3_bucket)
		
		# XCom used to skip downstream tasks if body object size is 0
		self.xcom_push(context=context, key='obj_len', value=len(combined))

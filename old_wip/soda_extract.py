import requests
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import sys

# Authentication settings for Socrata Open Data API (SODA) endpoint
SODA_url = 'https://data.sfgov.org/resource/5cei-gny5'
SODA_headers = {
    'keyId':'#####################',
    'keySecret':'##########################################'
}

# AWS S3 Configuration
s3_access_key = '#####################'
s3_secret_key = '##########################################'

# AWS S3 Paths
s3_bucket = 'sf-evictionmeter'
s3_directory, s3_log_directory = 'soda_jsons/', 'logs/'


def s3_session():
    """Establish AWS S3 session."""
    aws_session = boto3.Session(
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    return aws_session.resource('s3')


def get_json(endpoint, headers):
    """Calls API, requests all created & updated records >/= 180 days."""
    headers['Accept'] = 'application/json'
    pull_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%S")
    combined = []
    offset, counter = 0, 1
    error = False
    while True:
        params = f"""$query=SELECT:*,* WHERE :created_at >= '{pull_date}' OR :updated_at >= '{pull_date}'
                    ORDER BY :id LIMIT 10000 OFFSET {offset}"""
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code != 200:
            error = f'api_request-endpoint|{endpoint}|params|{params}|'
            break
        captured = response.json()
        if len(captured) == 0:
            break
        combined.extend(captured)
        offset = 10000 * counter
        counter += 1
    if error:
        log_exit(filename=error, api_error=response.status_code)
        return -1, -1

    # params = f"""$query=SELECT:*,* WHERE :created_at >= '{pull_date}' OR :updated_at >= '{pull_date}'
    #                     ORDER BY :id LIMIT 1500"""
    # response = requests.get(endpoint, headers=headers, params=params)
    # captured = response.json()
    # combined.extend(captured)

    metadata = parse_metadata(response.headers)
    print('get_json complete')
    return metadata, combined


def get_size(obj, seen=None):
    """Recursively finds size of object."""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size


def parse_metadata(header):
    """Parses metadata from API response."""
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


def write_to_s3(metadata, body):
    """Writes data from API response to S3 as JSON file."""
    obj_name = 'soda_evictions_import_' + datetime.now().strftime("%Y-%m-%dT%H%M%S") + '.json'
    try:
        s3_object = s3.Object(s3_bucket, s3_directory + obj_name)
        print('writing to s3...')
        s3_object.put(Body=(bytes(json.dumps(body).encode('UTF-8'))), Metadata=metadata)
    except ClientError as e:
        print('client error')
        error = e.response['Error']
        return log_exit(filename=obj_name, s3_error=error)
    print('write_to_s3 complete')
    return log_exit(filename=obj_name, metadata=metadata)


def log_exit(filename, metadata=None, api_error=None, s3_error=None):
    """Build log and write to S3."""
    log = {
        'filename': filename,
        'metadata': metadata,
        'api_error': api_error,
        's3_error': s3_error
    }
    obj_name = 'soda_evictions_import_log_' + datetime.now().strftime("%Y-%m-%dT%H%M%S")
    s3_object = s3.Object(s3_bucket, s3_log_directory + obj_name)
    print('logging...')
    s3_object.put(Body=(bytes(json.dumps(log).encode('UTF-8'))))
    if api_error or s3_error:
        return -1
    return 1


write_result = -1
print('establishing s3 session...')
s3 = s3_session()
print('calling api...')
head, content = get_json(SODA_url, SODA_headers)
obj_size = get_size(head) + get_size(content)
if obj_size > 500000000:
    log_exit(f'Size limit exceeded at {obj_size} bytes, upload aborted.')
    content = -1
    head = -1
    print('obj_size limit exceeded')
if head != -1 and content != -1:
    write_result = write_to_s3(head, content)
else:
    print('api error')
if write_result == 1:
    print('succeeded')
elif write_result == -1:
    print('write_to_s3 failed')

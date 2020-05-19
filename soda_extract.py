import requests
import json
import boto3
from datetime import datetime, timedelta

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
s3_bucket = '#####################'
SODA_s3_directory, LOG_s3_directory = 'soda_jsons', 'soda_logs'


def s3_session():
    """Establishes s3 session using access/secret access keys."""
    aws_session = boto3.Session(
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    s3 = aws_session.resource('s3')

    return s3


def get_json(endpoint, headers):
    """Calls API, requests all created & updated records from the last 180 days."""
    headers['Accept'] = 'application/json'
    pull_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%S")
    combined = []
    offset, counter = 0, 1
    while True:
        params = f"""$query=SELECT:*,* WHERE :created_at >= '{pull_date}' OR :updated_at >= '{pull_date}'
                    ORDER BY :id LIMIT 10000 OFFSET {offset}"""
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code != 200:
            #return log_exit(response.status_code)
            return None
        body = response.json()
        if len(body) == 0:
            break
        combined.extend(body)
        offset = 10000 * counter
        counter += 1

    metadata = parse_metadata(response.headers)
    print('get_json_done')
    return metadata, combined, counter, response.status_code


def parse_metadata(header):
    """Collects metadata from API response."""
    try:
        metadata = {
            'api-call-date': header['Date'],
            'content-type': header['Content-Type'],
            'source-last-modified': header['X-SODA2-Truth-Last-Modified'],
            'fields': header['X-SODA2-Fields'],
            'types': header['X-SODA2-Types']
        }
    except KeyError:
        metadata = {'KeyError': 'Metadata not found, please see error log.'}

    return metadata


def write_to_s3(s3, metadata, body):
    """Write data from API response to s3 as JSON file."""
    obj_name = 'soda_evictions_import_' + datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    s3_object = s3.Object(s3_bucket, obj_name)
    try:
        s3_object.put(Body=(bytes(json.dumps(body).encode('UTF-8'))), Metadata=metadata)
    except ClientError:
        print('Client error.')

    s3_response = ''

    print('Writing to s3 done')
    return obj_name, s3_response
    #return log_exit()


def log_exit(s3, filename=None, metadata=None, counter=None, api_error=None, s3_error=None):
    """Build log and write to s3."""
    log = {
        'filename':filename,
        'metadata': metadata,
        'qty_api_requests': counter,
        'api_error': api_error,
        's3_error': s3_error
    }
    s3_object = s3.Object(s3_bucket, 'filename')

    s3_object.put(Body=(bytes(json.dumps(log).encode('UTF-8'))), Metadata=metadata)

    return 1


s3_session = s3_session()
meta, content, count, code = get_json(SODA_url, SODA_headers)
s3_file, s3_response = write_to_s3(s3_session, meta, content)
log_exit(s3=s3_session, filename=s3_file, metadata=meta, counter=count, api_error=code, s3_error=s3_response)
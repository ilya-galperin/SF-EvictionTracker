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
SODA_s3_directory, LOG_s3_directory = '/soda_jsons/', '/soda_jsons/logs/'


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

        params = 'ZZZZZZZZZ'

        response = requests.get(endpoint, headers=headers, params=params)
        print('1')
        print(response.status_code)
        if response.status_code != 200:
            error = f'api_request: endpoint|{endpoint} |header|{headers} |params|{params} |'
            print('2')
            break
        captured = response.json()
        if len(captured) == 0:
            break
        combined.extend(captured)
        offset = 10000 * counter
        counter += 1

    print('3')
    if error:
        log_exit(filename=error, api_error=response.status_code)
        return -1
    print('4')
    metadata = parse_metadata(response.headers)
    print('get_json complete')
    return metadata, combined


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


def write_to_s3(metadata, body):
    """Writes data from API response to s3 as JSON file."""
    obj_name = 'soda_evictions_import_' + datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    s3_object = s3.Object(s3_bucket, obj_name)
    try:
        s3_object.put(Body=(bytes(json.dumps(body).encode('UTF-8'))), Metadata=metadata)
    except ClientError as e:
        error = e.response['Error']
        return log_exit(filename=obj_name, s3_error=error)

    print('write_to_s3 complete')
    return log_exit(filename=obj_name, metadata=metadata)


def log_exit(filename=None, metadata=None, api_error=None, s3_error=None):
    """Build log and write to s3."""
    log = {
        'filename': filename,
        'metadata': metadata,
        'api_error': api_error,
        's3_error': s3_error
    }
    s3_object = s3.Object(s3_bucket, 'soda_evictions_import_log_' + datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    s3_object.put(Body=(bytes(json.dumps(log).encode('UTF-8'))))

    if api_error or s3_error:
        return -1

    return 1


s3 = s3_session()
head, body = get_json(SODA_url, SODA_headers)
result = write_to_s3(head, body)

if result == -1:
    print('Failure')
elif result == 1:
    print('Success')

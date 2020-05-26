import boto3
from botocore.exceptions import ClientError

# AWS S3 Configuration
s3_access_key = '#####################'
s3_secret_key = '##########################################'

# AWS S3 Paths
s3_bucket = 'sf-evictionmeter'
s3_directory = 'soda_jsons/'


def s3_session():
    """Establish AWS S3 session."""
    aws_session = boto3.Session(
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    return aws_session.resource('s3')


def fetch_json():

    return 1


def insert_raw():
    sql_truncate = 'TRUNCATE TABLE raw.soda_evictions'
    print(sql_truncate)
    return 2

print('establishing s3 session...')
session = s3_session()

print('fetching json data...')
fetch_json()

print('inserting into raw schema...')
insert_raw()
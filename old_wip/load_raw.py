import json
import boto3
import psycopg2

# AWS S3 Configuration
s3_access_key = '#####################'
s3_secret_key = '##########################################'

# AWS S3 Paths
s3_bucket = 'sf-evictionmeter'
s3_directory = 'soda_jsons/'

def rds_engine():
    return psycopg2.connect(
        host='sf-evictions-1.cx5nhk8wh4xt.us-west-1.rds.amazonaws.com',
        port='5432',
        database='sf-evictions',
        user='postgres',
        password='TTvOFN1fkVc6DNItOsnV',
    )

def s3_client():
    """Establish AWS S3 session."""
    client = boto3.client(
        's3',
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key
    )
    return client


def fetch_json():
    objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_directory)['Contents']
    latest = max(objects, key=lambda x: x['LastModified'])
    latest_obj = s3.get_object(Bucket=s3_bucket, Key=latest['Key'])

    file_content = latest_obj['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)

    return json_content


def insert_raw(json_content):

    with open('C:\Pentaho\soda_evictions_import_2020-05-27T202404.json') as f:
        d = json.load(f)
    json_content = d

    #json_content = json.loads()

    cur = conn.cursor()

    sql_truncate = 'TRUNCATE TABLE raw.soda_evictions'
    cur.execute(sql_truncate)
    conn.commit()

    cur.executemany("""
        INSERT INTO raw.soda_evictions(
            raw_id,
            created_at,
            updated_at,
            eviction_id,
            address,
            city,
            state,
            zip,
            file_date,
	    non_payment,
	    breach,
	    nuisance,
	    illegal_use,
            failure_to_sign_renewal,
            access_denial,
            unapproved_subtenant,
            owner_move_in,
            demolition,
            capital_improvement,
            substantial_rehab,
            ellis_act_withdrawal,
            condo_conversion,
            roommate_same_unit,
            other_cause,
            late_payments,
            lead_remediation,
            development,
            good_samaritan_ends,
            constraints_date,
            supervisor_district,
            neighborhood
        )
        VALUES(
            %(:id)s, %(:created_at)s, %(:updated_at)s, %(eviction_id)s, %(address)s, %(city)s, %(state)s, %(zip)s,
            %(file_date)s, %(non_payment)s, %(breach)s, %(nuisance)s, %(illegal_use)s, %(failure_to_sign_renewal)s,
            %(access_denial)s, %(unapproved_subtenant)s, %(owner_move_in)s, %(demolition)s, %(capital_improvement)s,
            %(substantial_rehab)s, %(ellis_act_withdrawal)s, %(condo_conversion)s, %(roommate_same_unit)s,
            %(other_cause)s, %(late_payments)s, %(lead_remediation)s, %(development)s, %(good_samaritan_ends)s,
            %(constraints_date)s, %(supervisor_district)s, %(neighborhood)s
        );
    """,({
        ':id': line[':id'],
        ':created_at': line[':created_at'],
        ':updated_at': line[':updated_at'],
        'eviction_id': line['eviction_id'],
        'address': line.get('address', None),
        'city': line.get('city', None),
        'state': line.get('state', None),
        'zip': line.get('zip', None),
        'file_date': line.get('file_date', None),
        'non_payment': line.get('non_payment', None),
        'breach': line.get('breach', None),
        'nuisance': line.get('nuisance', None),
        'illegal_use': line.get('illegal_use', None),
        'failure_to_sign_renewal': line.get('failure_to_sign_renewal', None),
        'access_denial': line.get('access_denial', None),
        'unapproved_subtenant': line.get('unapproved_subtenant', None),
        'owner_move_in': line.get('owner_move_in', None),
        'demolition': line.get('demolition', None),
        'capital_improvement': line.get('capital_improvement', None),
        'substantial_rehab': line.get('substantial_rehab', None),
        'ellis_act_withdrawal': line.get('ellis_act_withdrawal', None),
        'condo_conversion': line.get('condo_conversion', None),
        'roommate_same_unit': line.get('roommate_same_unit', None),
        'other_cause': line.get('other_cause', None),
        'late_payments': line.get('late_payments', None),
        'lead_remediation': line.get('lead_remediation', None),
        'development': line.get('development', None),
        'good_samaritan_ends': line.get('good_samaritan_ends', None),
        'constraints_date': line.get('constraints_date', None),
        'supervisor_district': line.get('supervisor_district', None),
        'neighborhood': line.get('neighborhood', None)
    } for line in json_content))


    conn.commit()

    cur.execute("""SELECT * FROM raw.soda_evictions""")
    results = cur.fetchall()
    #print(results)
    #
    # cur.execute("Select * FROM raw.soda_evictions LIMIT 0")
    # colnames = [desc[0] for desc in cur.description]
    # print(colnames)

    cur.close()
    print('done')
    return 2

# print('establishing s3 session...')
# s3 = s3_client()
#
# print('fetching json data...')
# fetched = fetch_json()

conn = rds_engine()

print('inserting into raw schema...')
#insert_raw(fetched)

insert_raw('')

conn.close()

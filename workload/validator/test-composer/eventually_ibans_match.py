#!/usr/bin/env -S python3 -u

import boto3
import os
import json
import psycopg2

from antithesis.assertions import always

endpoint_url=f"http://{os.getenv('DYNAMO_ENDPOINT', 'ddb:8000')}"
client = boto3.client('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
table = dynamodb.Table('accounts')

def scan_all(table):
    response = table.scan()
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    return items

accounts = scan_all(table)
ddb_ibans = [account['iban'] for account in accounts]

conn = psycopg2.connect(host='state-tracker', user='u', password='p', database='d')
cursor = conn.cursor()

cursor.execute('''
    SELECT 
        data::jsonb->>'iban'
    FROM
        producer
    WHERE
        topic = '_banktest_fund_account'
''')

dg_ibans = [account[0] for account in cursor.fetchall()]

diff = set(ddb_ibans) - set(dg_ibans)

always(len(diff) == 0, "produced ibans match ibans in dynamo", { "diff": json.dumps(diff, default=list) })

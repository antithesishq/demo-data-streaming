#!/usr/bin/env -S python3 -u

import json
import os
import sys
import time

import boto3
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError

from antithesis.assertions import always

RETRY_FOR_SECONDS = 120

conn = psycopg2.connect(host='state-tracker', user='u', password='p', database='d')
cursor = conn.cursor()
cursor.execute('''
    SELECT
        data::jsonb->>'iban'
    FROM
        producer
    WHERE
        consumed_timestamp IS NOT NULL
        AND topic = '_banktest_fund_account'
''')
dg_ibans = [account[0] for account in cursor.fetchall()]

if not dg_ibans:
    print("no funding messages consumed yet; nothing to check")
    sys.exit(0)

endpoint_url = f"http://{os.getenv('DYNAMO_ENDPOINT', 'ddb:8000')}"
dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
table = dynamodb.Table('accounts')


def scan_all_ibans():
    ibans = []
    kwargs = {'ConsistentRead': True}
    while True:
        response = table.scan(**kwargs)
        ibans.extend(account['iban'] for account in response['Items'])
        if 'LastEvaluatedKey' not in response:
            return ibans
        kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']


deadline = time.monotonic() + RETRY_FOR_SECONDS
while True:
    try:
        ddb_ibans = scan_all_ibans()
        break
    except (BotoCoreError, ClientError) as e:
        if time.monotonic() >= deadline:
            raise
        print(f"DynamoDB not available yet, retrying: {e}")
        time.sleep(2)

diff = set(ddb_ibans) - set(dg_ibans)

always(len(diff) == 0, "produced ibans match ibans in dynamo", {"diff": json.dumps(sorted(diff))})

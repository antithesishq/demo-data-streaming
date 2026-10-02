#!/usr/bin/env -S python3 -u

import boto3
import os
import json
from decimal import Decimal

from antithesis.assertions import always

initial_balance = Decimal(str(os.getenv('INITIAL_FUNDING_AMOUNT', 1000)))

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
final_total = sum(account['balance'] for account in accounts)
expected_total = len(accounts) * initial_balance

always(
    final_total == expected_total, 
    'Final total equals expected total', 
    { 
        'finalTotal': str(final_total), 
        'expectedTotal': str(expected_total) 
    }
)

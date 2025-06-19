#!/usr/bin/env -S python3 -u

import boto3
import flask
import os
import json

from kafka import KafkaConsumer
from antithesis import always

initial_balance = os.getenv('INITIAL_FUNDING_AMOUNT', 1000)

endpoint_url=f"http://{os.getenv('DYNAMO_ENDPOINT', 'ddb')}"
client = boto3.client('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
table = dynamodb.Table('accounts')

response = table.scan()
accounts = response['Items']
final_total = sum(account['balance'] for account in accounts)
expectedTotal = len(accounts) * initial_balance

always(final_total == expectedTotal, 'Final total equals expected total', {
    'finalTotal': final_total,
    'expectedTotal': expectedTotal
})
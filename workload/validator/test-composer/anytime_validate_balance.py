#!/usr/bin/env -S python3 -u
# Invariant: every account is funded with the initial amount exactly once, and
# transfers only move money between accounts, so the total balance always
# equals (number of accounts) x (initial amount).
#
# This runs at any time, including while DynamoDB is faulted. If DynamoDB is
# unreachable or the table doesn't exist yet, there is nothing to check right
# now, so exit 0 rather than failing the command.

import os
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from antithesis.assertions import always

initial_balance = int(os.getenv('INITIAL_FUNDING_AMOUNT', 1000))
endpoint_url = f"http://{os.getenv('DYNAMO_ENDPOINT', 'ddb:8000')}"

dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
table = dynamodb.Table('accounts')

try:
    response = table.scan(ConsistentRead=True)
except (BotoCoreError, ClientError) as e:
    print(f"DynamoDB unavailable, skipping balance check: {e}")
    sys.exit(0)

# Only a single-page scan is one consistent read. Across pages, a transfer
# can move money between a page already read and one not yet read.
if 'LastEvaluatedKey' in response:
    print("accounts table spans more than one scan page; skipping balance check")
    sys.exit(0)

accounts = response['Items']
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

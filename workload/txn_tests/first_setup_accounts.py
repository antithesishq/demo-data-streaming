#!/usr/bin/env python3

from antithesis.random import get_random
import boto3

endpoint_url = "http://localhost.localstack.cloud:4566"

client = boto3.client('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')

try:
    client.delete_table(
        TableName="accounts"
    )
except Exception as e:
    print(e)
    pass

table = dynamodb.create_table(
    TableName='accounts',
    AttributeDefinitions=[
        {
            'AttributeName': 'account_id',
            'AttributeType': 'S'
        },
    ],
    KeySchema=[
        {
            'AttributeName': 'account_id',
            'KeyType': 'HASH'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 40000,
        'WriteCapacityUnits': 40000
    }
)

table.wait_until_exists()

for id in range(100):
    table.put_item(
        Item={
            'account_id': str(id),
            'balance': get_random()
        }
    )


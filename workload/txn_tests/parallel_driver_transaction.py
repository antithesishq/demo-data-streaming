#!/usr/bin/env -S python3 -u

import json
import requests
from antithesis.random import get_random

headers = { 'Content-Type': 'application/json' }

txn_data = {
    'sender': 1,
    'recipient': 2,
    'amount': 10000
}

response = requests.post(
    f'http://localhost:5000/txn', 
    json=json.dumps(txn_data), 
    headers=headers, 
    timeout=30
)

print(response.status_code)
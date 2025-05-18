#!/usr/bin/env -S python3 -u

import json
import requests
from antithesis.random import get_random

headers = { 'Content-Type': 'application/json' }

txn_data = {
    'sender': 'a1',
    'receiver': 'a2',
    'sender_amt': get_random(),
    'recipient_amt': get_random()
}

response = requests.post(f'http://producer-py:5000/txn', json=json.dumps(txn_data), headers=headers, timeout=30 )

print(response.status_code)
print(response.text)
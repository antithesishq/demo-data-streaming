#!/usr/bin/env -S python3 -u

import os
import requests
from antithesis.assertions import sometimes
from antithesis.random import get_random

headers = { 'Content-Type': 'application/json' }

try:
    response = requests.post(f'http://consumer:3000/pass_through_consumer/bank/{get_random() % 100 + 1}', headers=headers, timeout=30)
except Exception as e:
    sometimes(True, 'Consumer passthrough fails', { 'error': str(e) })
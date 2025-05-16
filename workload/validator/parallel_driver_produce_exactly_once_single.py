#!/usr/bin/env -S python3 -u

import os
import requests
from antithesis.assertions import sometimes
from antithesis.random import get_random

headers = { 'Content-Type': 'application/json' }

try:
    response = requests.post(f'http://producer:3000/exactly_once_single/bank/{get_random() % 100 + 1}', headers=headers, timeout=30)
except Exception as e:
    sometimes(True, 'Produce at least once (batch) fails', { 'error': str(e) })
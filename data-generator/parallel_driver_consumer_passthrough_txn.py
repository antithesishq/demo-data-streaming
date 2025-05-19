#!/usr/bin/env -S python3 -u

import json
import requests
from antithesis.random import get_random

headers = { 'Content-Type': 'application/json' }

response = requests.post("http://consumer:3000/pass_through_consumer/txn/1", timeout=30)

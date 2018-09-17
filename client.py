import random
import json
import sys
import collections

import requests

from settings import PROPOSER_URLS

failures = collections.defaultdict(int)
for i in range(1):
    url = random.choice(PROPOSER_URLS)
    response = requests.post(url + "/",
        data=json.dumps({"value": "Hello world!"}), headers={'Content-Type': 'application/json'})
    print(response.text)
    if response.status_code == 200:
        sys.stdout.write('.')
    else:
        failures[url] += 1
        sys.stdout.write('x')
    sys.stdout.flush()

print("Failures", failures)

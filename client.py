import random
import json
import sys
import collections

import requests

from settings import PROPOSER_URLS
    
    
failures = collections.defaultdict(int)


def sync():
    url = random.choice(PROPOSER_URLS)
    response = requests.post(url + "/",
        data=json.dumps({
            "key": "foo", 
            "value": "This is the {} update!".format(random.random())
        }), headers={'Content-Type': 'application/json'})
    print(response.text)
    if 200 <= response.status_code < 300:
        sys.stdout.write('.')
    else:
        failures[url] += 1
        sys.stdout.write('x')
    sys.stdout.flush()

sync()
print("Failures", failures)


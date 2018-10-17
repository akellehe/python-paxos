import random
import json
import sys
import collections

import requests

from settings import AGENT_URL, AGENT_PORTS
    
failures = collections.defaultdict(int)

# Distinguished proposer/learner
port = random.choice(AGENT_PORTS)
url = AGENT_URL + ':8888/write'

def sync():
    response = requests.post(
        url,
        data=json.dumps({
            "key": "foo", 
            "predicate": "This is the {} update!".format(random.random()),
            "argument": 1
        }), 
        headers={'Content-Type': 'application/json'})

    print(response.text)
    if 200 <= response.status_code < 300:
        sys.stdout.write('.')
    else:
        failures[url] += 1
        sys.stdout.write('x')
    sys.stdout.flush()

sync()
print("Failures", failures)


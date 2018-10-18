import random
import tornado.httpclient
import tornado.ioloop
import json
import sys
import collections

import requests

from settings import AGENT_URL, AGENT_PORTS
    
failures = collections.defaultdict(int)

# Distinguished proposer/learner
url = AGENT_URL + ':8888/write'

client = tornado.httpclient.AsyncHTTPClient()

def get_results(resp):
    if 200 <= resp.code < 300:
        sys.stdout.write(resp.body + "\n")
        # sys.stdout.write('.')
    else:
        sys.stdout.write('x')
    sys.stdout.flush()

for i in range(10):
    print("url", url)
    request = tornado.httpclient.HTTPRequest(
        url=url,
        method='POST',
        headers={'Content-Type': 'application/json'},
        body=json.dumps({
            "key": "foo", 
            "predicate": "This is update #{}".format(i),
            "argument": random.random()
        }), 
    )
    client.fetch(request, callback=get_results)

tornado.ioloop.IOLoop.current().start()

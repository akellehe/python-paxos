import threading
import logging
import collections
import random
import json

import tornado.httpclient
import tornado.gen

from settings import AGENT_URL, AGENT_PORTS

logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
prepare_id_mutex = threading.Lock()
logger = logging.getLogger('agent')

class Agent:

    def __init__(self, url, port):
        self.url = url
        self.port = port

    @tornado.gen.coroutine
    def send(self, message):
        http_client = tornado.httpclient.AsyncHTTPClient()
        request = tornado.httpclient.HTTPRequest(
            url=self.url + ':' + str(self.port) + message.endpoint,
            method='POST',
            headers={'Content-Type': 'application/json'},
            body=json.dumps(message.to_json())
        )
        resp = yield http_client.fetch(request, raise_error=False)
        raise tornado.gen.Return(resp)

    def __repr__(self):
        return "<Agent url={}, port={}>".format(self.url, self.port)
       

class Agents:

    def __init__(self, agents):
        self.agents = agents

    def quorum(self, excluding=None):
        random.shuffle(self.agents)
        agents = [a for a in self.agents if a.port != excluding]
        required_for_quorum = int(len(self.agents) / 2) + 1
        return agents[0:required_for_quorum]

    def all(self):
        return self.agents


agents = Agents([Agent(AGENT_URL, port) 
    for port in AGENT_PORTS])


class Phase:

    def __init__(self, prepare=None):
        self.prepare = prepare 

    def to_json(self):
        return {
            'prepare': self.prepare.to_json() if self.prepare else None
        }
    
    @classmethod
    def from_request(cls, request):
        js = json.loads(request.body)
        prepare = None
        if js.get('prepare'):
            prepare = Prepare(**js.get('prepare'))
        return cls(prepare=prepare)

    @classmethod
    def from_response(cls, response):
        return cls.from_request(response)


    @tornado.gen.coroutine
    def fanout(self, expected=None):
        if not hasattr(self, 'endpoint'):
            raise NotImplementedError("Set an endpoint for the model")
        responses = []
        for agent in agents.all():
            resp = yield agent.send(self)
            if expected is not None:
                responses.append(expected.from_response(resp))
        raise tornado.gen.Return(responses)

    @tornado.gen.coroutine
    def send(self, quorum):
        if not hasattr(self, 'endpoint'):
            raise NotImplementedError("Set an endpoint for the model.")
        responses, issued, conflicting = [], [], []
        for agent in quorum:
            logger.info("Sending request to agent %s", agent)
            resp = yield agent.send(self)
            responses.append(resp)
            if resp.code == 200:
                issued.append(resp)
            elif resp.code == 400:
                conflicting.append(resp)
        raise tornado.gen.Return(tuple([responses, issued, conflicting]))


class Prepare(Phase):

    _id = 0
    endpoint = '/prepare'
    
    def __init__(self, id=None, key=None, predicate=None, argument=None):
        self.id = id
        if id is None:
            with prepare_id_mutex: 
                self.id = Prepare._id
                Prepare._id += 1
        
        self.key = key
        self.predicate = predicate
        self.argument = argument

    def to_json(self):
        return {
            'id': self.id,
            'key': self.key,
            'predicate': self.predicate,
            'argument': self.argument
        }

    @classmethod
    def from_request(cls, request):
        return Prepare(**json.loads(request.body))

    def __repr__(self):
        return "<Prepare id={}>".format(self.id)


class Promise(Phase):

    endpoint = '/promise'

    @classmethod
    def from_response(cls, response):
        resp = json.loads(response.body)
        prepare = resp.get('prepare')
        if prepare:
            return Promise(
                prepare=Prepare(**prepare))
        return Promise()

    def __repr__(self):
        if self.prepare:
            return "<Promise prepare={}>".format(self.prepare.to_json())
        else:
            return "<Promise (empty)>"


class Promises(Phase):

    def __init__(self, promises=None):
        self.promises = collections.defaultdict(dict)
        if promises is None:
            return
        for promise in promises:
            self.promises[promise.prepare.key][promise.prepare.id] = promise

    def clear(self):
        self.promises = collections.defaultdict(dict)

    def __contains__(self, promise):
        key = promise.prepare.key
        id = promise.prepare.id
        return key in self.promises and id in self.promises[key]

    def remove(self, prepare):
        try:
            del self.promises[prepare.key][prepare.id]
            if not self.promises[prepare.key]:
                del self.promises[prepare.key]
        except KeyError:
            logger.warning("Already removed promise %s", prepare)

    @classmethod
    def from_responses(cls, responses):
        promises = [Promise.from_response(response)
            for response in responses]
        return Promises([p for p in promises if p.prepare is not None])

    def highest_promise_for_key(self, key):
        promises = self.promises[key]
        if promises:
            return promises[max(promises)]

    def highest_numbered(self, key=None):
        highest = None
        if key:
            return self.highest_promise_for_key(key)
        for key in self.promises:
            promise = self.highest_promise_for_key(key)
            if promise.prepare:
                if not highest:
                    highest = promise
                elif highest.prepare.id < promise.prepare.id:
                    highest = promise
        return highest

    def add(self, promise):
        self.promises[promise.prepare.key][promise.prepare.id] = promise

    def get(self, key):
        if not self.promises[key]:
            return
        id = max(self.promises[key])
        return self.promises[key][id]

class Propose(Phase):
    endpoint = '/propose'
 
class Accept(Phase):
    endpoint = '/accept'

class Learn(Phase):
    endpoint = '/learn'

    def __repr__(self):
        return "<Learn id={}>".format(self.prepare.id)

class Success(Phase):

    def to_json(self):
        return {
            'prepare': self.prepare.to_json(),
            'status': 'SUCCESS',
        }


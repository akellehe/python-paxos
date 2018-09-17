import json
import logging

import tornado.web
import tornado.gen
import tornado.httpclient


logger = logging.getLogger('utils')


class Handler(tornado.web.RequestHandler):

    def respond(self, message):
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(message.to_json()))
        self.finish()


class Message:

    def to_json(self):
        raise NotImplemented(
            "to_json() is not implemented for this class")


class Proposal:

    _id = 0

    def __init__(self, id=None, key=None, value=None):
        self.id = id
        self.key = key
        self.value = value
        if id is None:
            self.id = Proposal._id
            logger.info("Incrementing Proposal._id to %s", Proposal._id)
            Proposal._id += 1

    def to_json(self):
        return {'id': self.id, 'key': self.key, 'value': self.value}

    @classmethod
    def from_json(cls, js):
        return Proposal(id=js.get('id'), key=js.get('key'), value=js.get('value'))

    def update(self, highest_accepted_proposal_id):
        print("Updating id to", max(self.id, highest_accepted_proposal_id))
        self.id = max(self.id, highest_accepted_proposal_id)
        Proposal._id = max(Proposal._id, highest_accepted_proposal_id)


class Prepare(Message):

    def __init__(self, proposal=None):
        if proposal is not None:
            self.proposal = proposal
        else:
            self.proposal = Proposal()

    def to_json(self):
        return {'proposal': self.proposal.to_json()}

    @classmethod
    def from_json(cls, js):
        return Prepare(proposal=Proposal.from_json(js.get('proposal')))


class AcceptRequest(Message):

    def __init__(self, proposal):
        self.proposal = proposal

    def to_json(self):
        return {'proposal': self.proposal.to_json()}

    @classmethod
    def from_json(cls, js):
        return AcceptRequest(proposal=Proposal.from_json(js.get('proposal')))


class AcceptRequestResponse(Message):

    ACK = 'ACK'
    NACK = 'NACK'
    COMMITTED = 'COMMITTED'

    def __init__(self, proposal, status='ACK'):
        self.proposal = proposal
        self.status = status
        self.error = None

    def set_status(self, status):
        self.status = status

    def to_json(self):
        return {'proposal': self.proposal.to_json(), 'status': self.status}

    @classmethod
    def from_json(cls, js):
        return AcceptRequestResponse(
            proposal=Proposal.from_json(js.get('proposal')),
            status=js.get('status')
        )


class Promise:

    ACK = 'ACK'
    NACK = 'NACK'

    def __init__(self, prepare, status='ACK'):
        self.prepare = prepare
        self.status = status

    def to_json(self):
        return {'prepare': self.prepare.to_json(),
                'status': self.status}

    @classmethod
    def from_json(cls, js):
        return Promise(prepare=Prepare.from_json(js.get('prepare')),
                       status=js.get('status'))


class PrepareResponse:

    def __init__(self, promise, last_promise):
        self.promise = promise
        self.last_promise = last_promise

    def to_json(self):
        return {
            'promise': self.promise.to_json(),
            'last_promise': self.last_promise.to_json() if self.last_promise else None
        }

    @classmethod
    def from_json(cls, js):
        last_promise = None
        logger.info("Converting to prepared response from json %s", js)
        if js.get('last_promise'):
            last_promise = Promise.from_json(js.get('last_promise'))
        return PrepareResponse(promise=Promise.from_json(js.get('promise')),
                               last_promise=last_promise)


@tornado.gen.coroutine
def send(url, message):
    """
    Anything fitting the `Message` type must implement `to_json()`
    """
    client = tornado.httpclient.AsyncHTTPClient()
    request = tornado.httpclient.HTTPRequest(
        url=url,
        method='POST',
        headers={'Content-Type': 'application/json'},
        body=json.dumps(message.to_json())
    )
    resp = yield client.fetch(request, raise_error=True)
    raise tornado.gen.Return(resp)

class ClientResponse(Message):

    COMMITTED = 'COMMITTED'
    ACK = 'ACK'
    NACK = 'NACK'

    def __init__(self, proposal, status):
        self.proposal = proposal
        self.status = status

    def to_json(self):
        return {'proposal': proposal.to_json(),
                'status': self.status}


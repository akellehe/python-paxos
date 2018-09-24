import json
import logging

import tornado.web
import tornado.gen
import tornado.httpclient



class Handler(tornado.web.RequestHandler):

    def respond(self, message, code=201):
        self.set_status(code)
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

    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'

    def __init__(self, proposal, status='ACCEPTED', error=None):
        self.proposal = proposal
        self.status = status
        self.error = error

    def set_status(self, status):
        self.status = status

    def to_json(self):
        return {'proposal': self.proposal.to_json(), 'status': self.status, 'error': self.error}

    @classmethod
    def from_json(cls, js):
        return AcceptRequestResponse(
            proposal=Proposal.from_json(js.get('proposal')),
            status=js.get('status'),
            error=js.get('error')
        )


class Promise:

    def __init__(self, prepare):
        self.prepare = prepare

    def to_json(self):
        return {'prepare': self.prepare.to_json()}

    @classmethod
    def from_json(cls, js):
        return Promise(prepare=Prepare.from_json(js.get('prepare')))


class PrepareResponse:

    def __init__(self, promise=None, last_promise=None):
        self.promise = promise
        self.last_promise = last_promise

    def to_json(self):
        return {
            'promise': self.promise.to_json() if self.promise else None,
            'last_promise': self.last_promise.to_json() if self.last_promise else None
        }

    @classmethod
    def from_json(cls, js):
        promise, last_promise = None, None
        logger.info("Converting to prepared response from json %s", js)
        if js.get('last_promise'):
            last_promise = Promise.from_json(js.get('last_promise'))
        if js.get('promise'):
            promise = Promise.from_json(js.get('promise'))
        return PrepareResponse(promise=promise, last_promise=last_promise)


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
    resp = yield client.fetch(request, raise_error=False)
    raise tornado.gen.Return(resp)


class ClientResponse(Message):

    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'

    def __init__(self, proposal, status):
        self.proposal = proposal
        self.status = status

    def to_json(self):
        return {'proposal': self.proposal.to_json(),
                'status': self.status}


class LearnerResponse(Message):

    def __init__(self, proposal, status):
        self.proposal = proposal
        self.status = status

    def to_json(self):
        return {'proposal': self.proposal.to_json(),
                'status': self.status}

    @classmethod
    def from_json(cls, js):
        return cls(
            proposal=Proposal.from_json(js.get('proposal')),
            status=js.get('status')
        )


def get_logger(name):
    logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
    return logging.getLogger(name)

logger = get_logger('utils')

"""
If the proposal's number N is higher than any previous proposal number received from any Proposer by the Acceptor, then the Acceptor must return a promise to ignore all future proposals having a number less than N. If the Acceptor accepted a proposal at some point in the past, it must include the previous proposal number and previous value in its response to the Proposer.

Otherwise, the Acceptor can ignore the received proposal. It does not have to answer in this case for Paxos to work. However, for the sake of optimization, sending a denial (Nack) response would tell the Proposer that it can stop its attempt to create consensus with proposal N.
"""
import json
import logging

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.gen

from tornado.options import define, options

from settings import LEARNER_URLS
from utils import (
    AcceptRequest,
    AcceptRequestResponse,
    Handler,
    Prepare,
    PrepareResponse,
    Promise,
    send,
)

define("port", default=8889, help="run on the given port", type=int)
logger = logging.getLogger("acceptor")

class Acceptor:

    _highest_proposal_to_date = -1
    _current_requests = {}

    def __init__(self):
        self.last_promise = None

    def set_last_promise(self, last_promise):
        self.last_promise = last_promise
        Acceptor._highest_proposal_to_date = last_promise.prepare.proposal.id

    def get_last_promise(self):
        return self.last_promise

    @classmethod
    def highest_proposal(cls, highest_proposal=None):
        if highest_proposal:
            cls._highest_proposal_to_date = highest_proposal
        return cls._highest_proposal_to_date

    @classmethod
    def should_promise(cls, prepare):
        """
        :return: A boolean: Whether or not to make a promise based on the proposal.
        """
        logger.info("Proposal id %s highest proposal %s", prepare.proposal.id, cls.highest_proposal())
        return prepare.proposal.id > cls.highest_proposal()

    @classmethod
    def should_accept(cls, accept_request):
        return accept_request.proposal.id >= cls.highest_proposal()


acceptor = Acceptor()


class PrepareHandler(Handler):

    @tornado.gen.coroutine
    def post(self):
        """
        Receive the prepare statement from the proposer.
        """
        prepare = Prepare.from_json(json.loads(self.request.body))
        last_promise = acceptor.get_last_promise()
        promise = Promise(prepare=prepare,
                          status=Promise.ACK if acceptor.should_promise(prepare) else Promise.NACK)

        if promise.status == promise.NACK:
            logger.info("Acceptor N'acked.")
        else:
            logger.info("Acceptor is promising to accept proposals >= %s", prepare.proposal.id)
            acceptor.set_last_promise(promise)
        self.respond(PrepareResponse(promise, last_promise))

    @tornado.gen.coroutine
    def get(self):
        self.write({"status": "SUCCESS"})
        self.finish()


class AcceptRequestHandler(Handler):

    @tornado.gen.coroutine
    def post(self):
        """
        Receive the AcceptRequest statement from the proposer.
        """
        logger.info("Request body in AcceptRequestHandler: %s", self.request.body)
        accept_request = AcceptRequest.from_json(json.loads(self.request.body))
        accept_request_response = AcceptRequestResponse(accept_request.proposal,
                                                        status=AcceptRequestResponse.NACK)
        if acceptor.should_accept(accept_request):
            for learner_url in LEARNER_URLS:
                resp = yield send(learner_url + "/learn", accept_request_response)  # send to all learners.
                learner_response = json.loads(resp.body)
                if learner_response.get('status') == AcceptRequestResponse.COMMITTED:
                    accept_request_response.set_status(AcceptRequestResponse.COMMITTED)
                elif resp.code == 200:
                    accept_request_response.set_status(AcceptRequestResponse.ACK)
                else:
                    accept_request_response.set_status(AcceptRequestResponse.NACK)
        else:
            logger.error("Acceptor received proposal id=%s but the highest is %s. REJECTED",
                         accept_request.proposal.id, acceptor.highest_proposal())
            accept_request_response.set_status(AcceptRequestResponse.NACK)

        if accept_request_response.status == AcceptRequestResponse.NACK:
            self.set_status(409)

        self.respond(accept_request_response)  # Send to proposer

    @tornado.gen.coroutine
    def get(self):
        self.write({"status": "SUCCESS"})
        self.finish()


def main():
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/prepare", PrepareHandler),
        (r"/accept_request", AcceptRequestHandler),
    ])
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.bind(options.port)
    http_server.start()
    logger.info("Acceptor listening on port %s", options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

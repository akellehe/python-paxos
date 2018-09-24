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
    get_logger,
    Handler,
    Prepare,
    PrepareResponse,
    Promise,
    send,
)

define("port", default=8889, help="run on the given port", type=int)
logger = get_logger('acceptor')

class Acceptor:

    _highest_proposal_to_date = -1
    _current_requests = {}

    @classmethod
    def remove_last_promise(cls, key):
        logger.warning("Removing promises for key %s", key)
        if key not in Acceptor._current_requests:
            logger.warning("Key not found in current requests: k=%s, r=%s", key, Acceptor._current_requests)
            return None
        logger.warning("Removing current request for key %s: %s", key, Acceptor._current_requests)
        del Acceptor._current_requests[key]
        logger.warning("after: %s", Acceptor._current_requests)

    @classmethod
    def set_last_promise(cls, last_promise):
        logger.warning("Setting last promise for key %s", last_promise.prepare.proposal.key)
        Acceptor._current_requests[last_promise.prepare.proposal.key] = last_promise
        cls._highest_proposal_to_date = last_promise.prepare.proposal.id

    @classmethod
    def get_last_promise(cls, key):
        if key in Acceptor._current_requests:
            return Acceptor._current_requests[key]
        return None

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
    
    @tornado.gen.coroutine
    def send_to_learners(self, proposal):
        successes, failures = [], []
        for learner_url in LEARNER_URLS:
            resp = yield send(learner_url + "/learn", proposal)  # send to all learners.
            if resp.code == 201:
                successes.append(learner_url)
            else:
                logger.error("Learner failed with response: %s: %s", resp.code, resp.body)
                failures.append(learner_url)
        raise tornado.gen.Return(tuple([len(successes) > len(failures), failures]))


acceptor = Acceptor()


class PrepareHandler(Handler):

    def reject_prepare(self, prepare):
        # The proposal number was too low
        self.respond(PrepareResponse())

    def issue_promise(self, prepare):
        last_promise = acceptor.get_last_promise(prepare.proposal.key)
        promise = Promise(prepare=prepare)
        logger.info("Acceptor is promising to accept proposals >= %s", prepare.proposal.id)
        if last_promise:
            logger.info("But the acceptor has an existing promise for key %s: %s", prepare.proposal.key, last_promise.to_json())
        self.respond(PrepareResponse(promise, last_promise),
                     code=202 if not last_promise else 409)
        acceptor.set_last_promise(promise)

    def post(self):
        """
        Receive the prepare statement from the proposer.
        """
        prepare = Prepare.from_json(json.loads(self.request.body))
        if acceptor.should_promise(prepare):
            self.issue_promise(prepare)
        else:
            self.reject_prepare(prepare)

    def get(self):
        self.write({"status": "SUCCESS"})
        self.finish()


class AcceptRequestHandler(Handler):
        
    def reject_accept_request(self, accept_request, failures=None):
        if failures is None:
            failures = []
        self.respond(AcceptRequestResponse(
            accept_request.proposal,
            status=AcceptRequestResponse.REJECTED,
            error="\n".join(failures)
        ), code=500)

    def accept_accept_request(self, accept_request):
        acceptor.remove_last_promise(accept_request.proposal.key)
        self.respond(AcceptRequestResponse(
            accept_request.proposal,
            status=AcceptRequestResponse.ACCEPTED
        ), code=202)

    @tornado.gen.coroutine
    def post(self):
        """
        Receive the AcceptRequest statement from the proposer.
        """
        logger.info("Request body in AcceptRequestHandler: %s", self.request.body)
        accept_request = AcceptRequest.from_json(json.loads(self.request.body))
        success, failures = False, []
        if acceptor.should_accept(accept_request):
            success, failures = yield acceptor.send_to_learners(accept_request.proposal)
        if not success:
            self.reject_accept_request(accept_request, failures)
        else:
            self.accept_accept_request(accept_request)

    @tornado.gen.coroutine
    def get(self):
        self.write({"status": "SUCCESS"})
        self.finish()


def main():
    """
    TODO: When we send a promise to one majority, then we send the "accept" to another majority; the first majority
    does not know to retire the promise. That means new requests will be interpreted as a conflict. What is the most
    stateless way to maintain information about what should be done with the pending promise?

     - In the case that a promise is lost (e.g. something falls over during it's processing) it won't be written to the
       learners and needs to be restarted by the proposer.
     - If the proposer remembers the quorum to which it submitted the prepare; then we have a single point of failure
       for tracking the completeness of that processing (the proposer).
     - If we leave the pending request in the state of the acceptors; there is no way to know on which acceptors the
       stale promise should be retired.
    """
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

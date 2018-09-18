import random
import logging
import json

import tornado.httpclient
import tornado.ioloop
import tornado.httpserver
import tornado.options
import tornado.web
import tornado.gen
from tornado.options import define, options

from settings import ACCEPTOR_URLS, TORNADO_SETTINGS
from utils import Prepare, Proposal, PrepareResponse, AcceptRequest, AcceptRequestResponse, send, Promise, Handler, ClientResponse

define("port", default=8888, help="run on the given port", type=int)
logger = logging.getLogger("proposer")


class Proposer:

    def __init__(self, acceptor_urls):
        self.acceptors = acceptor_urls

    def how_many_is_a_quorum(self):
        num_candidates = len(self.acceptors) # Naive. Some may have failed.
        return int(num_candidates/2) + 1

    def get_quorum(self):
        quorum = self.how_many_is_a_quorum()
        random.shuffle(self.acceptors)
        return self.acceptors[0:quorum]

    @tornado.gen.coroutine
    def send_prepare(self, key, value):
        print("Proposer is sending prepare...")
        prepare_responses = []
        failed_responses = []
        prepare = Prepare(proposal=Proposal(key=key, value=value))
        print("Prepare created with proposal.id=", prepare.proposal.id)
        for url in self.get_quorum():
            resp = yield send(url + "/prepare", prepare)
            if resp.code == 200:
                prepare_response = PrepareResponse.from_json(json.loads(resp.body))
                if prepare_response.promise.status == Promise.ACK:
                    prepare_responses.append(prepare_response)
                else:
                    failed_responses.append(prepare_response)
            else:
                raise Exception("/prepare call failed. " + resp.body)
        raise tornado.gen.Return([prepare, prepare_responses, failed_responses])

    @tornado.gen.coroutine
    def send_accept_request(self, accept_request):
        print("Proposer is sending AcceptRequest")
        accept_request_responses = []
        for url in self.get_quorum():
            resp = yield send(url + "/accept_request", accept_request)
            if resp.code == 200:
                accept_request_responses.append(
                    AcceptRequestResponse.from_json(
                        json.loads(resp.body)))
            else:
                arr = AcceptRequestResponse(
                        proposal=accept_request.proposal,
                        status=AcceptRequestResponse.NACK)
                arr.error = str(resp.code) + ": " + resp.text
                accept_request_responses.append(arr)

        raise tornado.gen.Return(accept_request_responses)

    @classmethod
    def get_max_proposal_id(cls, prepare_responses):
        max_proposal = -1
        for prepare_response in prepare_responses:
            if prepare_response.last_promise:

        print("Max proposal id promised by acceptors is", max_proposal)
        return max_proposal


class ClientHandler(Handler):

    proposer = Proposer(ACCEPTOR_URLS)

    @tornado.gen.coroutine
    def post(self):
        logger.info("Got request body %s", self.request.body)
        client_request = json.loads(self.request.body)
        key, value = client_request.get('key'), client_request.get('value')

        # Phase 1a: Prepare
        # Send prepare with id >= any other prepare from this proposer.
        prepare, prepare_responses, failed_responses = yield self.proposer.send_prepare(key, value)


        # Phase 2a: Accept Request
        # If there are enough promises set; maybe-update the proposal value.
        if len(prepare_responses) < self.proposer.how_many_is_a_quorum():
            raise tornado.web.HTTPError(status_code=409,
                                        log_message="Failed to get the required number of promises. {}/{}".format(
                                        len(prepare_responses), self.proposer.how_many_is_a_quorum()))

        accept_request_responses = yield self.proposer.send_accept_request(
            AcceptRequest(prepare.proposal))

        client_response = ClientResponse(prepare.proposal, status=ClientResponse.NACK)
        for accept_request_response in accept_request_responses:
            if accept_request_response.status == AcceptRequestResponse.COMMITTED:
                client_response.status = ClientResponse.COMMITTED
                break

        self.respond(accept_request_response)

    @tornado.gen.coroutine
    def get(self):
        self.write({"status": "SUCCESS"})
        self.finish()


def main():
    """
    This guy has to run synchronously in order to prevent proposal ids from conflicting.

    We're using only a single proposer to avoid the dueling proposers issue.

    :return:
    """
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/", ClientHandler),
    ], **TORNADO_SETTINGS)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    print("Proposer listening on port", options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

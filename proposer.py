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
from utils import get_logger, Prepare, Proposal, PrepareResponse, AcceptRequest, AcceptRequestResponse, send, Promise, Handler, ClientResponse

define("port", default=8888, help="run on the given port", type=int)
logger = get_logger('proposer')


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
        logger.info("Proposer is sending prepare...")
        prepare_responses = []
        failed_responses = []
        prepare = Prepare(proposal=Proposal(key=key, value=value))
        logger.info("Prepare created with proposal.id=%s", prepare.proposal.id)
        for url in self.get_quorum():
            resp = yield send(url + "/prepare", prepare)
            if resp.code == 409:
                logger.warning("There is an earlier promise to be handled.")
            if resp.code == 202 or resp.code == 409:  # 202=Accepted, a promise was issued with no earlier promise
                prepare_response = PrepareResponse.from_json(json.loads(resp.body))
                prepare_responses.append(prepare_response)
            else:
                failed_responses.append(resp)
        raise tornado.gen.Return([prepare, prepare_responses, failed_responses])

    @tornado.gen.coroutine
    def send_propose(self, accept_request):
        logger.info("Proposer is sending AcceptRequest")
        accept_request_responses = []
        for url in self.get_quorum():
            resp = yield send(url + "/accept_request", accept_request)
            if resp.code == 202:  # 202=Accepted
                accept_request_responses.append(
                    AcceptRequestResponse.from_json(
                        json.loads(resp.body)))
            else:
                raise tornado.web.HTTPError(status_code=resp.code,
                                            log_message="Proposal failed to be accepted by a quorum.")
        raise tornado.gen.Return(accept_request_responses)


class ClientHandler(Handler):

    proposer = Proposer(ACCEPTOR_URLS)

    @tornado.gen.coroutine
    def post(self):
        logger.info("Got request body %s", self.request.body)
        client_request = json.loads(self.request.body)
        key, value = client_request.get('key'), client_request.get('value')

        prepare, prepare_responses, failed_responses = yield self.proposer.send_prepare(
            key, value)

        if len(prepare_responses) < self.proposer.how_many_is_a_quorum():
            raise tornado.web.HTTPError(status_code=412,  # Precondition failed. The required quorum is not available.
                                        log_message="Failed to reach a quorum of acceptors. {}/{}".format(
                                        len(prepare_responses), self.proposer.how_many_is_a_quorum()))

        for prepare_response in prepare_responses:
            last_promise = prepare_response.last_promise
            promise = prepare_response.promise
            if last_promise and last_promise.prepare.proposal.value == promise.prepare.proposal.value:
                # Another proposer has this in progress. Update the ID to match the in-progress version.
                prepare.proposal.id = last_promise.prepare.proposal.id
            elif last_promise and last_promise.prepare.proposal.value != promise.prepare.proposal.value:
                raise tornado.web.HTTPError(status_code=412,  # Precondition failed.
                                            log_message="There is an earlier promise issued for the given record.")

        accept_request_responses = yield self.proposer.send_propose(
            AcceptRequest(prepare.proposal))

        client_response = ClientResponse(prepare.proposal, status=ClientResponse.FAILURE)
        for accept_request_response in accept_request_responses:
            if accept_request_response.status == AcceptRequestResponse.ACCEPTED:
                client_response.status = ClientResponse.SUCCESS

        self.respond(client_response, code=201)

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
    logger.info("Proposer listening on port %s", options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

import json
import logging
import collections

import tornado.httpserver
import tornado.options
import tornado.web
import tornado.ioloop

from tornado.options import options, define

from utils import Handler, Proposal, LearnerResponse, get_logger

from settings import TORNADO_SETTINGS, ACCEPTOR_URLS


payloads = collections.defaultdict(lambda: {'payload': None})
logger = get_logger('learner')

define("port", default=8888, help="run on the given port", type=int)


class Learner(Handler):

    def post(self):
        """
        Receive the AcceptRequest statement from the proposer.
        """
        proposal = Proposal.from_json(json.loads(self.request.body))
        logger.info("Got a proposal for key %s", proposal.key)
        logger.info("Setting payload to %s", proposal.value)
        payloads[proposal.key]['payload'] = proposal.value
        self.respond(LearnerResponse(
            proposal=proposal,
            status='SUCCESS'
        ), 201)

    @tornado.gen.coroutine
    def get(self):
        self.write({'payloads': payloads})
        self.finish()


def main():
    tornado.options.parse_command_line()
    application = tornado.web.Application([
        (r"/learn", Learner),
    ], **TORNADO_SETTINGS)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    logger.info("Learner listening on port %s", options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(payloads)

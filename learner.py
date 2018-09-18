import json
import logging
import collections

import tornado.httpserver
import tornado.options
import tornado.web
import tornado.ioloop

from tornado.options import options, define

from utils import Handler, Proposal

from settings import TORNADO_SETTINGS, ACCEPTOR_URLS


QUORUM = int(len(ACCEPTOR_URLS) / 2) + 1
payloads = collections.defaultdict(lambda: {'votes': 0, 'payload': None})
logger = logging.getLogger('learner')

define("port", default=8888, help="run on the given port", type=int)


class Learner(Handler):

    def post(self):
        """
        Receive the AcceptRequest statement from the proposer.
        """
        proposal = Proposal.from_json(json.loads(self.request.body).get('proposal'))
        logger.info("Got a proposal for key %s", proposal.key)
        logger.info("Setting payload to %s", proposal.value)
        payloads[proposal.key]['payload'] = proposal.value
        payloads[proposal.key]['votes'] += 1
        logger.info("Votes is now %s", payloads[proposal.key]['votes'])

        self.set_header('Content-Type', 'application/json')
        if payloads[proposal.key]['votes'] >= QUORUM:
            self.write({'status': 'COMMITTED'})
        elif payloads[proposal.key]['votes'] < QUORUM:
            self.write({'status': 'VOTED'})
        self.finish()

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
    print("Learniner listening on port", options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(payloads)

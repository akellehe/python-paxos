import logging
import json

import tornado.httpclient
import tornado.ioloop
import tornado.httpserver
import tornado.options
import tornado.web
import tornado.gen
from tornado.options import define, options

from paxos.acceptor import PrepareAcceptor, ProposeAcceptor
from paxos.proposer import Proposer
from paxos.learner import Learner

from paxos.api import Handler

from settings import TORNADO_SETTINGS

define("port", default=8888, help="run on the given port", type=int)

logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
logger = logging.getLogger('agent')


class Reader(Handler):

    def get(self):
        for promise in Learner.ordered_rounds:
            self.write(json.dumps(promise.to_json()) + "\n")
        self.set_status(200)
        self.set_header('Content-Type', 'application/json')
        self.finish()


def get_app():
    return tornado.web.Application([
        (r"/read", Reader),
        (r"/write", Proposer),
        (r"/prepare", PrepareAcceptor),
        (r"/propose", ProposeAcceptor),
        (r"/learn", Learner)
    ], **TORNADO_SETTINGS)


def main():
    """
    This guy has to run synchronously in order to prevent proposal ids from conflicting.

    We're using only a single proposer to avoid the dueling proposers issue.

    :return:
    """
    tornado.options.parse_command_line()
    application = get_app()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    logger.info("Proposer listening on port %s", options.port)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()

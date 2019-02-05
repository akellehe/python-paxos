import logging

from paxos.api import Handler
from paxos.models import Learn, Promises, Success

import tornado.gen

logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
logger = logging.getLogger('agent')


class Learner(Handler):

    ordered_rounds = []
    completed_rounds = Promises()

    @tornado.gen.coroutine
    def post(self):
        learn = Learn.from_request(self.request)
        logger.info("Adding new learn, %s, to completed rounds.", learn.to_json())
        Learner.completed_rounds.add(learn)
        Learner.ordered_rounds.append(learn)
        success = Success(prepare=learn.prepare)
        self.respond(code=200, message=success)
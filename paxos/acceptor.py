import logging

import tornado.gen

from paxos.api import Handler
from paxos.learner import Learner
from paxos.models import (
    Accept,
    Prepare,
    Promise,
    Promises,
    Propose,
)

logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
logger = logging.getLogger('agent')


class PrepareAcceptor(Handler):

    @tornado.gen.coroutine
    def post(self):
        prepare = Prepare.from_request(self.request)
        in_progress = Promises.current.get(prepare.key)
        last_accepted = Learner.completed_rounds.highest_numbered(prepare.key)
        if in_progress:
            logger.info("Promise in progress already %s", in_progress)
            if in_progress.prepare.id == prepare.id:
                raise Exception("Prepare IDs match.")
            if in_progress.prepare.id > prepare.id:
                # Some replica has issued a higher promise
                # than ours. Abort.
                logger.warning("Existing promise is higher.")
                self.respond(code=400, message=in_progress)
            elif last_accepted is None or (
                    in_progress.prepare.id > last_accepted.prepare.id):  # >= since we could have just learned but not removed the existing process because this is all async
                # Complete the in-progress promise first
                # Possible for the incoming promise to have the same ID as the existing one.
                logger.info("Must complete earlier promise first: %s", in_progress)
                self.respond(code=200, message=in_progress)
            else:
                logger.info("New promise is higher. Issuing promise.")
                self.respond(code=200, message=Promise())
        elif last_accepted is None or prepare.id > last_accepted.prepare.id:
            logger.info("Adding a new promise for prepare %s", prepare)
            Promises.current.add(Promise(prepare=prepare))
            self.respond(code=200, message=Promise())
        else:
            logger.warning("Prepare has a lower ID than the last accepted proposal")
            logger.warning("prepare: %s, last_accepted: %s", prepare, last_accepted)
            self.respond(code=400, message=last_accepted)


class ProposeAcceptor(Handler):

    @tornado.gen.coroutine
    def post(self):
        propose = Propose.from_request(self.request)
        logger.info("Removing old promise, %s, on Accept", propose.prepare)
        Promises.current.remove(propose.prepare)
        self.respond(code=200,
                     message=Accept(prepare=propose.prepare))

import logging
import collections
import json

import tornado.httpclient
import tornado.ioloop
import tornado.httpserver
import tornado.options
import tornado.web
import tornado.gen
from tornado.options import options

from paxos.api import Handler

from paxos.models import (
    agents,
    Learn,
    MultiPrepare,
    MultiPromise,
    Prepare,
    Promise,
    Promises,
    Propose,
    Success
)

logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
logger = logging.getLogger('agent')


def get_promises_for_key(key, start=None, stop=None):
    """
    Makes the initial request for all promises [0, inf] for a given `key`.

    The acceptor will respond with the
    """
    mp = MultiPrepare(key=key)
    quorum = agents.quorum(excluding=options.port)
    Promises.current.add(MultiPromise(mp))


class Proposer(Handler):

    @tornado.gen.coroutine
    def post(self):
        """
        {
            key: <str>,
            predicate: <str>,
            argument: <str|int>
        }
        """
        successes = []
        request = json.loads(self.request.body)
        prepare = Prepare(**request)
        prepares = collections.deque([prepare])
        Promises.current.add(Promise(prepare=prepare))
        quorum = agents.quorum(excluding=options.port)
        while prepares:  # TODO: Timeout here.
            prepare = prepares.popleft()
            logging.info("Sending prepare for %s", prepare)
            send_response = yield prepare.send(quorum)
            responses, issued, conflicting = send_response
            logger.info("Got %s issued and %s conflicting", len(issued), len(conflicting))
            logger.info("Response codes: %s", ", ".join([str(r.code) for r in responses]))
            if conflicting:  # Issue another promise.
                logger.warning("%s was pre-empted by a higher ballot. retrying.".format(prepare.id))
                prepares.append(
                    Prepare(key=prepare.key,
                            predicate=prepare.predicate,
                            argument=prepare.argument))
                continue
            elif len(issued) != len(quorum):
                raise tornado.web.HTTPError(status_code=500,
                                            log_message='FAILED to acquire quorum on Promise')
            promises = Promises.from_responses(responses)
            earlier_promise = promises.highest_numbered()
            if earlier_promise and earlier_promise not in Promises.current:  # Repair.
                prepares.append(prepare)
                prepare = earlier_promise.prepare

            # Now we have a promise.
            responses, issued, conflicting = yield Propose(prepare=prepare).send(quorum)
            if len(issued) == len(quorum):
                logger.info("Got success for propose %s. Learning...", prepare)
                successes = yield Learn(prepare).fanout(expected=Success)
            elif conflicting:
                logger.error("Conflicting promise detected. Will re-issue.")
            else:
                raise tornado.web.HTTPError(status_code=500,
                                            log_message='Failed to acquire quorum on Accept')

        if len(successes) == len(agents.all()):
            Promises.current.remove(prepare)
            self.respond(Success(prepare))
        else:
            logger.error("Got %s successes with a required quorum of %s", len(successes), len(agents.all()))
            raise tornado.web.HTTPError(status_code=500,
                                        log_message='Failed to acquire quorum on Learn')

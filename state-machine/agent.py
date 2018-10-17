import logging
import collections
import json

import tornado.httpclient
import tornado.ioloop
import tornado.httpserver
import tornado.options
import tornado.web
import tornado.gen
from tornado.options import define, options

from settings import TORNADO_SETTINGS
from models import (
    Accept, 
    agents,
    Learn,
    Prepare, 
    Promise, 
    Promises,
    Propose, 
    Success
)

define("port", default=8888, help="run on the given port", type=int)

logging.basicConfig(format='%(levelname)s - %(filename)s:L%(lineno)d pid=%(process)d - %(message)s')
logger = logging.getLogger('agent')
current_promises = Promises()
completed_rounds = Promises()


class Handler(tornado.web.RequestHandler):
    
    def respond(self, message, code=200):
        self.set_status(code)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(message.to_json()))
        self.finish()


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
        request = json.loads(self.request.body)
        prepares = collections.deque([Prepare(**request)])
        quorum = agents.quorum(excluding=options.port)
        while prepares: # TODO: Timeout here.
            prepare = prepares.popleft()
            logging.info("Sending prepare for %s", prepare)
            responses = yield prepare.send(quorum)
            if len(responses) < len(quorum):
                raise tornado.web.HTTPError(status_code=500,
                    log_message='Failed to acquire quorum on Promise')
            logger.info("Got %s responses", len(responses))
            logger.info("Codes %s", ",".join([str(r.code) for r in responses]))
            promises = Promises.from_responses(responses)
            earlier_promise = promises.highest_numbered()
            if earlier_promise:
                prepares.append(earlier_promise.prepare)
                prepares.append(prepare)

        # Now we have a promise.
        responses = yield Propose(prepare=prepare).send(quorum)
        responses = [r for r in responses if r.code == 200]
        if len(responses) == len(quorum):
            logger.info("Got success for propose %s. Learning...", prepare)
            successes = yield Learn(prepare).fanout(
                expected=Success)
        else:
            raise tornado.web.HTTPError(status_code=500,
                log_message='Failed to acquire quorum on Accept')

        if len(successes) == len(agents.all()):
            self.respond(Success(prepare))
        else:
            logger.error("Got %s successes with a required quorum of %s", len(successes), len(agents.all()))
            raise tornado.web.HTTPError(status_code=500,
                log_message='Failed to acquire quorum on Learn')


class PrepareAcceptor(Handler):

    @tornado.gen.coroutine
    def post(self):
        prepare = Prepare.from_request(self.request)
        in_progress = current_promises.get(prepare.key)
        last_accepted = completed_rounds.highest_numbered(prepare.key)
        if in_progress:
            logger.info("Promise in progress already %s", in_progress)
            if in_progress.prepare.id >= prepare.id:
                # Some replica has issued a higher promise 
                # than ours. Abort.
                logger.warning("Existing promise is higher.")
                self.respond(code=400, message=in_progress) 
            elif last_accepted is None or (in_progress.prepare.id > last_accepted.prepare.id):
                # Complete the in-progress promise first
                logger.info("Must complete earlier promise first: %s", in_progress)
                self.respond(code=200, message=in_progress)
            else:
                raise Exception("We should never get here.")
        elif last_accepted is None or prepare.id > last_accepted.prepare.id:
            logger.info("Adding a new promise for prepare %s", prepare)
            current_promises.add(Promise(prepare=prepare))
            logger.info("Issuing a new promise for prepare %s", prepare)
            self.respond(code=200, message=Promise())
        else:
            logger.warning("Prepare has a lower ID than the last accepted proposal")
            logger.warning("prepare: %s, last_accepted: %s", prepare, last_accepted)
            raise Exception("What happened?")


class ProposeAcceptor(Handler):

    @tornado.gen.coroutine
    def post(self):
        propose = Propose.from_request(self.request)
        current_promises.remove(propose.prepare)
        self.respond(code=200, 
            message=Accept(prepare=propose.prepare))


class Learner(Handler):

    def post(self):
        learn = Learn.from_request(self.request)
        completed_rounds.add(learn)
        success = Success(prepare=learn.prepare)
        self.respond(code=200, message=success)


def get_app():
    return tornado.web.Application([
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

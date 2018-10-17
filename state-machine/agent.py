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
        prepare = Prepare(**request)
        prepares = collections.deque([prepare])
        current_promises.add(Promise(prepare=prepare))
        quorum = agents.quorum(excluding=options.port)
        while prepares: # TODO: Timeout here.
            prepare = prepares.popleft()
            logging.info("Sending prepare for %s", prepare)
            responses, issued, conflicting = yield prepare.send(quorum)
            logger.info("Got %s issued and %s conflicting", len(issued), len(conflicting))
            logger.info("Response codes: %s", ", ".join([str(r.code) for r in responses]))
            if conflicting: # Issue another promise.
                logger.warning("Issuing a later promise after %s was rejected".format(prepare.id))
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
            if earlier_promise and earlier_promise not in current_promises:
                prepares.append(prepare)
                prepare = earlier_promise.prepare

            # Now we have a promise.
            responses, issued, conflicting = yield Propose(prepare=prepare).send(quorum)
            if len(issued) == len(quorum):
                logger.info("Got success for propose %s. Learning...", prepare)
                successes = yield Learn(prepare).fanout(expected=Success)
            elif conflicting:
                logger.error("Conflicting promise detected. Will re-issue.")
                raise Exception("Conflicting promise detected.")
            else:
                raise tornado.web.HTTPError(status_code=500,
                    log_message='Failed to acquire quorum on Accept')

        if len(successes) == len(agents.all()):
            current_promises.remove(prepare)
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
            if in_progress.prepare.id == prepare.id:
                raise Exception("Prepare IDs match.")
            if in_progress.prepare.id > prepare.id:
                # Some replica has issued a higher promise 
                # than ours. Abort.
                logger.warning("Existing promise is higher.")
                self.respond(code=400, message=in_progress) 
            elif last_accepted is None or (in_progress.prepare.id > last_accepted.prepare.id): # >= since we could have just learned but not removed the existing process because this is all async
                # Complete the in-progress promise first
                # Possible for the incoming promise to have the same ID as the existing one.
                logger.info("Must complete earlier promise first: %s", in_progress)
                self.respond(code=200, message=in_progress)
            else:
                #raise Exception("We should never get here.")
                logger.info("New promise is higher. Issuing promise.")
                self.respond(code=200, message=Promise())
        elif last_accepted is None or prepare.id > last_accepted.prepare.id:
            logger.info("Adding a new promise for prepare %s", prepare)
            current_promises.add(Promise(prepare=prepare))
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
        current_promises.remove(propose.prepare)
        self.respond(code=200, 
            message=Accept(prepare=propose.prepare))


class Learner(Handler):

    @tornado.gen.coroutine
    def post(self):
        learn = Learn.from_request(self.request)
        logger.info("Adding new learn, %s, to completed rounds.", learn.to_json())
        completed_rounds.add(learn)
        success = Success(prepare=learn.prepare)
        self.respond(code=200, message=success)


class Reader(Handler):

    def get(self):
        committed = collections.defaultdict(dict)
        for key, promises in completed_rounds.promises.items():
            for id, promise in promises.items():
                committed[key][id] = promise.prepare.to_json()
        self.set_status(200)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(committed))
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

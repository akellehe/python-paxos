import json

import tornado.web


class Handler(tornado.web.RequestHandler):

    def respond(self, message, code=200):
        self.set_status(code)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(message.to_json()))
        self.finish()


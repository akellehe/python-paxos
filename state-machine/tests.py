import unittest
import mock
import json

import tornado.testing
import tornado.httpclient
import tornado.concurrent

import agent
from models import (
    Accept, Agent, Agents, agents, Learn, Phase, Prepare, Promise,
    Promises, Propose, Success
)


class TestPhase(tornado.testing.AsyncTestCase):

    def test_to_json(self):
        prepare = Prepare(id=1, 
            key='foo', predicate='incr', argument=1)
        phase = Phase(prepare=prepare)
        self.assertEqual(phase.to_json(),
            {'prepare': {
                'id': 1, 
                'key': 'foo', 
                'predicate': 'incr', 
                'argument': 1}})

    def test_from_request(self):
        request = tornado.httpclient.HTTPRequest(
            body= json.dumps({'prepare': {
                'id': 2, 
                'key': 'bar', 
                'predicate': 'decr', 
                'argument': -1}}),
            method='POST',
            headers={'Content-Type': 'application/json'},
            url='/testing')
        phase = Phase.from_request(request)
        self.assertIsInstance(phase.prepare, Prepare)
        self.assertEqual(phase.prepare.id, 2)
        self.assertEqual(phase.prepare.key, 'bar')
        self.assertEqual(phase.prepare.predicate, 'decr')
        self.assertEqual(phase.prepare.argument, -1)

    @tornado.testing.gen_test
    def test_fanout(self):
        prepare = Prepare(id=1,
            key='foo', predicate='incr', argument=1)
        phase = Phase(prepare=prepare)

        fut = tornado.concurrent.Future()
        response = mock.Mock()
        response.body = json.dumps(phase.to_json())
        fut.set_result(response)

        phase.endpoint = '/testing'

        client = mock.Mock()
        client.fetch = mock.Mock()
        client.fetch.return_value = fut
        with mock.patch('tornado.httpclient.AsyncHTTPClient',
                        return_value=client):
            successes = yield phase.fanout(expected=Success)
            self.assertEqual(len(successes), len(agents.all()))

    @tornado.testing.gen_test
    def test_fanout_raises_not_implemented(self):
        prepare = Prepare(id=1,
                          key='foo', predicate='incr', argument=1)
        phase = Phase(prepare=prepare)
        with self.assertRaises(NotImplementedError):
            should_fail = yield phase.fanout()

    @tornado.testing.gen_test
    def test_send(self):
        prepare = Prepare(id=1,
                          key='foo', predicate='incr', argument=1)
        phase = Phase(prepare=prepare)

        fut = tornado.concurrent.Future()
        response = mock.Mock()
        response.body = json.dumps(phase.to_json())
        fut.set_result(response)

        phase.endpoint = '/testing'

        client = mock.Mock()
        client.fetch = mock.Mock()
        client.fetch.return_value = fut
        with mock.patch('tornado.httpclient.AsyncHTTPClient',
                        return_value=client):
            responses = yield phase.send(agents.quorum())
            self.assertEqual(len(responses), len(agents.quorum()))


class TestSubclasses(tornado.testing.AsyncTestCase):

    @tornado.gen.coroutine
    def assert_send_works(self, obj, expected_endpoint):
        fut = tornado.concurrent.Future()
        response = mock.Mock()
        response.body = '{}'
        fut.set_result(response)

        client = mock.Mock()
        client.fetch = mock.Mock()
        client.fetch.return_value = fut

        random_agent = agents.quorum()[0]

        with mock.patch('tornado.httpclient.HTTPRequest') as req:
            with mock.patch('tornado.httpclient.AsyncHTTPClient',
                            return_value=client):
                agent = agents.all()[0]
                responses = yield obj.send([agent])
                self.assertEqual(len(responses), 1)
                req.assert_any_call(
                    url=agent.url + ':' + str(agent.port) + obj.endpoint,
                    method='POST',
                    headers={'Content-Type': 'application/json'},
                    body=json.dumps(obj.to_json())
                )


    @tornado.testing.gen_test
    def test_prepare(self):
        prepare = Prepare(id=3, key='biz', predicate='set', argument='a')
        self.assertEqual(prepare.to_json(), {
            'id': 3, 'key': 'biz', 'predicate': 'set', 'argument': 'a'})
        target = Prepare._id
        prepare = Prepare(key='buzz', predicate='a', argument='b')
        self.assertEqual(prepare.to_json(), {
            'id': target, 'key': 'buzz', 'predicate': 'a', 'argument': 'b'})
        _ = yield self.assert_send_works(prepare, '/prepare')

        request = tornado.httpclient.HTTPRequest(
            body=json.dumps(prepare.to_json()),
            method='POST',
            headers={'Content-Type': 'application/json'},
            url='/prepare')
        target = Prepare.from_request(request)
        self.assertEqual(target.to_json(), prepare.to_json())

    def test_promise(self):
        promise = Promise()
        self.assertEqual(promise.prepare, None)
        prepare = Prepare(id=3, key='biz', predicate='set', argument='a')
        promise = Promise(prepare=prepare)
        self.assertEqual(promise.prepare, prepare)
        self.assertEqual(promise.to_json(), {
            'prepare': prepare.to_json()
        })

    def test_propose(self):
        prepare = Prepare(id=3, key='biz', predicate='set', argument='a')
        propose = Propose(prepare=prepare)
        self.assert_send_works(propose, '/propose')

        request = tornado.httpclient.HTTPRequest(
            body=json.dumps(propose.to_json()),
            method='POST',
            headers={'Content-Type': 'application/json'},
            url='/propose')
        target = Propose.from_request(request)
        self.assertEqual(propose.to_json(), target.to_json())

    def test_accept(self):
        prepare = Prepare(id=3, key='biz', predicate='set', argument='a')
        accept = Accept(prepare=prepare)
        response = mock.Mock()
        response.body = json.dumps(accept.to_json())
        target = Accept.from_response(response)
        self.assertEqual(accept.to_json(), target.to_json())

    def test_learn(self):
        prepare = Prepare(id=3, key='biz', predicate='set', argument='a')
        learn = Learn(prepare=prepare)
        request = tornado.httpclient.HTTPRequest(
            body=json.dumps(learn.to_json()),
            method='POST',
            headers={'Content-Type': 'application/json'},
            url='/learn')
        self.assertEqual(learn.to_json(), Learn.from_request(request).to_json())

    def test_success(self):
        prepare = Prepare(id=3, key='biz', predicate='set', argument='a')
        success = Success(prepare=prepare)
        self.assertEqual(success.to_json(), {'status': 'SUCCESS',
                                             'prepare': prepare.to_json()})


class TestContainers(tornado.testing.AsyncTestCase):

    def test_promises(self):
        prepare1 = Prepare(id=1, key='biz', predicate='pa', argument='a')
        prepare2 = Prepare(id=2, key='biz', predicate='pb', argument='b')
        prepare3 = Prepare(id=3, key='biz', predicate='pc', argument='c')
        prepare4 = Prepare(id=4, key='baz', predicate='pd', argument='d')

        promise1 = Promise(prepare=prepare1)
        promise2 = Promise(prepare=prepare2)
        promise3 = Promise(prepare=prepare3)
        promise4 = Promise(prepare=prepare4)

        promises = Promises([promise1, promise2, promise3, promise4])

        self.assertEqual(promises.highest_numbered().to_json(),
                         promise4.to_json())
        self.assertEqual(promises.highest_numbered(key='biz').to_json(),
                         promise3.to_json())


class Base(tornado.testing.AsyncHTTPTestCase):

    def get_app(self):
        return agent.get_app()

    def post(self, url, body):
        return self.fetch(url,
                          method='POST',
                          body=json.dumps(body),
                          headers={'Content-Type': 'application/json'})

    def get_prepare(self):
        return Prepare(id=1, key='foo', predicate='set', argument='a')

    def setUp(self):
        agent.current_promises.clear()
        agent.completed_rounds.clear()
        super(Base, self).setUp()

class TestProposer(Base):

    def test_allows_non_conflicting_writes(self):
        prepare = Prepare(id=0, key='foo', predicate='set', argument='a')
        promise = Promise()
        prepare_success = mock.Mock()
        prepare_success.code = 200
        prepare_success.body = json.dumps(promise.to_json())
        fut = tornado.concurrent.Future()
        fut.set_result([prepare_success, prepare_success])

        propose_success = mock.Mock()
        propose_success.code = 200
        propose_success.body = json.dumps(Promise(prepare=prepare).to_json())
        propose_fut = tornado.concurrent.Future()
        propose_fut.set_result([propose_success, propose_success])

        learn_success = mock.Mock()
        learn_success.code = 200
        learn_success.body = ''
        learn_fut = tornado.concurrent.Future()
        learn_fut.set_result([learn_success, learn_success])
        with mock.patch('models.Prepare.send', return_value=fut) as send:
            with mock.patch('models.Propose.send', return_value=propose_fut) as propose_send:
                with mock.patch('models.Learn.fanout', return_value=learn_fut) as learn_fanout:
                    response = self.post('/write', body={
                        'key': 'foo',
                        'predicate': 'set',
                        'argument': 'a'})

        self.assertEqual(response.code, 200)


class TestPrepareAcceptor(Base):

    def test_rejects_when_there_is_a_higher_numbered_promise_in_progress(self):
        lower_prepare = Prepare(id=0, key='foo', predicate='set', argument='a')
        higher_prepare = Prepare(id=1, key='foo', predicate='set', argument='b')

        success = self.post('/prepare', higher_prepare.to_json())
        self.assertEqual(success.code, 200)
        self.assertEqual(Promise.from_response(success).to_json(), {'prepare': None})
        self.assertEqual(agent.current_promises.highest_numbered().to_json(),
                         {'prepare': higher_prepare.to_json()})

        failure = self.post('/prepare', lower_prepare.to_json())
        self.assertEqual(failure.code, 400)
        target = Promise.from_response(failure)
        self.assertEqual(target.to_json(), {'prepare': higher_prepare.to_json()})

    def test_returns_lower_numbered_in_progress_promises(self):
        lower_prepare = Prepare(id=0, key='foo', predicate='set', argument='a')
        higher_prepare = Prepare(id=1, key='foo', predicate='set', argument='b')

        success = self.post('/prepare', lower_prepare.to_json())
        self.assertEqual(success.code, 200)
        self.assertEqual(Promise.from_response(success).to_json(), {'prepare': None})
        self.assertEqual(agent.current_promises.highest_numbered().to_json(),
                         {'prepare': lower_prepare.to_json()})

        failure = self.post('/prepare', higher_prepare.to_json())
        self.assertEqual(failure.code, 200)
        target = Promise.from_response(failure)
        self.assertEqual(target.to_json(), {'prepare': lower_prepare.to_json()})


class TestProposeAcceptor(Base):

    def test_propose_acceptor_removes_promise(self):
        promise = Promise(prepare=self.get_prepare())
        agent.current_promises.add(promise)

        self.assertEqual(agent.current_promises.highest_numbered().to_json(),
                         promise.to_json())

        propose = Propose(prepare=self.get_prepare())
        response = self.post('/propose', propose.to_json())
        self.assertEqual(response.code, 200)
        self.assertEqual(
            Accept.from_response(response).to_json(),
            {'prepare': self.get_prepare().to_json()})

        self.assertIsNone(agent.current_promises.highest_numbered())



class TestLearner(Base):

    def test_learner_learns(self):
        learn = Learn(prepare=self.get_prepare())
        response = self.post('/learn', learn.to_json())
        self.assertEqual(response.code, 200)
        self.assertEqual(
            Success.from_response(response).to_json(),
            {'status': 'SUCCESS', 'prepare': self.get_prepare().to_json()})
        self.assertEqual(agent.completed_rounds.highest_numbered().prepare.to_json(),
                         self.get_prepare().to_json())


if __name__ == '__main__':
    unittest.main()

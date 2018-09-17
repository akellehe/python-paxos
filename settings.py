ACCEPTOR_PORTS = [8889, 8890, 8891]
ACCEPTOR_URL = 'http://acceptor.io'
ACCEPTOR_URLS = ['{}:{}'.format(ACCEPTOR_URL, acceptor_port)
                 for acceptor_port in ACCEPTOR_PORTS]

LEARNER_URL = 'http://learner.io'
LEARNER_PORTS = [8892]
LEARNER_URLS = ['{}:{}'.format(LEARNER_URL, learner_port)
                for learner_port in LEARNER_PORTS]

PROPOSER_URL = 'http://proposer.io'
PROPOSER_PORTS = [8888]
PROPOSER_URLS = ['{}:{}'.format(PROPOSER_URL, proposer_port)
                 for proposer_port in PROPOSER_PORTS]

TORNADO_SETTINGS = {'autoreload': True}

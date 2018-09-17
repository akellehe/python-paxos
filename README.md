# Paxos

## Getting started

You should add the following lines to `/etc/hosts`

```
127.0.0.1   learner.io
127.0.0.1   acceptor.io
127.0.0.1   proposer.io
```

Then you can get up and running by just running `./bootstrap.sh`.

It will start:

 - 1 Proposer/Leader
 - 3 Acceptors
 - 1 Learner

Then you can call them using `client.py` by running 

```
python3 client.py
```

If you want to send new proposals, you can modify `client.py`

## Known Issues

 - The Acceptor needs to store the state of requests in progress
 - The Proposer needs to stop what it is doing when it gets an earlier promise, and finish the earlier promise.

Check out these resources for more information:

 - https://en.wikipedia.org/wiki/Paxos_(computer_science)#Phase_2b:_Accepted
 - https://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0
 - http://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/past/03F/notes/paxos-simple.pdf

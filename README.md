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

 - The Proposer needs to stop what it is doing when it gets an earlier promise, and finish the earlier promise.

## Questions for the next meetup.

 - Is it OK to consider a message `COMMITTED` if we only receive 1 `ACCEPTED` message from an acceptor?
 - What happens when a learner goes away? How does paxos provide a mechanism for them to re-sync?
 - What happens when there is an acceptor split brain?

Check out these resources for more information:
 - https://docs.google.com/presentation/d/1OGKyQZZ1aV6w8bGoWaQVwzjFDwoqfYM6xjAF22rA18k/edit#slide=id.g41ff1175c6_0_53
 - https://en.wikipedia.org/wiki/Paxos_(computer_science)#Phase_2b:_Accepted
 - https://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0
 - http://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/past/03F/notes/paxos-simple.pdf

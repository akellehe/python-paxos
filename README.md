# Paxos

Don't use this code. It is not a reliable implementation of the algorithm since it has no mechanism for bootstrapping, leader election, or failover. It is a bare bones implementation of the state machine algorithm described in Leslie Lamport's "Basic Paxos" paper.


## Getting started

Then you can get up and running by just running `./bootstrap.sh`.

It will start three agents. Each one is a proposer, a learner, and an acceptor.

The agent running on port 8888 is the _distinguished_ proposer and learner.

This implementation is completely ephemeral, so if a node goes down you do not get the full fault tolerance the algorithm would otherwise guarantee.


You can send a bunch of asynchronous requests by calling

```
python client.py
```

If you want to send new proposals, you can modify `client.py`

## Known issues

There are three failing tests. I updated a few things at the last minute, and those tests broke. I'm 95% sure this implementation is correct. I'll do another review of it at a later date.

## Questions for the next meetup.

Check out these resources for more information:
 - https://docs.google.com/presentation/d/1OGKyQZZ1aV6w8bGoWaQVwzjFDwoqfYM6xjAF22rA18k/edit#slide=id.g41ff1175c6_0_53
 - https://en.wikipedia.org/wiki/Paxos_(computer_science)#Phase_2b:_Accepted
 - https://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0
 - http://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/past/03F/notes/paxos-simple.pdf

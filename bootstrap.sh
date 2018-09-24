#!/bin/bash

echo "Killing proposers..."
pgrep -f proposer.py | xargs sudo kill -9 &
echo "Killing acceptors..."
pgrep -f acceptor.py | xargs sudo kill -9 &
echo "Killing learners..."
pgrep -f learner.py | xargs sudo kill -9 &

sleep 3

python3 proposer.py --port=8888 &
python3 proposer.py --port=8893 &
    python3 acceptor.py --port=8889 &
    python3 acceptor.py --port=8890 &
    python3 acceptor.py --port=8891 &
    python3 learner.py --port=8892


#!/bin/bash

echo "Killing existing processes...if any..."
pgrep -f agent.py | xargs sudo kill -9 &

sleep 3

python agent.py --port=8888 &
python agent.py --port=8889 &
    python3 agent.py --port=8890 &


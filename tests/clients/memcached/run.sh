#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

# Install python dependencies in a virtualenv if not exists
if [ ! -d "venv" ]; then
    python3 -m venv venv
    ./venv/bin/pip install pymemcache
fi

# Run test against default port (assuming server is started elsewhere)
PORT=${NOIDA_MEMCACHED_PORT:-11211}
./venv/bin/python test.py 127.0.0.1 $PORT

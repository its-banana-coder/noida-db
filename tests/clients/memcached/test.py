import sys
import os
from pymemcache.client.base import Client

def run_tests(host, port):
    client = Client((host, port))
    client.set('key1', b'value1')
    val = client.get('key1')
    assert val == b'value1', f"expected b'value1', got {val}"

    # Test add
    client.add('key2', b'value2')
    assert client.get('key2') == b'value2'

    # Test incr
    client.set('counter', 10)
    res = client.incr('counter', 5)
    assert res == 15, f"expected 15, got {res}"

    # Test delete
    client.delete('key1')
    assert client.get('key1') is None

    print("All python pymemcache tests passed!")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python test.py <host> <port>")
        sys.exit(1)
    run_tests(sys.argv[1], int(sys.argv[2]))

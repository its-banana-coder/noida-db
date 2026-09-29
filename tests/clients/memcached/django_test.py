import os
import sys

host = sys.argv[1]
port = int(sys.argv[2])

import django
from django.conf import settings

settings.configure(
    DEBUG=False,
    SECRET_KEY="x",
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.memcached.PyMemcacheCache",
            "LOCATION": f"{host}:{port}",
        }
    }
)
django.setup()

from django.core.cache import cache

def run_tests():
    # Test set/get
    cache.set("my_key", "hello, world!", 30)
    val = cache.get("my_key")
    assert val == "hello, world!", f"expected 'hello, world!', got {val}"

    # Test set/get with complex type (dict)
    cache.set("my_dict", {"a": 1, "b": 2}, 30)
    assert cache.get("my_dict") == {"a": 1, "b": 2}

    # Test delete
    cache.delete("my_key")
    assert cache.get("my_key") is None

    # Test increment/decrement
    cache.set("counter", 10)
    cache.incr("counter", 5)
    assert cache.get("counter") == 15
    cache.decr("counter", 2)
    assert cache.get("counter") == 13

    # Test add (only sets if not exists)
    cache.add("add_key", "initial")
    assert cache.get("add_key") == "initial"
    cache.add("add_key", "new")
    assert cache.get("add_key") == "initial"

    # Test get_many / set_many
    cache.set_many({"k1": "v1", "k2": "v2"})
    res = cache.get_many(["k1", "k2"])
    assert res == {"k1": "v1", "k2": "v2"}, f"expected {{'k1': 'v1', 'k2': 'v2'}}, got {res}"

    # Test touch
    cache.touch("k1", 100)

    # Test clear
    cache.clear()
    assert cache.get("k1") is None

    print("django PyMemcacheCache tests passed!")

if __name__ == "__main__":
    run_tests()

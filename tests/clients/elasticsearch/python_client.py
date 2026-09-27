"""elasticsearch-py 8.x smoke test against noida-db."""

import os

from elasticsearch import Elasticsearch


url = os.environ["ELASTICSEARCH_URL"]
index = "noida_python_client"
client = Elasticsearch(url)

info = client.info()
assert info["version"]["number"] == "8.15.3", info
client.options(ignore_status=404).indices.delete(index=index)
created = client.indices.create(index=index)
assert created["acknowledged"] is True, created
written = client.index(index=index, id="book-1", document={"title": "Dune", "pages": 412})
assert written["result"] == "created", written
found = client.get(index=index, id="book-1")
assert found["_source"] == {"title": "Dune", "pages": 412}, found
client.indices.delete(index=index)
print("elasticsearch-py: info/index/get passed")

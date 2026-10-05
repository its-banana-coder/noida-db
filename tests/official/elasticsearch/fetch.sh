#!/usr/bin/env bash
# Fetches Elasticsearch 8.15.3's REST API specs and YAML test suite (only
# those two directories) into target/official/elasticsearch.
set -euo pipefail
cd "$(dirname "$0")/../../.."
dest=target/official/elasticsearch
[ -d "$dest/rest-api-spec" ] && exit 0
mkdir -p "$dest"
tmp=$(mktemp -d)
git clone -q --depth 1 --branch v8.15.3 --filter=blob:none --sparse \
  https://github.com/elastic/elasticsearch.git "$tmp/es"
git -C "$tmp/es" sparse-checkout set \
  rest-api-spec/src/main/resources/rest-api-spec/api \
  rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test
mv "$tmp/es/rest-api-spec" "$dest/rest-api-spec"
rm -rf "$tmp"

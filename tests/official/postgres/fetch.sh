#!/usr/bin/env bash
# Fetches Postgres 14's regression suite (src/test/regress only) into
# target/official/postgres/regress.
set -euo pipefail
cd "$(dirname "$0")/../../.."
dest=target/official/postgres
[ -d "$dest/regress" ] && exit 0
mkdir -p "$dest"
tmp=$(mktemp -d)
git clone -q --depth 1 --branch REL_14_STABLE --filter=blob:none --sparse \
  https://github.com/postgres/postgres.git "$tmp/pg"
git -C "$tmp/pg" sparse-checkout set src/test/regress
mv "$tmp/pg/src/test/regress" "$dest/regress"
rm -rf "$tmp"

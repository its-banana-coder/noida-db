#!/usr/bin/env bash
# Builds a real Redis 7.2 (the newest patch release, as CI's redis:7.2 image) into target/oracle/ (no global install) and prints
# the path of its redis-server. The comparison and client tests use it as the
# reference. Needs gcc, make and curl.
set -euo pipefail
cd "$(dirname "$0")/.."
ver=7.2.16
dir=target/oracle
bin="$dir/redis-$ver/src/redis-server"
if [ ! -x "$bin" ]; then
  mkdir -p "$dir"
  curl -fsSL "https://download.redis.io/releases/redis-$ver.tar.gz" | tar xz -C "$dir"
  make -C "$dir/redis-$ver" -j"$(nproc)" BUILD_TLS=no MALLOC=libc >/dev/null 2>&1
fi
echo "$PWD/$bin"

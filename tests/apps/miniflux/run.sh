#!/usr/bin/env bash
# Runs a real, unmodified Miniflux (https://miniflux.app/) — a Go RSS reader,
# Postgres-only — against noida-db's Postgres, exercising the same workflow
# documented in the top-level README's "Tested against real applications"
# table:
#
#   1. Miniflux's schema migration (`miniflux -migrate`) — its full
#      production schema (~130 versioned migrations as of the pinned release
#      below, including a migration that uses a `DECLARE ... CURSOR` /
#      `FETCH` / `CLOSE` loop to backfill data row-by-row). A real
#      cursor-based migration is a meaningfully different Postgres feature
#      than straightforward DDL, and this project's docs specifically call
#      out cursor support as verified this way (see docs/LIMITATIONS.md's
#      Postgres section), so this is the most important thing this script
#      checks.
#   2. Creating an admin user at first boot (via CREATE_ADMIN/ADMIN_USERNAME/
#      ADMIN_PASSWORD env vars — Miniflux is configured entirely via env
#      vars, no config file).
#   3. Starting the real Miniflux server pointed at noida-db's Postgres, and
#      waiting for its /healthcheck endpoint to come up.
#   4. Logging in via the REST API (HTTP Basic Auth against /v1/me, which is
#      how every subsequent call below authenticates too).
#   5. Adding a real RSS feed via the API and fetching it.
#   6. Asserting the feed's entries were actually fetched and parsed (real
#      titles/content came back, not empty).
#   7. Marking one entry as read via the API and confirming the status
#      change by re-fetching it.
#   8. Full-text search via the API's `search` query parameter — this is the
#      tsvector/setweight/websearch_to_tsquery feature this project's docs
#      highlight as verified against Miniflux's own index (title/content
#      combined via setweight+||). We search for a distinctive real word
#      from a real fetched entry's title and assert that exact entry comes
#      back.
#
#   tests/apps/miniflux/run.sh
#
# A pinned Miniflux release binary (2.3.3, linux-amd64/arm64) is downloaded
# on first run, checksum-verified against its published .sha256, and cached
# under target/client-deps/miniflux/ — repeat runs reuse it.
#
# Feed used: https://hnrss.org/frontpage — a long-running, widely used RSS
# proxy for Hacker News' front page (running since ~2013). Chosen because
# its content (tech/software article titles) is distinctive enough for a
# meaningful full-text-search assertion, and it's stable/well-known enough
# to not disappear.
#
# If `curl`, `python3` or network access (to GitHub, to download the pinned
# binary, or to hnrss.org, to fetch the feed) is unavailable, the script
# prints "SKIPPED" and exits 0 rather than failing.
set -uo pipefail

run_start=$(date +%s)
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cache="${NOIDA_CLIENT_CACHE:-$root/target/client-deps}"
mf_cache="$cache/miniflux"
mkdir -p "$mf_cache"

MINIFLUX_VERSION="2.3.3"

skip() {
  echo "SKIPPED: $1"
  exit 0
}

command -v curl >/dev/null 2>&1 || skip "curl is not installed"
command -v python3 >/dev/null 2>&1 || skip "python3 is not installed"
command -v sha256sum >/dev/null 2>&1 || skip "sha256sum is not installed"

os="$(uname -s)"
[ "$os" = "Linux" ] || skip "Miniflux release binaries are only fetched for Linux (found: $os)"

arch="$(uname -m)"
case "$arch" in
  x86_64) mf_arch="amd64" ;;
  aarch64|arm64) mf_arch="arm64" ;;
  *) skip "no Miniflux release binary for arch: $arch" ;;
esac

mf_bin="$mf_cache/miniflux-$MINIFLUX_VERSION-$mf_arch"
if [ ! -x "$mf_bin" ]; then
  echo "== downloading Miniflux v$MINIFLUX_VERSION ($mf_arch)"
  base_url="https://github.com/miniflux/v2/releases/download/$MINIFLUX_VERSION"
  if ! curl -fsSL -o "$mf_bin.tmp" "$base_url/miniflux-linux-$mf_arch"; then
    rm -f "$mf_bin.tmp"
    skip "could not download Miniflux v$MINIFLUX_VERSION (network unavailable?)"
  fi
  if ! curl -fsSL -o "$mf_bin.tmp.sha256" "$base_url/miniflux-linux-$mf_arch.sha256"; then
    rm -f "$mf_bin.tmp" "$mf_bin.tmp.sha256"
    skip "could not download Miniflux checksum (network unavailable?)"
  fi
  got_sum=$(sha256sum "$mf_bin.tmp" | awk '{print $1}')
  want_sum=$(awk '{print $1}' "$mf_bin.tmp.sha256")
  if [ "$got_sum" != "$want_sum" ]; then
    rm -f "$mf_bin.tmp" "$mf_bin.tmp.sha256"
    echo "checksum mismatch: got $got_sum, want $want_sum"
    exit 1
  fi
  chmod +x "$mf_bin.tmp"
  mv "$mf_bin.tmp" "$mf_bin"
  rm -f "$mf_bin.tmp.sha256"
fi

# Quick, no-network check that hnrss.org is actually reachable before we
# spend time standing up noida-db and Miniflux; if it isn't, skip cleanly
# rather than fail deep into the run.
if ! curl -fsS -o /dev/null --max-time 10 "https://hnrss.org/frontpage"; then
  skip "https://hnrss.org/frontpage is not reachable (network unavailable?)"
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/noida-miniflux.XXXXXX")"
cleanup() {
  [ -n "${mf_pid:-}" ] && kill "$mf_pid" 2>/dev/null
  [ -n "${noida_pid:-}" ] && kill "$noida_pid" 2>/dev/null
  [ -n "${mf_pid:-}" ] && wait "$mf_pid" 2>/dev/null
  [ -n "${noida_pid:-}" ] && wait "$noida_pid" 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

pg_port=$(free_port)
web_port=$(free_port)

echo "== building noida-db (release)"
cargo build --quiet --release --features postgres --bin noida-db || exit 1

echo "== starting noida-db (postgres:$pg_port)"
"$root/target/release/noida-db" start --only postgres \
  --postgres-port "$pg_port" \
  --data-dir "$work/noida-data" >"$work/noida.log" 2>&1 &
noida_pid=$!

for _ in $(seq 1 50); do
  (echo >"/dev/tcp/127.0.0.1/$pg_port") 2>/dev/null && break
  sleep 0.2
done
if ! kill -0 "$noida_pid" 2>/dev/null; then
  echo "noida-db failed to start"
  cat "$work/noida.log"
  exit 1
fi

fail() {
  echo "** FAILED: $1"
  echo "-- noida-db log (tail):"
  tail -n 60 "$work/noida.log" 2>/dev/null
  echo "-- miniflux log (tail):"
  tail -n 60 "$work/miniflux.log" 2>/dev/null
  exit 1
}

database_url="user=postgres password=postgres host=127.0.0.1 port=$pg_port dbname=postgres sslmode=disable"

# --- 1. schema migration: ~130 versioned migrations, including a real -----
#        DECLARE/FETCH/CLOSE cursor-based data migration -------------------
echo "== running Miniflux schema migration against noida-db postgres"
migrate_start=$(date +%s)
if ! DATABASE_URL="$database_url" "$mf_bin" -migrate >"$work/migrate.log" 2>&1; then
  cat "$work/migrate.log"
  fail "miniflux -migrate"
fi
grep -qi "latest_version" "$work/migrate.log" || cat "$work/migrate.log"
echo "   migration finished in $(( $(date +%s) - migrate_start ))s"
echo "   $(grep -i 'latest_version' "$work/migrate.log" | tail -1)"

# --- 2+3. admin user (created at first boot) + start the real server ------
echo "== starting miniflux (creating admin user on first boot)"
admin_user="testadmin"
admin_pass="TestPass123!"
web_start=$(date +%s)
DATABASE_URL="$database_url" \
RUN_MIGRATIONS=0 \
CREATE_ADMIN=1 \
ADMIN_USERNAME="$admin_user" \
ADMIN_PASSWORD="$admin_pass" \
LISTEN_ADDR="127.0.0.1:$web_port" \
BASE_URL="http://127.0.0.1:$web_port" \
LOG_LEVEL=info \
WORKER_POOL_SIZE=1 \
POLLING_FREQUENCY=1440 \
  "$mf_bin" >"$work/miniflux.log" 2>&1 &
mf_pid=$!

base="http://127.0.0.1:$web_port"
up=""
for i in $(seq 1 120); do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$base/healthcheck" 2>/dev/null)
  if [ "$code" = "200" ]; then
    up=1
    break
  fi
  kill -0 "$mf_pid" 2>/dev/null || fail "miniflux process exited during startup"
  sleep 0.5
done
[ -n "$up" ] || fail "miniflux did not come up on $base within 60s"
grep -qi "Created new admin user" "$work/miniflux.log" || fail "admin user was not created at boot"
echo "== miniflux is up on $base (startup took $(( $(date +%s) - web_start ))s)"

auth=(-u "$admin_user:$admin_pass")

http_status() {
  # http_status <file> <curl args...>   -> prints status code, body saved to <file>
  local out="$1"; shift
  curl -s -o "$out" -w "%{http_code}" "$@"
}

json_get() {
  # json_get <file> <python expression on `d`>
  python3 -c "
import json, sys
with open('$1') as f:
    d = json.load(f)
print($2)
"
}

# --- 4. log in via the REST API ---------------------------------------------
echo "== logging in via /v1/me (HTTP Basic Auth)"
status=$(http_status "$work/me.json" "${auth[@]}" "$base/v1/me")
[ "$status" = "200" ] || { cat "$work/me.json"; fail "login via /v1/me (HTTP $status)"; }
who=$(json_get "$work/me.json" "d['username']")
[ "$who" = "$admin_user" ] || fail "/v1/me returned unexpected user: $who"
echo "   logged in as $who"

# --- 5. add a real RSS feed via the API and fetch it ------------------------
feed_url="https://hnrss.org/frontpage"
echo "== adding feed via API: $feed_url"
status=$(http_status "$work/create-feed.json" "${auth[@]}" -H 'Content-Type: application/json' \
  -d "{\"feed_url\":\"$feed_url\"}" "$base/v1/feeds")
[ "$status" = "201" ] || { cat "$work/create-feed.json"; fail "create feed (HTTP $status)"; }
feed_id=$(json_get "$work/create-feed.json" "d['feed_id']")
echo "   feed created: id=$feed_id"

echo "== refreshing feed via API"
# Retry a few times: this does a live HTTP fetch of a real external feed
# from inside the test process, which can hit a transient network blip even
# right after the reachability precheck above passed.
refresh_ok=""
for attempt in 1 2 3; do
  status=$(http_status "$work/refresh-feed.json" "${auth[@]}" -X PUT "$base/v1/feeds/$feed_id/refresh")
  if [ "$status" = "204" ]; then
    refresh_ok=1
    break
  fi
  echo "   refresh attempt $attempt failed (HTTP $status), retrying..."
  sleep 3
done
[ -n "$refresh_ok" ] || { cat "$work/refresh-feed.json"; fail "refresh feed (HTTP $status)"; }

# --- 6. assert entries were actually fetched and parsed --------------------
echo "== fetching entries via API"
entries_ok=""
for _ in $(seq 1 20); do
  status=$(http_status "$work/entries.json" "${auth[@]}" "$base/v1/feeds/$feed_id/entries?order=id&direction=asc")
  [ "$status" = "200" ] || { cat "$work/entries.json"; fail "list entries (HTTP $status)"; }
  total=$(json_get "$work/entries.json" "d['total']")
  if [ "$total" -gt 0 ] 2>/dev/null; then
    entries_ok=1
    break
  fi
  sleep 1
done
[ -n "$entries_ok" ] || fail "no entries were fetched from $feed_url"
first_title=$(json_get "$work/entries.json" "d['entries'][0]['title']")
first_id=$(json_get "$work/entries.json" "d['entries'][0]['id']")
first_content_len=$(json_get "$work/entries.json" "len(d['entries'][0]['content'])")
[ -n "$first_title" ] || fail "first entry has an empty title"
[ "$first_content_len" -gt 0 ] 2>/dev/null || fail "first entry has empty content"
echo "   $total entries fetched; first entry (id=$first_id): \"$first_title\""

# --- 7. mark one entry as read via the API ----------------------------------
echo "== marking entry $first_id as read via API"
status=$(http_status "$work/mark-read.json" "${auth[@]}" -X PUT -H 'Content-Type: application/json' \
  -d "{\"entry_ids\":[$first_id],\"status\":\"read\"}" "$base/v1/entries")
[ "$status" = "204" ] || { cat "$work/mark-read.json"; fail "mark entry read (HTTP $status)"; }

echo "== re-fetching entry $first_id to confirm status changed"
status=$(http_status "$work/entry-after.json" "${auth[@]}" "$base/v1/entries/$first_id")
[ "$status" = "200" ] || { cat "$work/entry-after.json"; fail "fetch entry (HTTP $status)"; }
new_status=$(json_get "$work/entry-after.json" "d['status']")
[ "$new_status" = "read" ] || fail "entry $first_id status is '$new_status', expected 'read'"
echo "   entry $first_id status is now '$new_status'"

# --- 8. full-text search: tsvector/setweight/websearch_to_tsquery ----------
# Pick a distinctive real word out of the first entry's real title so the
# assertion is meaningful (not a placeholder), then confirm that exact entry
# comes back via Miniflux's search API (which round-trips through Postgres
# full-text search: title/content combined via setweight()+||, queried with
# websearch_to_tsquery()).
search_term=$(python3 -c "
import re
title = '''$first_title'''
words = [w for w in re.findall(r\"[A-Za-z]+\", title) if len(w) >= 5]
print(words[0] if words else '')
")
[ -n "$search_term" ] || fail "could not extract a search term from title: $first_title"
echo "== full-text search for \"$search_term\" (from the real fetched entry's title)"
status=$(http_status "$work/search.json" "${auth[@]}" -G --data-urlencode "search=$search_term" "$base/v1/entries")
[ "$status" = "200" ] || { cat "$work/search.json"; fail "search entries (HTTP $status)"; }
search_total=$(json_get "$work/search.json" "d['total']")
[ "$search_total" -gt 0 ] 2>/dev/null || fail "full-text search for \"$search_term\" returned 0 results"
found=$(json_get "$work/search.json" "1 if any(e['id'] == $first_id for e in d['entries']) else 0")
[ "$found" = "1" ] || fail "full-text search for \"$search_term\" did not return entry $first_id in its results"
echo "   search matched $search_total entr(ies), including entry $first_id as expected"

echo "== all Miniflux real-app checks passed (schema migration incl. cursor migration, admin user, login, feed add+fetch, mark read, full-text search)"
echo "== total run time: $(( $(date +%s) - run_start ))s"

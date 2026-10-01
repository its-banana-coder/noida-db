#!/usr/bin/env bash
# Runs a real, unmodified Gitea (https://about.gitea.com/) against noida-db's
# Postgres and Redis, exercising the same workflow documented in the top-level
# README's "Tested against real applications" table:
#
#   1. Gitea's schema migration (`gitea migrate`) — its full production
#      schema, ~115 tables created via the xorm ORM. This is the heaviest
#      real-world DDL workload this project handles (batches of CREATE TABLE,
#      CREATE INDEX, and catalog introspection queries against
#      information_schema/pg_catalog run back-to-back for every table), so
#      it's the most important thing this script checks.
#   2. Creating an admin user via the Gitea CLI.
#   3. Starting the real `gitea web` server pointed at noida-db instead of a
#      real Postgres + Redis, and waiting for it to come up.
#   4. Creating a repository via the REST API.
#   5. A real `git clone` and `git push` over HTTP (the system `git` binary,
#      not an API call) against that repository.
#   6. Creating an issue and adding a comment via the REST API.
#   7. A full pull-request workflow: push a feature branch, open a PR via the
#      API, merge it, and verify the merge via the API.
#
# Gitea's session/cache backends are pointed at noida-db's Redis (session
# provider + cache adapter), and its DATABASE section is pointed at noida-db's
# Postgres, so both services are exercised together by one real application.
#
#   tests/apps/gitea/run.sh
#
# A pinned Gitea release binary (v1.27.3, linux-amd64/arm64) is downloaded on
# first run and cached under target/client-deps/gitea/ — repeat runs reuse it.
# noida-db is built in --release mode specifically for this test: the schema
# migration issues hundreds of catalog-introspection queries back-to-back and
# a debug build is too slow to finish it in reasonable CI time (see the note
# further down). If `git`, `curl`, `python3` or network access to GitHub is
# unavailable, the script prints "SKIPPED" and exits 0 rather than failing.
set -uo pipefail

run_start=$(date +%s)
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cache="${NOIDA_CLIENT_CACHE:-$root/target/client-deps}"
gitea_cache="$cache/gitea"
mkdir -p "$gitea_cache"

GITEA_VERSION="1.27.3"

skip() {
  echo "SKIPPED: $1"
  exit 0
}

command -v git >/dev/null 2>&1 || skip "git is not installed"
command -v curl >/dev/null 2>&1 || skip "curl is not installed"
command -v python3 >/dev/null 2>&1 || skip "python3 is not installed"

os="$(uname -s)"
[ "$os" = "Linux" ] || skip "Gitea release binaries are only fetched for Linux (found: $os)"

arch="$(uname -m)"
case "$arch" in
  x86_64) gitea_arch="amd64" ;;
  aarch64|arm64) gitea_arch="arm64" ;;
  *) skip "no Gitea release binary for arch: $arch" ;;
esac

gitea_bin="$gitea_cache/gitea-$GITEA_VERSION-$gitea_arch"
if [ ! -x "$gitea_bin" ]; then
  echo "== downloading Gitea v$GITEA_VERSION ($gitea_arch)"
  url="https://github.com/go-gitea/gitea/releases/download/v$GITEA_VERSION/gitea-$GITEA_VERSION-linux-$gitea_arch"
  if ! curl -fsSL -o "$gitea_bin.tmp" "$url"; then
    rm -f "$gitea_bin.tmp"
    skip "could not download Gitea v$GITEA_VERSION (network unavailable?)"
  fi
  chmod +x "$gitea_bin.tmp"
  mv "$gitea_bin.tmp" "$gitea_bin"
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/noida-gitea.XXXXXX")"
cleanup() {
  [ -n "${gitea_pid:-}" ] && kill "$gitea_pid" 2>/dev/null
  [ -n "${noida_pid:-}" ] && kill "$noida_pid" 2>/dev/null
  [ -n "${gitea_pid:-}" ] && wait "$gitea_pid" 2>/dev/null
  [ -n "${noida_pid:-}" ] && wait "$noida_pid" 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

pg_port=$(free_port)
redis_port=$(free_port)
web_port=$(free_port)

echo "== building noida-db (release; the schema migration below is too slow in debug)"
cargo build --quiet --release --features postgres,redis --bin noida-db || exit 1

echo "== starting noida-db (postgres:$pg_port redis:$redis_port)"
"$root/target/release/noida-db" start --only postgres,redis \
  --postgres-port "$pg_port" --redis-port "$redis_port" \
  --data-dir "$work/noida-data" >"$work/noida.log" 2>&1 &
noida_pid=$!

for _ in $(seq 1 50); do
  (echo >"/dev/tcp/127.0.0.1/$pg_port") 2>/dev/null && (echo >"/dev/tcp/127.0.0.1/$redis_port") 2>/dev/null && break
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
  echo "-- gitea web log (tail):"
  tail -n 60 "$work/gitea-web.log" 2>/dev/null
  exit 1
}

# --- configure Gitea against noida-db -------------------------------------
gitea_home="$work/gitea-home"
mkdir -p "$gitea_home/custom/conf" "$gitea_home/data" "$gitea_home/log" "$gitea_home/repos"

internal_token=$("$gitea_bin" generate secret INTERNAL_TOKEN)
secret_key=$("$gitea_bin" generate secret SECRET_KEY)

cat >"$gitea_home/custom/conf/app.ini" <<EOF
APP_NAME = noida-db gitea real-app test
RUN_MODE = prod
WORK_PATH = $gitea_home

[server]
HTTP_ADDR = 127.0.0.1
HTTP_PORT = $web_port
ROOT_URL = http://127.0.0.1:$web_port/
DISABLE_SSH = true
OFFLINE_MODE = true

[database]
DB_TYPE = postgres
HOST = 127.0.0.1:$pg_port
NAME = postgres
USER = postgres
PASSWD = postgres
SSL_MODE = disable

[session]
PROVIDER = redis
PROVIDER_CONFIG = addrs=127.0.0.1:$redis_port db=0

[cache]
ENABLED = true
ADAPTER = redis
HOST = addrs=127.0.0.1:$redis_port db=1

[queue]
TYPE = level

[repository]
ROOT = $gitea_home/repos

[log]
MODE = console
LEVEL = warn
ROOT_PATH = $gitea_home/log

[security]
INSTALL_LOCK = true
SECRET_KEY = $secret_key
INTERNAL_TOKEN = $internal_token
EOF

# --- 1. schema migration: the ~115-table production schema ----------------
echo "== running Gitea schema migration against noida-db postgres"
migrate_start=$(date +%s)
if ! "$gitea_bin" migrate -c "$gitea_home/custom/conf/app.ini" -w "$gitea_home" >"$work/migrate.log" 2>&1; then
  cat "$work/migrate.log"
  fail "gitea migrate"
fi
echo "   migration finished in $(( $(date +%s) - migrate_start ))s"

# --- 2. admin user via CLI --------------------------------------------------
echo "== creating admin user"
admin_user="testadmin"
admin_pass="TestPass123!"
if ! "$gitea_bin" admin user create -c "$gitea_home/custom/conf/app.ini" -w "$gitea_home" \
    --username "$admin_user" --password "$admin_pass" --email "test@example.com" \
    --admin --must-change-password=false >"$work/admin-create.log" 2>&1; then
  cat "$work/admin-create.log"
  fail "gitea admin user create"
fi

# --- 3. start the real web server ------------------------------------------
echo "== starting gitea web"
# NOTE: on top of the schema migration above, `gitea web` startup runs xorm's
# Sync2 check again (InitDBEngine), which re-verifies every table's columns
# against the Go structs with one information_schema query per table. Against
# noida-db this has been observed to take several minutes (not seconds, as
# real Postgres does it) even with a --release build — see the README note
# next to this script for what that points to. Give it a generous timeout
# rather than fail a slow-but-working run.
web_start=$(date +%s)
"$gitea_bin" web -c "$gitea_home/custom/conf/app.ini" -w "$gitea_home" >"$work/gitea-web.log" 2>&1 &
gitea_pid=$!

base="http://127.0.0.1:$web_port"
up=""
for i in $(seq 1 720); do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$base/api/v1/version" 2>/dev/null)
  if [ "$code" = "200" ]; then
    up=1
    break
  fi
  kill -0 "$gitea_pid" 2>/dev/null || fail "gitea web process exited during startup"
  if [ $((i % 30)) -eq 0 ]; then
    echo "   ...still waiting on gitea web startup ($((i))s elapsed)"
  fi
  sleep 1
done
[ -n "$up" ] || fail "gitea web did not come up on $base within 720s"
echo "== gitea web is up on $base (startup sync took $(( $(date +%s) - web_start ))s)"

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

# --- 4. create a repository via the API ------------------------------------
echo "== creating repository via API"
repo_name="demo-repo"
status=$(http_status "$work/create-repo.json" "${auth[@]}" -H 'Content-Type: application/json' \
  -d "{\"name\":\"$repo_name\",\"auto_init\":true,\"default_branch\":\"main\"}" \
  "$base/api/v1/user/repos")
[ "$status" = "201" ] || { cat "$work/create-repo.json"; fail "create repository (HTTP $status)"; }
clone_url=$(json_get "$work/create-repo.json" "d['clone_url']")
echo "   repo created: $clone_url"

# --- 5. real git clone / push over HTTP -------------------------------------
echo "== git clone over HTTP"
repo_url="http://$admin_user:$admin_pass@127.0.0.1:$web_port/$admin_user/$repo_name.git"
clone_dir="$work/clone"
git clone -q "$repo_url" "$clone_dir" || fail "git clone"

git -C "$clone_dir" config user.email "test@example.com"
git -C "$clone_dir" config user.name "Test Admin"
echo "hello from noida-db" >"$clone_dir/HELLO.md"
git -C "$clone_dir" add HELLO.md
git -C "$clone_dir" commit -q -m "add HELLO.md" || fail "git commit"

echo "== git push over HTTP"
git -C "$clone_dir" push -q origin main || fail "git push"

# --- 6. issue + comment via the API -----------------------------------------
echo "== creating issue via API"
status=$(http_status "$work/issue.json" "${auth[@]}" -H 'Content-Type: application/json' \
  -d '{"title":"Test issue","body":"opened by the noida-db real-app test"}' \
  "$base/api/v1/repos/$admin_user/$repo_name/issues")
[ "$status" = "201" ] || { cat "$work/issue.json"; fail "create issue (HTTP $status)"; }
issue_index=$(json_get "$work/issue.json" "d['number']")
echo "   issue #$issue_index created"

echo "== commenting on issue via API"
status=$(http_status "$work/comment.json" "${auth[@]}" -H 'Content-Type: application/json' \
  -d '{"body":"a comment from the noida-db real-app test"}' \
  "$base/api/v1/repos/$admin_user/$repo_name/issues/$issue_index/comments")
[ "$status" = "201" ] || { cat "$work/comment.json"; fail "create issue comment (HTTP $status)"; }

# --- 7. full pull-request workflow ------------------------------------------
echo "== pushing feature branch"
git -C "$clone_dir" checkout -q -b feature
echo "change from feature branch" >>"$clone_dir/HELLO.md"
git -C "$clone_dir" commit -q -am "feature change" || fail "git commit (feature branch)"
git -C "$clone_dir" push -q origin feature || fail "git push (feature branch)"

echo "== opening PR via API"
status=$(http_status "$work/pr.json" "${auth[@]}" -H 'Content-Type: application/json' \
  -d '{"title":"Feature PR","head":"feature","base":"main","body":"opened by the noida-db real-app test"}' \
  "$base/api/v1/repos/$admin_user/$repo_name/pulls")
[ "$status" = "201" ] || { cat "$work/pr.json"; fail "open pull request (HTTP $status)"; }
pr_index=$(json_get "$work/pr.json" "d['number']")
echo "   PR #$pr_index opened"

echo "== merging PR via API"
# Gitea runs its own async merge-readiness checks (mergeability, required
# status checks, ...) right after a PR opens, and returns 405 ("Please try
# again later") until they finish -- retry with backoff rather than
# treating that as a real failure.
merge_ok=""
for attempt in $(seq 1 10); do
  status=$(http_status "$work/merge.json" "${auth[@]}" -H 'Content-Type: application/json' \
    -d '{"Do":"merge"}' \
    "$base/api/v1/repos/$admin_user/$repo_name/pulls/$pr_index/merge")
  if [ "$status" = "200" ] || [ "$status" = "204" ]; then
    merge_ok=1
    break
  fi
  if [ "$status" != "405" ]; then
    cat "$work/merge.json"
    fail "merge pull request (HTTP $status)"
  fi
  sleep 2
done
[ -n "$merge_ok" ] || { cat "$work/merge.json"; fail "merge pull request (still HTTP 405 after retries)"; }

echo "== verifying merged state via API"
status=$(http_status "$work/pr-final.json" "${auth[@]}" "$base/api/v1/repos/$admin_user/$repo_name/pulls/$pr_index")
[ "$status" = "200" ] || fail "fetch merged PR (HTTP $status)"
merged=$(json_get "$work/pr-final.json" "d['merged']")
[ "$merged" = "True" ] || { cat "$work/pr-final.json"; fail "PR #$pr_index did not report merged=true"; }

echo "== all Gitea real-app checks passed (schema migration, user, repo, clone/push, issue+comment, PR merge)"
echo "== total run time: $(( $(date +%s) - run_start ))s"

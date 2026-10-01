#!/usr/bin/env bash
# Runs a real, unmodified WordPress (https://wordpress.org/) — installed and
# driven via WP-CLI (https://wp-cli.org/), the official WordPress CLI — against
# noida-db's MySQL, exercising the same workflow documented in the top-level
# README's "Tested against real applications" table:
#
#   1. WordPress's own installer (`wp core install`) creating its real
#      production schema: ~12 core tables (wp_posts, wp_postmeta, wp_options,
#      wp_users, wp_usermeta, wp_comments, wp_commentmeta, wp_terms,
#      wp_termmeta, wp_term_taxonomy, wp_term_relationships, wp_links) via
#      plain mysqli-backed SQL (no ORM) and an admin user in one step. This is
#      the heaviest real-world DDL+INSERT workload this project exercises for
#      MySQL specifically — see docs/LIMITATIONS.md's MySQL section for the
#      current state of table/column-type/AUTO_INCREMENT support this relies
#      on.
#   2. Starting the real site with PHP's built-in dev server
#      (`php -S 127.0.0.1:PORT`) pointed at noida-db's MySQL via wp-config.php.
#   3. Creating a post via WP-CLI (`wp post create`).
#   4. Adding a comment to that post via WP-CLI (`wp comment create`).
#   5. Fetching the post back through WordPress's real REST API
#      (`GET /index.php?rest_route=/wp/v2/posts/<id>`) and asserting the
#      real title/content round-tripped correctly — the actual "does
#      noida-db's MySQL correctly serve a real production PHP app's query
#      patterns" check.
#
#   tests/apps/wordpress/run.sh
#
# A pinned WordPress core release (6.6.2) and a pinned WP-CLI release
# (2.11.0, as a standalone .phar) are downloaded on first run,
# checksum-verified against their upstream-published checksums, and cached
# under target/client-deps/wordpress/ and target/client-deps/wp-cli/ —
# repeat runs reuse both.
#
# WordPress needs PHP (with the mysqli extension) to run at all — this is a
# real toolchain requirement, not optional, so if `php` isn't on PATH, or
# lacks the mysqli extension, or `git`/`curl`/`python3` isn't available, or
# network access to wordpress.org/github.com is unavailable, the script
# prints "SKIPPED" and exits 0 rather than failing.
#
# This script exercises WordPress's actual, unmodified core schema (not a
# trimmed-down stand-in) specifically so a green run means real WordPress
# compatibility, and a red run points at exactly which real-world MySQL
# feature is missing next — `wp core install`'s own log (captured on
# failure below) will show the first CREATE TABLE that errors.
#
# The MySQL gaps this schema originally hit at `wp core install` (BIGINT
# UNSIGNED/TINYINT/MEDIUMTEXT/LONGTEXT column types, table-level PRIMARY
# KEY) have since been fixed — see docs/LIMITATIONS.md's MySQL section for
# the current state and any gaps discovered by a later run of this script
# that still remain (e.g. ALTER TABLE, used by dbDelta() on upgrades but
# not a fresh-install blocker).
set -uo pipefail

run_start=$(date +%s)
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cache="${NOIDA_CLIENT_CACHE:-$root/target/client-deps}"
wp_cache="$cache/wordpress"
cli_cache="$cache/wp-cli"
mkdir -p "$wp_cache" "$cli_cache"

WORDPRESS_VERSION="6.6.2"
WPCLI_VERSION="2.11.0"

skip() {
  echo "SKIPPED: $1"
  exit 0
}

command -v curl >/dev/null 2>&1 || skip "curl is not installed"
command -v python3 >/dev/null 2>&1 || skip "python3 is not installed"
command -v sha1sum >/dev/null 2>&1 || skip "sha1sum is not installed"
command -v sha256sum >/dev/null 2>&1 || skip "sha256sum is not installed"
command -v tar >/dev/null 2>&1 || skip "tar is not installed"

command -v php >/dev/null 2>&1 || skip "php is not installed (WordPress requires PHP; noida-db itself doesn't need it, but the real WordPress app under test does)"

php_version=$(php -r 'echo PHP_VERSION;' 2>/dev/null)
php_major_minor=$(php -r 'echo PHP_MAJOR_VERSION."." .PHP_MINOR_VERSION;' 2>/dev/null)
# WordPress 6.6 requires PHP >= 7.2 (recommends 7.4+); WP-CLI 2.11 requires PHP >= 7.2.24.
php -r 'exit(version_compare(PHP_VERSION, "7.2.24", ">=") ? 0 : 1);' \
  || skip "php version $php_version is too old (WordPress 6.6 + WP-CLI 2.11 need PHP >= 7.2.24)"
php -m 2>/dev/null | grep -qi '^mysqli$' \
  || skip "php is missing the mysqli extension, which WordPress's database layer requires"

os="$(uname -s)"
[ "$os" = "Linux" ] || skip "this script only fetches Linux-targeted tooling (found: $os)"

# --- fetch + checksum-verify WordPress core --------------------------------
wp_tarball="$wp_cache/wordpress-$WORDPRESS_VERSION.tar.gz"
if [ ! -f "$wp_tarball" ]; then
  echo "== downloading WordPress v$WORDPRESS_VERSION"
  url="https://wordpress.org/wordpress-$WORDPRESS_VERSION.tar.gz"
  if ! curl -fsSL -o "$wp_tarball.tmp" "$url"; then
    rm -f "$wp_tarball.tmp"
    skip "could not download WordPress v$WORDPRESS_VERSION (network unavailable?)"
  fi
  if ! curl -fsSL -o "$wp_tarball.tmp.sha1" "$url.sha1"; then
    rm -f "$wp_tarball.tmp" "$wp_tarball.tmp.sha1"
    skip "could not download WordPress checksum (network unavailable?)"
  fi
  got_sum=$(sha1sum "$wp_tarball.tmp" | awk '{print $1}')
  want_sum=$(cat "$wp_tarball.tmp.sha1" | tr -d '[:space:]')
  if [ "$got_sum" != "$want_sum" ]; then
    rm -f "$wp_tarball.tmp" "$wp_tarball.tmp.sha1"
    echo "checksum mismatch: got $got_sum, want $want_sum"
    exit 1
  fi
  mv "$wp_tarball.tmp" "$wp_tarball"
  rm -f "$wp_tarball.tmp.sha1"
fi

# --- fetch + checksum-verify WP-CLI -----------------------------------------
wp_cli_phar="$cli_cache/wp-cli-$WPCLI_VERSION.phar"
if [ ! -f "$wp_cli_phar" ]; then
  echo "== downloading WP-CLI v$WPCLI_VERSION"
  base_url="https://github.com/wp-cli/wp-cli/releases/download/v$WPCLI_VERSION"
  if ! curl -fsSL -o "$wp_cli_phar.tmp" "$base_url/wp-cli-$WPCLI_VERSION.phar"; then
    rm -f "$wp_cli_phar.tmp"
    skip "could not download WP-CLI v$WPCLI_VERSION (network unavailable?)"
  fi
  if ! curl -fsSL -o "$wp_cli_phar.tmp.sha256" "$base_url/wp-cli-$WPCLI_VERSION.phar.sha256"; then
    rm -f "$wp_cli_phar.tmp" "$wp_cli_phar.tmp.sha256"
    skip "could not download WP-CLI checksum (network unavailable?)"
  fi
  got_sum=$(sha256sum "$wp_cli_phar.tmp" | awk '{print $1}')
  want_sum=$(awk '{print $1}' "$wp_cli_phar.tmp.sha256")
  if [ "$got_sum" != "$want_sum" ]; then
    rm -f "$wp_cli_phar.tmp" "$wp_cli_phar.tmp.sha256"
    echo "checksum mismatch: got $got_sum, want $want_sum"
    exit 1
  fi
  mv "$wp_cli_phar.tmp" "$wp_cli_phar"
  chmod +x "$wp_cli_phar"
  rm -f "$wp_cli_phar.tmp.sha256"
fi

wp() {
  php "$wp_cli_phar" --path="$site_dir" --allow-root "$@"
}

work="$(mktemp -d "${TMPDIR:-/tmp}/noida-wordpress.XXXXXX")"
cleanup() {
  [ -n "${php_pid:-}" ] && kill "$php_pid" 2>/dev/null
  [ -n "${noida_pid:-}" ] && kill "$noida_pid" 2>/dev/null
  [ -n "${php_pid:-}" ] && wait "$php_pid" 2>/dev/null
  [ -n "${noida_pid:-}" ] && wait "$noida_pid" 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

mysql_port=$(free_port)
web_port=$(free_port)

echo "== building noida-db (release)"
cargo build --quiet --release --features mysql --bin noida-db || exit 1

echo "== starting noida-db (mysql:$mysql_port)"
"$root/target/release/noida-db" start --only mysql \
  --mysql-port "$mysql_port" \
  --data-dir "$work/noida-data" >"$work/noida.log" 2>&1 &
noida_pid=$!

for _ in $(seq 1 50); do
  (echo >"/dev/tcp/127.0.0.1/$mysql_port") 2>/dev/null && break
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
  echo "-- php server log (tail):"
  tail -n 60 "$work/php-server.log" 2>/dev/null
  exit 1
}

# --- extract a fresh WordPress core into the site dir -----------------------
site_dir="$work/site"
mkdir -p "$site_dir"
echo "== extracting WordPress v$WORDPRESS_VERSION"
tar -xzf "$wp_tarball" -C "$work"
mv "$work/wordpress"/* "$site_dir"/
rmdir "$work/wordpress"

# --- configure wp-config.php against noida-db's MySQL -----------------------
# noida-db's MySQL only serves a fixed set of built-in schemas
# (information_schema/mysql/performance_schema/sys/test) — there is no real
# CREATE DATABASE yet (see docs/LIMITATIONS.md) — so DB_NAME must be one of
# those. "test" is the intended general-purpose one.
echo "== creating wp-config.php pointed at noida-db mysql:$mysql_port"
if ! wp config create \
    --dbname="test" \
    --dbuser="root" \
    --dbpass="anything" \
    --dbhost="127.0.0.1:$mysql_port" \
    --path="$site_dir" --allow-root \
    --skip-check >"$work/wp-config-create.log" 2>&1; then
  cat "$work/wp-config-create.log"
  fail "wp config create"
fi

# WordPress spawns wp-cron via a loopback HTTP request to itself on
# nearly every page load (unless disabled). PHP's built-in dev server is
# single-threaded/sequential, so that self-request can't be served while
# the original request that triggered it is still being handled --
# without this, every real request would block on its own cron-spawn
# request, either hanging or timing out. Standard practice for any
# WP-CLI/php-built-in-server setup, not specific to noida-db.
wp config set DISABLE_WP_CRON true --raw --type=constant \
    --path="$site_dir" --allow-root >"$work/wp-config-set.log" 2>&1 \
    || { cat "$work/wp-config-set.log"; fail "wp config set DISABLE_WP_CRON"; }

# --- 1. WordPress's own installer: the real production schema --------------
echo "== running WordPress core install (creates ~12 core tables) against noida-db mysql"
install_start=$(date +%s)
admin_user="testadmin"
admin_pass="TestPass123!"
admin_email="test@example.com"
if ! wp core install \
    --url="http://127.0.0.1:$web_port" \
    --title="noida-db real-app test" \
    --admin_user="$admin_user" \
    --admin_password="$admin_pass" \
    --admin_email="$admin_email" \
    --skip-email \
    >"$work/core-install.log" 2>&1; then
  cat "$work/core-install.log"
  fail "wp core install"
fi
echo "   install finished in $(( $(date +%s) - install_start ))s"

echo "== verifying core tables via WP-CLI"
if ! wp db tables --all-tables-with-prefix >"$work/tables.log" 2>&1; then
  cat "$work/tables.log"
  fail "wp db tables"
fi
table_count=$(wc -l <"$work/tables.log" | tr -d '[:space:]')
[ "$table_count" -ge 11 ] 2>/dev/null || { cat "$work/tables.log"; fail "expected >=11 core WordPress tables, found $table_count"; }
echo "   $table_count wp_* tables created"

# --- 2. start the real site with PHP's built-in server ----------------------
echo "== starting php -S (WordPress site) on 127.0.0.1:$web_port"
web_start=$(date +%s)
# `index.php` as the router: without one, PHP's built-in server only
# serves requests that map to a real file (or a directory's own
# index.php/index.html) and 404s everything else, instead of falling back
# to WordPress's own front controller for any pretty URL that doesn't
# correspond to a real file on disk. This script's own REST API calls use
# the `index.php?rest_route=...` query-string form specifically because
# pretty `/wp-json/...` URLs aren't reliably dispatched by WordPress's
# rewrite-rule matching under this server (see the REST API section
# below), but other requests (admin-ajax.php, a real permalink, etc.)
# still depend on this router for the same fallback reason.
(cd "$site_dir" && php -S "127.0.0.1:$web_port" index.php >"$work/php-server.log" 2>&1) &
php_pid=$!

base="http://127.0.0.1:$web_port"
up=""
for i in $(seq 1 60); do
  # index.php?rest_route=/ -- not /wp-json/: the pretty-URL rewrite rules
  # WordPress's REST API registers (rest-api.php's rest_api_init()) aren't
  # guaranteed to match under PHP's built-in server without a real flushed
  # rewrite/.htaccess, and silently fall through to an ordinary front-end
  # page render (HTTP 200, HTML, not JSON) when they don't -- a 200 here
  # proved misleading in exactly that way. The query-string form is what
  # WordPress's own REST responses advertise via their Link header
  # (`rel="https://api.w.org/"`) as the one guaranteed to work regardless
  # of permalink structure, so use that instead of guessing at pretty URLs.
  code=$(curl -s -o /dev/null -w "%{http_code}" "$base/index.php?rest_route=/" 2>/dev/null)
  if [ "$code" = "200" ]; then
    up=1
    break
  fi
  kill -0 "$php_pid" 2>/dev/null || fail "php -S process exited during startup"
  sleep 0.5
done
[ -n "$up" ] || fail "WordPress site did not come up on $base within 30s"
echo "== WordPress site is up on $base (startup took $(( $(date +%s) - web_start ))s)"

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

# --- 3. create a post via WP-CLI --------------------------------------------
echo "== creating post via WP-CLI"
post_title="Hello from noida-db"
post_content="This post was created by the noida-db real-app test, stored in wp_posts and served by noida-db MySQL."
post_id=$(wp post create --post_title="$post_title" --post_content="$post_content" --post_status=publish --porcelain 2>"$work/post-create.log")
[ -n "$post_id" ] && [ "$post_id" -gt 0 ] 2>/dev/null || { cat "$work/post-create.log"; fail "wp post create did not return a numeric post id (got: '$post_id')"; }
echo "   post created: id=$post_id"

echo "== verifying post via WP-CLI"
got_title=$(wp post get "$post_id" --field=post_title 2>"$work/post-get.log")
[ "$got_title" = "$post_title" ] || { cat "$work/post-get.log"; fail "wp post get returned title '$got_title', expected '$post_title'"; }

# --- 4. add a comment to that post via WP-CLI -------------------------------
echo "== adding comment via WP-CLI"
comment_content="A comment from the noida-db real-app test."
comment_id=$(wp comment create \
    --comment_post_ID="$post_id" \
    --comment_content="$comment_content" \
    --comment_author="Test Commenter" \
    --comment_author_email="commenter@example.com" \
    --comment_approved=1 \
    --porcelain 2>"$work/comment-create.log")
[ -n "$comment_id" ] && [ "$comment_id" -gt 0 ] 2>/dev/null || { cat "$work/comment-create.log"; fail "wp comment create did not return a numeric comment id (got: '$comment_id')"; }
echo "   comment created: id=$comment_id"

# --- 5. fetch the post back via the real REST API ---------------------------
# index.php?rest_route=... again, not a pretty /wp-json/... URL -- see the
# readiness-check loop above for why: that pretty form was silently
# falling through to an ordinary front-end page render (HTTP 200, full
# HTML homepage, not JSON) instead of ever reaching the REST handler at
# all, under PHP's built-in server with no real flushed rewrite rules.
echo "== fetching post via REST API: GET /index.php?rest_route=/wp/v2/posts/$post_id"
status=$(http_status "$work/rest-post.json" "$base/index.php?rest_route=/wp/v2/posts/$post_id")
[ "$status" = "200" ] || { cat "$work/rest-post.json"; fail "GET rest_route=/wp/v2/posts/$post_id (HTTP $status)"; }
rest_title=$(json_get "$work/rest-post.json" "d['title']['rendered']")
rest_content=$(json_get "$work/rest-post.json" "d['content']['rendered']")
[ "$rest_title" = "$post_title" ] || fail "REST API post title '$rest_title' != expected '$post_title'"
# The REST API's rendered content is run through WordPress's own content
# filters (wpautop() wraps it in <p>...</p>, wptexturize() turns a
# straight apostrophe into &#8217;, etc.) -- real, expected WordPress
# behavior, not something noida-db does. Decode entities and strip tags
# before comparing, the way any real client consuming this field would,
# instead of requiring a byte-exact substring match.
python3 -c "
import html, re, sys
content = html.unescape('''$rest_content''')
content = re.sub('<[^>]+>', '', content)
expected = '''$post_content'''
if expected not in content:
    print('-- diagnostic: expected repr:', repr(expected))
    print('-- diagnostic: actual repr:  ', repr(content))
sys.exit(0 if expected in content else 1)
" || fail "REST API post content did not contain the real posted content"
echo "   REST API returned the real title and content for post $post_id"

echo "== fetching post list via REST API: GET /index.php?rest_route=/wp/v2/posts"
status=$(http_status "$work/rest-list.json" "$base/index.php?rest_route=/wp/v2/posts")
[ "$status" = "200" ] || { cat "$work/rest-list.json"; fail "GET rest_route=/wp/v2/posts (HTTP $status)"; }
found=$(json_get "$work/rest-list.json" "1 if any(p['id'] == $post_id for p in d) else 0")
[ "$found" = "1" ] || { cat "$work/rest-list.json"; fail "post $post_id not present in REST API post listing"; }
echo "   post $post_id present in REST API post listing"

echo "== all WordPress real-app checks passed (core install/schema, admin user, post create+verify, comment, REST API round-trip)"
echo "== total run time: $(( $(date +%s) - run_start ))s"

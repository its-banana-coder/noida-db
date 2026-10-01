# Packaging and distribution

How to get noida-db onto a machine today, and the plan for the rest.

## Today

**From source** (works now, any platform with a Rust toolchain):

```sh
cargo install --path . --all-features
```

**Docker** (works now, a `Dockerfile` ships at the repo root):

```sh
docker build -t noida-db .
docker run -p 5432:5432 -p 3306:3306 -p 6379:6379 -p 9092:9092 -p 9200:9200 noida-db
```

The image is a two-stage build (`rust:1-bookworm` → `debian:bookworm-slim`),
runs as a non-root user, and binds `0.0.0.0` by default so the `-p` mappings
work (the binary's own CLI default is `127.0.0.1`, which is correct for a
bare-metal dev tool but would make a container silently unreachable). Data
persists to the `/home/noida/.noida-db` volume.

## Not yet — crates.io, Homebrew, npm, PyPI

Each of these distribution channels expects a **prebuilt release binary**
per platform, fetched by the package manager rather than compiled on the
user's machine (the exception is crates.io, which can build from source via
`cargo install noida-db`, but still expects a published crate with full
`[package]` metadata).

What's needed before each is real, in order:

1. **A release workflow** — a GitHub Actions job that builds
   `cargo build --release --all-features` for Linux (x86_64/aarch64), macOS
   (x86_64/aarch64), and Windows (x86_64) on a version tag, and attaches the
   binaries to a GitHub Release. Nothing below is possible without this; it
   needs no external credentials (`GITHUB_TOKEN` is automatic).
2. **crates.io** — `cargo publish` once `[package]` carries real metadata
   (`repository`, `readme`, `keywords`, `categories` — already added to
   `Cargo.toml`). Needs a crates.io account + API token. The name `noida-db`
   is confirmed available.
3. **Homebrew** — a formula (`Formula/noida-db.rb`) in a tap repository
   (e.g. `its-banana-coder/homebrew-noida-db`) that downloads the release
   binary and its checksum from step 1. Needs a tap repo to exist under the
   user's account; the formula itself is small once step 1 lands.
4. **npm** — a thin wrapper package (`package.json` + a `postinstall` script
   that downloads the right platform binary from the GitHub Release and
   drops it on `$PATH`), the same pattern `esbuild`/`swc` use. Needs an npm
   account + publish token. The name `noida-db` is confirmed available.
5. **PyPI** — the same wrapper idea via a `pyproject.toml` with
   platform-specific wheel tags, each wheel just containing the downloaded
   binary (no actual Python code to compile). Needs a PyPI account + publish
   token. The name `noida-db` is confirmed available.

Steps 2–5 can each be scaffolded ahead of time, but none can be *published*
without the account credentials for that registry, which only the project
owner holds.

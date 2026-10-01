# Packaging and distribution

How to get noida-db onto a machine today, and the plan for the rest. This
is the short summary — for exact workflow YAML, package layouts, and every
tradeoff behind the choices below, see
[docs/specs/release-binaries.md](specs/release-binaries.md).

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

## Status as of the `v0.1.0` release

| Channel | Status | Install |
|---|---|---|
| **crates.io** | Live | `cargo install noida-db --all-features` |
| **npm** | Live (linux-x64 only for now) | `npm install -g noida-db` |
| **Homebrew** | Tap repo exists, formula not published yet | — |
| **PyPI** | Not published yet | — |
| **Docker** | Works via local build; not published to a registry yet | `docker build -t noida-db .` (see above) |

`.github/workflows/release.yml` builds all 5 platform targets and publishes
crates.io/npm/PyPI/a Homebrew-formula PR automatically on any `v*.*.*` tag
push. crates.io and npm's first release (`v0.1.0`) were published by hand
from this sandbox instead, since it can only build the `linux-x64` target
(no macOS toolchain, no Docker for cross-compiling aarch64) — the npm
release is therefore **linux-x64 only**; the other 4 platform packages and
a real PyPI/Homebrew release need the full CI workflow to actually run,
which needs a few one-time account-level things only the project owner can
set up:

1. **`HOMEBREW_TAP_TOKEN`** repo secret — a GitHub PAT (repo scope) that
   can push to `its-banana-coder/homebrew-noida-db` (already created). The
   workflow's own `GITHUB_TOKEN` is scoped to this repo only and can't push
   to a different one, even under the same account.
2. **PyPI Trusted Publishing** — on pypi.org, Publishing → Trusted
   Publishers → add a pending publisher for `noida-db` naming this repo,
   the `release.yml` workflow filename, and the `publish-pip` job. OIDC-based,
   no token to store or rotate.
3. A real tag push (`git tag v0.2.0 && git push origin v0.2.0`, once
   `Cargo.toml`'s version is bumped to match) to actually run the full
   5-platform build and every publish job together.

Full workflow YAML, every package layout, and the reasoning behind each
choice: [docs/specs/release-binaries.md](specs/release-binaries.md).

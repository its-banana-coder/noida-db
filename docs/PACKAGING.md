# Packaging and distribution

How to get noida-db onto a machine today, and the plan for the rest. This
is the short summary — for exact workflow YAML, package layouts, and every
tradeoff behind the choices below, see
[docs/specs/release-binaries.md](specs/release-binaries.md).

## Install

Prebuilt binaries for 5 targets: Linux x64 and arm64, macOS Intel and
Apple Silicon, Windows x64.

| Channel | Install |
|---|---|
| **npm** | `npm install -g noida-db` (or `npx noida-db start`) |
| **PyPI** | `pip install noida-db` |
| **Homebrew** | `brew install its-banana-coder/noida-db/noida-db` |
| **Docker** | `docker run -p 5432:5432 -p 3306:3306 -p 6379:6379 -p 9092:9092 -p 9200:9200 ghcr.io/its-banana-coder/noida-db` |
| **crates.io** | `cargo install noida-db --all-features` (builds from source) |
| **GitHub Releases** | an archive per target, with the binary inside |
| **From source** | `cargo install --path . --all-features` |

**Docker**: `ghcr.io/its-banana-coder/noida-db` (tags `latest` and each
`vX.Y.Z`, amd64 + arm64), pushed by the `Docker Publish` workflow on each
release. To build it yourself from the repo's `Dockerfile`:

```sh
docker build -t noida-db .
docker run -p 5432:5432 -p 3306:3306 -p 6379:6379 -p 9092:9092 -p 9200:9200 noida-db
```

The image is a two-stage build (`rust:1-bookworm` → `debian:bookworm-slim`),
runs as a non-root user, and binds `0.0.0.0` by default so the `-p` mappings
work (the binary's own CLI default is `127.0.0.1`, which is correct for a
bare-metal dev tool but would make a container silently unreachable). Data
persists to the `/home/noida/.noida-db` volume.

## How a release works

Bump `version` in `Cargo.toml`, merge, then push a matching tag
(`git tag v0.1.4 && git push origin v0.1.4`).
`.github/workflows/release.yml` checks the tag matches `Cargo.toml`, builds
all 5 targets, then publishes crates.io, npm (one package per platform
plus the `noida-db` meta package, whose `optionalDependencies` pick the
right one), PyPI (Trusted Publishing), the GitHub Release, and opens a
formula-update PR on `its-banana-coder/homebrew-noida-db` (merge it to
update Homebrew). The tag also runs the `real-apps` and `Docker Publish`
workflows.

Full workflow YAML, every package layout, and the reasoning behind each
choice: [docs/specs/release-binaries.md](specs/release-binaries.md).

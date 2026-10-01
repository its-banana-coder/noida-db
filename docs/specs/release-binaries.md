# Binary releases and package-manager distribution: noida-db spec

- **New files:** `.github/workflows/release.yml`, `npm/` (new package dir),
  `pip/` (new package dir), a Homebrew tap repo (separate GitHub repo, not
  in this one), `.github/workflows/docker-publish.yml`
- **Edits:** `Cargo.toml` (already has publish metadata as of 2026-10-01 —
  see §3), none needed elsewhere
- **Depends on:** nothing else in this repo. Independent of
  `docs/specs/mysql-persistence.md` / `docs/specs/kafka-persistence.md` —
  can be built in parallel by a different agent.
- **Context:** `docs/PACKAGING.md` is the short, already-merged summary of
  this plan (what's live today vs. not). This file is the detailed spec
  behind it — read `docs/PACKAGING.md` first for the two-minute version,
  then this for exactly how to build each piece.

## 1. Why this is sequenced the way it is

Every "install with your favorite package manager" workflow people expect
(`brew install`, `npm install -g`, `pip install`, `cargo install` from a
published crate) assumes a **prebuilt binary already exists somewhere
public** for each platform. None of it works, no matter how much
npm/pip/Homebrew-specific plumbing gets written, without that foundation
existing first. So:

1. **§2 — the release workflow** builds and publishes real binaries. Do
   this first, always.
2. **§3 — crates.io** (needs §2's binaries for nothing — `cargo install`
   builds from source — but needs real `[package]` metadata, already
   done; sequence it early anyway since it's the cheapest of the four).
3. **§4, §5 — npm, pip** both need §2's binaries to exist at stable,
   predictable URLs before their wrapper packages mean anything.
4. **§6 — Homebrew** needs §2's binaries too, plus a tap repo.
5. **§7 — Docker publishing** is independent of §2-6; the `Dockerfile`
   already exists and works (per-arch builds happen inside the Dockerfile
   itself via `docker buildx`, not via downloading a release asset).

If the agent picking this up can only do one thing, it should be §2.

## 2. The release workflow (`.github/workflows/release.yml`)

### 2.1 Trigger

```yaml
on:
  push:
    tags:
      - "v*.*.*"
```

A tag push (`git tag v0.2.0 && git push origin v0.2.0`) is the release
trigger — not a manual `workflow_dispatch` as the only way in, though
adding `workflow_dispatch` too as a manual-rerun escape hatch is fine and
cheap. The tag's `v` prefix strips off when deriving the release version
(`${GITHUB_REF_NAME#v}`) for anywhere that needs a bare semver string
(`Cargo.toml` version bump check, npm/pip package versions).

### 2.2 Build matrix

Five targets — this repo's own `[profile.release]` (`opt-level = "z",
lto = true, codegen-units = 1, panic = "abort", strip = true"` —
`Cargo.toml`) already applies to every one of these, no per-target profile
overrides needed:

| OS | `target` | Runner | Notes |
|---|---|---|---|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | `ubuntu-latest` | Native build. |
| Linux aarch64 | `aarch64-unknown-linux-gnu` | `ubuntu-latest` (cross) | Needs `cross` (see §2.3) — no native aarch64 GitHub runner on the free tier as of 2026-10-01; check current GitHub-hosted runner offerings before assuming this is still true. |
| macOS x86_64 | `x86_64-apple-darwin` | `macos-latest` (or `macos-13` if `macos-latest` has moved to Apple Silicon only) | Native build if the runner is Intel; cross-compile via `--target` otherwise (Xcode toolchain on an ARM runner can still target x86_64). |
| macOS aarch64 | `aarch64-apple-darwin` | `macos-latest` | Native build on an Apple Silicon runner. |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `windows-latest` | Native build. |

```yaml
jobs:
  build:
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            use_cross: false
          - os: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            use_cross: true
          - os: macos-latest
            target: x86_64-apple-darwin
            use_cross: false
          - os: macos-latest
            target: aarch64-apple-darwin
            use_cross: false
          - os: windows-latest
            target: x86_64-pc-windows-msvc
            use_cross: false
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      - name: Install cross
        if: matrix.use_cross
        run: cargo install cross --git https://github.com/cross-rs/cross
      - name: Build
        shell: bash
        run: |
          if [ "${{ matrix.use_cross }}" = "true" ]; then
            cross build --release --all-features --target ${{ matrix.target }}
          else
            cargo build --release --all-features --target ${{ matrix.target }}
          fi
```

### 2.3 Why `cross` for aarch64 Linux specifically

`cross` (the `cross-rs` project) runs the build inside a pinned Docker
image with the right cross-linker already set up — needed because
`aarch64-unknown-linux-gnu` from an `x86_64` Ubuntu runner needs a real
cross-linker (`aarch64-linux-gnu-gcc`), which plain `cargo build
--target` alone doesn't configure. **Check this project's dependency
tree first**: `mlua` (vendored Lua, C code compiled via `cc`),
`kafka-protocol`, `amq-protocol` and friends are all pure Rust, but `mlua`
with the `vendored` feature compiles a C library — confirm `cross`'s
Docker image has a C cross-compiler for `aarch64` available (it does, by
default, but verify against the actual `cross` image tag in use rather
than assuming). Mac and Windows targets don't need `cross` — native
runners for each exist.

### 2.4 Package and name each binary

After build, each matrix job packages its own binary before the
upload-to-release step. Naming convention (match what `rustup`/`ripgrep`/
similar Rust CLI tools use, since npm/Homebrew installers downstream will
construct this same filename pattern programmatically — see §4-6):

```
noida-db-${VERSION}-${target}.tar.gz   (Linux, macOS)
noida-db-${VERSION}-${target}.zip      (Windows)
```

e.g. `noida-db-0.2.0-x86_64-unknown-linux-gnu.tar.gz`,
`noida-db-0.2.0-x86_64-pc-windows-msvc.zip`. Each archive contains just
the single binary at its root (`noida-db` or `noida-db.exe`) — no nested
directory, so a tool unpacking it can `chmod +x` and move it directly.

```yaml
      - name: Package (Unix)
        if: runner.os != 'Windows'
        shell: bash
        run: |
          cd target/${{ matrix.target }}/release
          tar czf noida-db-${{ env.VERSION }}-${{ matrix.target }}.tar.gz noida-db
      - name: Package (Windows)
        if: runner.os == 'Windows'
        shell: bash
        run: |
          cd target/${{ matrix.target }}/release
          7z a noida-db-${{ env.VERSION }}-${{ matrix.target }}.zip noida-db.exe
```

(`env.VERSION` set once near the top of the workflow from
`${GITHUB_REF_NAME#v}`.)

### 2.5 Checksums

Generate a `SHA256SUMS` file covering every archive — Homebrew's formula
(§6) and any manual-download instructions need a published checksum, and
bundling them all into one file (rather than one `.sha256` per archive)
is simpler to consume and simpler to verify by hand:

```yaml
      - name: Checksum
        shell: bash
        run: sha256sum noida-db-${{ env.VERSION }}-${{ matrix.target }}.* > noida-db-${{ env.VERSION }}-${{ matrix.target }}.sha256
```

(On macOS, `shasum -a 256` instead of `sha256sum`, which isn't preinstalled
there — branch on `runner.os` or install `coreutils` via `brew` in the
job. Verify which is actually needed against the current macOS runner
image rather than assuming either way.)

### 2.6 Publish to a GitHub Release

A separate job, depending on every matrix build (`needs: build`), downloads
every artifact and creates the release:

```yaml
  release:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          path: dist
          merge-multiple: true
      - name: Combine checksums
        run: cat dist/*.sha256 > dist/SHA256SUMS
      - uses: softprops/action-gh-release@v2
        with:
          files: |
            dist/*.tar.gz
            dist/*.zip
            dist/SHA256SUMS
          generate_release_notes: true
```

Each matrix build job needs an `actions/upload-artifact@v4` step uploading
its own archive + `.sha256` before this job can download them — add that
as the last step of §2.4's job. No extra permissions/secrets needed:
`GITHUB_TOKEN` (automatic, already scoped for this repo) covers creating
a release and uploading its assets.

### 2.7 A version-bump check (optional but recommended)

Before trusting a tag's build, add a cheap guard job that fails the whole
workflow if `Cargo.toml`'s own `version` doesn't match the pushed tag
(`${GITHUB_REF_NAME#v} != $(cargo metadata --no-deps --format-version 1 |
jq -r '.packages[0].version')`) — catches the easy mistake of tagging a
release without actually bumping `Cargo.toml` first, which would
otherwise silently publish binaries that report the wrong `--version`.

## 3. crates.io

### 3.1 What's already done

`Cargo.toml`'s `[package]` already has `repository`, `homepage`, `readme`,
`keywords`, `categories` (added 2026-10-01, alongside this spec). Nothing
left to prepare on the metadata side — confirm this is still true before
assuming it (`git log -- Cargo.toml` / just read the file) in case it
regressed.

### 3.2 Publishing

Manual, one-time setup: a crates.io account (logged in via GitHub OAuth),
an API token (`cargo login`), added to this repo's GitHub Actions secrets
as `CARGO_REGISTRY_TOKEN` — **only the project owner can do this part**,
it needs their own crates.io account.

Once the secret exists, add a publish step to the §2.6 `release` job (or
a separate job gated the same way):

```yaml
  publish-crate:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo publish --all-features --token ${{ secrets.CARGO_REGISTRY_TOKEN }}
```

Run `cargo publish --dry-run --all-features` locally first, at least
once, before wiring this into CI — it validates the package (catches a
missing `license`/`description`, an overly large crate from an
accidentally-included file, etc.) without actually publishing, and is the
cheapest way to catch a packaging mistake before it's irreversible
(crates.io versions can be yanked but never deleted or overwritten).

### 3.3 What `cargo install noida-db` actually gets a user

Building from source on their machine (crates.io doesn't host binaries at
all) — slower than §4-6's prebuilt-binary paths, but works on any
platform with a Rust toolchain and needs nothing from §2. This is already
true today via `cargo install --path .`; publishing to crates.io only
changes *where* the source comes from (the registry instead of a local
checkout), not the build step itself.

## 4. npm

### 4.1 The two real options, and which to use

**Option A — postinstall download script.** A single npm package with a
`postinstall` script that detects `process.platform`/`process.arch` at
install time and downloads the matching archive from the GitHub Release
(§2) into a local `bin/` directory, matching the `package.json` `bin`
field there. Simple, one package to publish, but: breaks offline/airgapped
installs, breaks in environments that block `postinstall` scripts
(increasingly common for supply-chain-security reasons — many CI
systems and some corporate npm mirrors disable them by default), and
fails loudly if GitHub is unreachable at install time.

**Option B — per-platform optional-dependency packages**, the pattern
`esbuild`, `swc`, and `@napi-rs`-based tools use. A thin "meta" package
(`noida-db`) with **no** `postinstall` script, whose `package.json` lists
every platform's binary as an `optionalDependency`
(`@noida-db/linux-x64`, `@noida-db/darwin-arm64`, etc. — one tiny package
per platform, each containing just that platform's binary as a published
npm asset, no download step). npm itself resolves and installs only the
one matching the current platform (`os`/`cpu` fields in each platform
package's `package.json` make npm skip the rest automatically) — no
script execution needed at all, works offline once the registry has
cached the tarball, and isn't blocked by `--ignore-scripts`.

**Use Option B.** It's more files to publish but meaningfully more robust,
and is the pattern every comparable modern Rust/Go CLI-via-npm project
has converged on for exactly the reasons above. Don't build Option A.

### 4.2 Package layout

```
npm/
  noida-db/                       # the meta package users actually `npm install`
    package.json
    bin/noida-db.js                # thin shim, see §4.4
  platforms/
    linux-x64/package.json
    linux-arm64/package.json
    darwin-x64/package.json
    darwin-arm64/package.json
    win32-x64/package.json
```

Each `platforms/*/package.json`:

```json
{
  "name": "@noida-db/linux-x64",
  "version": "0.2.0",
  "os": ["linux"],
  "cpu": ["x64"],
  "files": ["noida-db"]
}
```

The actual binary (`noida-db` or `noida-db.exe`) gets copied into each
platform directory from the matching §2 release archive as a build step
(a small script, not committed to the repo — binaries don't belong in git
history) right before `npm publish` runs for that package, in CI.

### 4.3 The meta package

```json
{
  "name": "noida-db",
  "version": "0.2.0",
  "bin": { "noida-db": "bin/noida-db.js" },
  "optionalDependencies": {
    "@noida-db/linux-x64": "0.2.0",
    "@noida-db/linux-arm64": "0.2.0",
    "@noida-db/darwin-x64": "0.2.0",
    "@noida-db/darwin-arm64": "0.2.0",
    "@noida-db/win32-x64": "0.2.0"
  }
}
```

### 4.4 The shim

`bin/noida-db.js` resolves to the right installed platform package and
`exec`s the real binary, forwarding argv and the exit code:

```js
#!/usr/bin/env node
const { spawnSync } = require("node:child_process");
const path = require("node:path");

const platformPkg = {
  "linux-x64": "@noida-db/linux-x64",
  "linux-arm64": "@noida-db/linux-arm64",
  "darwin-x64": "@noida-db/darwin-x64",
  "darwin-arm64": "@noida-db/darwin-arm64",
  "win32-x64": "@noida-db/win32-x64",
}[`${process.platform}-${process.arch}`];

if (!platformPkg) {
  console.error(`noida-db: unsupported platform ${process.platform}-${process.arch}`);
  process.exit(1);
}

let binPath;
try {
  binPath = require.resolve(`${platformPkg}/noida-db${process.platform === "win32" ? ".exe" : ""}`);
} catch {
  console.error(`noida-db: optional dependency ${platformPkg} failed to install`);
  process.exit(1);
}

const result = spawnSync(binPath, process.argv.slice(2), { stdio: "inherit" });
process.exit(result.status ?? 1);
```

### 4.5 Versioning and publish automation

Every package's `version` must match the release tag exactly (npm doesn't
let a dependent install a different-version optional dependency
automatically the way a loose semver range would — pin exact versions
here on purpose, so a user always gets the platform binary that matches
their meta package's own version, never a mismatched pair). A CI job
(triggered by the same `v*.*.*` tag, after §2's binaries exist):

1. For each platform, download that target's §2 archive, extract the
   binary into `npm/platforms/<name>/`, bump `version` in its
   `package.json` to match the tag, `npm publish --access public`.
2. Bump `npm/noida-db/package.json`'s own `version` and every
   `optionalDependencies` entry to match, `npm publish --access public`.

Needs an `NPM_TOKEN` secret (an npm automation token, scoped to this
account/org — **only the project owner can create this**, same caveat as
crates.io).

### 4.6 `noida-db` name availability

Already confirmed available on npm as of 2026-09-30 (checked via the npm
registry API). Re-check before actually publishing, in case it's been
claimed since — a name squat on a popular-sounding package name is not
unheard of.

## 5. pip / PyPI

### 5.1 Why this is structurally different from npm

A Python wheel is expected to be installable **without running arbitrary
code or hitting the network** at install time — `pip install noida-db`
should Just Work offline once the wheel is cached, the same property
Option B gave npm. Unlike npm, though, pip has no "optional dependency
that's really just a different package per platform resolved
automatically" mechanism in the way `optionalDependencies` works — the
equivalent in the Python packaging world is **per-platform wheels of the
*same* package name**, distinguished by their own filename platform tag
(`noida_db-0.2.0-py3-none-manylinux_2_17_x86_64.whl`,
`..._macosx_11_0_arm64.whl`, `..._win_amd64.whl`, ...). `pip` itself picks
the one matching the installing machine automatically from whichever ones
are uploaded to PyPI under that one package name/version — this is the
standard, well-supported mechanism (the same one `ruff`/`uv`/
`python-ripgrep`-style projects use), not a workaround.

### 5.2 Build each wheel as "just embed the binary, no compilation"

Because the actual binary is pre-built already (§2's Rust cross-compile
matrix), there's no need for `maturin`'s full Rust-extension build path —
a much simpler plain wheel packaging a single data file is enough. Use
`setuptools` with a minimal `setup.py`/`pyproject.toml` whose only job is
to declare the right platform tag and include the downloaded binary as
package data, run once per platform in CI (not built from source on the
end user's machine at all):

```
pip/
  pyproject.toml
  src/noida_db/
    __init__.py
    cli.py          # thin shim, same idea as the npm one
    noida-db        # the binary, copied in by CI before building each wheel
```

`pyproject.toml`:

```toml
[project]
name = "noida-db"
version = "0.2.0"
description = "One tiny local binary that speaks Postgres, MySQL, Redis, Kafka and Elasticsearch."
readme = "README.md"
requires-python = ">=3.8"

[project.scripts]
noida-db = "noida_db.cli:main"

[tool.setuptools.package-data]
noida_db = ["noida-db", "noida-db.exe"]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"
```

`src/noida_db/cli.py` (same forwarding idea as §4.4):

```python
import os
import subprocess
import sys
from importlib.resources import files

def main():
    binname = "noida-db.exe" if os.name == "nt" else "noida-db"
    binpath = files("noida_db").joinpath(binname)
    os.chmod(binpath, 0o755)
    sys.exit(subprocess.call([str(binpath), *sys.argv[1:]]))
```

### 5.3 Producing a correctly-tagged wheel per platform in CI

`setuptools` alone doesn't know how to force an arbitrary platform tag
onto a wheel that contains no compiled extension (it would otherwise
build a `py3-none-any` wheel, which is wrong here — a `noida-db.exe` isn't
"any" platform). Use `wheel tags` (the `wheel` package's own CLI, part of
the standard toolchain) as a post-build step to retag it explicitly:

```yaml
      - run: python -m build --wheel
      - run: python -m wheel tags --platform-tag manylinux_2_17_x86_64 dist/*.whl
        # repeat per target: manylinux_2_17_aarch64, macosx_11_0_x86_64,
        # macosx_11_0_arm64, win_amd64 -- one job per platform, each
        # copying in only that platform's §2 binary before building.
```

Confirm the exact `wheel tags` invocation and flag names against the
`wheel` package's current CLI docs before relying on this verbatim — API
surface for this specific subcommand is worth double-checking rather than
trusting this spec's memory of it.

### 5.4 Publish

`twine upload dist/*.whl` (or `pypa/gh-action-pypi-publish`, the
GitHub-Actions-native equivalent using **PyPI's Trusted Publishing**
— OIDC-based, no long-lived API token stored as a secret at all, the
current recommended approach as of this spec's writing). Needs the PyPI
project to exist and Trusted Publishing configured once by the project
owner (links a specific GitHub repo + workflow filename to the PyPI
project — no npm/crates.io-style manually-copied token needed for this
one, which is a meaningfully better security posture; prefer it over a
plain `PYPI_API_TOKEN` secret if setting this up fresh).

### 5.5 `noida-db` name availability

Already confirmed available on PyPI as of 2026-09-30. Re-check before
publishing, same caveat as npm.

## 6. Homebrew

### 6.1 Needs a tap repo

Homebrew formulae for anything not already in `homebrew-core` (and a tiny
dev tool like this has no realistic path into `homebrew-core` itself —
that repo has real notability/maintenance-burden bars) live in a
**separate GitHub repo** named `homebrew-<tapname>` under the project
owner's account, e.g. `its-banana-coder/homebrew-noida-db`. **This repo
needs to be created by the project owner** — not something this spec or
an agent can do unilaterally (it's a new repo under their account).

### 6.2 The formula

`Formula/noida-db.rb` in that tap repo:

```ruby
class NoidaDb < Formula
  desc "One tiny local binary that speaks Postgres, MySQL, Redis, Kafka and Elasticsearch"
  homepage "https://github.com/its-banana-coder/noida-db"
  version "0.2.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v0.2.0/noida-db-0.2.0-aarch64-apple-darwin.tar.gz"
      sha256 "<from SHA256SUMS>"
    end
    on_intel do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v0.2.0/noida-db-0.2.0-x86_64-apple-darwin.tar.gz"
      sha256 "<from SHA256SUMS>"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v0.2.0/noida-db-0.2.0-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "<from SHA256SUMS>"
    end
    on_intel do
      url "https://github.com/its-banana-coder/noida-db/releases/download/v0.2.0/noida-db-0.2.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "<from SHA256SUMS>"
    end
  end

  def install
    bin.install "noida-db"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/noida-db --version")
  end
end
```

The `test do` block assumes `noida-db --version` exists and prints a
version string containing the formula's own `version` — confirm this
against the real CLI (`src/main.rs`'s arg parsing) before relying on it;
if `--version` isn't implemented yet, add it (a small, independent,
worthwhile fix regardless of this spec — most CLIs have one).

### 6.3 Keeping the formula in sync with each release

Write a small script (`scripts/update-homebrew-formula.sh` in *this*
repo, or directly in the tap repo — either is defensible, but keeping it
in this repo means it's versioned alongside the release workflow it
depends on) that, given a version and the `SHA256SUMS` file §2.6
publishes, regenerates the four `url`/`sha256` pairs. Run it as a step in
the same release workflow (needs a `GITHUB_TOKEN` with write access to
the *tap* repo specifically, or a separate PAT — a same-repo
`GITHUB_TOKEN` can't push to a different repo), opening a PR against the
tap repo rather than pushing directly, so the project owner reviews each
formula bump before it goes live — Homebrew formula updates are something
users' `brew upgrade` runs unattended, so a bad formula is unusually high
blast-radius for a small mistake.

### 6.4 Install experience once this exists

```sh
brew install its-banana-coder/noida-db/noida-db
```

(or `brew tap its-banana-coder/noida-db` once, then plain
`brew install noida-db` afterward).

## 7. Docker publishing

The `Dockerfile` itself already exists and works (content-verified against
source in this repo, build untested — no Docker available in the sandbox
that wrote it; needs a real `docker build` test before anyone relies on
it). What's missing is **publishing the built image** anywhere, so `docker
run noida-db` works without a local checkout/`docker build` at all.

### 7.1 Where to publish

**GitHub Container Registry (GHCR)**, not Docker Hub: no separate account
needed (uses the same GitHub identity/`GITHUB_TOKEN` as everything else
in this repo), and avoids Docker Hub's anonymous-pull rate limits biting
noida-db's own users. Publish as `ghcr.io/its-banana-coder/noida-db`.

### 7.2 Multi-arch build

```yaml
name: docker-publish
on:
  push:
    tags: ["v*.*.*"]
jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: |
            ghcr.io/its-banana-coder/noida-db:${{ github.ref_name }}
            ghcr.io/its-banana-coder/noida-db:latest
```

`docker/build-push-action` with `platforms: linux/amd64,linux/arm64`
builds **inside the Dockerfile's own `rust:1-bookworm` builder stage** via
QEMU emulation for the non-native arch — this is independent of §2's
cross-compiled binaries (it's compiling from source again, inside the
container, for each arch) and does not need `cross`; `setup-qemu-action`
handles the emulation `buildx` needs. This is slower than reusing a §2
binary directly (QEMU-emulated compilation is meaningfully slower than
native), but is the standard, simple way to do a multi-arch Docker build
and is fine for a release cadence of "whenever a version tag is pushed,"
not an every-commit cadence.

### 7.3 Install experience once this exists

```sh
docker run -p 5432:5432 -p 3306:3306 -p 6379:6379 -p 9092:9092 -p 9200:9200 \
  ghcr.io/its-banana-coder/noida-db:latest
```

No local `docker build` needed at all — update `README.md`'s "Installing"
section's Docker snippet to show this instead of (or alongside) the
local-build version, once this workflow is live and has actually
published at least once.

## 8. Versioning and release process (ties §2-7 together)

A single `git tag vX.Y.Z && git push origin vX.Y.Z` should be the **one**
action that triggers everything: §2's binaries, §3's crate (if wired into
the same trigger), §4/§5's packages (gated on §2 finishing, via
`needs:`/a separate workflow triggered by the same tag), §7's Docker
image. §6 (Homebrew) is the one exception — it's a PR against a different
repo, reviewed before merging, not an automatic publish.

Recommend (not mandatory, but worth deciding explicitly rather than
drifting into inconsistency): semantic versioning
(`MAJOR.MINOR.PATCH`), a `CHANGELOG.md` updated as part of the same PR
that bumps `Cargo.toml`'s `version` (not written after the fact from
`git log` — write it as changes land, the way most well-kept Rust
projects do), and the version-bump PR merged to `main` **before** tagging
(so the tag always points at a commit where `Cargo.toml`'s version matches
the tag — this is exactly what §2.7's guard job checks).

## 9. Suggested build order for whoever picks this up

1. §2 (release workflow) — everything else is blocked on this.
2. §3 (crates.io) — cheap, mostly already done, do it right after §2 so
   there's at least one more install path live quickly.
3. §7 (Docker publishing) — also cheap and independent, good next win.
4. §6 (Homebrew) — needs a tap repo from the project owner first; flag
   that dependency early rather than discovering it mid-task.
5. §4 and §5 (npm, pip) — the most work (per-platform package
   scaffolding, wheel retagging), do these last.

## 10. Explicitly out of scope

- **Linux distro packages** (`.deb`/`.rpm`, an AUR package, a `snap`/
  `flatpak`). Not requested; revisit only if a real user asks.
- **Auto-update / self-update machinery** inside the binary itself
  (`noida-db update`). Package managers already own "how does a user get
  a new version"; don't duplicate that inside the app.
- **Signing/notarization** (Windows code-signing, macOS notarization).
  Real friction for Homebrew/direct-download users (Gatekeeper/SmartScreen
  warnings) but a separate, nontrivial undertaking (needs paid certs/
  Apple Developer Program enrollment) — out of scope for this spec; note
  it as a known rough edge in `docs/PACKAGING.md` instead of solving it
  here.

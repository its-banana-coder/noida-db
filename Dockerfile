# syntax=docker/dockerfile:1

# --- builder ------------------------------------------------------------
FROM rust:1-bookworm AS builder
WORKDIR /build

# Cache the dependency build separately from the project's own source, so
# editing src/ doesn't force every dependency to recompile. Cargo needs
# *something* at src/lib.rs and src/main.rs to resolve the [lib]/[[bin]]
# targets in Cargo.toml; these placeholders are overwritten below once the
# real source is copied in.
COPY Cargo.toml Cargo.lock ./
# Cargo.toml declares [[test]] targets; cargo refuses to resolve the
# manifest if their files are missing, so they come along too. (Every
# Docker publish failed on this before.)
COPY tests ./tests
RUN mkdir -p src \
    && echo "fn main() {}" > src/main.rs \
    && echo "" > src/lib.rs \
    && cargo build --release --all-features \
    && rm -rf src

COPY src ./src
# Touch both crate roots so cargo doesn't treat them as unchanged from the
# placeholder build above (same mtime as Cargo.toml would otherwise look
# "older" than the cached fingerprint in some edge cases).
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --all-features \
    && strip target/release/noida-db

# --- runtime --------------------------------------------------------------
FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /home/noida noida
COPY --from=builder /build/target/release/noida-db /usr/local/bin/noida-db

USER noida
WORKDIR /home/noida
VOLUME ["/home/noida/.noida-db"]

EXPOSE 5432 3306 6379 9092 9200

# --host 0.0.0.0: the binary's own default (127.0.0.1) only accepts
# connections from inside the container's own network namespace, which
# would make `docker run -p ...` silently unreachable from the host.
ENTRYPOINT ["noida-db"]
CMD ["start", "--host", "0.0.0.0", "--data-dir", "/home/noida/.noida-db"]

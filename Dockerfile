# syntax=docker/dockerfile:1

###############################################################################
# Build stage
###############################################################################
FROM rust:1-bookworm AS build

WORKDIR /build

# Cache dependencies.
COPY Cargo.toml Cargo.lock ./
COPY crates/lk-proto/Cargo.toml crates/lk-proto/Cargo.toml
COPY crates/lk-server/Cargo.toml crates/lk-server/Cargo.toml
RUN mkdir -p crates/lk-proto/src crates/lk-server/src \
    && echo 'fn main() {}' > crates/lk-server/src/main.rs \
    && echo '' > crates/lk-proto/src/lib.rs \
    && echo '' > crates/lk-server/src/lib.rs \
    && echo '' > crates/lk-proto/build.rs \
    && mkdir -p crates/lk-proto/../lk-server && touch crates/lk-server/src/main.rs \
    && cargo build --release -p lk-server 2>/dev/null || true

# Real sources.
COPY . .
RUN cargo build --release -p lk-server

###############################################################################
# Runtime stage
###############################################################################
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/release/livekit-voice /usr/local/bin/livekit-voice

# Signaling (HTTP/WS + Twirp), media UDP, media TCP fallback.
EXPOSE 7880/tcp 7881/udp 7881/tcp 7882/tcp

ENTRYPOINT ["livekit-voice"]
CMD ["--config", "/etc/livekit/livekit.yaml"]

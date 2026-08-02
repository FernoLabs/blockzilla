# syntax=docker/dockerfile:1.7
ARG BUILD_REVISION=unknown
FROM rust:1.97-bookworm AS builder

ARG BUILD_REVISION
ENV CARGO_BUILD_JOBS=2 \
    CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
    CARGO_PROFILE_RELEASE_LTO=false \
    CARGO_PROFILE_RELEASE_STRIP=symbols \
    SHRED_READER_BUILD_REVISION=$BUILD_REVISION

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        clang \
        cmake \
        libclang-dev \
        libssl-dev \
        libudev-dev \
        make \
        pkg-config \
        protobuf-compiler \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    touch services/shred-reader/src/*.rs \
    && cargo build --locked --release --package shred-reader \
    && cp target/release/shred-reader /tmp/shred-reader

FROM builder AS test

RUN rustup component add clippy

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo test --locked --release --package shred-reader \
    && cargo clippy --locked --release --package shred-reader --all-targets --no-deps -- -D warnings

FROM debian:bookworm-slim AS runtime

ARG BUILD_REVISION

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        libstdc++6 \
        libssl3 \
        libudev1 \
        zlib1g \
    && useradd --create-home --uid 10001 app \
    && install --directory --owner app --group app /data \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/shred-reader /usr/local/bin/shred-reader

LABEL org.opencontainers.image.source="https://github.com/FernoLabs/blockzilla"
LABEL org.opencontainers.image.revision="$BUILD_REVISION"

USER app
ENV APP_ENV=production
ENV RUST_LOG=shred_reader=info,solana_gossip=warn,solana_streamer=warn
ENV IDENTITY_PATH=/data/identity.json
ENV BIND_IP=0.0.0.0
ENV GOSSIP_PORT=18001
ENV TVU_PORT=18002
ENV METRICS_ADDR=127.0.0.1:19090

VOLUME ["/data"]
EXPOSE 18001/tcp 18001/udp 18002/udp
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:19090/readyz || exit 1

ENTRYPOINT ["/usr/local/bin/shred-reader"]

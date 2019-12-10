# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------
FROM rust:1.39 AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential \
    openssl \
    libssl-dev \
    pkg-config \
    cmake \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    zlib1g-dev \
    ca-certificates


COPY Cargo.toml .
COPY ./src/ ./src

RUN cargo build --release
RUN cargo install --path .

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------
FROM ubuntu:18.10

RUN apt-get update && apt-get install -y openssl libc6 libsasl2-modules-gssapi-mit libsasl2-dev ca-certificates

COPY --from=builder /usr/local/cargo/bin/ethereum-poller /usr/local/bin/ethereum-poller

ENTRYPOINT ["ethereum-poller"]

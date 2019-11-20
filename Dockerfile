FROM rust:1.39 AS builder

WORKDIR /app

COPY ./src .
COPY Cargo.toml .

RUN cargo build --release

FROM alpine

WORKDIR /app

COPY --from=builder /app/target/release/ethereum-poller ./ethereum-poller

ENTRYPOINT ['./ethereum-poller']

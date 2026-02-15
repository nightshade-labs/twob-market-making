FROM rust:1.85-slim AS builder

ARG BIN_NAME

WORKDIR /app

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY idls ./idls

RUN cargo build --release --bin ${BIN_NAME}

FROM debian:bookworm-slim

ARG BIN_NAME

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/${BIN_NAME} /usr/local/bin/app

CMD ["app"]
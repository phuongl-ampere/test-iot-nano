FROM rust:1.96-bookworm AS build

WORKDIR /workspace
COPY . .

RUN cargo build --release --package iot-nano-monolith

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/target/release/iot-nano-monolith /opt/rush-iot-nano/iot-nano-monolith

WORKDIR /opt/rush-iot-nano
ENTRYPOINT ["/opt/rush-iot-nano/iot-nano-monolith"]

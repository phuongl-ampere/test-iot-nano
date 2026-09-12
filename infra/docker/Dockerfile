FROM rust:1.96-bookworm AS build

WORKDIR /workspace
COPY . .

RUN cargo build --release \
    --package iot-nano-api \
    --package iot-nano-core \
    --package iot-nano-mqttd \
    --package iot-nano-stream

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /workspace/target/release/iot-nano-api /opt/rush-iot-nano/iot-nano-api
COPY --from=build /workspace/target/release/iot-nano-core /opt/rush-iot-nano/iot-nano-core
COPY --from=build /workspace/target/release/iot-nano-mqttd /opt/rush-iot-nano/iot-nano-mqttd
COPY --from=build /workspace/target/release/iot-nano-stream /opt/rush-iot-nano/iot-nano-stream

WORKDIR /opt/rush-iot-nano

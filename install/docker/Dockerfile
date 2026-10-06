FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --package iot-nano-monolith

FROM debian:bookworm-slim
RUN apt-get update && apt-get install --no-install-recommends -y ca-certificates openssl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/iot-nano-monolith /usr/local/bin/iot-nano-monolith
COPY install/docker/entrypoint.sh /usr/local/bin/iot-nano-entrypoint
RUN chmod 0755 /usr/local/bin/iot-nano-monolith /usr/local/bin/iot-nano-entrypoint && mkdir -p /var/lib/iot-nano/platform /var/lib/iot-nano/internal /var/lib/iot-nano/tls
VOLUME ["/var/lib/iot-nano"]
EXPOSE 18080 1883 8883
ENTRYPOINT ["/usr/local/bin/iot-nano-entrypoint"]

# Single-container Docker install

From this directory, start IoT Nano with one container and one persistent
Docker volume:

```sh
cp .env.example .env
docker compose up -d --build
```

The first build on a new machine compiles the Rust service locally. Subsequent
changes limited to Docker configuration or this documentation reuse the Rust
build layer.

Verify it:

```sh
curl http://127.0.0.1:17180/healthz
curl http://127.0.0.1:17180/readyz
```

The ports are configured in `.env`; the supplied defaults are HTTP `17180`,
MQTT `17183`, and MQTT TLS `17184`. Change only these values before starting
the container when a host port is already occupied.

The container owns SQLite state, internal service state, a generated device
token vault key, and an MQTT TLS self-signed certificate in the
`iot-nano-data` volume. Do not remove that volume unless intentionally
resetting the platform:

```sh
docker compose down -v
```

HTTP runs without browser HTTPS by default. MQTT TLS uses the generated
self-signed certificate.

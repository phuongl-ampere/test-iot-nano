# IoT Nano Monolith Release Evidence

Date: 2026-09-16

## Artifacts

- Source commit: `3818da1f3cb94bd5101615b9fc44f60b3691bd69`
- Rust/Cargo: `1.96.0`
- Node/npm: `v26.3.0` / `11.16.0`
- Local container image: `iot-nano-monolith:test`
- Local container image ID:
  `sha256:5a0c1aaeed8144dbb3dce65423adb3173c9de541692a8acefcd8604e5204c32e`
- Image entrypoint: `/opt/rush-iot-nano/iot-nano-monolith`

## Verified Gates

- `cargo fmt --all -- --check`
- `cargo check --workspace --locked`
- Full Rust suites for `iot-storage`, `iot-nano-stream`,
  `iot-nano-core`, `iot-nano-api`, `iot-nano-mqttd`, and
  `iot-nano-monolith`, each serialized with `--test-threads=1`.
- Plan-named API and port gates:
  `oauth`, `public_v1`, `sqlite_auth`, and `ports`.
- Disposable Timescale storage contracts and monolith Timescale E2E.
- `scripts/e2e-monolith.sh`
- `scripts/verify-monolith-topology.sh`
- `scripts/verify-no-legacy-runtime.sh` and its verifier fixture suite.
- PowerMonitor and operator-console test and production-build gates.
- `docker compose --file infra/compose.yaml config --quiet` with a
  process-local test vault key.
- Docker build plus `iot-nano-monolith:test --help` smoke test.

One initial full Timescale-contract invocation reported a PostgreSQL deadlock.
The failing test passed in isolation on a fresh disposable container, then the
full Timescale contract suite passed on another fresh disposable container.
No test database or temporary contract container was retained.

## Release Conditions

This is local release evidence, not a deployment approval. Before production,
run the same gates in clean CI, build a signed image with checksums, and follow
the monolith-only migration and rollback procedure in
`docs/operations-monolith.md`. Do not use this release to import a
four-service production database or internal-state directory.

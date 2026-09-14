#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_args=(--file "$root/infra/compose.yaml")
if [[ "${IOT_NANO_TIMESCALE_COMPOSE:-0}" == "1" ]]; then
  compose_args+=(--file "$root/infra/compose.timescale.yaml")
fi

if [[ -n "${ROLLBACK_SQLITE_BACKUP:-}" ]]; then
  if [[ ! -f "$ROLLBACK_SQLITE_BACKUP" ]]; then
    printf 'ROLLBACK_SQLITE_BACKUP must name an existing SQLite backup\n' >&2
    exit 1
  fi
  docker compose "${compose_args[@]}" stop iot-nano-monolith
  docker compose "${compose_args[@]}" run --rm --no-deps \
    --volume "$ROLLBACK_SQLITE_BACKUP:/rollback/platform.sqlite:ro" \
    --entrypoint /bin/sh iot-nano-monolith \
    -c 'cp /rollback/platform.sqlite /var/lib/iot-nano/platform/platform.sqlite'
  docker compose "${compose_args[@]}" up --detach iot-nano-monolith
  exit 0
fi

if [[ -n "${TIMESCALE_RESTORE_POINT:-}" && -n "${TIMESCALE_RESTORE_COMMAND:-}" ]]; then
  if [[ ! -x "$TIMESCALE_RESTORE_COMMAND" ]]; then
    printf 'TIMESCALE_RESTORE_COMMAND must name an executable restore command\n' >&2
    exit 1
  fi
  docker compose "${compose_args[@]}" stop iot-nano-monolith
  "$TIMESCALE_RESTORE_COMMAND" "$TIMESCALE_RESTORE_POINT"
  docker compose "${compose_args[@]}" up --detach iot-nano-monolith
  exit 0
fi

printf 'rollback requires ROLLBACK_SQLITE_BACKUP or TIMESCALE_RESTORE_POINT plus TIMESCALE_RESTORE_COMMAND\n' >&2
exit 1

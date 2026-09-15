#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_args=(--file "$root/infra/compose.yaml")
if [[ "${IOT_NANO_TIMESCALE_COMPOSE:-0}" == "1" ]]; then
  compose_args+=(--file "$root/infra/compose.timescale.yaml")
fi

backup_root="${IOT_NANO_MONOLITH_BACKUP_DIR:-$root/backups/monolith}"
case "$backup_root" in
  /*) ;;
  *)
    printf 'IOT_NANO_MONOLITH_BACKUP_DIR must be an absolute path\n' >&2
    exit 1
    ;;
esac

verify_archive() {
  local archive="$1"
  [[ -f "$archive" && ! -L "$archive" && -s "$archive" ]] || {
    printf 'backup archive is missing or unsafe\n' >&2
    exit 1
  }
  tar -tzf "$archive" >/dev/null
  chmod 0600 "$archive"
}

verify_restore_point() {
  local restore_point="$1"
  [[ -e "$restore_point" && ! -L "$restore_point" ]] || {
    printf 'Timescale restore point was not created safely\n' >&2
    exit 1
  }
  if [[ -f "$restore_point" ]]; then
    [[ -s "$restore_point" ]] || {
      printf 'Timescale restore point is empty\n' >&2
      exit 1
    }
    chmod 0600 "$restore_point"
  elif [[ -d "$restore_point" ]]; then
    find "$restore_point" -mindepth 1 -print -quit | grep -q .
    chmod 0700 "$restore_point"
  else
    printf 'Timescale restore point must be a file or directory\n' >&2
    exit 1
  fi
}

docker compose "${compose_args[@]}" stop iot-nano-monolith

umask 077
mkdir -p "$backup_root"
chmod 0700 "$backup_root"
backup_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
backup_dir="$backup_root/$backup_id"
mkdir -m 0700 "$backup_dir"
backup_owner="$(id -u):$(id -g)"

if [[ "${IOT_NANO_TIMESCALE_COMPOSE:-0}" == "1" ]]; then
  if [[ -z "${TIMESCALE_BACKUP_COMMAND:-}" || ! -x "$TIMESCALE_BACKUP_COMMAND" ||
    -L "$TIMESCALE_BACKUP_COMMAND" ]]; then
    printf 'TIMESCALE_BACKUP_COMMAND must name a non-symlink executable\n' >&2
    exit 1
  fi

  restore_point="$backup_dir/timescale-restore-point"
  "$TIMESCALE_BACKUP_COMMAND" "$restore_point" >/dev/null
  verify_restore_point "$restore_point"

  docker compose "${compose_args[@]}" run --rm --no-deps \
    --volume "$backup_dir:/backup" \
    --env "BACKUP_OWNER=$backup_owner" \
    --entrypoint /bin/sh iot-nano-monolith \
    -c 'set -eu
      umask 077
      tar -C /var/lib/iot-nano/internal -czf /backup/internal-state.tar.gz .
      tar -tzf /backup/internal-state.tar.gz >/dev/null
      chown "$BACKUP_OWNER" /backup/internal-state.tar.gz
      chmod 0600 /backup/internal-state.tar.gz'
  verify_archive "$backup_dir/internal-state.tar.gz"
  printf '%s\n' \
    "backup_id=$backup_id" \
    'storage=timescale' \
    'internal_archive=internal-state.tar.gz' \
    'timescale_restore_point=timescale-restore-point' > "$backup_dir/manifest"
else
  docker compose "${compose_args[@]}" run --rm --no-deps \
    --volume "$backup_dir:/backup" \
    --env "BACKUP_OWNER=$backup_owner" \
    --entrypoint /bin/sh iot-nano-monolith \
    -c 'set -eu; umask 077
      tar -C /var/lib/iot-nano/platform -czf /backup/platform-state.tar.gz .
      tar -C /var/lib/iot-nano/internal -czf /backup/internal-state.tar.gz .
      tar -tzf /backup/platform-state.tar.gz >/dev/null
      tar -tzf /backup/internal-state.tar.gz >/dev/null
      chown "$BACKUP_OWNER" /backup/platform-state.tar.gz /backup/internal-state.tar.gz
      chmod 0600 /backup/platform-state.tar.gz /backup/internal-state.tar.gz'
  verify_archive "$backup_dir/platform-state.tar.gz"
  verify_archive "$backup_dir/internal-state.tar.gz"
  printf '%s\n' \
    "backup_id=$backup_id" \
    'storage=sqlite' \
    'platform_archive=platform-state.tar.gz' \
    'internal_archive=internal-state.tar.gz' > "$backup_dir/manifest"
fi

chmod 0600 "$backup_dir/manifest"
docker compose "${compose_args[@]}" run --rm --no-deps iot-nano-monolith --migrate-only
docker compose "${compose_args[@]}" up --detach iot-nano-monolith

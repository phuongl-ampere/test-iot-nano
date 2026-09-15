#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_args=(--file "$root/infra/compose.yaml")
if [[ "${IOT_NANO_TIMESCALE_COMPOSE:-0}" == "1" ]]; then
  compose_args+=(--file "$root/infra/compose.timescale.yaml")
fi

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

verify_archive() {
  local archive="$1"
  [[ -f "$archive" && ! -L "$archive" && -s "$archive" ]] ||
    fail 'rollback backup archive is missing or unsafe'
  tar -tzf "$archive" >/dev/null ||
    fail 'rollback backup archive cannot be read'
  tar -tzf "$archive" |
    awk '/^\// || /(^|\/)\.\.($|\/)/ { exit 1 }' ||
    fail 'rollback backup archive contains an unsafe path'
  tar -tvzf "$archive" |
    awk '$1 ~ /^[lh]/ { exit 1 }' ||
    fail 'rollback backup archive contains links'
}

restore_archives() {
  local restore_command="$1"
  docker compose "${compose_args[@]}" run --rm --no-deps \
    --volume "$backup_dir:/rollback:ro" \
    --entrypoint /bin/sh iot-nano-monolith \
    -c "$restore_command"
}

backup_dir="${ROLLBACK_BACKUP_DIR:-}"
[[ -n "$backup_dir" ]] || fail 'rollback requires ROLLBACK_BACKUP_DIR'
case "$backup_dir" in
  /*) ;;
  *) fail 'ROLLBACK_BACKUP_DIR must be an absolute path' ;;
esac
[[ -d "$backup_dir" && ! -L "$backup_dir" ]] ||
  fail 'ROLLBACK_BACKUP_DIR must name a non-symlink directory'

backup_id="$(basename "$backup_dir")"
manifest="$backup_dir/manifest"
[[ -f "$manifest" && ! -L "$manifest" ]] ||
  fail 'ROLLBACK_BACKUP_DIR must contain a manifest'
grep -qxF "backup_id=$backup_id" "$manifest" ||
  fail 'rollback backup manifest does not match its directory'

if [[ "${IOT_NANO_TIMESCALE_COMPOSE:-0}" == "1" ]]; then
  grep -qxF 'storage=timescale' "$manifest" ||
    fail 'rollback backup is not a Timescale paired backup'
  grep -qxF 'internal_archive=internal-state.tar.gz' "$manifest" ||
    fail 'rollback backup is missing its internal-state archive'
  grep -qxF 'timescale_restore_point=timescale-restore-point' "$manifest" ||
    fail 'rollback backup is missing its Timescale restore point'
  verify_archive "$backup_dir/internal-state.tar.gz"

  restore_point="$backup_dir/timescale-restore-point"
  [[ -e "$restore_point" && ! -L "$restore_point" ]] ||
    fail 'rollback Timescale restore point is missing or unsafe'
  if [[ -f "$restore_point" ]]; then
    [[ -s "$restore_point" ]] || fail 'rollback Timescale restore point is empty'
  elif [[ -d "$restore_point" ]]; then
    find "$restore_point" -mindepth 1 -print -quit | grep -q . ||
      fail 'rollback Timescale restore point is empty'
  else
    fail 'rollback Timescale restore point must be a file or directory'
  fi

  if [[ -z "${TIMESCALE_RESTORE_COMMAND:-}" || ! -x "$TIMESCALE_RESTORE_COMMAND" ||
    -L "$TIMESCALE_RESTORE_COMMAND" ]]; then
    fail 'TIMESCALE_RESTORE_COMMAND must name a non-symlink executable'
  fi

  docker compose "${compose_args[@]}" stop iot-nano-monolith
  restore_archives '
    set -eu
    umask 077
    stage="$(mktemp -d)"
    tar -xzf /rollback/internal-state.tar.gz -C "$stage"
    find /var/lib/iot-nano/internal -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    tar -C "$stage" -cf - . | tar -C /var/lib/iot-nano/internal -xf -
    find /var/lib/iot-nano/internal -type d -exec chmod 0700 {} +
    find /var/lib/iot-nano/internal -type f -exec chmod 0600 {} +
    rm -rf "$stage"'
  "$TIMESCALE_RESTORE_COMMAND" "$restore_point" >/dev/null
  docker compose "${compose_args[@]}" up --detach iot-nano-monolith
  exit 0
fi

grep -qxF 'storage=sqlite' "$manifest" ||
  fail 'rollback backup is not a SQLite paired backup'
grep -qxF 'platform_archive=platform-state.tar.gz' "$manifest" ||
  fail 'rollback backup is missing its platform-state archive'
grep -qxF 'internal_archive=internal-state.tar.gz' "$manifest" ||
  fail 'rollback backup is missing its internal-state archive'
verify_archive "$backup_dir/platform-state.tar.gz"
verify_archive "$backup_dir/internal-state.tar.gz"

docker compose "${compose_args[@]}" stop iot-nano-monolith
restore_archives '
  set -eu
  umask 077
  restore_archive() {
    archive="$1"
    target="$2"
    stage="$(mktemp -d)"
    tar -xzf "$archive" -C "$stage"
    find "$target" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    tar -C "$stage" -cf - . | tar -C "$target" -xf -
    find "$target" -type d -exec chmod 0700 {} +
    find "$target" -type f -exec chmod 0600 {} +
    rm -rf "$stage"
  }
  restore_archive /rollback/platform-state.tar.gz /var/lib/iot-nano/platform
  restore_archive /rollback/internal-state.tar.gz /var/lib/iot-nano/internal'
docker compose "${compose_args[@]}" up --detach iot-nano-monolith

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

validate_volume_path() {
  local name="$1"
  local path="$2"
  local component
  local -a components

  [[ "$path" == /* && "$path" != / ]] ||
    fail "$name must be a non-root absolute path"
  case "$path" in
    *:* | *$'\n'* | *$'\r'* | *'//'*) fail "$name contains an unsafe Docker volume path" ;;
  esac
  IFS=/ read -r -a components <<< "${path#/}"
  for component in "${components[@]}"; do
    [[ "$component" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] ||
      fail "$name contains an unsafe path component"
  done
}

verify_safe_directory_path() {
  local name="$1"
  local path="$2"
  local component
  local current
  local -a components

  validate_volume_path "$name" "$path"
  IFS=/ read -r -a components <<< "${path#/}"
  current=""
  for component in "${components[@]}"; do
    current="$current/$component"
    if [[ -L "$current" ]]; then
      fail "$name must not traverse symbolic links"
    fi
    [[ -d "$current" ]] ||
      fail "$name contains a missing or non-directory path component"
  done
  [[ ! -L "$path" && -O "$path" ]] ||
    fail "$name must name an owner-controlled non-symlink directory"
  chmod 0700 "$path" || fail "$name must be owner-only"
}

verify_backup_directory() {
  verify_safe_directory_path ROLLBACK_BACKUP_DIR "$1"
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

verify_restore_point() {
  local name="$1"
  local root_name="$2"
  local restore_root="$3"
  local restore_point="$4"
  local component
  local current
  local relative_path
  local symlink_path
  local -a components

  verify_safe_directory_path "$root_name" "$restore_root"
  validate_volume_path "$name" "$restore_point"
  [[ "$restore_point" == "$restore_root/"* ]] ||
    fail "$name must be inside $root_name"

  relative_path="${restore_point#"$restore_root/"}"
  IFS=/ read -r -a components <<< "$relative_path"
  current="$restore_root"
  for component in "${components[@]}"; do
    current="$current/$component"
    [[ ! -L "$current" ]] ||
      fail "$name must not traverse symbolic links"
    if [[ "$current" != "$restore_point" ]]; then
      [[ -d "$current" ]] ||
        fail "$name contains a missing or non-directory path component"
    fi
  done

  [[ -e "$restore_point" && ! -L "$restore_point" && -r "$restore_point" ]] ||
    fail "$name is missing or unsafe"
  if [[ -f "$restore_point" ]]; then
    [[ -s "$restore_point" ]] || fail "$name is empty"
  elif [[ -d "$restore_point" ]]; then
    [[ -x "$restore_point" ]] || fail "$name is missing or unsafe"
    symlink_path="$(find "$restore_point" -type l -print -quit)"
    [[ -z "$symlink_path" ]] || fail "$name contains symbolic links"
    find "$restore_point" -mindepth 1 -print -quit | grep -q . ||
      fail "$name is empty"
  else
    fail "$name must be a file or directory"
  fi
}

verify_external_command() {
  local name="$1"
  local command="$2"

  [[ -n "$command" && -x "$command" && ! -L "$command" ]] ||
    fail "$name must name a non-symlink executable"
}

restore_point_file_identity() {
  local path="$1"
  local identity

  if identity="$(stat -c '%d:%i' "$path" 2>/dev/null)"; then
    printf '%s\n' "$identity"
    return
  fi
  if identity="$(stat -f '%d:%i' "$path" 2>/dev/null)"; then
    printf '%s\n' "$identity"
    return
  fi
  fail "could not determine file identity for $path"
}

run_rollback_container() {
  local backup_mode="$1"
  local restore_command="$2"

  docker compose "${compose_args[@]}" run --rm --no-deps \
    --volume "$backup_dir:/rollback$backup_mode" \
    --env "BACKUP_OWNER=$backup_owner" \
    --env "ROLLBACK_CURRENT_PLATFORM_ARCHIVE=$current_platform_archive" \
    --env "ROLLBACK_CURRENT_INTERNAL_ARCHIVE=$current_internal_archive" \
    --env "ROLLBACK_RUN_ID=$rollback_run_id" \
    --entrypoint /bin/sh iot-nano-monolith \
    -c "$restore_command"
}

restore_archives() {
  run_rollback_container ':ro' "$1"
}

write_archives() {
  run_rollback_container '' "$1"
}

prepare_timescale_internal_state() {
  write_archives '
    set -eu
    umask 077
    target=/var/lib/iot-nano/internal
    archive="/rollback/$ROLLBACK_CURRENT_INTERNAL_ARCHIVE"
    candidate="$target/.rollback-internal-candidate.$ROLLBACK_RUN_ID"
    validation_archive="$(mktemp)"

    cleanup() {
      status=$?
      if [ "$status" -ne 0 ]; then
        rm -rf "$candidate"
      fi
      rm -f "$validation_archive"
      exit "$status"
    }
    trap cleanup EXIT

    [ ! -e "$archive" ] || exit 10
    [ ! -e "$candidate" ] || exit 10
    tar -C "$target" -czf "$archive" .
    tar -tzf "$archive" >/dev/null
    chown "$BACKUP_OWNER" "$archive"
    chmod 0600 "$archive"

    mkdir -m 0700 "$candidate"
    tar -xzf /rollback/internal-state.tar.gz -C "$candidate"
    tar -C "$candidate" -cf "$validation_archive" .
    tar -tf "$validation_archive" >/dev/null
    find "$candidate" -type d -exec chmod 0700 {} +
    find "$candidate" -type f -exec chmod 0600 {} +'
}

swap_timescale_internal_candidate() {
  write_archives '
    set -eu
    umask 077
    target=/var/lib/iot-nano/internal
    candidate="$target/.rollback-internal-candidate.$ROLLBACK_RUN_ID"
    previous="$target/.rollback-internal-previous.$ROLLBACK_RUN_ID"

    path_exists() {
      [ -e "$1" ] || [ -L "$1" ]
    }

    move_state() {
      source="$1"
      destination="$2"
      for entry in "$source"/* "$source"/.[!.]* "$source"/..?*; do
        path_exists "$entry" || continue
        mv -- "$entry" "$destination" || return 1
      done
    }

    move_live_state() {
      for entry in "$target"/* "$target"/.[!.]* "$target"/..?*; do
        path_exists "$entry" || continue
        case "$entry" in
          "$candidate" | "$previous") continue ;;
        esac
        mv -- "$entry" "$previous" || return 1
      done
    }

    [ -d "$candidate" ] || exit 10
    [ ! -e "$previous" ] || exit 10
    mkdir -m 0700 "$previous"
    move_live_state
    move_state "$candidate" "$target"
    rm -rf "$candidate" "$previous"'
}

restore_current_timescale_internal_state() {
  write_archives '
    set -eu
    umask 077
    target=/var/lib/iot-nano/internal
    archive="/rollback/$ROLLBACK_CURRENT_INTERNAL_ARCHIVE"
    candidate="$target/.rollback-internal-candidate.$ROLLBACK_RUN_ID"
    previous="$target/.rollback-internal-previous.$ROLLBACK_RUN_ID"

    path_exists() {
      [ -e "$1" ] || [ -L "$1" ]
    }

    clear_live_state() {
      for entry in "$target"/* "$target"/.[!.]* "$target"/..?*; do
        path_exists "$entry" || continue
        case "$entry" in
          "$candidate" | "$previous") continue ;;
        esac
        rm -rf -- "$entry" || return 1
      done
    }

    clear_live_state
    tar -xpf "$archive" -C "$target"
    find "$target" -type d -exec chmod 0700 {} +
    find "$target" -type f -exec chmod 0600 {} +
    rm -rf "$candidate" "$previous"'
}

restore_sqlite_pair() {
  write_archives '
    set -eu
    umask 077
    platform=/var/lib/iot-nano/platform
    internal=/var/lib/iot-nano/internal
    platform_candidate="$platform/.rollback-platform-candidate.$ROLLBACK_RUN_ID"
    internal_candidate="$internal/.rollback-internal-candidate.$ROLLBACK_RUN_ID"
    platform_previous="$platform/.rollback-platform-previous.$ROLLBACK_RUN_ID"
    internal_previous="$internal/.rollback-internal-previous.$ROLLBACK_RUN_ID"
    platform_current="/rollback/$ROLLBACK_CURRENT_PLATFORM_ARCHIVE"
    internal_current="/rollback/$ROLLBACK_CURRENT_INTERNAL_ARCHIVE"
    validation_archive="$(mktemp)"

    path_exists() {
      [ -e "$1" ] || [ -L "$1" ]
    }

    cleanup() {
      status=$?
      rm -rf \
        "$platform_candidate" \
        "$internal_candidate" \
        "$platform_previous" \
        "$internal_previous"
      rm -f "$validation_archive"
      exit "$status"
    }
    trap cleanup EXIT

    snapshot_current_state() {
      target="$1"
      archive="$2"
      [ ! -e "$archive" ] || return 1
      tar -C "$target" -czf "$archive" . || return 1
      tar -tzf "$archive" >/dev/null || return 1
      chown "$BACKUP_OWNER" "$archive" || return 1
      chmod 0600 "$archive" || return 1
    }

    stage_candidate() {
      archive="$1"
      candidate="$2"
      [ ! -e "$candidate" ] || return 1
      mkdir -m 0700 "$candidate" || return 1
      tar -xzf "$archive" -C "$candidate" || return 1
      tar -C "$candidate" -cf "$validation_archive" . || return 1
      tar -tf "$validation_archive" >/dev/null || return 1
      find "$candidate" -type d -exec chmod 0700 {} + || return 1
      find "$candidate" -type f -exec chmod 0600 {} + || return 1
    }

    move_state() {
      source="$1"
      destination="$2"
      for entry in "$source"/* "$source"/.[!.]* "$source"/..?*; do
        path_exists "$entry" || continue
        mv -- "$entry" "$destination" || return 1
      done
    }

    move_live_state() {
      target="$1"
      candidate="$2"
      previous="$3"
      for entry in "$target"/* "$target"/.[!.]* "$target"/..?*; do
        path_exists "$entry" || continue
        case "$entry" in
          "$candidate" | "$previous") continue ;;
        esac
        mv -- "$entry" "$previous" || return 1
      done
    }

    clear_live_state() {
      target="$1"
      candidate="$2"
      previous="$3"
      for entry in "$target"/* "$target"/.[!.]* "$target"/..?*; do
        path_exists "$entry" || continue
        case "$entry" in
          "$candidate" | "$previous") continue ;;
        esac
        rm -rf -- "$entry" || return 1
      done
    }

    restore_current_state() {
      archive="$1"
      target="$2"
      candidate="$3"
      previous="$4"
      clear_live_state "$target" "$candidate" "$previous" || return 1
      tar -xpf "$archive" -C "$target" || return 1
      find "$target" -type d -exec chmod 0700 {} + || return 1
      find "$target" -type f -exec chmod 0600 {} + || return 1
    }

    restore_current_pair() {
      platform_status=0
      internal_status=0
      restore_current_state \
        "$platform_current" "$platform" "$platform_candidate" "$platform_previous" ||
        platform_status=1
      restore_current_state \
        "$internal_current" "$internal" "$internal_candidate" "$internal_previous" ||
        internal_status=1
      [ "$platform_status" -eq 0 ] && [ "$internal_status" -eq 0 ]
    }

    apply_candidate_pair() {
      mkdir -m 0700 "$platform_previous" || return 1
      mkdir -m 0700 "$internal_previous" || return 1
      move_live_state "$platform" "$platform_candidate" "$platform_previous" || return 1
      move_state "$platform_candidate" "$platform" || return 1
      move_live_state "$internal" "$internal_candidate" "$internal_previous" || return 1
      move_state "$internal_candidate" "$internal" || return 1
    }

    snapshot_current_state "$platform" "$platform_current" || exit 10
    snapshot_current_state "$internal" "$internal_current" || exit 10
    stage_candidate /rollback/platform-state.tar.gz "$platform_candidate" || exit 10
    stage_candidate /rollback/internal-state.tar.gz "$internal_candidate" || exit 10
    if apply_candidate_pair; then
      exit 0
    fi
    if restore_current_pair; then
      exit 20
    fi
    exit 21'
}

compensate_timescale_current_state() {
  local database_status=0
  local internal_status=0

  "$TIMESCALE_COMPENSATE_COMMAND" "$timescale_current_restore_point" >/dev/null ||
    database_status=1
  restore_current_timescale_internal_state || internal_status=1
  [[ "$database_status" -eq 0 && "$internal_status" -eq 0 ]]
}

if [[ "${IOT_NANO_MONOLITH_TEST_LIB:-0}" == 1 ]]; then
  return 0
fi

backup_dir="${ROLLBACK_BACKUP_DIR:-}"
[[ -n "$backup_dir" ]] || fail 'rollback requires ROLLBACK_BACKUP_DIR'
verify_backup_directory "$backup_dir"
backup_owner="$(id -u):$(id -g)"
rollback_run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
current_platform_archive="rollback-current-platform-state-$rollback_run_id.tar.gz"
current_internal_archive="rollback-current-internal-state-$rollback_run_id.tar.gz"

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

  timescale_target_restore_point="$backup_dir/timescale-restore-point"
  timescale_current_restore_point="${TIMESCALE_CURRENT_RESTORE_POINT:-}"
  timescale_restore_root="${TIMESCALE_RESTORE_ROOT:-}"
  [[ -n "$timescale_current_restore_point" &&
    -n "$timescale_restore_root" &&
    -n "${TIMESCALE_COMPENSATE_COMMAND:-}" ]] ||
    fail 'rollback Timescale requires TIMESCALE_CURRENT_RESTORE_POINT, TIMESCALE_RESTORE_ROOT, and TIMESCALE_COMPENSATE_COMMAND'
  verify_restore_point 'rollback Timescale target restore point' \
    ROLLBACK_BACKUP_DIR "$backup_dir" "$timescale_target_restore_point"
  verify_restore_point 'rollback Timescale current restore point' \
    TIMESCALE_RESTORE_ROOT "$timescale_restore_root" "$timescale_current_restore_point"
  [[ "$(restore_point_file_identity "$timescale_target_restore_point")" != "$(restore_point_file_identity "$timescale_current_restore_point")" ]] ||
    fail 'rollback Timescale target and current restore points must differ by file identity'
  verify_external_command TIMESCALE_RESTORE_COMMAND "${TIMESCALE_RESTORE_COMMAND:-}"
  verify_external_command TIMESCALE_COMPENSATE_COMMAND "${TIMESCALE_COMPENSATE_COMMAND:-}"

  docker compose "${compose_args[@]}" stop iot-nano-monolith
  set +e
  prepare_timescale_internal_state
  preparation_status=$?
  set -e
  [[ "$preparation_status" -eq 0 ]] ||
    fail 'rollback Timescale state preparation failed; service remains stopped'

  set +e
  "$TIMESCALE_RESTORE_COMMAND" "$timescale_target_restore_point" >/dev/null
  target_restore_status=$?
  set -e
  if [[ "$target_restore_status" -eq 0 ]]; then
    set +e
    swap_timescale_internal_candidate
    internal_swap_status=$?
    set -e
    if [[ "$internal_swap_status" -eq 0 ]]; then
      docker compose "${compose_args[@]}" up --detach iot-nano-monolith
      exit 0
    fi
  fi

  if compensate_timescale_current_state; then
    fail 'rollback Timescale target restore failed; current platform and internal state were restored; service remains stopped'
  fi
  fail "rollback Timescale compensation failed. Manual recovery required: restore Timescale from $timescale_current_restore_point with TIMESCALE_COMPENSATE_COMMAND and restore internal state from $backup_dir/$current_internal_archive; service remains stopped"
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
set +e
restore_sqlite_pair
sqlite_restore_status=$?
set -e
case "$sqlite_restore_status" in
  0)
    docker compose "${compose_args[@]}" up --detach iot-nano-monolith
    ;;
  10)
    fail 'rollback SQLite state preparation failed; service remains stopped'
    ;;
  20)
    fail 'rollback SQLite target restore failed; original platform and internal state were restored; service remains stopped'
    ;;
  *)
    fail "rollback SQLite compensation failed. Manual recovery required: restore platform from $backup_dir/$current_platform_archive and internal state from $backup_dir/$current_internal_archive; service remains stopped"
    ;;
esac

#!/usr/bin/env bash
set -euo pipefail

unset -f docker 2>/dev/null || true

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
rollback_script="${1:-$root/infra/monolith/rollback.sh}"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

sandbox="$(mktemp -d "$root/rollback-copy-failure.XXXXXX")"
trap 'rm -rf "$sandbox"' EXIT

backup_dir="$sandbox/backups/rollback-copy-failure"
internal_dir="$sandbox/internal"
archive_source="$sandbox/archive-source"
fake_bin="$sandbox/bin"
docker_log="$sandbox/docker.log"
restore_log="$sandbox/restore.log"
original_state="$sandbox/original-state"
timescale_restore_root="$sandbox/timescale-restore-points"
current_restore_point="$timescale_restore_root/current-timescale-restore-point"
real_mv="$(command -v mv)"
real_tar="$(command -v tar)"
mkdir -p \
  "$backup_dir" \
  "$internal_dir" \
  "$archive_source" \
  "$fake_bin" \
  "$timescale_restore_root"
chmod 0700 "$backup_dir"
chmod 0700 "$timescale_restore_root"

printf '%s\n' \
  'backup_id=rollback-copy-failure' \
  'storage=timescale' \
  'internal_archive=internal-state.tar.gz' \
  'timescale_restore_point=timescale-restore-point' > "$backup_dir/manifest"
printf 'restored-state\n' > "$archive_source/restored-state"
tar -C "$archive_source" -czf "$backup_dir/internal-state.tar.gz" .
printf 'restore-point\n' > "$backup_dir/timescale-restore-point"
printf 'current-restore-point\n' > "$current_restore_point"
chmod 0600 "$current_restore_point"
printf 'old-state\0must-survive\n' > "$internal_dir/state"
chmod 0600 "$internal_dir/state"
cp -p "$internal_dir/state" "$original_state"

cat > "$sandbox/restore-success" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" >> "$ROLLBACK_FIXTURE_RESTORE_LOG"
EOF
chmod 0700 "$sandbox/restore-success"

cat > "$fake_bin/tar" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

for argument in "$@"; do
  if [[ "${ROLLBACK_FIXTURE_FAIL_TAR_CREATE:-0}" == 1 &&
    "$argument" == -cf ]]; then
    "$ROLLBACK_FIXTURE_REAL_TAR" -cf - --files-from /dev/null
    exit 1
  fi
done

exec "$ROLLBACK_FIXTURE_REAL_TAR" "$@"
EOF
chmod 0700 "$fake_bin/tar"

cat > "$fake_bin/mv" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

arguments=("$@")
if [[ "${arguments[0]:-}" == -- ]]; then
  arguments=("${arguments[@]:1}")
fi
source="${arguments[0]:-}"
destination="${arguments[1]:-}"

if [[ "${ROLLBACK_FIXTURE_FAIL_CANDIDATE_MOVE:-0}" == 1 &&
  "$source" == "$ROLLBACK_FIXTURE_INTERNAL_DIR"/.rollback-internal-candidate.*/* &&
  "$destination" == "$ROLLBACK_FIXTURE_INTERNAL_DIR" ]]; then
  previous_state="$(find "$ROLLBACK_FIXTURE_INTERNAL_DIR"/.rollback-internal-previous.* \
    -type f -name state -print -quit)"
  [[ -n "$previous_state" ]] &&
    cmp -s "$previous_state" "$ROLLBACK_FIXTURE_ORIGINAL_STATE" ||
    exit 97
  printf 'candidate-move-after-live-state-moved\n' >> "$ROLLBACK_FIXTURE_DOCKER_LOG"
  exit 1
fi

exec "$ROLLBACK_FIXTURE_REAL_MV" "${arguments[@]}"
EOF
chmod 0700 "$fake_bin/mv"

cat > "$fake_bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

[[ "${1:-}" == compose ]] || exit 64
shift
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    stop)
      printf 'stop\n' >> "$ROLLBACK_FIXTURE_DOCKER_LOG"
      exit 0
      ;;
    up)
      printf 'up\n' >> "$ROLLBACK_FIXTURE_DOCKER_LOG"
      exit 0
      ;;
    run)
      break
      ;;
  esac
  shift
done

[[ "${1:-}" == run ]] || exit 64
shift
backup_dir=""
restore_command=""
environment=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --volume)
      case "$2" in
        *:/rollback:ro) backup_dir="${2%:/rollback:ro}" ;;
        *:/rollback) backup_dir="${2%:/rollback}" ;;
        *) exit 64 ;;
      esac
      shift 2
      ;;
    --env)
      environment+=("$2")
      shift 2
      ;;
    -c)
      restore_command="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

[[ -n "$backup_dir" && -n "$restore_command" ]] || exit 64
restore_command="${restore_command//\/rollback/$backup_dir}"
restore_command="${restore_command//\/var\/lib\/iot-nano\/internal/$ROLLBACK_FIXTURE_INTERNAL_DIR}"
env "${environment[@]}" PATH="$ROLLBACK_FIXTURE_FAKE_BIN:$PATH" /bin/sh -c "$restore_command"
EOF
chmod 0700 "$fake_bin/docker"

run_rollback() {
  local tar_create_failure="$1"
  local candidate_move_failure="$2"

  set +e
  PATH="$fake_bin:$PATH" \
    IOT_NANO_TIMESCALE_COMPOSE=1 \
    ROLLBACK_BACKUP_DIR="$backup_dir" \
    TIMESCALE_RESTORE_COMMAND="$sandbox/restore-success" \
    TIMESCALE_RESTORE_ROOT="$timescale_restore_root" \
    TIMESCALE_CURRENT_RESTORE_POINT="$current_restore_point" \
    TIMESCALE_COMPENSATE_COMMAND="$sandbox/restore-success" \
    ROLLBACK_FIXTURE_DOCKER_LOG="$docker_log" \
    ROLLBACK_FIXTURE_RESTORE_LOG="$restore_log" \
    ROLLBACK_FIXTURE_INTERNAL_DIR="$internal_dir" \
    ROLLBACK_FIXTURE_ORIGINAL_STATE="$original_state" \
    ROLLBACK_FIXTURE_FAKE_BIN="$fake_bin" \
    ROLLBACK_FIXTURE_REAL_MV="$real_mv" \
    ROLLBACK_FIXTURE_REAL_TAR="$real_tar" \
    ROLLBACK_FIXTURE_FAIL_TAR_CREATE="$tar_create_failure" \
    ROLLBACK_FIXTURE_FAIL_CANDIDATE_MOVE="$candidate_move_failure" \
    "$rollback_script" > "$sandbox/rollback.log" 2>&1
  status=$?
  set -e
}

run_rollback 1 0

[[ "$status" -ne 0 ]] ||
  fail 'rollback succeeded after the injected staging archive failure'
cmp -s "$internal_dir/state" "$original_state" ||
  fail 'staging archive failure changed the original internal state'
[[ ! -s "$restore_log" ]] ||
  fail 'external Timescale restore ran after the injected staging archive failure'
if [[ -f "$docker_log" ]] && grep -qx 'up' "$docker_log"; then
  fail 'compose up ran after the injected staging archive failure'
fi

: > "$docker_log"
run_rollback 0 1

[[ "$status" -ne 0 ]] ||
  fail 'rollback succeeded after the injected candidate-move failure'
grep -qx 'candidate-move-after-live-state-moved' "$docker_log" ||
  fail 'injected candidate move did not observe the original internal state moved'
cmp -s "$internal_dir/state" "$original_state" ||
  fail 'rollback did not restore the original internal state'
[[ ! -e "$internal_dir/restored-state" ]] ||
  fail 'rollback left a partial replacement in internal state'
[[ -s "$restore_log" ]] ||
  fail 'external Timescale restore did not run'
if [[ -f "$docker_log" ]] && grep -qx 'up' "$docker_log"; then
  fail 'compose up ran after the injected candidate-move failure'
fi

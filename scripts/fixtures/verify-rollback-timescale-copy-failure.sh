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
mkdir -p "$backup_dir" "$internal_dir" "$archive_source" "$fake_bin"
chmod 0700 "$backup_dir"

printf '%s\n' \
  'backup_id=rollback-copy-failure' \
  'storage=timescale' \
  'internal_archive=internal-state.tar.gz' \
  'timescale_restore_point=timescale-restore-point' > "$backup_dir/manifest"
printf 'restored-state\n' > "$archive_source/restored-state"
tar -C "$archive_source" -czf "$backup_dir/internal-state.tar.gz" .
printf 'restore-point\n' > "$backup_dir/timescale-restore-point"
printf 'old-state\n' > "$internal_dir/state"
chmod 0600 "$internal_dir/state"

cat > "$sandbox/restore-success" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$1" >> "$ROLLBACK_FIXTURE_RESTORE_LOG"
EOF
chmod 0700 "$sandbox/restore-success"

cat > "$fake_bin/cp" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
chmod 0700 "$fake_bin/cp"

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
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --volume)
      backup_dir="${2%:/rollback:ro}"
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
PATH="$ROLLBACK_FIXTURE_FAKE_BIN:$PATH" /bin/sh -c "$restore_command"
EOF
chmod 0700 "$fake_bin/docker"

set +e
PATH="$fake_bin:$PATH" \
  IOT_NANO_TIMESCALE_COMPOSE=1 \
  ROLLBACK_BACKUP_DIR="$backup_dir" \
  TIMESCALE_RESTORE_COMMAND="$sandbox/restore-success" \
  ROLLBACK_FIXTURE_DOCKER_LOG="$docker_log" \
  ROLLBACK_FIXTURE_RESTORE_LOG="$restore_log" \
  ROLLBACK_FIXTURE_INTERNAL_DIR="$internal_dir" \
  ROLLBACK_FIXTURE_FAKE_BIN="$fake_bin" \
  "$rollback_script" > "$sandbox/rollback.log" 2>&1
status=$?
set -e

[[ "$status" -ne 0 ]] ||
  fail 'rollback succeeded after the injected target-copy failure'
cmp -s "$internal_dir/state" <(printf 'old-state\n') ||
  fail 'rollback did not restore the original internal state'
[[ ! -e "$internal_dir/restored-state" ]] ||
  fail 'rollback left a partial replacement in internal state'
[[ -s "$restore_log" ]] ||
  fail 'external Timescale restore did not run'
if [[ -f "$docker_log" ]] && grep -qx 'up' "$docker_log"; then
  fail 'compose up ran after the injected target-copy failure'
fi

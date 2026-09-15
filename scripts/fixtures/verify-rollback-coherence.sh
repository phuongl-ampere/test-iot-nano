#!/usr/bin/env bash
set -euo pipefail

unset -f docker 2>/dev/null || true
shopt -s nullglob

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
rollback_script="${1:-$root/infra/monolith/rollback.sh}"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

sandbox="$(mktemp -d "$root/rollback-coherence.XXXXXX")"
trap 'rm -rf "$sandbox"' EXIT

backup_dir="$sandbox/backups/rollback-coherence"
platform_dir="$sandbox/platform"
internal_dir="$sandbox/internal"
platform_target_source="$sandbox/platform-target"
internal_target_source="$sandbox/internal-target"
fake_bin="$sandbox/bin"
docker_log="$sandbox/docker.log"
restore_log="$sandbox/restore.log"
rollback_log="$sandbox/rollback.log"
target_restore_point="$backup_dir/timescale-restore-point"
timescale_restore_root="$sandbox/timescale-restore-points"
current_restore_point="$timescale_restore_root/current-timescale-restore-point"
real_mv="$(command -v mv)"
real_tar="$(command -v tar)"

mkdir -p \
  "$backup_dir" \
  "$platform_dir" \
  "$internal_dir" \
  "$platform_target_source" \
  "$internal_target_source" \
  "$fake_bin" \
  "$timescale_restore_root"
chmod 0700 "$backup_dir"
chmod 0700 "$timescale_restore_root"

printf '%s\n' \
  'backup_id=rollback-coherence' \
  'storage=sqlite' \
  'platform_archive=platform-state.tar.gz' \
  'internal_archive=internal-state.tar.gz' > "$backup_dir/manifest.sqlite"
printf '%s\n' \
  'backup_id=rollback-coherence' \
  'storage=timescale' \
  'internal_archive=internal-state.tar.gz' \
  'timescale_restore_point=timescale-restore-point' > "$backup_dir/manifest.timescale"

printf 'platform-target\n' > "$platform_target_source/state"
printf 'internal-target\n' > "$internal_target_source/state"
"$real_tar" -C "$platform_target_source" -czf "$backup_dir/platform-state.tar.gz" .
"$real_tar" -C "$internal_target_source" -czf "$backup_dir/internal-state.tar.gz" .

reset_restore_points() {
  rm -rf "$target_restore_point" "$current_restore_point"
  printf 'platform-target\n' > "$target_restore_point"
  printf 'platform-original\n' > "$current_restore_point"
  chmod 0600 "$target_restore_point" "$current_restore_point"
}

reset_restore_points

cat > "$sandbox/restore-platform" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

restore_point="$1"
printf '%s\n' "$restore_point" >> "$ROLLBACK_FIXTURE_RESTORE_LOG"
if [[ "${ROLLBACK_FIXTURE_FAIL_COMPENSATION:-0}" == 1 &&
  "$restore_point" == "$ROLLBACK_FIXTURE_CURRENT_RESTORE_POINT" ]]; then
  exit 1
fi
cat "$restore_point" > "$ROLLBACK_FIXTURE_PLATFORM_DIR/state"
EOF
chmod 0700 "$sandbox/restore-platform"

cat > "$fake_bin/tar" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

extracting=0
for argument in "$@"; do
  [[ "$argument" == -xpf ]] && extracting=1
done
for argument in "$@"; do
  if [[ "$extracting" == 1 &&
    "${ROLLBACK_FIXTURE_FAIL_SQLITE_COMPENSATION:-0}" == 1 &&
    "$argument" == *rollback-current-platform-state-* ]]; then
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

if [[ "${ROLLBACK_FIXTURE_FAIL_INTERNAL_SWAP:-0}" == 1 &&
  "$source" == "$ROLLBACK_FIXTURE_INTERNAL_DIR"/.rollback-internal-candidate.*/* &&
  "$destination" == "$ROLLBACK_FIXTURE_INTERNAL_DIR" ]]; then
  cmp -s "$ROLLBACK_FIXTURE_PLATFORM_DIR/state" \
    <(printf 'platform-target\n') || exit 97
  previous_state="$(find "$ROLLBACK_FIXTURE_INTERNAL_DIR"/.rollback-internal-previous.* \
    -type f -name state -print -quit)"
  [[ -n "$previous_state" ]] &&
    cmp -s "$previous_state" <(printf 'internal-original\n') ||
    exit 97
  printf 'target-side-io-after-partial-pair\n' >> "$ROLLBACK_FIXTURE_DOCKER_LOG"
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
restore_command="${restore_command//\/var\/lib\/iot-nano\/platform/$ROLLBACK_FIXTURE_PLATFORM_DIR}"
restore_command="${restore_command//\/var\/lib\/iot-nano\/internal/$ROLLBACK_FIXTURE_INTERNAL_DIR}"
env "${environment[@]}" PATH="$ROLLBACK_FIXTURE_FAKE_BIN:$PATH" /bin/sh -c "$restore_command"
EOF
chmod 0700 "$fake_bin/docker"

select_manifest() {
  local storage="$1"
  cp "$backup_dir/manifest.$storage" "$backup_dir/manifest"
  chmod 0600 "$backup_dir/manifest"
}

reset_live_pair() {
  rm -rf "$platform_dir"/* "$platform_dir"/.[!.]* "$platform_dir"/..?*
  rm -rf "$internal_dir"/* "$internal_dir"/.[!.]* "$internal_dir"/..?*
  printf 'platform-original\n' > "$platform_dir/state"
  printf 'internal-original\n' > "$internal_dir/state"
  chmod 0600 "$platform_dir/state" "$internal_dir/state"
}

assert_pair() {
  local platform_expected="$1"
  local internal_expected="$2"

  cmp -s "$platform_dir/state" <(printf '%s\n' "$platform_expected") ||
    fail "unexpected platform state: expected $platform_expected"
  cmp -s "$internal_dir/state" <(printf '%s\n' "$internal_expected") ||
    fail "unexpected internal state: expected $internal_expected"
}

assert_stopped() {
  grep -qx 'stop' "$docker_log" ||
    fail 'rollback did not stop the monolith'
  if grep -qx 'up' "$docker_log"; then
    fail 'rollback restarted the monolith after a failed restore'
  fi
}

assert_started() {
  grep -qx 'stop' "$docker_log" ||
    fail 'rollback did not stop the monolith before a successful restore'
  grep -qx 'up' "$docker_log" ||
    fail 'rollback did not start the monolith after a successful restore'
}

assert_preflight_rejected() {
  local description="$1"
  local message="$2"

  [[ "$status" -ne 0 ]] ||
    fail "$description was accepted"
  [[ ! -s "$docker_log" ]] ||
    fail "$description touched the service before validation failed"
  grep -Fqx "$message" "$rollback_log" ||
    fail "$description did not report the expected validation failure"
}

assert_missing_state() {
  local directory="$1"
  local name="$2"

  [[ ! -e "$directory/state" && ! -L "$directory/state" ]] ||
    fail "$name state was unexpectedly restored after compensation failed"
}

assert_recovery_archive() {
  local archive="$1"
  local expected_state="$2"

  [[ -f "$archive" && ! -L "$archive" && -s "$archive" ]] ||
    fail "required recovery archive is missing: $archive"
  "$real_tar" -xOzf "$archive" ./state |
    cmp -s - <(printf '%s\n' "$expected_state") ||
    fail "recovery archive does not contain the original $expected_state state"
}

run_rollback() {
  local timescale="$1"
  local fail_internal_swap="$2"
  local fail_compensation="$3"
  local fail_sqlite_compensation="$4"
  local include_timescale_contract="$5"
  local current_point="${6:-$current_restore_point}"
  local restore_root="${7:-$timescale_restore_root}"
  local compensate_command="$sandbox/restore-platform"

  if [[ "$include_timescale_contract" == 0 ]]; then
    current_point=""
    compensate_command=""
  fi

  : > "$docker_log"
  : > "$restore_log"
  set +e
  PATH="$fake_bin:$PATH" \
    IOT_NANO_TIMESCALE_COMPOSE="$timescale" \
    ROLLBACK_BACKUP_DIR="$backup_dir" \
    TIMESCALE_RESTORE_COMMAND="$sandbox/restore-platform" \
    TIMESCALE_CURRENT_RESTORE_POINT="$current_point" \
    TIMESCALE_RESTORE_ROOT="$restore_root" \
    TIMESCALE_COMPENSATE_COMMAND="$compensate_command" \
    ROLLBACK_FIXTURE_DOCKER_LOG="$docker_log" \
    ROLLBACK_FIXTURE_RESTORE_LOG="$restore_log" \
    ROLLBACK_FIXTURE_PLATFORM_DIR="$platform_dir" \
    ROLLBACK_FIXTURE_INTERNAL_DIR="$internal_dir" \
    ROLLBACK_FIXTURE_CURRENT_RESTORE_POINT="$current_restore_point" \
    ROLLBACK_FIXTURE_FAKE_BIN="$fake_bin" \
    ROLLBACK_FIXTURE_REAL_MV="$real_mv" \
    ROLLBACK_FIXTURE_REAL_TAR="$real_tar" \
    ROLLBACK_FIXTURE_FAIL_INTERNAL_SWAP="$fail_internal_swap" \
    ROLLBACK_FIXTURE_FAIL_COMPENSATION="$fail_compensation" \
    ROLLBACK_FIXTURE_FAIL_SQLITE_COMPENSATION="$fail_sqlite_compensation" \
    "$rollback_script" > "$rollback_log" 2>&1
  status=$?
  set -e
}

current_hardlink="$timescale_restore_root/current-timescale-restore-point-hardlink"
ln "$target_restore_point" "$current_hardlink"
select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 1 "$current_hardlink"
assert_preflight_rejected \
  'Timescale rollback hardlinked current restore point' \
  'rollback Timescale target and current restore points must differ by file identity'
rm "$current_hardlink"

outside_restore_point="$sandbox/outside-timescale-restore-point"
printf 'platform-original\n' > "$outside_restore_point"
chmod 0600 "$outside_restore_point"
select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 1 "$outside_restore_point"
assert_preflight_rejected \
  'Timescale rollback current restore point outside its safe root' \
  'rollback Timescale current restore point must be inside TIMESCALE_RESTORE_ROOT'
run_rollback \
  1 0 0 0 1 \
  "$timescale_restore_root/../$(basename "$outside_restore_point")"
assert_preflight_rejected \
  'Timescale rollback current restore point that traverses outside its safe root' \
  'rollback Timescale current restore point contains an unsafe path component'
rm "$outside_restore_point"

outside_restore_directory="$sandbox/outside-timescale-restore-directory"
mkdir -m 0700 "$outside_restore_directory"
printf 'platform-original\n' > "$outside_restore_directory/current"
current_symlink_parent="$timescale_restore_root/symlinked-parent"
ln -s "$outside_restore_directory" "$current_symlink_parent"
select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 1 "$current_symlink_parent/current"
assert_preflight_rejected \
  'Timescale rollback current restore point with a symlinked parent' \
  'rollback Timescale current restore point must not traverse symbolic links'
rm "$current_symlink_parent"
rm -rf "$outside_restore_directory"

current_restore_directory="$timescale_restore_root/current-timescale-restore-directory"
mkdir -m 0700 "$current_restore_directory"
ln -s "$current_restore_point" "$current_restore_directory/escape"
select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 1 "$current_restore_directory"
assert_preflight_rejected \
  'Timescale rollback current restore point with symlinked contents' \
  'rollback Timescale current restore point contains symbolic links'
rm -rf "$current_restore_directory"

rm "$target_restore_point"
mkdir -m 0700 "$target_restore_point"
ln -s "$current_restore_point" "$target_restore_point/escape"
select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 1
assert_preflight_rejected \
  'Timescale rollback target restore point with symlinked contents' \
  'rollback Timescale target restore point contains symbolic links'
reset_restore_points

select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 1
[[ "$status" -eq 0 ]] ||
  fail 'Timescale rollback did not succeed with complete compensation inputs'
assert_pair platform-target internal-target
assert_started
grep -Fx "$target_restore_point" "$restore_log" >/dev/null ||
  fail 'Timescale rollback did not use the target restore point'

select_manifest timescale
reset_live_pair
run_rollback 1 1 0 0 1
[[ "$status" -ne 0 ]] ||
  fail 'Timescale rollback succeeded after the injected internal swap failure'
assert_pair platform-original internal-original
assert_stopped
grep -qx 'target-side-io-after-partial-pair' "$docker_log" ||
  fail 'Timescale target I/O failure did not occur after partial pair changes'
grep -Fx "$target_restore_point" "$restore_log" >/dev/null ||
  fail 'Timescale rollback did not restore the target before the injected failure'
grep -Fx "$current_restore_point" "$restore_log" >/dev/null ||
  fail 'Timescale rollback did not compensate the restored target database'
grep -Fqx \
  'rollback Timescale target restore failed; current platform and internal state were restored; service remains stopped' \
  "$rollback_log" ||
  fail 'Timescale recovery did not report the stopped coherent-pair outcome'

select_manifest timescale
reset_live_pair
run_rollback 1 1 1 0 1
[[ "$status" -ne 0 ]] ||
  fail 'Timescale rollback succeeded after the injected compensation failure'
assert_pair platform-target internal-original
assert_stopped
grep -F \
  "rollback Timescale compensation failed. Manual recovery required: restore Timescale from $current_restore_point with TIMESCALE_COMPENSATE_COMMAND and restore internal state from $backup_dir/rollback-current-internal-state-" \
  "$rollback_log" >/dev/null ||
  fail 'Timescale compensation failure did not report exact manual recovery inputs'

select_manifest sqlite
reset_live_pair
run_rollback 0 0 0 0 1
[[ "$status" -eq 0 ]] ||
  fail 'SQLite rollback did not succeed with complete paired archives'
assert_pair platform-target internal-target
assert_started

select_manifest sqlite
reset_live_pair
run_rollback 0 1 0 0 1
[[ "$status" -ne 0 ]] ||
  fail 'SQLite rollback succeeded after the injected internal I/O failure'
assert_pair platform-original internal-original
assert_stopped
grep -qx 'target-side-io-after-partial-pair' "$docker_log" ||
  fail 'SQLite target I/O failure did not occur after partial pair changes'
grep -Fqx \
  'rollback SQLite target restore failed; original platform and internal state were restored; service remains stopped' \
  "$rollback_log" ||
  fail 'SQLite recovery did not report the stopped coherent-pair outcome'

select_manifest sqlite
reset_live_pair
rm -f \
  "$backup_dir"/rollback-current-platform-state-*.tar.gz \
  "$backup_dir"/rollback-current-internal-state-*.tar.gz
run_rollback 0 1 0 1 1
[[ "$status" -ne 0 ]] ||
  fail 'SQLite rollback succeeded after the injected compensation I/O failure'
assert_missing_state "$platform_dir" platform
cmp -s "$internal_dir/state" <(printf 'internal-original\n') ||
  fail 'internal state changed after platform compensation I/O failure'
assert_stopped
platform_recovery_archives=("$backup_dir"/rollback-current-platform-state-*.tar.gz)
internal_recovery_archives=("$backup_dir"/rollback-current-internal-state-*.tar.gz)
[[ "${#platform_recovery_archives[@]}" -eq 1 &&
  "${#internal_recovery_archives[@]}" -eq 1 ]] ||
  fail 'SQLite compensation failure did not preserve exactly one recovery archive per state'
platform_recovery_archive="${platform_recovery_archives[0]}"
internal_recovery_archive="${internal_recovery_archives[0]}"
assert_recovery_archive "$platform_recovery_archive" platform-original
assert_recovery_archive "$internal_recovery_archive" internal-original
grep -Fqx \
  "rollback SQLite compensation failed. Manual recovery required: restore platform from $platform_recovery_archive and internal state from $internal_recovery_archive; service remains stopped" \
  "$rollback_log" ||
  fail 'SQLite compensation failure did not report exact manual recovery inputs'

select_manifest timescale
reset_live_pair
run_rollback 1 0 0 0 0
[[ "$status" -ne 0 ]] ||
  fail 'Timescale rollback accepted a missing compensation contract'
[[ ! -s "$docker_log" ]] ||
  fail 'Timescale rollback touched the service before rejecting the compensation contract'
grep -Fqx \
  'rollback Timescale requires TIMESCALE_CURRENT_RESTORE_POINT, TIMESCALE_RESTORE_ROOT, and TIMESCALE_COMPENSATE_COMMAND' \
  "$rollback_log" ||
  fail 'Timescale rollback did not identify the missing compensation contract'

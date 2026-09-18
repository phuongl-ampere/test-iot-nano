#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
temporary_directory="$(mktemp -d)"
trap 'rm -rf "$temporary_directory"' EXIT

mkdir -p "$temporary_directory/bin"
capture_path="$temporary_directory/cargo-arguments"

cat >"$temporary_directory/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '<%s>\n' "$@" >"$IOT_NANO_TEST_CARGO_ARGUMENTS"
EOF
chmod +x "$temporary_directory/bin/cargo"

env -u IOT_NANO_TIMESCALE_TEST_URL \
  PATH="$temporary_directory/bin:$PATH" \
  IOT_NANO_TEST_CARGO_ARGUMENTS="$capture_path" \
  "$root/scripts/e2e-monolith.sh"

if ! grep -Fqx -- '<e2e_sqlite>' "$capture_path"; then
  printf 'expected e2e-monolith.sh to invoke the SQLite E2E target\n' >&2
  exit 1
fi

if ! grep -Fqx -- '<--ignored>' "$capture_path"; then
  printf 'expected e2e-monolith.sh to run the ignored SQLite E2E\n' >&2
  exit 1
fi

grep -Fqx -- '<--test-threads=1>' "$capture_path"

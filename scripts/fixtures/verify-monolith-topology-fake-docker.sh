#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
topology_verifier="$root/scripts/verify-monolith-topology.sh"
sandbox="$(mktemp -d "$root/topology-fake-docker.XXXXXX")"
trap 'rm -rf "$sandbox"' EXIT

mkdir -p "$sandbox/bin"
cat > "$sandbox/bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

[[ "${1:-}" == compose ]] || exit 64
shift
timescale=0
config=0
services=0
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --file)
      [[ "${2:-}" == *compose.timescale.yaml ]] && timescale=1
      shift 2
      ;;
    config)
      config=1
      shift
      ;;
    --services)
      services=1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

[[ "$config" == 1 && "$services" == 1 ]] || exit 64
printf '%s\n' iot-nano-monolith
if [[ "$timescale" == 1 ]]; then
  printf '%s\n' timescaledb
fi
EOF
chmod 0700 "$sandbox/bin/docker"

DOCKER_BIN="$sandbox/bin/docker" "$topology_verifier"

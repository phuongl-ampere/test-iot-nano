#!/usr/bin/env bash
set -euo pipefail

work_dir="$(mktemp -d)"
container_id=""
subscriber_a=""
subscriber_b=""

cleanup() {
  for pid in "$subscriber_a" "$subscriber_b"; do
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  if [ -n "$container_id" ]; then
    docker rm --force "$container_id" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

container_id="$(docker run --detach \
  --publish 127.0.0.1::1883 \
  --volume "$PWD/infra/nanomq/virtual-rpc-spike.conf:/etc/nanomq/nanomq.conf:ro" \
  emqx/nanomq:0.25.6 \
  nanomq start --conf /etc/nanomq/nanomq.conf)"
port="$(docker port "$container_id" 1883/tcp | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p')"
if [ -z "$port" ]; then
  printf 'Could not determine temporary NanoMQ port\n' >&2
  exit 1
fi

topic="v1/devices/me/rpc/request/018f6da9-1234-7abc-8def-0123456789ab"
payload='{"method":"sample_now","params":{}}'
mosquitto_sub --host 127.0.0.1 --port "$port" \
  --topic 'v1/devices/me/rpc/request/+' -C 1 >"$work_dir/a.out" &
subscriber_a="$!"
mosquitto_sub --host 127.0.0.1 --port "$port" \
  --topic 'v1/devices/me/rpc/request/+' -C 1 >"$work_dir/b.out" &
subscriber_b="$!"
sleep 1
mosquitto_pub --host 127.0.0.1 --port "$port" --topic "$topic" --message "$payload"
wait "$subscriber_a"
wait "$subscriber_b"

test "$(cat "$work_dir/a.out")" = "$payload"
test "$(cat "$work_dir/b.out")" = "$payload"
printf 'NanoMQ broadcasts literal virtual RPC topics to both subscribers as expected.\n'

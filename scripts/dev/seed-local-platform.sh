#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
runtime_helper="${IOT_NANO_LOCAL_RUNTIME_HELPER:-$root/scripts/dev/local-platform-runtime.sh}"
seed_file="${IOT_NANO_LOCAL_SEED_FILE:-$root/infra/monolith/local-platform-seed.env}"
management_url="${IOT_NANO_HTTP_URL:-http://127.0.0.1:18081}"
powermonitor_url="${IOT_NANO_POWERMONITOR_URL:-http://localhost:3002}"

if [[ ! -f "$runtime_helper" ]]; then
  printf 'Local runtime helper does not exist: %s\n' "$runtime_helper" >&2
  exit 2
fi
# shellcheck disable=SC1090
source "$runtime_helper"
local_platform_require_reset "$@"
local_platform_configure
management_url="http://$IOT_NANO_HTTP_ADDRESS"

if [[ "${IOT_NANO_ALLOW_LOCAL_SEED:-}" != "1" ]]; then
  printf '%s\n' 'Set IOT_NANO_ALLOW_LOCAL_SEED=1 to run the fixed local seed.' >&2
  exit 2
fi

case "$management_url" in
  http://127.0.0.1:*|http://localhost:*) ;;
  *)
    printf '%s\n' 'The local seed only permits a loopback management URL.' >&2
    exit 2
    ;;
esac

case "$powermonitor_url" in
  http://127.0.0.1:*|http://localhost:*) ;;
  *)
    printf '%s\n' 'The local seed only permits a loopback PowerMonitor URL.' >&2
    exit 2
    ;;
esac
powermonitor_url="${powermonitor_url%/}"

if [[ ! -f "$seed_file" ]]; then
  printf 'Seed file does not exist: %s\n' "$seed_file" >&2
  exit 2
fi

# shellcheck disable=SC1090
source "$seed_file"
: "${IOT_NANO_SEED_CONTROLLER_USERNAME:=seed-controller}"
: "${IOT_NANO_SEED_CONTROLLER_PASSWORD:=SeedControllerOnly2026!}"
: "${IOT_NANO_SEED_VIEWER_USERNAME:=${IOT_NANO_SEED_RECIPIENT_USERNAME:-seed-viewer}}"
: "${IOT_NANO_SEED_VIEWER_PASSWORD:=${IOT_NANO_SEED_RECIPIENT_PASSWORD:-SeedViewerOnly2026!}}"
: "${IOT_NANO_SEED_UNASSIGNED_USERNAME:=seed-unassigned}"
: "${IOT_NANO_SEED_UNASSIGNED_PASSWORD:=SeedUnassignedOnly2026!}"


require_seed_variables() {
  local variable

  for variable in \
    IOT_NANO_SEED_SYSTEM_USERNAME \
    IOT_NANO_SEED_SYSTEM_PASSWORD \
    IOT_NANO_SEED_TENANT_SLUG \
    IOT_NANO_SEED_TENANT_USERNAME \
    IOT_NANO_SEED_TENANT_PASSWORD \
    IOT_NANO_SEED_OWNER_USERNAME \
    IOT_NANO_SEED_OWNER_PASSWORD \
    IOT_NANO_SEED_CONTROLLER_USERNAME \
    IOT_NANO_SEED_CONTROLLER_PASSWORD \
    IOT_NANO_SEED_VIEWER_USERNAME \
    IOT_NANO_SEED_VIEWER_PASSWORD \
    IOT_NANO_SEED_UNASSIGNED_USERNAME \
    IOT_NANO_SEED_UNASSIGNED_PASSWORD; do
    [[ -n "${!variable:-}" ]] || {
      printf 'Seed variable is required: %s\n' "$variable" >&2
      return 1
    }
  done
}

require_seed_variables
local_platform_preflight
local_platform_stop
local_platform_clear_state
local_platform_bootstrap "$IOT_NANO_SEED_SYSTEM_USERNAME" "$IOT_NANO_SEED_SYSTEM_PASSWORD"
local_platform_start

state_dir="$(mktemp -d "${TMPDIR:-/tmp}/iot-nano-local-seed.XXXXXX")"
trap 'rm -rf "$state_dir"' EXIT
system_cookie="$state_dir/system.cookie"
tenant_cookie="$state_dir/tenant.cookie"
owner_cookie="$state_dir/owner.cookie"

request_status() {
  local response_file="$1"
  shift
  curl --silent --show-error --output "$response_file" --write-out '%{http_code}' "$@"
}

require_status() {
  local actual="$1"
  local expected="$2"
  local operation="$3"
  if [[ "$actual" != "$expected" ]]; then
    printf '%s failed with HTTP %s\n' "$operation" "$actual" >&2
    exit 1
  fi
}

system_login_body="$state_dir/system-login.json"
system_login_status="$(request_status "$system_login_body" \
  --cookie-jar "$system_cookie" \
  --header 'Content-Type: application/json' \
  --data "$(jq -nc \
    --arg username "$IOT_NANO_SEED_SYSTEM_USERNAME" \
    --arg password "$IOT_NANO_SEED_SYSTEM_PASSWORD" \
    '{username: $username, password: $password}')" \
  "$management_url/api/v1/system/auth/login")"
require_status "$system_login_status" 200 'System login'

tenant_create_body="$state_dir/tenant-create.json"
tenant_create_status="$(request_status "$tenant_create_body" \
  --cookie "$system_cookie" \
  --header 'Content-Type: application/json' \
  --data "$(jq -nc \
    --arg slug "$IOT_NANO_SEED_TENANT_SLUG" \
    --arg username "$IOT_NANO_SEED_TENANT_USERNAME" \
    --arg password "$IOT_NANO_SEED_TENANT_PASSWORD" \
    '{slug: $slug, metadata: {}, tenant_account_username: $username, tenant_account_password: $password}')" \
  "$management_url/api/v1/system/tenants")"
case "$tenant_create_status" in
  201|409) ;;
  *) require_status "$tenant_create_status" 201 'Tenant seed' ;;
esac

tenant_login_body="$state_dir/tenant-login.json"
tenant_login_status="$(request_status "$tenant_login_body" \
  --cookie-jar "$tenant_cookie" \
  --header 'Content-Type: application/json' \
  --data "$(jq -nc \
    --arg tenant_slug "$IOT_NANO_SEED_TENANT_SLUG" \
    --arg password "$IOT_NANO_SEED_TENANT_PASSWORD" \
    '{tenant_slug: $tenant_slug, password: $password}')" \
  "$management_url/api/v1/tenant/auth/login")"
require_status "$tenant_login_status" 200 'Tenant Account login'

list_users() {
  curl --fail --silent --show-error --cookie "$tenant_cookie" \
    "$management_url/api/v1/management/users"
}

ensure_user() {
  local username="$1"
  local password="$2"
  local users_body="$state_dir/users-${username}.json"
  local user_id
  local match_count
  local create_status

  list_users >"$users_body"
  match_count="$(jq --arg username "$username" '[.[] | select(.username == $username)] | length' "$users_body")"
  if [[ "$match_count" == '1' ]]; then
    jq -r --arg username "$username" '.[] | select(.username == $username) | .id' "$users_body"
    return
  fi
  if [[ "$match_count" != '0' ]]; then
    printf 'Seed user %s is ambiguous.\n' "$username" >&2
    return 1
  fi

  create_status="$(request_status "$state_dir/user-create-${username}.html" \
    --cookie "$tenant_cookie" \
    --request POST \
    --data-urlencode "username=$username" \
    --data-urlencode "password=$password" \
    "$management_url/tenant/users")"
  require_status "$create_status" 303 "User seed ($username)"

  list_users >"$users_body"
  user_id="$(jq -r --arg username "$username" \
    '[.[] | select(.username == $username)] | if length == 1 then .[0].id else empty end' \
    "$users_body")"
  if [[ -z "$user_id" ]]; then
    printf 'Seed user %s could not be resolved.\n' "$username" >&2
    return 1
  fi
  printf '%s' "$user_id"
}

list_assets() {
  curl --fail --silent --show-error --cookie "$tenant_cookie" \
    "$management_url/api/v1/management/assets"
}

powermonitor_app_id='powermonitor'

ensure_powermonitor_application() {
  local application_status
  local application_payload

  application_payload="$(jq -nc --arg powermonitor_url "$powermonitor_url" '{
    app_id: "powermonitor",
    kind: "full_stack",
    launch_url: $powermonitor_url,
    client_id: "powermonitor-client",
    redirect_uris: [($powermonitor_url + "/api/v1/auth/callback")],
    allowed_scopes: [
      "assets:read", "assets:write", "alerts:read", "alerts:write",
      "authorization:read", "authorization:write", "commands:read", "commands:write",
      "devices:read", "devices:write", "telemetry:read"
    ],
    enabled: true
  }')"
  application_status="$(request_status "$state_dir/powermonitor-application.json" \
    --cookie "$tenant_cookie" \
    --header 'Content-Type: application/json' \
    --data "$application_payload" \
    "$management_url/api/v1/management/applications")"
  require_status "$application_status" 201 'PowerMonitor application seed'
}

create_device_profile() {
  local name="$1"
  local telemetry_schema="$2"
  local metric_mapping="$3"
  local reporting_settings="$4"
  local profile_status
  local profile_body="$state_dir/device-profile-${name// /-}.json"

  profile_status="$(request_status "$profile_body" \
    --cookie "$tenant_cookie" \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc \
      --arg name "$name" \
      --argjson telemetry_schema "$telemetry_schema" \
      --argjson metric_mapping "$metric_mapping" \
      --argjson reporting_settings "$reporting_settings" \
      '{name: $name, telemetry_schema: $telemetry_schema, metric_mapping: $metric_mapping, reporting_settings: $reporting_settings}')" \
    "$management_url/api/v1/management/profiles/device-profiles")"
  require_status "$profile_status" 201 "Device profile seed ($name)"
  jq -r '.id' "$profile_body"
}

create_asset_profile() {
  local name="$1"
  local fields="$2"
  local dashboard_defaults="$3"
  local profile_status
  local profile_body="$state_dir/asset-profile-${name// /-}.json"

  profile_status="$(request_status "$profile_body" \
    --cookie "$tenant_cookie" \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc \
      --arg name "$name" \
      --argjson fields "$fields" \
      --argjson dashboard_defaults "$dashboard_defaults" \
      '{name: $name, fields: $fields, dashboard_defaults: $dashboard_defaults}')" \
    "$management_url/api/v1/management/profiles/asset-profiles")"
  require_status "$profile_status" 201 "Asset profile seed ($name)"
  jq -r '.id' "$profile_body"
}

ensure_asset() {
  local name="$1"
  local parent_asset_id="$2"
  local asset_profile_id="$3"
  local assets_body="$state_dir/assets.json"
  local match_count
  local asset_id
  local actual_parent
  local expected_parent
  local create_status

  list_assets >"$assets_body"
  match_count="$(jq --arg name "$name" '[.[] | select(.name == $name)] | length' "$assets_body")"
  if [[ "$match_count" == '1' ]]; then
    asset_id="$(jq -r --arg name "$name" '.[] | select(.name == $name) | .id' "$assets_body")"
    actual_parent="$(jq -r --arg name "$name" '.[] | select(.name == $name) | .parent_asset_id // "null"' "$assets_body")"
    expected_parent="${parent_asset_id:-null}"
    if [[ "$actual_parent" != "$expected_parent" ]]; then
      printf 'Seed asset %s has parent %s; expected %s.\n' "$name" "$actual_parent" "$expected_parent" >&2
      return 1
    fi
    printf '%s' "$asset_id"
    return
  fi
  if [[ "$match_count" != '0' ]]; then
    printf 'Seed asset %s is ambiguous.\n' "$name" >&2
    return 1
  fi

  create_status="$(request_status "$state_dir/asset-create.json" \
    --cookie "$tenant_cookie" \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc --arg name "$name" --arg parent_asset_id "$parent_asset_id" --arg asset_profile_id "$asset_profile_id" \
      '{name: $name, asset_profile_id: $asset_profile_id, parent_asset_id: (if $parent_asset_id == "" then null else $parent_asset_id end), metadata: {local_seed: "owner-sharing-demo"}, attributes: null}')" \
    "$management_url/api/v1/management/assets")"
  require_status "$create_status" 201 "Asset seed ($name)"
  asset_id="$(jq -r '.id' "$state_dir/asset-create.json")"
  if [[ -z "$asset_id" || "$asset_id" == 'null' ]]; then
    printf 'Seed asset %s did not return an id.\n' "$name" >&2
    return 1
  fi
  printf '%s' "$asset_id"
}

list_devices() {
  curl --fail --silent --show-error --cookie "$tenant_cookie" \
    "$management_url/api/v1/management/devices"
}

ensure_device() {
  local name="$1"
  local asset_id="$2"
  local device_profile_id="$3"
  local devices_body="$state_dir/devices.json"
  local match_count
  local device_id
  local serial_number
  local actual_asset_id
  local create_status

  list_devices >"$devices_body"
  match_count="$(jq --arg name "$name" '[.[] | select(.display_name == $name)] | length' "$devices_body")"
  if [[ "$match_count" == '1' ]]; then
    device_id="$(jq -r --arg name "$name" '.[] | select(.display_name == $name) | .device_id' "$devices_body")"
    actual_asset_id="$(jq -r --arg name "$name" '.[] | select(.display_name == $name) | .asset_id // "null"' "$devices_body")"
    if [[ "$actual_asset_id" != "$asset_id" ]]; then
      printf 'Seed device %s belongs to asset %s; expected %s.\n' "$name" "$actual_asset_id" "$asset_id" >&2
      return 1
    fi
    printf '%s' "$device_id"
    return
  fi
  if [[ "$match_count" != '0' ]]; then
    printf 'Seed device %s is ambiguous.\n' "$name" >&2
    return 1
  fi

  serial_number="SEED-$(printf '%s' "$name" | tr '[:lower:]' '[:upper:]' | tr -cs 'A-Z0-9' '-')"
  serial_number="${serial_number%-}"

  create_status="$(request_status "$state_dir/device-create.json" \
    --cookie "$tenant_cookie" \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc --arg serial_number "$serial_number" --arg name "$name" --arg asset_id "$asset_id" --arg device_profile_id "$device_profile_id" \
      '{serial_number: $serial_number, display_name: $name, asset_id: $asset_id, device_profile_id: $device_profile_id, attributes: {local_seed: "owner-sharing-demo"}}')" \
    "$management_url/api/v1/management/devices")"
  require_status "$create_status" 201 "Device seed ($name)"
  device_id="$(jq -r '.device_id' "$state_dir/device-create.json")"
  if [[ -z "$device_id" || "$device_id" == 'null' ]]; then
    printf 'Seed device %s did not return an id.\n' "$name" >&2
    return 1
  fi
  printf '%s' "$device_id"
}

ensure_high_power_alert_rule() {
  local device_id="$1"
  local rules_body="$state_dir/alert-rules.json"
  local match_count
  local rule_status

  curl --fail --silent --show-error --cookie "$tenant_cookie" \
    "$management_url/api/v1/management/alert-rules" >"$rules_body"
  match_count="$(jq --arg device_id "$device_id" \
    '[.[] | select(.device_id == $device_id and .name == "High active power")] | length' \
    "$rules_body")"
  if [[ "$match_count" == '1' ]]; then
    return
  fi
  if [[ "$match_count" != '0' ]]; then
    printf 'Seed alert rule for device %s is ambiguous.\n' "$device_id" >&2
    return 1
  fi

  rule_status="$(request_status "$state_dir/alert-rule-${device_id}.json" \
    --cookie "$tenant_cookie" \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc --arg device_id "$device_id" '{
      name: "High active power",
      enabled: true,
      device_id: $device_id,
      metric_key: "power_w",
      rule_type: "event_threshold",
      comparison: "gt",
      threshold: 500,
      window_seconds: null,
      for_seconds: 0,
      resolve_after_seconds: 300,
      reopen_grace_seconds: 3600,
      hysteresis: null,
      severity: "warning",
      reminder_interval_seconds: 86400
    }')" \
    "$management_url/api/v1/management/alert-rules")"
  require_status "$rule_status" 201 "High-power alert rule seed ($device_id)"
}

assign_asset_owner() {
  local asset_id="$1"
  local owner_user_id="$2"
  local assets_body="$state_dir/assets-owner.json"
  local current_owner
  local owner_status

  list_assets >"$assets_body"
  current_owner="$(jq -r --arg asset_id "$asset_id" '.[] | select(.id == $asset_id) | .owner_user_id // "null"' "$assets_body")"
  if [[ "$current_owner" == "$owner_user_id" ]]; then
    return
  fi
  owner_status="$(request_status "$state_dir/asset-owner.json" \
    --cookie "$tenant_cookie" \
    --request PUT \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc --arg user_id "$owner_user_id" '{user_id: $user_id}')" \
    "$management_url/api/v1/management/assets/$asset_id/owner")"
  require_status "$owner_status" 204 "Asset owner assignment ($asset_id)"
}

assign_device_owner() {
  local device_id="$1"
  local owner_user_id="$2"
  local devices_body="$state_dir/devices-owner.json"
  local current_owner
  local owner_status

  list_devices >"$devices_body"
  current_owner="$(jq -r --arg device_id "$device_id" '.[] | select(.device_id == $device_id) | .owner_user_id // "null"' "$devices_body")"
  if [[ "$current_owner" == "$owner_user_id" ]]; then
    return
  fi
  owner_status="$(request_status "$state_dir/device-owner.json" \
    --cookie "$tenant_cookie" \
    --request PUT \
    --header 'Content-Type: application/json' \
    --data "$(jq -nc --arg user_id "$owner_user_id" '{user_id: $user_id}')" \
    "$management_url/api/v1/management/devices/$device_id/owner")"
  require_status "$owner_status" 204 "Device owner assignment ($device_id)"
}

login_owner() {
  local owner_login_status
  owner_login_status="$(request_status "$state_dir/owner-login.html" \
    --cookie-jar "$owner_cookie" \
    --data-urlencode "username=$IOT_NANO_SEED_OWNER_USERNAME" \
    --data-urlencode "password=$IOT_NANO_SEED_OWNER_PASSWORD" \
    "$management_url/login")"
  require_status "$owner_login_status" 303 'Seed owner login'
}

ensure_direct_share() {
  local username="$1"
  local permission="$2"
  local resource_kind="$3"
  local resource_id="$4"
  local detail_path
  local page_body
  local permission_label
  local share_status

  case "$permission" in
    view) permission_label='View' ;;
    control) permission_label='Control' ;;
    *) printf 'Unknown seed share permission: %s\n' "$permission" >&2; return 1 ;;
  esac

  case "$resource_kind" in
    asset) detail_path="/app/assets/$resource_id" ;;
    device) detail_path="/app/devices/$resource_id" ;;
    *) printf 'Unknown seed share resource type: %s\n' "$resource_kind" >&2; return 1 ;;
  esac

  page_body="$state_dir/${resource_kind}-share-page.html"
  curl --fail --silent --show-error --cookie "$owner_cookie" \
    "$management_url$detail_path" >"$page_body"
  if grep -Fq "<td>${username}</td><td><span class=\"status-chip\">${permission_label}</span>" "$page_body"; then
    return
  fi
  if grep -Fq "<td>${username}</td>" "$page_body"; then
    printf 'Seed user %s already has a different share on %s %s.\n' \
      "$username" "$resource_kind" "$resource_id" >&2
    return 1
  fi
  share_status="$(request_status "$state_dir/${resource_kind}-share-result.html" \
    --cookie "$owner_cookie" \
    --request POST \
    --data-urlencode "username=$username" \
    --data-urlencode "permission=$permission" \
    "$management_url$detail_path/permissions")"
  require_status "$share_status" 303 "Direct $permission share ($resource_kind $resource_id)"
}

ensure_powermonitor_application
owner_user_id="$(ensure_user "$IOT_NANO_SEED_OWNER_USERNAME" "$IOT_NANO_SEED_OWNER_PASSWORD")"
ensure_user "$IOT_NANO_SEED_CONTROLLER_USERNAME" "$IOT_NANO_SEED_CONTROLLER_PASSWORD" >/dev/null
ensure_user "$IOT_NANO_SEED_VIEWER_USERNAME" "$IOT_NANO_SEED_VIEWER_PASSWORD" >/dev/null
ensure_user "$IOT_NANO_SEED_UNASSIGNED_USERNAME" "$IOT_NANO_SEED_UNASSIGNED_PASSWORD" >/dev/null

power_meter_profile_id="$(create_device_profile \
  'Power Meter' \
  '{"power_w":{"type":"number","unit":"W"},"voltage_v":{"type":"number","unit":"V"},"current_a":{"type":"number","unit":"A"},"energy_kwh":{"type":"number","unit":"kWh"}}' \
  '{"power_w":"Active power","voltage_v":"Voltage","current_a":"Current","energy_kwh":"Energy"}' \
  '{"interval_seconds":60}')"
power_farm_profile_id="$(create_asset_profile \
  'Power Farm' \
  '{"location":{"type":"string"},"capacity_kw":{"type":"number"}}' \
  '{"primary_metric":"power_w","aggregation":"sum"}')"
power_zone_profile_id="$(create_asset_profile \
  'Power Zone' \
  '{"location":{"type":"string"}}' \
  '{"primary_metric":"power_w","aggregation":"sum"}')"

farm_1_id="$(ensure_asset 'Power Farm 1' '' "$power_farm_profile_id")"
farm_1_zone_1_id="$(ensure_asset 'Farm 1 / Zone 1' "$farm_1_id" "$power_zone_profile_id")"
farm_1_zone_2_id="$(ensure_asset 'Farm 1 / Zone 2' "$farm_1_id" "$power_zone_profile_id")"
farm_2_id="$(ensure_asset 'Power Farm 2' '' "$power_farm_profile_id")"
farm_2_zone_1_id="$(ensure_asset 'Farm 2 / Zone 1' "$farm_2_id" "$power_zone_profile_id")"
farm_2_zone_2_id="$(ensure_asset 'Farm 2 / Zone 2' "$farm_2_id" "$power_zone_profile_id")"

farm_1_zone_1_device_1_id="$(ensure_device 'Farm 1 / Zone 1 / Device 1' "$farm_1_zone_1_id" "$power_meter_profile_id")"
farm_1_zone_1_device_2_id="$(ensure_device 'Farm 1 / Zone 1 / Device 2' "$farm_1_zone_1_id" "$power_meter_profile_id")"
farm_1_zone_2_device_1_id="$(ensure_device 'Farm 1 / Zone 2 / Device 1' "$farm_1_zone_2_id" "$power_meter_profile_id")"
farm_1_zone_2_device_2_id="$(ensure_device 'Farm 1 / Zone 2 / Device 2' "$farm_1_zone_2_id" "$power_meter_profile_id")"
farm_2_zone_1_device_1_id="$(ensure_device 'Farm 2 / Zone 1 / Device 1' "$farm_2_zone_1_id" "$power_meter_profile_id")"
farm_2_zone_1_device_2_id="$(ensure_device 'Farm 2 / Zone 1 / Device 2' "$farm_2_zone_1_id" "$power_meter_profile_id")"
farm_2_zone_2_device_1_id="$(ensure_device 'Farm 2 / Zone 2 / Device 1' "$farm_2_zone_2_id" "$power_meter_profile_id")"
farm_2_zone_2_device_2_id="$(ensure_device 'Farm 2 / Zone 2 / Device 2' "$farm_2_zone_2_id" "$power_meter_profile_id")"

for asset_id in \
  "$farm_1_id" "$farm_1_zone_1_id" "$farm_1_zone_2_id" \
  "$farm_2_id" "$farm_2_zone_1_id" "$farm_2_zone_2_id"; do
  assign_asset_owner "$asset_id" "$owner_user_id"
done
for device_id in \
  "$farm_1_zone_1_device_1_id" "$farm_1_zone_1_device_2_id" \
  "$farm_1_zone_2_device_1_id" "$farm_1_zone_2_device_2_id" \
  "$farm_2_zone_1_device_1_id" "$farm_2_zone_1_device_2_id" \
  "$farm_2_zone_2_device_1_id" "$farm_2_zone_2_device_2_id"; do
  assign_device_owner "$device_id" "$owner_user_id"
  ensure_high_power_alert_rule "$device_id"
done

login_owner
ensure_direct_share "$IOT_NANO_SEED_CONTROLLER_USERNAME" control asset "$farm_1_zone_2_id"
ensure_direct_share "$IOT_NANO_SEED_CONTROLLER_USERNAME" control device "$farm_1_zone_2_device_1_id"
ensure_direct_share "$IOT_NANO_SEED_VIEWER_USERNAME" view asset "$farm_1_zone_1_id"
ensure_direct_share "$IOT_NANO_SEED_VIEWER_USERNAME" view device "$farm_2_zone_2_device_2_id"

printf '%s\n' 'Local platform PowerMonitor seed is ready.'
printf 'Owner: %s\n' "$IOT_NANO_SEED_OWNER_USERNAME"
printf 'Controller: %s; control shares: Farm 1 / Zone 2 asset and Device 1.\n' \
  "$IOT_NANO_SEED_CONTROLLER_USERNAME"
printf 'Viewer: %s; view shares: Farm 1 / Zone 1 asset and Farm 2 / Zone 2 / Device 2.\n' \
  "$IOT_NANO_SEED_VIEWER_USERNAME"
printf 'Unassigned: %s; no seeded resources.\n' "$IOT_NANO_SEED_UNASSIGNED_USERNAME"
printf '%s\n' 'Profiles: Power Farm, Power Zone, and Power Meter.'

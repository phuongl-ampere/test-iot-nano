export type TimeRange = "1h" | "24h" | "7d";
export type Role = "admin" | "viewer";
export type AccountClass = "system" | "admin" | "user";
export type ResourcePermission = "viewer" | "controller" | "manager" | "owner";
export type RpcMode = "one_way" | "two_way";
export type DeviceCommandMethod =
  | "sample_now"
  | "reboot"
  | "switch_on"
  | "switch_off"
  | "set_power"
  | "set_brightness";

export interface ApiClient {
  apiBaseUrl: string;
  sessionId: string;
}

export interface UserSession {
  role: Role;
  accountClass: AccountClass;
  username: string;
  defaultApp: string;
  grantedApps: string[];
}

export interface DeviceSummary {
  device_id: string;
  display_name: string | null;
  online: boolean;
  last_seen_at: string | null;
}

export interface DeviceToken {
  id: string;
  device_id: string;
  token_prefix: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
  token?: string;
}

export interface DeviceCommandLifecycle {
  id: string;
  state: "queued" | "published_to_broker" | "responded" | "expired" | "failed";
  mode: RpcMode;
  expires_at: string;
  response: Record<string, unknown> | null;
  responded_at: string | null;
}

export interface TelemetryPoint {
  at: string;
  temperature_c: number | null;
  humidity_pct: number | null;
  event_count: number;
}

export interface PowerSummary {
  device_count: number;
  online_device_count: number;
  asset_count: number;
  total_power_w: number;
  total_energy_kwh: number;
}

export interface PowerAsset {
  id: string;
  name: string;
  permission: ResourcePermission;
  asset_profile_id: string | null;
  parent_asset_id: string | null;
  metadata: Record<string, unknown>;
  device_count: number;
  total_power_w: number;
  total_energy_kwh: number;
}

export interface PowerDevice {
  device_id: string;
  display_name: string | null;
  permission: ResourcePermission;
  asset_id: string | null;
  device_profile_id?: string | null;
  device_profile_name?: string | null;
  online: boolean;
  last_seen_at: string | null;
  is_gateway?: boolean;
  gateway_device_id?: string | null;
  gateway_status?: "online" | "offline" | null;
  child_status?: "fresh" | "stale" | "unavailable" | null;
  voltage_v: number | null;
  current_a: number | null;
  power_w: number | null;
  energy_kwh: number | null;
  frequency_hz: number | null;
  power_factor: number | null;
  switch_state?: boolean | null;
  brightness_pct?: number | null;
}

export interface PowerTelemetryPoint {
  at: string;
  voltage_v: number | null;
  current_a: number | null;
  power_w: number | null;
  energy_kwh: number | null;
  frequency_hz: number | null;
  power_factor: number | null;
  event_count: number;
}

export interface PowerTelemetryRecord {
  at: string;
  measurements: Record<string, unknown>;
}

export interface ManagementDevice {
  device_id: string;
  display_name: string | null;
  asset_id: string | null;
  device_profile_id: string | null;
  attributes: Record<string, unknown>;
  online: boolean;
  last_seen_at: string | null;
  is_gateway?: boolean;
  gateway_device_id?: string | null;
  gateway_status?: "online" | "offline" | null;
  child_status?: "fresh" | "stale" | "unavailable" | null;
}

export interface ManagementAsset {
  id: string;
  name: string;
  asset_profile_id: string | null;
  parent_asset_id: string | null;
  metadata: Record<string, unknown>;
  attributes: Record<string, unknown>;
}

export interface ManagementUser {
  id: string;
  username: string;
  role: Role;
  account_class: AccountClass;
  default_app: string;
  granted_apps: string[];
}

export interface ManagementDeviceProfile {
  id: string;
  name: string;
  telemetry_schema: Record<string, unknown>;
  metric_mapping: Record<string, unknown>;
  reporting_settings: Record<string, unknown>;
}

export interface ManagementAssetProfile {
  id: string;
  name: string;
  fields: Record<string, unknown>;
  dashboard_defaults: Record<string, unknown>;
}

export type AlertRuleType = "event_threshold" | "window_average";
export type AlertComparison = "gt" | "gte" | "lt" | "lte";
export type AlertSeverity = "info" | "warning" | "critical";

export interface AlertRule {
  id: string;
  name: string;
  enabled: boolean;
  device_id: string | null;
  metric_key: string;
  rule_type: AlertRuleType;
  comparison: AlertComparison;
  threshold: number;
  window_seconds: number | null;
  for_seconds: number;
  resolve_after_seconds: number;
  reopen_grace_seconds: number;
  hysteresis: number | null;
  severity: AlertSeverity;
  reminder_interval_seconds: number;
  created_at: string;
  updated_at: string;
}

export interface CreateAlertRule {
  name: string;
  device_id?: string;
  metric_key: string;
  rule_type: AlertRuleType;
  comparison: AlertComparison;
  threshold: number;
  window_seconds?: number;
  for_seconds?: number;
  resolve_after_seconds?: number;
  reopen_grace_seconds?: number;
  hysteresis?: number;
  severity?: AlertSeverity;
  reminder_interval_seconds?: number;
}

export interface AlertIncident {
  id: string;
  rule_id: string;
  rule_name: string;
  severity: AlertSeverity;
  device_id: string;
  status: "pending" | "open" | "resolved";
  condition_started_at: string;
  opened_at: string | null;
  resolved_at: string | null;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
  last_value: number | null;
  updated_at: string;
}

export interface SystemConfiguration {
  mqtt: MqttConfiguration;
  smtp: SmtpConfiguration;
  tuning: IngestTuning;
}

export interface MqttConfiguration {
  host: string;
  port: number;
}

export interface MqttConfigurationUpdate {
  host: string;
  port: number;
}

export interface SmtpConfiguration {
  enabled: boolean;
  host: string | null;
  port: number;
  username: string | null;
  password_configured: boolean;
  from: string | null;
  to: string | null;
  timeout_seconds: number;
}

export interface SmtpConfigurationUpdate {
  enabled: boolean;
  host: string | null;
  port: number;
  username: string | null;
  password?: string;
  from: string | null;
  to: string | null;
  timeout_seconds: number;
}

export interface IngestTuning {
  retention_bytes: number;
  retention_seconds: number;
  segment_bytes: number;
  max_record_bytes: number;
  writer_batch_size: number;
  alert_batch_size: number;
  notification_batch_size: number;
  writer_flush_seconds: number;
  alert_event_interval_milliseconds: number;
  alert_window_interval_seconds: number;
  notification_interval_seconds: number;
  retention_interval_seconds: number;
  notification_lease_seconds: number;
  notification_retry_base_seconds: number;
  notification_retry_max_seconds: number;
}

export interface SystemConfigurationUpdate {
  mqtt: MqttConfigurationUpdate;
  smtp: SmtpConfigurationUpdate;
  tuning: IngestTuning;
}

export class UnauthorizedApiError extends Error {
  constructor() {
    super("Session is invalid or expired.");
    this.name = "UnauthorizedApiError";
  }
}

const rangeConfiguration: Record<TimeRange, { milliseconds: number; bucket: string }> = {
  "1h": { milliseconds: 60 * 60 * 1_000, bucket: "raw" },
  "24h": { milliseconds: 24 * 60 * 60 * 1_000, bucket: "5m" },
  "7d": { milliseconds: 7 * 24 * 60 * 60 * 1_000, bucket: "1h" },
};

export function createApiClient(apiBaseUrl: string, sessionId: string): ApiClient {
  return { apiBaseUrl, sessionId };
}

function baseUrl(apiBaseUrl: string): string {
  return apiBaseUrl.replace(/\/$/, "");
}

function authenticatedHeaders(client: ApiClient, json = false): Record<string, string> {
  return {
    authorization: `Session ${client.sessionId}`,
    ...(json ? { "content-type": "application/json" } : {}),
  };
}

async function authenticatedRequest(
  client: ApiClient,
  path: string,
  options: { body?: string; method?: string } = {},
): Promise<Response> {
  const response = await fetch(`${baseUrl(client.apiBaseUrl)}${path}`, {
    cache: "no-store",
    method: options.method,
    headers: authenticatedHeaders(client, options.body !== undefined),
    body: options.body,
  });
  if (response.status === 401) {
    throw new UnauthorizedApiError();
  }
  return response;
}

function assertOk(response: Response, message: string): void {
  if (!response.ok) {
    throw new Error(`${message} with status ${response.status}`);
  }
}

export async function login(
  apiBaseUrl: string,
  username: string,
  password: string,
): Promise<UserSession & { sessionId: string }> {
  const response = await fetch(`${baseUrl(apiBaseUrl)}/api/auth/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  assertOk(response, "Login failed");
  const body = await response.json() as {
    role: Role;
    account_class?: AccountClass;
    session_id: string;
    username: string;
    default_app?: string;
    granted_apps?: string[];
  };
  return {
    role: body.role,
    accountClass: body.account_class ?? (body.role === "admin" ? "admin" : "user"),
    sessionId: body.session_id,
    username: body.username,
    defaultApp: body.default_app ?? "/apps/powermonitor",
    grantedApps: body.granted_apps ?? [],
  };
}

export async function getCurrentRole(client: ApiClient): Promise<Role> {
  return (await getCurrentUser(client)).role;
}

export async function getCurrentUser(client: ApiClient): Promise<UserSession> {
  const response = await authenticatedRequest(client, "/api/auth/me");
  assertOk(response, "Profile request failed");
  const body = await response.json() as {
    role: Role;
    account_class?: AccountClass;
    username: string;
    default_app?: string;
    granted_apps?: string[];
  };
  return {
    role: body.role,
    accountClass: body.account_class ?? (body.role === "admin" ? "admin" : "user"),
    username: body.username,
    defaultApp: body.default_app ?? "/apps/powermonitor",
    grantedApps: body.granted_apps ?? [],
  };
}

export async function logout(client: ApiClient): Promise<void> {
  const response = await authenticatedRequest(client, "/api/auth/logout", { method: "POST" });
  assertOk(response, "Logout failed");
}

export async function changePassword(
  client: ApiClient,
  currentPassword: string,
  newPassword: string,
): Promise<void> {
  const response = await authenticatedRequest(client, "/api/auth/password", {
    method: "PUT",
    body: JSON.stringify({
      current_password: currentPassword,
      new_password: newPassword,
    }),
  });
  assertOk(response, "Password update failed");
}

export async function fetchSystemConfiguration(client: ApiClient): Promise<SystemConfiguration> {
  const response = await authenticatedRequest(client, "/api/system-configuration");
  assertOk(response, "System configuration request failed");
  return response.json() as Promise<SystemConfiguration>;
}

export async function updateSystemConfiguration(
  client: ApiClient,
  configuration: SystemConfigurationUpdate,
): Promise<SystemConfiguration> {
  const response = await authenticatedRequest(client, "/api/system-configuration", {
    method: "PUT",
    body: JSON.stringify(configuration),
  });
  assertOk(response, "System configuration update failed");
  return response.json() as Promise<SystemConfiguration>;
}

export function telemetryRequest(
  apiBaseUrl: string,
  deviceId: string,
  range: TimeRange,
  now = new Date(),
): string {
  const configuration = rangeConfiguration[range];
  const from = new Date(now.getTime() - configuration.milliseconds);
  const parameters = new URLSearchParams({
    from: from.toISOString(),
    to: now.toISOString(),
    bucket: configuration.bucket,
  });

  return `${baseUrl(apiBaseUrl)}/api/devices/${encodeURIComponent(deviceId)}/telemetry?${parameters.toString()}`;
}

export async function fetchDevices(client: ApiClient): Promise<DeviceSummary[]> {
  const response = await authenticatedRequest(client, "/api/devices");
  assertOk(response, "Device request failed");
  return response.json() as Promise<DeviceSummary[]>;
}

export async function fetchDeviceTokens(
  client: ApiClient,
  deviceId: string,
): Promise<DeviceToken[]> {
  const response = await authenticatedRequest(
    client,
    `/api/devices/${encodeURIComponent(deviceId)}/tokens`,
  );
  assertOk(response, "Device token request failed");
  return response.json() as Promise<DeviceToken[]>;
}

export async function createDeviceToken(
  client: ApiClient,
  deviceId: string,
): Promise<DeviceToken> {
  const response = await authenticatedRequest(
    client,
    `/api/devices/${encodeURIComponent(deviceId)}/tokens`,
    { method: "POST" },
  );
  assertOk(response, "Device token creation failed");
  return response.json() as Promise<DeviceToken>;
}

export async function provisionDeviceToken(
  client: ApiClient,
  displayName: string,
): Promise<DeviceToken> {
  const response = await authenticatedRequest(client, "/api/device-tokens", {
    method: "POST",
    body: JSON.stringify({ display_name: displayName }),
  });
  assertOk(response, "Device token provisioning failed");
  return response.json() as Promise<DeviceToken>;
}

export async function rotateDeviceToken(client: ApiClient, id: string): Promise<DeviceToken> {
  const response = await authenticatedRequest(
    client,
    `/api/device-tokens/${encodeURIComponent(id)}/rotate`,
    { method: "POST" },
  );
  assertOk(response, "Device token rotation failed");
  return response.json() as Promise<DeviceToken>;
}

export async function revokeDeviceToken(client: ApiClient, id: string): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/device-tokens/${encodeURIComponent(id)}/revoke`,
    { method: "POST" },
  );
  assertOk(response, "Device token revocation failed");
}

export async function fetchTelemetry(
  client: ApiClient,
  deviceId: string,
  range: TimeRange,
): Promise<TelemetryPoint[]> {
  const response = await authenticatedRequest(
    client,
    telemetryRequest(client.apiBaseUrl, deviceId, range).replace(baseUrl(client.apiBaseUrl), ""),
  );
  assertOk(response, "Telemetry request failed");
  return response.json() as Promise<TelemetryPoint[]>;
}

function powerTelemetryPath(
  resourcePath: string,
  range: TimeRange,
  now = new Date(),
): string {
  const configuration = rangeConfiguration[range];
  const from = new Date(now.getTime() - configuration.milliseconds);
  return `${resourcePath}?${new URLSearchParams({
    from: from.toISOString(),
    to: now.toISOString(),
    bucket: configuration.bucket,
  }).toString()}`;
}

export async function fetchPowerSummary(client: ApiClient): Promise<PowerSummary> {
  const response = await authenticatedRequest(client, "/api/apps/powermonitor/summary");
  assertOk(response, "Power Monitor summary request failed");
  return response.json() as Promise<PowerSummary>;
}

export async function fetchPowerAssets(client: ApiClient): Promise<PowerAsset[]> {
  const response = await authenticatedRequest(client, "/api/apps/powermonitor/assets");
  assertOk(response, "Power Monitor assets request failed");
  return response.json() as Promise<PowerAsset[]>;
}

export async function fetchPowerDevices(client: ApiClient): Promise<PowerDevice[]> {
  const response = await authenticatedRequest(client, "/api/apps/powermonitor/devices");
  assertOk(response, "Power Monitor devices request failed");
  return response.json() as Promise<PowerDevice[]>;
}

export async function fetchPowerDeviceTelemetry(
  client: ApiClient,
  deviceId: string,
  range: TimeRange,
): Promise<PowerTelemetryPoint[]> {
  const path = powerTelemetryPath(
    `/api/apps/powermonitor/devices/${encodeURIComponent(deviceId)}/telemetry`,
    range,
  );
  const response = await authenticatedRequest(client, path);
  assertOk(response, "Power Monitor telemetry request failed");
  return response.json() as Promise<PowerTelemetryPoint[]>;
}

export async function fetchPowerDeviceTelemetryRecords(
  client: ApiClient,
  deviceId: string,
  range: TimeRange,
): Promise<PowerTelemetryRecord[]> {
  const path = powerTelemetryPath(
    `/api/apps/powermonitor/devices/${encodeURIComponent(deviceId)}/telemetry/records`,
    range,
  );
  const response = await authenticatedRequest(client, path);
  assertOk(response, "Power Monitor telemetry records request failed");
  return response.json() as Promise<PowerTelemetryRecord[]>;
}

export async function fetchPowerAssetTelemetry(
  client: ApiClient,
  assetId: string,
  range: TimeRange,
): Promise<PowerTelemetryPoint[]> {
  const path = powerTelemetryPath(
    `/api/apps/powermonitor/assets/${encodeURIComponent(assetId)}/telemetry`,
    range,
  );
  const response = await authenticatedRequest(client, path);
  assertOk(response, "Power Monitor asset telemetry request failed");
  return response.json() as Promise<PowerTelemetryPoint[]>;
}

export async function fetchManagementDevices(client: ApiClient): Promise<ManagementDevice[]> {
  const response = await authenticatedRequest(client, "/api/management/devices");
  assertOk(response, "Device management request failed");
  return response.json() as Promise<ManagementDevice[]>;
}

export async function provisionManagementDevice(
  client: ApiClient,
  displayName: string,
): Promise<DeviceToken> {
  const response = await authenticatedRequest(client, "/api/management/devices", {
    method: "POST",
    body: JSON.stringify({ display_name: displayName }),
  });
  assertOk(response, "Device provisioning failed");
  return response.json() as Promise<DeviceToken>;
}

export async function updateManagementDevice(
  client: ApiClient,
  deviceId: string,
  update: Pick<ManagementDevice, "display_name" | "asset_id" | "device_profile_id" | "attributes"> & {
    topology?: {
      is_gateway: boolean;
      gateway_device_id: string | null;
    };
  },
): Promise<ManagementDevice> {
  const response = await authenticatedRequest(
    client,
    `/api/management/devices/${encodeURIComponent(deviceId)}`,
    {
      method: "PUT",
      body: JSON.stringify(update),
    },
  );
  assertOk(response, "Device update failed");
  return response.json() as Promise<ManagementDevice>;
}

export async function deleteManagementDevice(
  client: ApiClient,
  deviceId: string,
): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/management/devices/${encodeURIComponent(deviceId)}`,
    { method: "DELETE" },
  );
  assertOk(response, "Device deletion failed");
}

export async function fetchManagementAssets(client: ApiClient): Promise<ManagementAsset[]> {
  const response = await authenticatedRequest(client, "/api/management/assets");
  assertOk(response, "Asset management request failed");
  return response.json() as Promise<ManagementAsset[]>;
}

export async function createManagementAsset(
  client: ApiClient,
  asset: Omit<ManagementAsset, "id">,
): Promise<ManagementAsset> {
  const response = await authenticatedRequest(client, "/api/management/assets", {
    method: "POST",
    body: JSON.stringify(asset),
  });
  assertOk(response, "Asset creation failed");
  return response.json() as Promise<ManagementAsset>;
}

export async function createMyAsset(
  client: ApiClient,
  asset: Pick<ManagementAsset, "name" | "asset_profile_id" | "parent_asset_id" | "metadata" | "attributes">,
): Promise<ManagementAsset> {
  const response = await authenticatedRequest(client, "/api/my/assets", {
    method: "POST",
    body: JSON.stringify(asset),
  });
  assertOk(response, "Asset creation failed");
  return response.json() as Promise<ManagementAsset>;
}

export async function provisionMyDevice(
  client: ApiClient,
  displayName: string,
  assetId: string | null,
): Promise<DeviceToken> {
  const response = await authenticatedRequest(client, "/api/my/devices", {
    method: "POST",
    body: JSON.stringify({ display_name: displayName, asset_id: assetId }),
  });
  assertOk(response, "Device provisioning failed");
  return response.json() as Promise<DeviceToken>;
}

export async function assignMyDeviceAsset(
  client: ApiClient,
  deviceId: string,
  assetId: string | null,
): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/my/devices/${encodeURIComponent(deviceId)}/asset`,
    { method: "PUT", body: JSON.stringify({ asset_id: assetId }) },
  );
  assertOk(response, "Device assignment failed");
}

export async function updateManagementAsset(
  client: ApiClient,
  id: string,
  asset: Omit<ManagementAsset, "id">,
): Promise<ManagementAsset> {
  const response = await authenticatedRequest(
    client,
    `/api/management/assets/${encodeURIComponent(id)}`,
    { method: "PUT", body: JSON.stringify(asset) },
  );
  assertOk(response, "Asset update failed");
  return response.json() as Promise<ManagementAsset>;
}

export async function deleteManagementAsset(client: ApiClient, id: string): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/management/assets/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
  assertOk(response, "Asset deletion failed");
}

export async function fetchManagementUsers(client: ApiClient): Promise<ManagementUser[]> {
  const response = await authenticatedRequest(client, "/api/management/users");
  assertOk(response, "User management request failed");
  return response.json() as Promise<ManagementUser[]>;
}

export async function updateManagementUser(
  client: ApiClient,
  username: string,
  update: Pick<ManagementUser, "default_app" | "granted_apps">,
): Promise<ManagementUser> {
  const response = await authenticatedRequest(
    client,
    `/api/management/users/${encodeURIComponent(username)}`,
    {
      method: "PUT",
      body: JSON.stringify(update),
    },
  );
  assertOk(response, "User update failed");
  return response.json() as Promise<ManagementUser>;
}

export async function fetchManagementDeviceProfiles(
  client: ApiClient,
): Promise<ManagementDeviceProfile[]> {
  const response = await authenticatedRequest(client, "/api/management/profiles/device-profiles");
  assertOk(response, "Device profile request failed");
  return response.json() as Promise<ManagementDeviceProfile[]>;
}

export async function createManagementDeviceProfile(
  client: ApiClient,
  profile: Omit<ManagementDeviceProfile, "id">,
): Promise<ManagementDeviceProfile> {
  const response = await authenticatedRequest(client, "/api/management/profiles/device-profiles", {
    method: "POST",
    body: JSON.stringify(profile),
  });
  assertOk(response, "Device profile creation failed");
  return response.json() as Promise<ManagementDeviceProfile>;
}

export async function updateManagementDeviceProfile(
  client: ApiClient,
  id: string,
  profile: Omit<ManagementDeviceProfile, "id">,
): Promise<ManagementDeviceProfile> {
  const response = await authenticatedRequest(
    client,
    `/api/management/profiles/device-profiles/${encodeURIComponent(id)}`,
    { method: "PUT", body: JSON.stringify(profile) },
  );
  assertOk(response, "Device profile update failed");
  return response.json() as Promise<ManagementDeviceProfile>;
}

export async function deleteManagementDeviceProfile(
  client: ApiClient,
  id: string,
): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/management/profiles/device-profiles/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
  assertOk(response, "Device profile deletion failed");
}

export async function fetchManagementAssetProfiles(
  client: ApiClient,
): Promise<ManagementAssetProfile[]> {
  const response = await authenticatedRequest(client, "/api/management/profiles/asset-profiles");
  assertOk(response, "Asset profile request failed");
  return response.json() as Promise<ManagementAssetProfile[]>;
}

export async function createManagementAssetProfile(
  client: ApiClient,
  profile: Omit<ManagementAssetProfile, "id">,
): Promise<ManagementAssetProfile> {
  const response = await authenticatedRequest(client, "/api/management/profiles/asset-profiles", {
    method: "POST",
    body: JSON.stringify(profile),
  });
  assertOk(response, "Asset profile creation failed");
  return response.json() as Promise<ManagementAssetProfile>;
}

export async function updateManagementAssetProfile(
  client: ApiClient,
  id: string,
  profile: Omit<ManagementAssetProfile, "id">,
): Promise<ManagementAssetProfile> {
  const response = await authenticatedRequest(
    client,
    `/api/management/profiles/asset-profiles/${encodeURIComponent(id)}`,
    { method: "PUT", body: JSON.stringify(profile) },
  );
  assertOk(response, "Asset profile update failed");
  return response.json() as Promise<ManagementAssetProfile>;
}

export async function deleteManagementAssetProfile(
  client: ApiClient,
  id: string,
): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/management/profiles/asset-profiles/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
  assertOk(response, "Asset profile deletion failed");
}

export async function sendDeviceCommand(
  client: ApiClient,
  deviceId: string,
  command: DeviceCommandMethod,
  params: Record<string, unknown> = {},
  mode: RpcMode = "one_way",
): Promise<DeviceCommandLifecycle> {
  const response = await authenticatedRequest(
    client,
    `/api/devices/${encodeURIComponent(deviceId)}/commands`,
    {
      method: "POST",
      body: JSON.stringify({ method: command, params, mode }),
    },
  );
  assertOk(response, "Command request failed");
  return response.json() as Promise<DeviceCommandLifecycle>;
}

export async function fetchDeviceCommand(
  client: ApiClient,
  commandId: string,
): Promise<DeviceCommandLifecycle> {
  const response = await authenticatedRequest(
    client,
    `/api/device-commands/${encodeURIComponent(commandId)}`,
  );
  assertOk(response, "Command lifecycle request failed");
  return response.json() as Promise<DeviceCommandLifecycle>;
}

export async function fetchAlertRules(client: ApiClient): Promise<AlertRule[]> {
  const response = await authenticatedRequest(client, "/api/alert-rules");
  assertOk(response, "Alert rule request failed");
  return response.json() as Promise<AlertRule[]>;
}

export async function createAlertRule(
  client: ApiClient,
  rule: CreateAlertRule,
): Promise<AlertRule> {
  const response = await authenticatedRequest(client, "/api/alert-rules", {
    method: "POST",
    body: JSON.stringify(rule),
  });
  assertOk(response, "Alert rule creation failed");
  return response.json() as Promise<AlertRule>;
}

export async function updateAlertRule(
  client: ApiClient,
  id: string,
  rule: CreateAlertRule,
): Promise<AlertRule> {
  const response = await authenticatedRequest(
    client,
    `/api/alert-rules/${encodeURIComponent(id)}`,
    {
      method: "PUT",
      body: JSON.stringify(rule),
    },
  );
  assertOk(response, "Alert rule update failed");
  return response.json() as Promise<AlertRule>;
}

export async function archiveAlertRule(client: ApiClient, id: string): Promise<void> {
  const response = await authenticatedRequest(
    client,
    `/api/alert-rules/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
  assertOk(response, "Alert rule archive failed");
}

export async function toggleAlertRule(
  client: ApiClient,
  id: string,
  enabled: boolean,
): Promise<AlertRule> {
  const response = await authenticatedRequest(
    client,
    `/api/alert-rules/${encodeURIComponent(id)}/toggle`,
    {
      method: "POST",
      body: JSON.stringify({ enabled }),
    },
  );
  assertOk(response, "Alert rule update failed");
  return response.json() as Promise<AlertRule>;
}

export async function fetchAlertIncidents(client: ApiClient): Promise<AlertIncident[]> {
  const response = await authenticatedRequest(client, "/api/alert-incidents");
  assertOk(response, "Alert incident request failed");
  return response.json() as Promise<AlertIncident[]>;
}

export async function acknowledgeAlertIncident(
  client: ApiClient,
  id: string,
): Promise<AlertIncident> {
  const response = await authenticatedRequest(
    client,
    `/api/alert-incidents/${encodeURIComponent(id)}/acknowledge`,
    { method: "POST" },
  );
  assertOk(response, "Alert acknowledge failed");
  return response.json() as Promise<AlertIncident>;
}

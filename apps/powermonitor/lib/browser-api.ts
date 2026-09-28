export type TimeRange = "1h" | "24h" | "7d";

export type Permission = "viewer" | "controller" | "manager" | "owner";

export type Device = {
  id: string;
  serial_number?: string | null;
  name?: string;
  asset_id?: string | null;
  device_profile_id?: string | null;
  attributes?: Record<string, unknown>;
  online?: boolean;
  last_seen_at?: string | null;
  permission?: Permission;
  readings?: Record<string, unknown>;
  capabilities?: string[];
  switch_state?: boolean | null;
  brightness_pct?: number | null;
};

export type Asset = {
  id: string;
  name: string;
  asset_profile_id?: string | null;
  attributes?: Record<string, unknown>;
  parent_id?: string | null;
  permission?: Permission;
};

export type ResourceInvitation = {
  id: string;
  permission: "viewer" | "manager";
  resource_id: string;
  resource_kind: "asset" | "device";
  resource_name: string;
  sender_username: string;
};

export type TelemetryPoint = {
  at: string;
  device_id?: string;
  measurements?: Record<string, unknown>;
  power_w?: number | null;
  voltage_v?: number | null;
  current_a?: number | null;
  energy_kwh?: number | null;
};

export type LiveChartAggregation = "last" | "sum" | "avg" | "min" | "max";

export type LiveChart = {
  metric: string;
  label: string;
  unit?: string;
  color?: string;
  aggregation: LiveChartAggregation;
};

export type LiveView = {
  profile: { id: string; name: string } | null;
  charts: LiveChart[];
};

export type TenantProfile = {
  id: string;
  name: string;
};

export type ResourceProfile = {
  id: string;
  name: string;
};

export type TenantProfileAssignment = {
  profile_id: string | null;
};

export type DeviceToken = {
  id?: string;
  token?: string;
  token_prefix?: string;
};

export type DeviceAlertRule = {
  id: string;
  name: string;
  enabled: boolean;
  device_id: string;
  metric_key: string;
  rule_type: "event_threshold" | "window_average";
  comparison: "gt" | "gte" | "lt" | "lte";
  threshold: number;
  window_seconds?: number | null;
  for_seconds: number;
  resolve_after_seconds: number;
  reopen_grace_seconds: number;
  hysteresis?: number | null;
  severity: "info" | "warning" | "critical";
  reminder_interval_seconds: number;
};

export type DeviceAlertRuleInput = Omit<DeviceAlertRule, "device_id" | "id">;

export type Alert = {
  id: string;
  message: string;
  severity?: "info" | "warning" | "critical";
  status?: "open" | "acknowledged" | "resolved";
  device_id?: string;
};

export type CommandLifecycle = {
  id: string;
  state: string;
  response?: Record<string, unknown> | null;
};

export type CommandMode = "one_way" | "two_way";

export class BffApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "BffApiError";
  }
}

export async function listDevices(): Promise<Device[]> {
  return (await listResponse<PublicDevice>("/api/v1/devices")).map(normalizeDevice);
}

export async function listUserCapabilities(): Promise<string[]> {
  const response = await request<{ capabilities?: unknown }>("/api/v1/user-capabilities");
  return stringList(response.capabilities) ?? [];
}

export async function claimDevice(serialNumber: string, code: string): Promise<Device> {
  return normalizeDevice(await request<PublicDevice>(
    "/api/v1/devices/claim",
    jsonRequest({ serial_number: serialNumber, code }),
  ));
}

export async function listAssets(): Promise<Asset[]> {
  return (await listResponse<PublicAsset>("/api/v1/assets")).map(normalizeAsset);
}

export async function createAsset(input: {
  name: string;
  parent_asset_id?: string | null;
}): Promise<Asset> {
  return normalizeAsset(await request<PublicAsset>(
    "/api/v1/assets",
    jsonRequest({
      metadata: {},
      name: input.name,
      parent_asset_id: input.parent_asset_id ?? null,
    }),
  ));
}

export async function listResourceInvitations(): Promise<ResourceInvitation[]> {
  return listResponse<ResourceInvitation>("/api/v1/resource-invitations");
}

export async function listTenantProfiles(
  kind: "asset" | "device",
): Promise<TenantProfile[]> {
  return (await request<PublicProfile[]>("/api/v1/tenant-profile/profiles?kind=" + kind)).map(normalizeProfile);
}

export async function listResourceProfiles(
  kind: "asset" | "device",
): Promise<ResourceProfile[]> {
  const path = kind === "asset" ? "/api/v1/asset-profiles" : "/api/v1/device-profiles";
  return (await listResponse<PublicProfile>(path)).map(normalizeProfile);
}

export async function assignDeviceTenantProfile(
  deviceId: string,
  profileId: string | null,
): Promise<TenantProfileAssignment> {
  return request<TenantProfileAssignment>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/tenant-profile",
    jsonRequest({ profile_id: profileId }, "PUT"),
  );
}

export async function assignAssetTenantProfile(
  assetId: string,
  profileId: string | null,
): Promise<TenantProfileAssignment> {
  return request<TenantProfileAssignment>(
    "/api/v1/assets/" + encodeURIComponent(assetId) + "/tenant-profile",
    jsonRequest({ profile_id: profileId }, "PUT"),
  );
}

export async function updateDevice(
  deviceId: string,
  input: { display_name?: string; asset_id?: string | null; device_profile_id?: string | null },
): Promise<Device> {
  return normalizeDevice(await request<PublicDevice>(
    "/api/v1/devices/" + encodeURIComponent(deviceId),
    jsonRequest(input, "PATCH"),
  ));
}

export async function updateAsset(
  assetId: string,
  input: { name?: string; parent_asset_id?: string | null; asset_profile_id?: string | null },
): Promise<Asset> {
  return normalizeAsset(await request<PublicAsset>(
    "/api/v1/assets/" + encodeURIComponent(assetId),
    jsonRequest(input, "PATCH"),
  ));
}

export async function revealDeviceToken(deviceId: string): Promise<DeviceToken> {
  return request<DeviceToken>("/api/v1/devices/" + encodeURIComponent(deviceId) + "/token");
}

export async function regenerateDeviceToken(deviceId: string): Promise<DeviceToken> {
  return request<DeviceToken>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/token",
    { method: "POST" },
  );
}

export async function listDeviceAlertRules(deviceId: string): Promise<DeviceAlertRule[]> {
  return listResponse<DeviceAlertRule>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/alert-rules",
  );
}

export async function createDeviceAlertRule(
  deviceId: string,
  input: DeviceAlertRuleInput,
): Promise<DeviceAlertRule> {
  return request<DeviceAlertRule>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/alert-rules",
    jsonRequest(input),
  );
}

export async function updateDeviceAlertRule(
  deviceId: string,
  ruleId: string,
  input: DeviceAlertRuleInput,
): Promise<DeviceAlertRule> {
  return request<DeviceAlertRule>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/alert-rules/" + encodeURIComponent(ruleId),
    jsonRequest(input, "PUT"),
  );
}

export async function archiveDeviceAlertRule(deviceId: string, ruleId: string): Promise<void> {
  await request(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/alert-rules/" + encodeURIComponent(ruleId),
    { method: "DELETE" },
  );
}

export async function createDeviceResourceInvitation(
  deviceId: string,
  username: string,
  permission: "viewer" | "manager",
): Promise<ResourceInvitation> {
  return request<ResourceInvitation>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/resource-invitations",
    jsonRequest({ username, permission }),
  );
}

export async function createAssetResourceInvitation(
  assetId: string,
  username: string,
  permission: "viewer" | "manager",
): Promise<ResourceInvitation> {
  return request<ResourceInvitation>(
    "/api/v1/assets/" + encodeURIComponent(assetId) + "/resource-invitations",
    jsonRequest({ username, permission }),
  );
}

export async function acceptResourceInvitation(invitationId: string): Promise<void> {
  await request("/api/v1/resource-invitations/" + encodeURIComponent(invitationId) + "/accept", {
    method: "POST",
  });
}

export async function cancelResourceInvitation(invitationId: string): Promise<void> {
  await request("/api/v1/resource-invitations/" + encodeURIComponent(invitationId) + "/cancel", {
    method: "POST",
  });
}

export async function listAlerts(): Promise<Alert[]> {
  return (await listResponse<PublicAlert>("/api/v1/alerts")).map(normalizeAlert);
}

export async function getDeviceTelemetry(
  deviceId: string,
  range: TimeRange,
  now = new Date(),
): Promise<TelemetryPoint[]> {
  return (await listResponse<PublicTelemetry>(
    "/api/v1/telemetry/" + encodeURIComponent(deviceId) + "?" + telemetryRangeQuery(range, now),
  )).map(normalizeTelemetry);
}

export async function getAssetTelemetry(
  assetId: string,
  range: TimeRange,
  now = new Date(),
): Promise<TelemetryPoint[]> {
  const query = new URLSearchParams({
    asset_id: assetId,
    aggregate: "asset",
  });
  query.append("from", rangeStart(range, now).toISOString());
  query.append("to", now.toISOString());
  return (await listResponse<PublicTelemetry>(
    "/api/v1/telemetry?" + query.toString(),
  )).map(normalizeTelemetry);
}

export async function getDeviceLiveView(deviceId: string): Promise<LiveView> {
  return normalizeLiveView(await request<PublicLiveView>(
    "/api/v1/devices/" + encodeURIComponent(deviceId) + "/live-view",
  ));
}

export async function getAssetLiveView(assetId: string): Promise<LiveView> {
  return normalizeLiveView(await request<PublicLiveView>(
    "/api/v1/assets/" + encodeURIComponent(assetId) + "/live-view",
  ));
}

export async function acknowledgeAlert(alertId: string): Promise<void> {
  await request("/api/v1/alerts/" + encodeURIComponent(alertId) + "/acknowledge", {
    method: "POST",
  });
}

export async function submitDeviceCommand(
  deviceId: string,
  method: string,
  params: Record<string, unknown>,
  mode: CommandMode = "one_way",
): Promise<CommandLifecycle> {
  return request<CommandLifecycle>("/api/v1/devices/" + encodeURIComponent(deviceId) + "/commands", {
    body: JSON.stringify({ method, mode, params }),
    headers: {
      "content-type": "application/json",
      "idempotency-key": idempotencyKey(),
    },
    method: "POST",
  });
}

export async function getCommand(commandId: string): Promise<CommandLifecycle> {
  return request<CommandLifecycle>("/api/v1/commands/" + encodeURIComponent(commandId));
}

export async function sendDeviceCommandAndWait(
  deviceId: string,
  method: string,
  params: Record<string, unknown>,
  mode: CommandMode,
  options: CommandPollingOptions = {},
): Promise<CommandLifecycle> {
  const initial = await submitDeviceCommand(deviceId, method, params, mode);
  options.onProgress?.(initial);
  return waitForCommand(initial, mode, options);
}

export async function waitForCommand(
  initial: CommandLifecycle,
  mode: CommandMode,
  options: CommandPollingOptions = {},
): Promise<CommandLifecycle> {
  const readCommand = options.readCommand ?? getCommand;
  const wait = options.wait ?? defaultCommandDelay;
  const maxAttempts = options.maxAttempts ?? 35;
  let lifecycle = initial;

  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    if (isTerminalCommandState(lifecycle.state, mode)) {
      return lifecycle;
    }
    await wait();
    lifecycle = await readCommand(lifecycle.id);
    options.onProgress?.(lifecycle);
  }
  throw new Error("Command did not reach a terminal state.");
}

async function listResponse<T>(path: string): Promise<T[]> {
  const items: T[] = [];
  const seenCursors = new Set<string>();
  let nextPath = path;
  let pageCount = 0;

  for (;;) {
    if (pageCount >= maxPaginationPages) {
      throw new Error("Platform pagination page budget exceeded.");
    }
    pageCount += 1;
    const payload = await request<T[] | CursorPage<T>>(nextPath);
    if (Array.isArray(payload)) {
      appendItems(items, payload);
      return items;
    }
    appendItems(items, payload.items ?? []);

    const nextCursor = payload.next_cursor;
    if (nextCursor === undefined || nextCursor === null || nextCursor === "") {
      if (payload.has_more === true) {
        throw new Error("Platform pagination response is missing next_cursor.");
      }
      return items;
    }
    if (payload.has_more === false) {
      return items;
    }
    if (seenCursors.has(nextCursor)) {
      throw new Error("Platform pagination cursor repeated.");
    }
    seenCursors.add(nextCursor);
    nextPath = appendCursor(path, nextCursor);
  }
}

async function request<T = undefined>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    cache: "no-store",
    credentials: "same-origin",
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => null) as { message?: string } | null;
    throw new BffApiError(response.status, payload?.message ?? "PowerMonitor request failed.");
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json() as Promise<T>;
}

function jsonRequest(body: object, method = "POST"): RequestInit {
  return {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    method,
  };
}

function idempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return Date.now().toString(36) + "-" + Math.random().toString(36).slice(2);
}

type CursorPage<T> = {
  has_more?: boolean;
  items?: T[];
  next_cursor?: string | null;
};

type PublicDevice = {
  asset_id?: string | null;
  device_profile_id?: string | null;
  attributes?: unknown;
  brightness_pct?: unknown;
  capabilities?: unknown;
  device_id?: string;
  display_name?: string | null;
  effective_permission?: Permission;
  id?: string;
  last_seen_at?: string | null;
  metadata?: unknown;
  name?: string;
  online?: unknown;
  permission?: Permission;
  serial_number?: string | null;
  switch_state?: unknown;
};

type PublicAsset = {
  attributes?: unknown;
  asset_profile_id?: string | null;
  effective_permission?: Permission;
  id: string;
  metadata?: unknown;
  name: string;
  parent_asset_id?: string | null;
  parent_id?: string | null;
  permission?: Permission;
};

type PublicTelemetry = {
  at?: string;
  current_a?: number | null;
  device_id?: string;
  energy_kwh?: number | null;
  event_at?: string;
  measurements?: unknown;
  power_w?: number | null;
  voltage_v?: number | null;
};

type PublicLiveView = {
  profile?: { id?: unknown; name?: unknown } | null;
  charts?: unknown;
};

type PublicProfile = {
  id?: unknown;
  name?: unknown;
};

type PublicAlert = Omit<Alert, "message"> & {
  message?: string;
  rule_name?: string;
};

const maxPaginationPages = 25;
const maxPaginationItems = 2_500;

function normalizeDevice(response: PublicDevice): Device {
  const id = response.id ?? response.device_id;
  if (id === undefined || id.length === 0) {
    throw new Error("Platform device response omitted device_id.");
  }
  const device: Device = {
    id,
    name: response.name ?? response.display_name ?? id,
  };
  if (response.serial_number !== undefined) device.serial_number = response.serial_number;
  if (response.asset_id !== undefined) device.asset_id = response.asset_id;
  if (response.device_profile_id !== undefined) device.device_profile_id = response.device_profile_id;
  const permission = response.permission ?? response.effective_permission;
  if (permission !== undefined) device.permission = permission;
  const attributes = recordValue(response.attributes ?? response.metadata);
  const capabilities = stringList(response.capabilities ?? attributes.capabilities);
  if (capabilities !== undefined) device.capabilities = capabilities;
  const switchState = booleanValue(response.switch_state ?? attributes.switch_state);
  if (switchState !== null) device.switch_state = switchState;
  const brightness = numberValue(response.brightness_pct ?? attributes.brightness_pct);
  if (brightness !== null) device.brightness_pct = brightness;
  const online = booleanValue(response.online ?? attributes.online);
  if (online !== null) device.online = online;
  if (response.last_seen_at !== undefined) {
    device.last_seen_at = response.last_seen_at;
  } else {
    const lastSeenAt = stringValue(attributes.last_seen_at);
    if (lastSeenAt !== undefined) device.last_seen_at = lastSeenAt;
  }
  return device;
}

function normalizeAsset(response: PublicAsset): Asset {
  const asset: Asset = {
    id: response.id,
    name: response.name,
  };
  const parentId = response.parent_id ?? response.parent_asset_id;
  if (parentId !== undefined) asset.parent_id = parentId;
  if (response.asset_profile_id !== undefined) asset.asset_profile_id = response.asset_profile_id;
  const permission = response.permission ?? response.effective_permission;
  if (permission !== undefined) asset.permission = permission;
  if (response.attributes !== undefined || response.metadata !== undefined) {
    asset.attributes = recordValue(response.attributes ?? response.metadata);
  }
  return asset;
}

function normalizeTelemetry(response: PublicTelemetry): TelemetryPoint {
  const at = response.at ?? response.event_at;
  if (at === undefined) {
    throw new Error("Platform telemetry response omitted event_at.");
  }
  const measurements = recordValue(response.measurements);
  const point: TelemetryPoint = { at };
  if (response.device_id !== undefined) point.device_id = response.device_id;
  if (response.measurements !== undefined) point.measurements = measurements;
  assignNumber(point, "current_a", measurements.current_a ?? response.current_a);
  assignNumber(point, "energy_kwh", measurements.energy_kwh ?? response.energy_kwh);
  assignNumber(point, "power_w", measurements.power_w ?? response.power_w);
  assignNumber(point, "voltage_v", measurements.voltage_v ?? response.voltage_v);
  return point;
}

function normalizeLiveView(response: PublicLiveView): LiveView {
  const profile = response.profile !== null
    && response.profile !== undefined
    && typeof response.profile.id === "string"
    && typeof response.profile.name === "string"
    ? { id: response.profile.id, name: response.profile.name }
    : null;
  return {
    profile,
    charts: Array.isArray(response.charts)
      ? response.charts.map(normalizeLiveChart).filter((chart): chart is LiveChart => chart !== null)
      : [],
  };
}

function normalizeProfile(value: PublicProfile): TenantProfile {
  const id = stringValue(value.id)?.trim();
  const name = stringValue(value.name)?.trim();
  if (id === undefined || id.length === 0 || name === undefined || name.length === 0) {
    throw new Error("Platform profile response omitted id or name.");
  }
  return { id, name };
}

function normalizeLiveChart(value: unknown): LiveChart | null {
  const chart = recordValue(value);
  const metric = stringValue(chart.metric)?.trim();
  const label = stringValue(chart.label)?.trim();
  const aggregation = stringValue(chart.aggregation);
  if (metric === undefined || metric.length === 0 || label === undefined || label.length === 0) {
    return null;
  }
  if (aggregation !== "last" && aggregation !== "sum" && aggregation !== "avg"
    && aggregation !== "min" && aggregation !== "max") {
    return null;
  }
  const unit = stringValue(chart.unit)?.trim();
  const color = stringValue(chart.color);
  return {
    metric,
    label,
    aggregation,
    ...(unit === undefined || unit.length === 0 ? {} : { unit }),
    ...(color === undefined ? {} : { color }),
  };
}

function normalizeAlert({ message, rule_name, ...alert }: PublicAlert): Alert {
  return {
    ...alert,
    message: message ?? rule_name ?? "Alert " + alert.id,
  };
}

function recordValue(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function assignNumber(
  target: TelemetryPoint,
  key: "current_a" | "energy_kwh" | "power_w" | "voltage_v",
  value: unknown,
): void {
  const number = numberValue(value);
  if (number !== null) target[key] = number;
}

function booleanValue(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function stringList(value: unknown): string[] | undefined {
  return Array.isArray(value) && value.every((item) => typeof item === "string")
    ? value
    : undefined;
}

type CommandPollingOptions = {
  maxAttempts?: number;
  onProgress?(command: CommandLifecycle): void;
  readCommand?(commandId: string): Promise<CommandLifecycle>;
  wait?(): Promise<void>;
};

function appendCursor(path: string, cursor: string): string {
  return path + (path.includes("?") ? "&" : "?") + new URLSearchParams({ after: cursor }).toString();
}

function appendItems<T>(target: T[], page: T[]): void {
  if (target.length + page.length > maxPaginationItems) {
    throw new Error("Platform pagination item budget exceeded.");
  }
  target.push(...page);
}

function telemetryRangeQuery(range: TimeRange, now: Date): string {
  return new URLSearchParams({
    from: rangeStart(range, now).toISOString(),
    to: now.toISOString(),
  }).toString();
}

function rangeStart(range: TimeRange, now: Date): Date {
  const duration = range === "1h" ? 60 * 60 * 1_000 : range === "24h"
    ? 24 * 60 * 60 * 1_000
    : 7 * 24 * 60 * 60 * 1_000;
  return new Date(now.getTime() - duration);
}

function isTerminalCommandState(state: string, mode: CommandMode): boolean {
  if (mode === "two_way") {
    return state === "responded" || state === "expired" || state === "failed";
  }
  return state === "published_to_broker" || state === "expired" || state === "failed";
}

function defaultCommandDelay(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, 1_000);
  });
}

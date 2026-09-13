export type TimeRange = "1h" | "24h" | "7d";

export type Device = {
  id: string;
  name?: string;
  asset_id?: string | null;
  online?: boolean;
  last_seen_at?: string | null;
  permission?: "viewer" | "controller" | "manager" | "owner";
  readings?: Record<string, unknown>;
  capabilities?: string[];
  switch_state?: boolean | null;
  brightness_pct?: number | null;
};

export type Asset = {
  id: string;
  name: string;
  parent_id?: string | null;
};

export type TelemetryPoint = {
  at: string;
  measurements?: Record<string, unknown>;
  power_w?: number | null;
  voltage_v?: number | null;
  current_a?: number | null;
  energy_kwh?: number | null;
};

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
  return listResponse<Device>("/api/v1/devices");
}

export async function listAssets(): Promise<Asset[]> {
  return listResponse<Asset>("/api/v1/assets");
}

export async function listAlerts(): Promise<Alert[]> {
  return listResponse<Alert>("/api/v1/alerts");
}

export async function getDeviceTelemetry(
  deviceId: string,
  range: TimeRange,
  now = new Date(),
): Promise<TelemetryPoint[]> {
  return listResponse<TelemetryPoint>(
    "/api/v1/telemetry/" + encodeURIComponent(deviceId) + "?" + telemetryRangeQuery(range, now),
  );
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
  return listResponse<TelemetryPoint>(
    "/api/v1/telemetry?" + query.toString(),
  );
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
      return items.concat(payload);
    }
    items.push(...(payload.items ?? []));

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

const maxPaginationPages = 25;

type CommandPollingOptions = {
  maxAttempts?: number;
  onProgress?(command: CommandLifecycle): void;
  readCommand?(commandId: string): Promise<CommandLifecycle>;
  wait?(): Promise<void>;
};

function appendCursor(path: string, cursor: string): string {
  return path + (path.includes("?") ? "&" : "?") + new URLSearchParams({ after: cursor }).toString();
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

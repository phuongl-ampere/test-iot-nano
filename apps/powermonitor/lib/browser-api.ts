export type TimeRange = "1h" | "24h" | "7d";

export type Device = {
  id: string;
  name?: string;
  asset_id?: string | null;
  online?: boolean;
  last_seen_at?: string | null;
  readings?: Record<string, unknown>;
  capabilities?: string[];
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
  const duration = range === "1h" ? 60 * 60 * 1_000 : range === "24h"
    ? 24 * 60 * 60 * 1_000
    : 7 * 24 * 60 * 60 * 1_000;
  const query = new URLSearchParams({
    from: new Date(now.getTime() - duration).toISOString(),
    to: now.toISOString(),
  });
  return listResponse<TelemetryPoint>(
    "/api/v1/telemetry/" + encodeURIComponent(deviceId) + "?" + query.toString(),
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
): Promise<CommandLifecycle> {
  return request<CommandLifecycle>("/api/v1/devices/" + encodeURIComponent(deviceId) + "/commands", {
    body: JSON.stringify({ method, params }),
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

async function listResponse<T>(path: string): Promise<T[]> {
  const payload = await request<T[] | { items?: T[] }>(path);
  return Array.isArray(payload) ? payload : payload.items ?? [];
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

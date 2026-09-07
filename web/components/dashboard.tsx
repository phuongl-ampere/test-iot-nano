"use client";

import {
  Activity,
  Database,
  Droplets,
  RefreshCw,
  Thermometer,
  Wifi,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  type ApiClient,
  type AlertIncident,
  type AlertRule,
  type DeviceSummary,
  type Role,
  type TelemetryPoint,
  type TimeRange,
  UnauthorizedApiError,
  createApiClient,
  fetchAlertIncidents,
  fetchAlertRules,
  fetchDevices,
  fetchTelemetry,
  getCurrentRole,
  logout,
  sendDeviceCommand,
} from "../lib/api";
import { browserDateTime } from "../lib/time";
import { AlertPanel } from "./alert-panel";
import { CommandControl, type DeviceCommand } from "./command-control";
import { DeviceTable } from "./device-table";
import { DeviceTokenPanel } from "./device-token-panel";
import { LoginScreen } from "./login-screen";
import { ProfileMenu } from "./profile-menu";
import { SystemConfigurationPanel } from "./system-configuration";
import { TelemetryChart } from "./telemetry-chart";
import { TimeRangeControl } from "./time-range-control";
import { UserProfilePanel } from "./user-profile";

const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8080";
const sessionStorageKey = "rush-iot-nano.session-id";
function formattedNumber(value: number | null, suffix: string) {
  return value === null ? "--" : `${value.toFixed(1)}${suffix}`;
}

function lastSeen(value: string | null) {
  return browserDateTime(value, "No signal");
}

export function Dashboard() {
  const [client, setClient] = useState<ApiClient | null>(null);
  const [role, setRole] = useState<Role | null>(null);
  const [authReady, setAuthReady] = useState(false);
  const [devices, setDevices] = useState<DeviceSummary[]>([]);
  const [selectedDeviceId, setSelectedDeviceId] = useState<string | null>(null);
  const [points, setPoints] = useState<TelemetryPoint[]>([]);
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [incidents, setIncidents] = useState<AlertIncident[]>([]);
  const [range, setRange] = useState<TimeRange>("1h");
  const [view, setView] = useState<"dashboard" | "user_profile" | "system_configuration">("dashboard");
  const [loadingDevices, setLoadingDevices] = useState(true);
  const [loadingTelemetry, setLoadingTelemetry] = useState(false);
  const [command, setCommand] = useState<DeviceCommand>("sample_now");
  const [commandStatus, setCommandStatus] = useState<string | null>(null);
  const [sendingCommand, setSendingCommand] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const selectedDevice = useMemo(
    () => devices.find((device) => device.device_id === selectedDeviceId) ?? null,
    [devices, selectedDeviceId],
  );
  const latest = points.at(-1) ?? null;
  const onlineCount = devices.filter((device) => device.online).length;

  const clearAuthentication = useCallback(() => {
    window.sessionStorage.removeItem(sessionStorageKey);
    setClient(null);
    setRole(null);
    setDevices([]);
    setPoints([]);
    setRules([]);
    setIncidents([]);
    setError(null);
    setView("dashboard");
  }, []);

  useEffect(() => {
    const sessionId = window.sessionStorage.getItem(sessionStorageKey);
    if (sessionId === null) {
      setAuthReady(true);
      return;
    }
    const storedClient = createApiClient(apiBaseUrl, sessionId);
    void getCurrentRole(storedClient)
      .then((storedRole) => {
        setClient(storedClient);
        setRole(storedRole);
      })
      .catch((restoreError) => {
        if (restoreError instanceof UnauthorizedApiError) {
          window.sessionStorage.removeItem(sessionStorageKey);
        } else {
          setError("Unable to restore the current session.");
        }
      })
      .finally(() => setAuthReady(true));
  }, []);

  const loadDevices = useCallback(async () => {
    if (client === null) {
      return;
    }
    setLoadingDevices(true);
    setError(null);
    try {
      const nextDevices = await fetchDevices(client);
      setDevices(nextDevices);
      setSelectedDeviceId((current) => {
        if (current !== null && nextDevices.some((device) => device.device_id === current)) {
          return current;
        }
        return nextDevices[0]?.device_id ?? null;
      });
    } catch (loadError) {
      if (loadError instanceof UnauthorizedApiError) {
        clearAuthentication();
        return;
      }
      setError(loadError instanceof Error ? loadError.message : "Device request failed.");
    } finally {
      setLoadingDevices(false);
    }
  }, [clearAuthentication, client]);

  const loadTelemetry = useCallback(async () => {
    if (client === null || selectedDeviceId === null) {
      setPoints([]);
      return;
    }

    setLoadingTelemetry(true);
    setError(null);
    try {
      setPoints(await fetchTelemetry(client, selectedDeviceId, range));
    } catch (loadError) {
      if (loadError instanceof UnauthorizedApiError) {
        clearAuthentication();
        return;
      }
      setError(loadError instanceof Error ? loadError.message : "Telemetry request failed.");
    } finally {
      setLoadingTelemetry(false);
    }
  }, [clearAuthentication, client, range, selectedDeviceId]);

  const loadAlerts = useCallback(async () => {
    if (client === null) {
      return;
    }
    try {
      const [nextRules, nextIncidents] = await Promise.all([
        fetchAlertRules(client),
        fetchAlertIncidents(client),
      ]);
      setRules(nextRules);
      setIncidents(nextIncidents);
    } catch (loadError) {
      if (loadError instanceof UnauthorizedApiError) {
        clearAuthentication();
        return;
      }
      setError(loadError instanceof Error ? loadError.message : "Alert request failed.");
    }
  }, [clearAuthentication, client]);

  useEffect(() => {
    if (client !== null) {
      void loadDevices();
    }
  }, [client, loadDevices]);

  useEffect(() => {
    if (client !== null) {
      void loadTelemetry();
    }
  }, [client, loadTelemetry]);

  useEffect(() => {
    if (client !== null) {
      void loadAlerts();
    }
  }, [client, loadAlerts]);

  const refresh = useCallback(() => {
    void loadDevices();
    void loadTelemetry();
    void loadAlerts();
  }, [loadAlerts, loadDevices, loadTelemetry]);

  const sendCommand = useCallback(async () => {
    if (client === null || role !== "admin" || selectedDeviceId === null) {
      return;
    }

    setSendingCommand(true);
    setCommandStatus(null);
    try {
      await sendDeviceCommand(client, selectedDeviceId, command);
      setCommandStatus("Queued");
    } catch (commandError) {
      if (commandError instanceof UnauthorizedApiError) {
        clearAuthentication();
        return;
      }
      setError(commandError instanceof Error ? commandError.message : "Command request failed.");
    } finally {
      setSendingCommand(false);
    }
  }, [clearAuthentication, client, command, role, selectedDeviceId]);

  const authenticated = useCallback((nextClient: ApiClient, nextRole: Role) => {
    window.sessionStorage.setItem(sessionStorageKey, nextClient.sessionId);
    setClient(nextClient);
    setRole(nextRole);
    setAuthReady(true);
  }, []);

  const signOut = useCallback(() => {
    if (client !== null) {
      void logout(client).catch(() => undefined);
    }
    clearAuthentication();
  }, [clearAuthentication, client]);

  if (!authReady) {
    return <main aria-busy="true" className="login-shell" />;
  }

  if (client === null || role === null) {
    return (
      <LoginScreen
        apiBaseUrl={apiBaseUrl}
        onAuthenticated={authenticated}
      />
    );
  }

  if (view === "system_configuration" && role === "admin") {
    return (
      <main className="system-shell">
        <SystemConfigurationPanel
          client={client}
          onBack={() => setView("dashboard")}
          onUnauthorized={clearAuthentication}
        />
      </main>
    );
  }

  if (view === "user_profile") {
    return (
      <main className="system-shell">
        <UserProfilePanel
          client={client}
          onBack={() => setView("dashboard")}
          onPasswordChanged={clearAuthentication}
          onUnauthorized={clearAuthentication}
          role={role}
        />
      </main>
    );
  }

  return (
    <main className="dashboard-shell">
      <aside className="directory">
        <div className="brand-lockup">
          <span className="brand-mark"><Activity aria-hidden="true" size={18} /></span>
          <span>
            <strong>Rush IoT Nano</strong>
            <small>Fleet monitor</small>
          </span>
        </div>
        <div className="directory-heading">
          <div>
            <span className="eyebrow">Devices</span>
            <strong>{loadingDevices ? "Loading" : `${devices.length} registered`}</strong>
          </div>
          <span className="fleet-online"><Wifi aria-hidden="true" size={14} /> {onlineCount}</span>
        </div>
        <DeviceTable devices={devices} onSelect={setSelectedDeviceId} selectedDeviceId={selectedDeviceId} />
      </aside>

      <section className="telemetry-workspace">
        <header className="workspace-header">
          <div>
            <span className="eyebrow">Selected device</span>
            <h1>{selectedDevice?.display_name ?? selectedDevice?.device_id ?? "No device selected"}</h1>
            {selectedDevice !== null && <p>{selectedDevice.device_id} · Last seen {lastSeen(selectedDevice.last_seen_at)}</p>}
          </div>
          <div className="workspace-actions">
            <TimeRangeControl onChange={setRange} value={range} />
            <CommandControl
              disabled={selectedDeviceId === null || role !== "admin"}
              onChange={setCommand}
              onSend={() => void sendCommand()}
              sending={sendingCommand}
              value={command}
            />
            <button
              aria-label="Refresh telemetry"
              className="icon-button"
              disabled={loadingDevices || loadingTelemetry}
              onClick={refresh}
              title="Refresh telemetry"
              type="button"
            >
              <RefreshCw aria-hidden="true" size={17} />
            </button>
            <ProfileMenu
              onLogout={signOut}
              onOpenProfile={() => setView("user_profile")}
              onOpenSystemConfiguration={() => setView("system_configuration")}
              role={role}
            />
          </div>
        </header>

        {commandStatus !== null && <div className="command-status">{commandStatus}</div>}
        {role === "admin" && (
          <DeviceTokenPanel
            client={client}
            deviceId={selectedDevice?.device_id ?? null}
            onUnauthorized={clearAuthentication}
          />
        )}

        {error !== null && (
          <div className="error-banner" role="alert">
            <span>{error}</span>
            <button onClick={refresh} type="button">Retry</button>
          </div>
        )}

        <dl className="reading-strip">
          <div>
            <dt><Thermometer aria-hidden="true" size={16} /> Temperature</dt>
            <dd>{formattedNumber(latest?.temperature_c ?? null, " C")}</dd>
          </div>
          <div>
            <dt><Droplets aria-hidden="true" size={16} /> Humidity</dt>
            <dd>{formattedNumber(latest?.humidity_pct ?? null, " %")}</dd>
          </div>
          <div>
            <dt><Database aria-hidden="true" size={16} /> Events</dt>
            <dd>{points.reduce((total, point) => total + point.event_count, 0).toLocaleString()}</dd>
          </div>
          <div>
            <dt><Activity aria-hidden="true" size={16} /> Presence</dt>
            <dd className={selectedDevice?.online ? "value-online" : "value-offline"}>
              {selectedDevice?.online ? "Online" : "Offline"}
            </dd>
          </div>
        </dl>

        <section className="chart-section" aria-label="Telemetry chart">
          <div className="section-heading">
            <div>
              <span className="eyebrow">Signal trace</span>
              <h2>Telemetry</h2>
            </div>
            <span>{loadingTelemetry ? "Updating" : `${points.length} points`}</span>
          </div>
          <TelemetryChart points={points} />
        </section>

        <section className="readings-section" aria-label="Recent telemetry readings">
          <div className="section-heading">
            <div>
              <span className="eyebrow">Latest samples</span>
              <h2>Recent readings</h2>
            </div>
          </div>
          <div className="readings-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Timestamp</th>
                  <th>Temperature</th>
                  <th>Humidity</th>
                  <th>Events</th>
                </tr>
              </thead>
              <tbody>
                {points.slice(-12).reverse().map((point) => (
                  <tr key={`${point.at}-${point.event_count}`}>
                    <td>{new Date(point.at).toLocaleString("en")}</td>
                    <td>{formattedNumber(point.temperature_c, " C")}</td>
                    <td>{formattedNumber(point.humidity_pct, " %")}</td>
                    <td>{point.event_count}</td>
                  </tr>
                ))}
                {points.length === 0 && (
                  <tr>
                    <td className="table-empty" colSpan={4}>No readings in this range.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <AlertPanel
          client={client}
          incidents={incidents}
          onRefresh={() => void loadAlerts()}
          onUnauthorized={clearAuthentication}
          role={role}
          rules={rules}
        />
      </section>
    </main>
  );
}

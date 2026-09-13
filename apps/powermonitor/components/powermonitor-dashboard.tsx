"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import {
  BffApiError,
  acknowledgeAlert,
  getDeviceTelemetry,
  listAlerts,
  listAssets,
  listDevices,
  submitDeviceCommand,
  type Alert,
  type Asset,
  type CommandLifecycle,
  type Device,
  type TelemetryPoint,
  type TimeRange,
} from "../lib/browser-api";
import { AlertPanel } from "./alert-panel";
import { CommandPanel } from "./command-panel";
import { PowerTelemetryChart } from "./power-telemetry-chart";
import { PowerTelemetryTable } from "./power-telemetry-table";
import { PowerMonitorTree } from "./powermonitor-tree";
import { TimeRangeControl } from "./time-range-control";

type PowerMonitorDashboardProps = {
  initialAssetId?: string;
  initialDeviceId?: string;
};

export function PowerMonitorDashboard({
  initialAssetId,
  initialDeviceId,
}: PowerMonitorDashboardProps) {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [command, setCommand] = useState<CommandLifecycle | null>(null);
  const [commandBusy, setCommandBusy] = useState(false);
  const [devices, setDevices] = useState<Device[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [range, setRange] = useState<TimeRange>("1h");
  const [selectedAssetId, setSelectedAssetId] = useState<string | null>(initialAssetId ?? null);
  const [selectedDeviceId, setSelectedDeviceId] = useState<string | null>(initialDeviceId ?? null);
  const [telemetry, setTelemetry] = useState<TelemetryPoint[]>([]);
  const [telemetryLoading, setTelemetryLoading] = useState(false);
  const [workingAlertId, setWorkingAlertId] = useState<string | null>(null);

  const selectedDevice = useMemo(
    () => devices.find((device) => device.id === selectedDeviceId) ?? null,
    [devices, selectedDeviceId],
  );
  const selectedAsset = useMemo(
    () => assets.find((asset) => asset.id === selectedAssetId) ?? null,
    [assets, selectedAssetId],
  );
  const onlineCount = devices.filter((device) => device.online).length;

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextDevices, nextAssets, nextAlerts] = await Promise.all([
        listDevices(),
        listAssets(),
        listAlerts(),
      ]);
      setDevices(nextDevices);
      setAssets(nextAssets);
      setAlerts(nextAlerts);
      setSelectedDeviceId((current) => {
        if (initialDeviceId !== undefined && nextDevices.some((device) => device.id === initialDeviceId)) {
          return initialDeviceId;
        }
        return current !== null && nextDevices.some((device) => device.id === current)
          ? current
          : nextDevices[0]?.id ?? null;
      });
      setSelectedAssetId((current) => {
        if (initialAssetId !== undefined && nextAssets.some((asset) => asset.id === initialAssetId)) {
          return initialAssetId;
        }
        return current !== null && nextAssets.some((asset) => asset.id === current)
          ? current
          : null;
      });
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setLoading(false);
    }
  }, [initialAssetId, initialDeviceId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (selectedDeviceId === null) {
      setTelemetry([]);
      return;
    }
    let cancelled = false;
    setTelemetryLoading(true);
    void getDeviceTelemetry(selectedDeviceId, range)
      .then((points) => {
        if (!cancelled) {
          setTelemetry(points);
        }
      })
      .catch((reason) => {
        if (!cancelled) {
          setError(errorMessage(reason));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setTelemetryLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [range, selectedDeviceId]);

  const sendCommand = async (method: string, params: Record<string, unknown>) => {
    if (selectedDevice === null) {
      return;
    }
    setCommandBusy(true);
    setError(null);
    try {
      setCommand(await submitDeviceCommand(selectedDevice.id, method, params));
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setCommandBusy(false);
    }
  };

  const acknowledge = async (alertId: string) => {
    setWorkingAlertId(alertId);
    setError(null);
    try {
      await acknowledgeAlert(alertId);
      setAlerts((current) => current.map((alert) => (
        alert.id === alertId ? { ...alert, status: "acknowledged" } : alert
      )));
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setWorkingAlertId(null);
    }
  };

  const title = selectedDevice?.name ?? selectedDevice?.id ?? selectedAsset?.name ?? "Fleet overview";
  return (
    <main className="powermonitor-shell">
      <aside className="explorer">
        <header className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">P</span>
          <span>
            <strong>Power Monitor</strong>
            <small>Operational energy view</small>
          </span>
        </header>
        <div className="explorer-heading">
          <span className="eyebrow">Asset explorer</span>
          <strong>{loading ? "Loading" : devices.length + " devices"}</strong>
          <span>{onlineCount} online</span>
        </div>
        <PowerMonitorTree
          assets={assets}
          devices={devices}
          onSelectAsset={(assetId) => {
            setSelectedAssetId(assetId);
            setSelectedDeviceId(null);
          }}
          onSelectDevice={(deviceId) => {
            setSelectedDeviceId(deviceId);
            setSelectedAssetId(null);
          }}
          selectedAssetId={selectedAssetId}
          selectedDeviceId={selectedDeviceId}
        />
      </aside>

      <section className="workspace">
        <header className="workspace-header">
          <div>
            <span className="eyebrow">Power Monitor</span>
            <h1>{title}</h1>
            <p>{selectedDevice?.id ?? selectedAsset?.id ?? "All accessible resources"}</p>
          </div>
          <div className="workspace-actions">
            <TimeRangeControl onChange={setRange} value={range} />
            <button aria-label="Refresh Power Monitor" disabled={loading} onClick={() => void refresh()} type="button">
              Refresh
            </button>
          </div>
        </header>

        {error !== null && (
          <div className="error-banner" role="alert">
            <span>{error}</span>
            {error === "Sign in is required." && <a href="/api/auth/login">Sign in</a>}
          </div>
        )}

        <section aria-label="Fleet summary" className="reading-strip">
          <div><span>Devices</span><strong>{devices.length}</strong></div>
          <div><span>Online</span><strong>{onlineCount}</strong></div>
          <div><span>Assets</span><strong>{assets.length}</strong></div>
          <div><span>Alerts</span><strong>{alerts.length}</strong></div>
        </section>

        <section aria-label="Telemetry" className="telemetry-section">
          <header className="section-heading">
            <div>
              <span className="eyebrow">Selected device</span>
              <h2>Telemetry</h2>
            </div>
            <span>{telemetryLoading ? "Updating" : telemetry.length + " samples"}</span>
          </header>
          {selectedDevice === null ? (
            <>
              <p className="empty-state">Select a device to inspect telemetry and send commands.</p>
              <CommandPanel busy={false} disabled onSubmit={sendCommand} />
            </>
          ) : (
            <>
              <PowerTelemetryChart points={telemetry} />
              <PowerTelemetryTable points={telemetry} />
              <CommandPanel busy={commandBusy} onSubmit={sendCommand} state={command?.state} />
              <a className="detail-link" href={"/devices/" + encodeURIComponent(selectedDevice.id)}>Open device details</a>
            </>
          )}
        </section>

        {selectedAsset !== null && (
          <section aria-label="Asset details" className="asset-details">
            <header className="section-heading">
              <div>
                <span className="eyebrow">Asset details</span>
                <h2>{selectedAsset.name}</h2>
              </div>
            </header>
            <p>{devices.filter((device) => device.asset_id === selectedAsset.id).length} assigned devices</p>
            <a className="detail-link" href={"/assets/" + encodeURIComponent(selectedAsset.id)}>Open asset details</a>
          </section>
        )}

        <AlertPanel alerts={alerts} onAcknowledge={(alertId) => void acknowledge(alertId)} workingId={workingAlertId} />
      </section>
    </main>
  );
}

function errorMessage(reason: unknown): string {
  if (reason instanceof BffApiError && reason.status === 401) {
    return "Sign in is required.";
  }
  return reason instanceof Error ? reason.message : "PowerMonitor request failed.";
}

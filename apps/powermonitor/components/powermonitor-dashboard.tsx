"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  BffApiError,
  acknowledgeAlert,
  getAssetTelemetry,
  getDeviceTelemetry,
  listAlerts,
  listAssets,
  listDevices,
  sendDeviceCommandAndWait,
  type Alert,
  type Asset,
  type CommandLifecycle,
  type CommandMode,
  type Device,
  type TelemetryPoint,
  type TimeRange,
} from "../lib/browser-api";
import { AlertPanel } from "./alert-panel";
import { CommandPanel } from "./command-panel";
import { DeviceControlPanel } from "./device-control-panel";
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
  const selectedAssetIdRef = useRef<string | null>(initialAssetId ?? null);
  const selectedDeviceIdRef = useRef<string | null>(initialDeviceId ?? null);
  const selectionGenerationRef = useRef(0);
  const telemetryRequestIdRef = useRef(0);
  const workspaceRequestIdRef = useRef(0);
  const rangeRef = useRef<TimeRange>("1h");

  const selectedDevice = useMemo(
    () => devices.find((device) => device.id === selectedDeviceId) ?? null,
    [devices, selectedDeviceId],
  );
  const selectedAsset = useMemo(
    () => assets.find((asset) => asset.id === selectedAssetId) ?? null,
    [assets, selectedAssetId],
  );
  const onlineCount = devices.filter((device) => device.online).length;

  const refresh = useCallback(async (): Promise<boolean> => {
    const requestId = workspaceRequestIdRef.current + 1;
    const selectionGeneration = selectionGenerationRef.current;
    workspaceRequestIdRef.current = requestId;
    setLoading(true);
    setError(null);
    try {
      const [nextDevices, nextAssets, nextAlerts] = await Promise.all([
        listDevices(),
        listAssets(),
        listAlerts(),
      ]);
      if (
        workspaceRequestIdRef.current !== requestId
        || selectionGenerationRef.current !== selectionGeneration
      ) {
        return false;
      }
      setDevices(nextDevices);
      setAssets(nextAssets);
      setAlerts(nextAlerts);
      const requestedAssetId = selectedAssetIdRef.current;
      const nextAssetId = requestedAssetId !== null && nextAssets.some((asset) => asset.id === requestedAssetId)
        ? requestedAssetId
        : null;
      const requestedDeviceId = selectedDeviceIdRef.current;
      const nextDeviceId = nextAssetId !== null
        ? null
        : requestedDeviceId !== null && nextDevices.some((device) => device.id === requestedDeviceId)
          ? requestedDeviceId
          : nextDevices[0]?.id ?? null;
      selectedAssetIdRef.current = nextAssetId;
      selectedDeviceIdRef.current = nextDeviceId;
      setSelectedAssetId(nextAssetId);
      setSelectedDeviceId(nextDeviceId);
      return true;
    } catch (reason) {
      if (
        workspaceRequestIdRef.current === requestId
        && selectionGenerationRef.current === selectionGeneration
      ) {
        setError(errorMessage(reason));
      }
      return false;
    } finally {
      if (workspaceRequestIdRef.current === requestId) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const refreshTelemetry = useCallback(async (): Promise<void> => {
    const assetId = selectedAssetIdRef.current;
    const deviceId = selectedDeviceIdRef.current;
    if (assetId === null && deviceId === null) {
      telemetryRequestIdRef.current += 1;
      setTelemetry([]);
      return;
    }
    const requestId = telemetryRequestIdRef.current + 1;
    telemetryRequestIdRef.current = requestId;
    setTelemetryLoading(true);
    try {
      const points = assetId !== null
        ? await getAssetTelemetry(assetId, rangeRef.current)
        : await getDeviceTelemetry(deviceId as string, rangeRef.current);
      if (telemetryRequestIdRef.current === requestId) {
        setTelemetry(points);
      }
    } catch (reason) {
      if (telemetryRequestIdRef.current === requestId) {
        setError(errorMessage(reason));
      }
    } finally {
      if (telemetryRequestIdRef.current === requestId) {
        setTelemetryLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refreshTelemetry();
  }, [range, refreshTelemetry, selectedAssetId, selectedDeviceId]);

  const refreshWorkspaceAndTelemetry = useCallback(async (): Promise<void> => {
    if (await refresh()) {
      await refreshTelemetry();
    }
  }, [refresh, refreshTelemetry]);

  const sendCommand = async (
    method: string,
    params: Record<string, unknown>,
    mode: CommandMode = "one_way",
  ) => {
    if (selectedDevice === null) {
      return;
    }
    setCommandBusy(true);
    setError(null);
    try {
      const lifecycle = await sendDeviceCommandAndWait(
        selectedDevice.id,
        method,
        params,
        mode,
        { onProgress: setCommand },
      );
      setCommand(lifecycle);
      await refreshWorkspaceAndTelemetry();
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
            selectionGenerationRef.current += 1;
            selectedAssetIdRef.current = assetId;
            selectedDeviceIdRef.current = null;
            setSelectedAssetId(assetId);
            setSelectedDeviceId(null);
          }}
          onSelectDevice={(deviceId) => {
            selectionGenerationRef.current += 1;
            selectedAssetIdRef.current = null;
            selectedDeviceIdRef.current = deviceId;
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
            <TimeRangeControl
              onChange={(nextRange) => {
                rangeRef.current = nextRange;
                setRange(nextRange);
              }}
              value={range}
            />
            <button aria-label="Refresh Power Monitor" disabled={loading} onClick={() => void refreshWorkspaceAndTelemetry()} type="button">
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
              <span className="eyebrow">{selectedAsset !== null ? "Selected asset" : "Selected device"}</span>
              <h2>Telemetry</h2>
            </div>
            <span>{telemetryLoading ? "Updating" : telemetry.length + " samples"}</span>
          </header>
          {selectedDevice === null && selectedAsset === null ? (
            <>
              <p className="empty-state">Select a device to inspect telemetry and send commands.</p>
              <CommandPanel busy={false} disabled onSubmit={sendCommand} />
            </>
          ) : (
            <>
              <PowerTelemetryChart points={telemetry} />
              <PowerTelemetryTable points={telemetry} />
              {selectedDevice !== null && (
                <>
                  <DeviceControlPanel
                    brightnessPct={selectedDevice.brightness_pct}
                    busy={commandBusy}
                    canControl={selectedDevice.permission !== "viewer"}
                    capabilities={selectedDevice.capabilities}
                    commandState={command?.state ?? null}
                    onCommand={(method, params, mode) => void sendCommand(method, params, mode)}
                    switchState={selectedDevice.switch_state}
                  />
                  <CommandPanel busy={commandBusy} onSubmit={sendCommand} state={command?.state} />
                  <a className="detail-link" href={"/devices/" + encodeURIComponent(selectedDevice.id)}>Open device details</a>
                </>
              )}
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

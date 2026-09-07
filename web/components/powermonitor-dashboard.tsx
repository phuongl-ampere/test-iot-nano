"use client";

import { Activity, BatteryCharging, Copy, Gauge, RefreshCw, Save, Search, Trash2, Zap } from "lucide-react";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import {
  type ApiClient,
  type DeviceToken,
  type ManagementAsset,
  type ManagementAssetProfile,
  type ManagementDevice,
  type ManagementDeviceProfile,
  type PowerAsset,
  type PowerDevice,
  type PowerSummary,
  type PowerTelemetryPoint,
  type PowerTelemetryRecord,
  type Role,
  type TimeRange,
  UnauthorizedApiError,
  createManagementAsset,
  deleteManagementAsset,
  deleteManagementDevice,
  fetchManagementAssetProfiles,
  fetchManagementAssets,
  fetchManagementDeviceProfiles,
  fetchManagementDevices,
  fetchPowerAssets,
  fetchPowerAssetTelemetry,
  fetchPowerDeviceTelemetry,
  fetchPowerDeviceTelemetryRecords,
  fetchPowerDevices,
  fetchPowerSummary,
  logout,
  provisionManagementDevice,
  updateManagementAsset,
  updateManagementDevice,
} from "../lib/api";
import { browserDateTime } from "../lib/time";
import { AttributeEditor, DeviceDrawer, ManagementDrawer } from "./management-panels";
import { PowerMonitorAdminTools } from "./powermonitor-admin-tools";
import { PowerTelemetryChart } from "./power-telemetry-chart";
import { PowerTelemetryTable } from "./power-telemetry-table";
import { PowerMonitorTree } from "./powermonitor-tree";
import { ProfileMenu } from "./profile-menu";
import { TimeRangeControl } from "./time-range-control";
import { UserProfilePanel } from "./user-profile";

type PowerMonitorDashboardProps = {
  client: ApiClient;
  initialDeviceId?: string;
  role: Role;
  onUnauthorized(): void;
};

function formatted(value: number | null, unit: string, digits = 1): string {
  return value === null ? "--" : `${value.toFixed(digits)}${unit}`;
}

function lastSeen(value: string | null): string {
  return browserDateTime(value, "No signal");
}

function deviceStatus(device: PowerDevice): string {
  if (device.is_gateway) {
    return `Gateway ${device.gateway_status ?? (device.online ? "online" : "offline")}`;
  }
  if (device.gateway_device_id !== null && device.gateway_device_id !== undefined) {
    return `Child ${device.child_status ?? "unavailable"}`;
  }
  return device.online ? "Direct online" : "Direct offline";
}

export function PowerMonitorDashboard({
  client,
  initialDeviceId,
  role,
  onUnauthorized,
}: PowerMonitorDashboardProps) {
  const router = useRouter();
  const [summary, setSummary] = useState<PowerSummary | null>(null);
  const [assets, setAssets] = useState<PowerAsset[]>([]);
  const [devices, setDevices] = useState<PowerDevice[]>([]);
  const [selectedDeviceId, setSelectedDeviceId] = useState<string | null>(null);
  const [selectedAssetId, setSelectedAssetId] = useState<string | null>(null);
  const [points, setPoints] = useState<PowerTelemetryPoint[]>([]);
  const [records, setRecords] = useState<PowerTelemetryRecord[]>([]);
  const [range, setRange] = useState<TimeRange>("1h");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingTelemetry, setLoadingTelemetry] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [assetProfiles, setAssetProfiles] = useState<ManagementAssetProfile[]>([]);
  const [deviceProfiles, setDeviceProfiles] = useState<ManagementDeviceProfile[]>([]);
  const [adminView, setAdminView] = useState<"asset" | "device" | "assign" | null>(null);
  const [adminWorking, setAdminWorking] = useState(false);
  const [adminError, setAdminError] = useState<string | null>(null);
  const [assetName, setAssetName] = useState("");
  const [assetParentId, setAssetParentId] = useState("");
  const [assetProfileId, setAssetProfileId] = useState("");
  const [deviceName, setDeviceName] = useState("");
  const [deviceAssetId, setDeviceAssetId] = useState("");
  const [deviceProfileId, setDeviceProfileId] = useState("");
  const [assignedAssetId, setAssignedAssetId] = useState("");
  const [assignedProfileId, setAssignedProfileId] = useState("");
  const [editingAsset, setEditingAsset] = useState<ManagementAsset | null>(null);
  const [editingDevice, setEditingDevice] = useState<ManagementDevice | null>(null);
  const [editorAssets, setEditorAssets] = useState<ManagementAsset[]>([]);
  const [editorDevices, setEditorDevices] = useState<ManagementDevice[]>([]);
  const [createdToken, setCreatedToken] = useState<DeviceToken | null>(null);

  const selectedDevice = useMemo(
    () => devices.find((device) => device.device_id === selectedDeviceId) ?? null,
    [devices, selectedDeviceId],
  );
  const selectedAsset = useMemo(
    () => assets.find((asset) => asset.id === selectedAssetId) ?? null,
    [assets, selectedAssetId],
  );
  const selectedDeviceIsUnprofiled = selectedDevice !== null && selectedDevice.device_profile_id == null;
  const visibleAssets = useMemo(() => {
    const term = search.trim().toLowerCase();
    return term === "" ? assets : assets.filter((asset) => asset.name.toLowerCase().includes(term));
  }, [assets, search]);

  const loadWorkspace = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextSummary, nextAssets, nextDevices] = await Promise.all([
        fetchPowerSummary(client),
        fetchPowerAssets(client),
        fetchPowerDevices(client),
      ]);
      setSummary(nextSummary);
      setAssets(nextAssets);
      setDevices(nextDevices);
      setSelectedDeviceId((current) => (
        initialDeviceId !== undefined && nextDevices.some((device) => device.device_id === initialDeviceId)
          ? initialDeviceId
          : current !== null && nextDevices.some((device) => device.device_id === current)
          ? current
          : nextDevices[0]?.device_id ?? null
      ));
      setSelectedAssetId((current) => (
        current !== null && nextAssets.some((asset) => asset.id === current) ? current : null
      ));
    } catch (loadError) {
      if (loadError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(loadError instanceof Error ? loadError.message : "Power Monitor request failed.");
    } finally {
      setLoading(false);
    }
  }, [client, initialDeviceId, onUnauthorized]);

  const loadTelemetry = useCallback(async () => {
    if (selectedDeviceId === null && selectedAssetId === null) {
      setPoints([]);
      setRecords([]);
      return;
    }
    setLoadingTelemetry(true);
    try {
      if (selectedDevice !== null && selectedDevice.device_profile_id == null) {
        setPoints([]);
        setRecords(await fetchPowerDeviceTelemetryRecords(client, selectedDevice.device_id, range));
      } else {
        setRecords([]);
        setPoints(
          selectedDeviceId !== null
            ? await fetchPowerDeviceTelemetry(client, selectedDeviceId, range)
            : await fetchPowerAssetTelemetry(client, selectedAssetId ?? "", range),
        );
      }
    } catch (loadError) {
      if (loadError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(loadError instanceof Error ? loadError.message : "Power telemetry request failed.");
    } finally {
      setLoadingTelemetry(false);
    }
  }, [client, onUnauthorized, range, selectedAssetId, selectedDevice, selectedDeviceId]);

  useEffect(() => {
    void loadWorkspace();
  }, [loadWorkspace]);
  useEffect(() => {
    void loadTelemetry();
  }, [loadTelemetry]);
  useEffect(() => {
    if (role !== "admin") {
      return;
    }
    void Promise.all([fetchManagementAssetProfiles(client), fetchManagementDeviceProfiles(client)])
      .then(([nextAssetProfiles, nextDeviceProfiles]) => {
        setAssetProfiles(nextAssetProfiles);
        setDeviceProfiles(nextDeviceProfiles);
      })
      .catch((loadError) => {
        if (loadError instanceof UnauthorizedApiError) {
          onUnauthorized();
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "Power Monitor profile request failed.");
      });
  }, [client, onUnauthorized, role]);

  const signOut = () => {
    void logout(client).catch(() => undefined);
    onUnauthorized();
    router.replace("/");
  };

  const openAsset = () => {
    setAdminError(null);
    setCreatedToken(null);
    setAssetName("");
    setAssetParentId(selectedAssetId ?? "");
    setAssetProfileId("");
    setAdminView("asset");
  };
  const openDevice = () => {
    setAdminError(null);
    setCreatedToken(null);
    setDeviceName("");
    setDeviceAssetId(selectedAssetId ?? selectedDevice?.asset_id ?? "");
    setDeviceProfileId("");
    setAdminView("device");
  };
  const openAssignment = async () => {
    if (selectedDevice === null) {
      return;
    }
    setAdminError(null);
    try {
      const managed = await fetchManagementDevices(client);
      const device = managed.find((item) => item.device_id === selectedDevice.device_id);
      if (device === undefined) {
        throw new Error("Selected meter is unavailable.");
      }
      setAssignedAssetId(device.asset_id ?? "");
      setAssignedProfileId(device.device_profile_id ?? "");
      setAdminView("assign");
    } catch (assignError) {
      if (assignError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(assignError instanceof Error ? assignError.message : "Meter assignment request failed.");
    }
  };
  const openAssetEditor = async (assetId: string) => {
    setAdminError(null);
    try {
      const [nextAssets, nextProfiles] = await Promise.all([
        fetchManagementAssets(client),
        fetchManagementAssetProfiles(client),
      ]);
      const asset = nextAssets.find((item) => item.id === assetId);
      if (asset === undefined) {
        throw new Error("Selected location is unavailable.");
      }
      setEditorAssets(nextAssets);
      setAssetProfiles(nextProfiles);
      setSelectedAssetId(assetId);
      setSelectedDeviceId(null);
      setEditingAsset(asset);
    } catch (editError) {
      if (editError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(editError instanceof Error ? editError.message : "Location editor request failed.");
    }
  };
  const openDeviceEditor = async (deviceId: string) => {
    setAdminError(null);
    try {
      const [nextDevices, nextAssets, nextProfiles] = await Promise.all([
        fetchManagementDevices(client),
        fetchManagementAssets(client),
        fetchManagementDeviceProfiles(client),
      ]);
      const device = nextDevices.find((item) => item.device_id === deviceId);
      if (device === undefined) {
        throw new Error("Selected meter is unavailable.");
      }
      setEditorDevices(nextDevices);
      setEditorAssets(nextAssets);
      setDeviceProfiles(nextProfiles);
      setSelectedDeviceId(deviceId);
      setSelectedAssetId(null);
      setEditingDevice(device);
    } catch (editError) {
      if (editError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(editError instanceof Error ? editError.message : "Meter editor request failed.");
    }
  };
  const createAsset = async () => {
    if (assetName.trim() === "") {
      return;
    }
    setAdminWorking(true);
    setAdminError(null);
    try {
      const asset = await createManagementAsset(client, {
        name: assetName.trim(),
        asset_profile_id: assetProfileId || null,
        parent_asset_id: assetParentId || null,
        metadata: {},
        attributes: {},
      });
      setSelectedAssetId(asset.id);
      setSelectedDeviceId(null);
      setAdminView(null);
      await loadWorkspace();
    } catch (createError) {
      if (createError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(createError instanceof Error ? createError.message : "Location creation failed.");
    } finally {
      setAdminWorking(false);
    }
  };
  const createDevice = async () => {
    if (deviceName.trim() === "") {
      return;
    }
    setAdminWorking(true);
    setAdminError(null);
    try {
      const token = await provisionManagementDevice(client, deviceName.trim());
      await updateManagementDevice(client, token.device_id, {
        display_name: deviceName.trim(),
        asset_id: deviceAssetId || null,
        device_profile_id: deviceProfileId || null,
        attributes: {},
      });
      setCreatedToken(token);
      setSelectedDeviceId(token.device_id);
      setSelectedAssetId(null);
      await loadWorkspace();
    } catch (createError) {
      if (createError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(createError instanceof Error ? createError.message : "Meter provisioning failed.");
    } finally {
      setAdminWorking(false);
    }
  };
  const assignDevice = async () => {
    if (selectedDevice === null) {
      return;
    }
    setAdminWorking(true);
    setAdminError(null);
    try {
      const managed = await fetchManagementDevices(client);
      const device = managed.find((item) => item.device_id === selectedDevice.device_id);
      if (device === undefined) {
        throw new Error("Selected meter is unavailable.");
      }
      await updateManagementDevice(client, device.device_id, {
        display_name: device.display_name ?? device.device_id,
        asset_id: assignedAssetId || null,
        device_profile_id: assignedProfileId || null,
        attributes: device.attributes,
      });
      setAdminView(null);
      await loadWorkspace();
    } catch (assignError) {
      if (assignError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(assignError instanceof Error ? assignError.message : "Meter assignment failed.");
    } finally {
      setAdminWorking(false);
    }
  };
  const saveEditedAsset = async () => {
    if (editingAsset === null || editingAsset.name.trim() === "") {
      return;
    }
    setAdminWorking(true);
    setAdminError(null);
    try {
      await updateManagementAsset(client, editingAsset.id, {
        name: editingAsset.name.trim(),
        asset_profile_id: editingAsset.asset_profile_id,
        parent_asset_id: editingAsset.parent_asset_id,
        metadata: editingAsset.metadata,
        attributes: editingAsset.attributes,
      });
      setEditingAsset(null);
      await loadWorkspace();
    } catch (saveError) {
      if (saveError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(saveError instanceof Error ? saveError.message : "Location update failed.");
    } finally {
      setAdminWorking(false);
    }
  };
  const saveEditedDevice = async () => {
    if (editingDevice === null || (editingDevice.display_name ?? "").trim() === "") {
      return;
    }
    setAdminWorking(true);
    setAdminError(null);
    try {
      await updateManagementDevice(client, editingDevice.device_id, {
        display_name: editingDevice.display_name?.trim() ?? editingDevice.device_id,
        asset_id: editingDevice.asset_id,
        device_profile_id: editingDevice.device_profile_id,
        attributes: editingDevice.attributes,
        topology: {
          is_gateway: editingDevice.is_gateway ?? false,
          gateway_device_id: editingDevice.gateway_device_id ?? null,
        },
      });
      setEditingDevice(null);
      await loadWorkspace();
    } catch (saveError) {
      if (saveError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(saveError instanceof Error ? saveError.message : "Meter update failed.");
    } finally {
      setAdminWorking(false);
    }
  };
  const deleteEditedAsset = async () => {
    if (editingAsset === null || !window.confirm(`Delete ${editingAsset.name}?`)) {
      return;
    }
    setAdminWorking(true);
    try {
      await deleteManagementAsset(client, editingAsset.id);
      setEditingAsset(null);
      await loadWorkspace();
    } catch (deleteError) {
      if (deleteError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(deleteError instanceof Error ? deleteError.message : "Location deletion failed.");
    } finally {
      setAdminWorking(false);
    }
  };
  const deleteEditedDevice = async () => {
    if (editingDevice === null || !window.confirm(`Delete ${editingDevice.display_name ?? editingDevice.device_id}?`)) {
      return;
    }
    setAdminWorking(true);
    try {
      await deleteManagementDevice(client, editingDevice.device_id);
      setEditingDevice(null);
      await loadWorkspace();
    } catch (deleteError) {
      if (deleteError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setAdminError(deleteError instanceof Error ? deleteError.message : "Meter deletion failed.");
    } finally {
      setAdminWorking(false);
    }
  };

  if (profileOpen) {
    return (
      <main className="system-shell">
        <UserProfilePanel
          client={client}
          onBack={() => setProfileOpen(false)}
          onPasswordChanged={() => {
            onUnauthorized();
            router.replace("/");
          }}
          onUnauthorized={onUnauthorized}
          role={role}
        />
      </main>
    );
  }

  return (
    <main className="dashboard-shell">
      <aside className="directory">
        <div className="brand-lockup">
          <span className="brand-mark"><Zap aria-hidden="true" size={18} /></span>
          <span><strong>Power Monitor</strong><small>Operational energy view</small></span>
        </div>
        <label className="directory-search">
          <Search aria-hidden="true" size={15} />
          <input aria-label="Search devices and assets" onChange={(event) => setSearch(event.target.value)} placeholder="Search devices and assets" value={search} />
        </label>
        <div className="directory-heading">
          <div><span className="eyebrow">Asset tree</span><strong>{loading ? "Loading" : `${devices.length} meters`}</strong></div>
          <span className="fleet-online">{summary?.online_device_count ?? 0} online</span>
        </div>
        {role === "admin" && (
          <div className="powermonitor-setup-actions">
            <PowerMonitorAdminTools
              onAddAsset={openAsset}
              onAddDevice={openDevice}
              onAssignDevice={() => void openAssignment()}
              role={role}
              selectedDevice={selectedDevice !== null}
            />
          </div>
        )}
        <PowerMonitorTree
          assets={assets}
          devices={devices}
          onEditAsset={role === "admin" ? (assetId) => { void openAssetEditor(assetId); } : undefined}
          onEditDevice={role === "admin" ? (deviceId) => { void openDeviceEditor(deviceId); } : undefined}
          onSelectAsset={(assetId) => {
            setSelectedAssetId(assetId);
            setSelectedDeviceId(null);
          }}
          onSelectDevice={(deviceId) => {
            setSelectedDeviceId(deviceId);
            setSelectedAssetId(null);
          }}
          search={search}
          selectedAssetId={selectedAssetId}
          selectedDeviceId={selectedDeviceId}
        />
      </aside>

      <section className="telemetry-workspace">
        <header className="workspace-header">
          <div>
            <span className="eyebrow">Power Monitor</span>
            <div className="workspace-title-row">
              <h1>{selectedDevice?.display_name ?? selectedDevice?.device_id ?? selectedAsset?.name ?? "No device selected"}</h1>
            </div>
            {selectedDevice !== null && <p>{selectedDevice.device_id} · {deviceStatus(selectedDevice)} · Last seen {lastSeen(selectedDevice.last_seen_at)}</p>}
            {selectedAsset !== null && <p>{selectedAsset.device_count} devices · {formatted(selectedAsset.total_power_w, " W")}</p>}
          </div>
          <div className="workspace-actions">
            <TimeRangeControl onChange={setRange} value={range} />
            <button aria-label="Refresh Power Monitor" className="icon-button" disabled={loading || loadingTelemetry} onClick={() => { void loadWorkspace(); void loadTelemetry(); }} title="Refresh Power Monitor" type="button">
              <RefreshCw aria-hidden="true" size={17} />
            </button>
            <ProfileMenu onLogout={signOut} onOpenProfile={() => setProfileOpen(true)} onOpenSystemConfiguration={() => router.push("/management/settings")} role={role} />
          </div>
        </header>

        {error !== null && <div className="error-banner" role="alert">{error}</div>}
        {selectedDeviceIsUnprofiled ? (
          <section className="chart-section" aria-label="Raw telemetry records">
            <div className="section-heading">
              <div><span className="eyebrow">Unprofiled device</span><h2>Telemetry records</h2></div>
              <span>{loadingTelemetry ? "Updating" : `${records.length} records`}</span>
            </div>
            <PowerTelemetryTable records={records} />
          </section>
        ) : (
          <>
            <dl className="reading-strip">
              <div><dt><Zap aria-hidden="true" size={16} /> Total power</dt><dd>{formatted(summary?.total_power_w ?? null, " W")}</dd></div>
              <div><dt><BatteryCharging aria-hidden="true" size={16} /> Energy</dt><dd>{formatted(summary?.total_energy_kwh ?? null, " kWh", 2)}</dd></div>
              <div><dt><Gauge aria-hidden="true" size={16} /> Voltage</dt><dd>{formatted(selectedDevice?.voltage_v ?? null, " V")}</dd></div>
              <div><dt><Activity aria-hidden="true" size={16} /> Current</dt><dd>{formatted(selectedDevice?.current_a ?? null, " A")}</dd></div>
            </dl>

            <section className="chart-section" aria-label="Power telemetry chart">
              <div className="section-heading">
                <div><span className="eyebrow">Selected device</span><h2>Power trace</h2></div>
                <span>{loadingTelemetry ? "Updating" : `${points.length} samples`}</span>
              </div>
              <PowerTelemetryChart points={points} />
            </section>

            <section className="readings-section" aria-label="Power device readings">
              <div className="section-heading">
                <div><span className="eyebrow">Current values</span><h2>Devices</h2></div>
                {selectedDevice !== null && <Link href={`/apps/powermonitor/devices/${selectedDevice.device_id}`}>Open detail</Link>}
              </div>
              <div className="readings-table-wrap">
                <table>
                  <thead><tr><th>Device</th><th>Voltage</th><th>Current</th><th>Power</th><th>Energy</th></tr></thead>
                  <tbody>
                    {devices.map((device) => <tr key={device.device_id}><td>{device.display_name ?? device.device_id}</td><td>{formatted(device.voltage_v, " V")}</td><td>{formatted(device.current_a, " A")}</td><td>{formatted(device.power_w, " W")}</td><td>{formatted(device.energy_kwh, " kWh", 2)}</td></tr>)}
                    {devices.length === 0 && <tr><td className="table-empty" colSpan={5}>No devices registered.</td></tr>}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="asset-summary" aria-label="Asset summary">
              <div className="section-heading"><div><span className="eyebrow">Assets</span><h2>Sites and equipment</h2></div><span>{assets.length} assets</span></div>
              <div className="asset-list">
                {visibleAssets.map((asset) => <Link href={`/apps/powermonitor/assets/${asset.id}`} key={asset.id}>{asset.name}<span>{formatted(asset.total_power_w, " W")} · {formatted(asset.total_energy_kwh, " kWh", 2)}</span></Link>)}
                {visibleAssets.length === 0 && <p className="chart-empty">No assets matched this view.</p>}
              </div>
            </section>
          </>
        )}
      </section>
      {adminView === "asset" && (
        <ManagementDrawer
          eyebrow="Power Monitor location"
          onClose={() => setAdminView(null)}
          subtitle="Add site, panel, or equipment location"
          title="Add location"
        >
          <form className="device-drawer-form" onSubmit={(event) => { event.preventDefault(); void createAsset(); }}>
            <label><span>Location name</span><input aria-label="Power Monitor location name" onChange={(event) => setAssetName(event.target.value)} value={assetName} /></label>
            <label><span>Parent location</span><select aria-label="Power Monitor parent location" onChange={(event) => setAssetParentId(event.target.value)} value={assetParentId}><option value="">Top level</option>{assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
            <label><span>Asset profile</span><select aria-label="Power Monitor asset profile" onChange={(event) => setAssetProfileId(event.target.value)} value={assetProfileId}><option value="">Unclassified</option>{assetProfiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
            {adminError !== null && <p className="system-error" role="alert">{adminError}</p>}
            <footer className="device-drawer-actions"><span /><button className="system-save" disabled={adminWorking} type="submit"><Zap aria-hidden="true" size={16} />Add location</button></footer>
          </form>
        </ManagementDrawer>
      )}
      {adminView === "device" && (
        <ManagementDrawer
          eyebrow="Power Monitor meter"
          onClose={() => { setAdminView(null); setCreatedToken(null); }}
          subtitle={createdToken === null ? "Provision a meter for a location" : "Copy the device token before closing"}
          title="Add meter"
        >
          <form className="device-drawer-form" onSubmit={(event) => { event.preventDefault(); void createDevice(); }}>
            <label><span>Meter name</span><input aria-label="Power Monitor meter name" onChange={(event) => setDeviceName(event.target.value)} value={deviceName} /></label>
            <label><span>Location</span><select aria-label="Power Monitor meter location" onChange={(event) => setDeviceAssetId(event.target.value)} value={deviceAssetId}><option value="">Unassigned</option>{assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
            <label><span>Meter profile</span><select aria-label="Power Monitor meter profile" onChange={(event) => setDeviceProfileId(event.target.value)} value={deviceProfileId}><option value="">Unclassified</option>{deviceProfiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
            {createdToken !== null && <div className="device-token-secret"><input aria-label="Power Monitor new meter token" readOnly value={createdToken.token ?? ""} /><button aria-label="Copy Power Monitor meter token" className="icon-button" onClick={() => void navigator.clipboard?.writeText(createdToken.token ?? "")} title="Copy Power Monitor meter token" type="button"><Copy aria-hidden="true" size={16} /></button></div>}
            {adminError !== null && <p className="system-error" role="alert">{adminError}</p>}
            <footer className="device-drawer-actions"><span />{createdToken === null && <button className="system-save" disabled={adminWorking} type="submit"><Zap aria-hidden="true" size={16} />Provision meter</button>}</footer>
          </form>
        </ManagementDrawer>
      )}
      {adminView === "assign" && selectedDevice !== null && (
        <ManagementDrawer
          eyebrow="Power Monitor meter"
          onClose={() => setAdminView(null)}
          subtitle={selectedDevice.device_id}
          title={`Assign ${selectedDevice.display_name ?? selectedDevice.device_id}`}
        >
          <form className="device-drawer-form" onSubmit={(event) => { event.preventDefault(); void assignDevice(); }}>
            <label><span>Location</span><select aria-label="Assign meter location" onChange={(event) => setAssignedAssetId(event.target.value)} value={assignedAssetId}><option value="">Unassigned</option>{assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
            <label><span>Meter profile</span><select aria-label="Assign meter profile" onChange={(event) => setAssignedProfileId(event.target.value)} value={assignedProfileId}><option value="">Unclassified</option>{deviceProfiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
            {adminError !== null && <p className="system-error" role="alert">{adminError}</p>}
            <footer className="device-drawer-actions"><span /><button className="system-save" disabled={adminWorking} type="submit"><Zap aria-hidden="true" size={16} />Assign meter</button></footer>
          </form>
        </ManagementDrawer>
      )}
      {editingAsset !== null && (
        <ManagementDrawer
          eyebrow="Power Monitor location"
          onClose={() => setEditingAsset(null)}
          subtitle={editingAsset.id}
          title={editingAsset.name}
        >
          <form className="device-drawer-form" onSubmit={(event) => { event.preventDefault(); void saveEditedAsset(); }}>
            <label><span>Name</span><input aria-label="Power Monitor location name" autoFocus onChange={(event) => setEditingAsset({ ...editingAsset, name: event.target.value })} value={editingAsset.name} /></label>
            <label><span>Parent location</span><select aria-label="Power Monitor location parent" onChange={(event) => setEditingAsset({ ...editingAsset, parent_asset_id: event.target.value || null })} value={editingAsset.parent_asset_id ?? ""}><option value="">Top level</option>{editorAssets.filter((asset) => asset.id !== editingAsset.id).map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
            <label><span>Asset profile</span><select aria-label="Power Monitor location profile" onChange={(event) => setEditingAsset({ ...editingAsset, asset_profile_id: event.target.value || null })} value={editingAsset.asset_profile_id ?? ""}><option value="">Unclassified</option>{assetProfiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
            <AttributeEditor label="Location attributes" onChange={(attributes) => setEditingAsset({ ...editingAsset, attributes, metadata: attributes })} value={editingAsset.attributes} />
            {adminError !== null && <p className="system-error" role="alert">{adminError}</p>}
            <footer className="device-drawer-actions"><button aria-label="Delete location" className="icon-button destructive-icon" disabled={adminWorking} onClick={() => void deleteEditedAsset()} title="Delete location" type="button"><Trash2 aria-hidden="true" size={16} /></button><button className="system-save" disabled={adminWorking} type="submit"><Save aria-hidden="true" size={16} />Save location</button></footer>
          </form>
        </ManagementDrawer>
      )}
      {editingDevice !== null && (
        <DeviceDrawer
          assets={editorAssets}
          client={client}
          device={editingDevice}
          error={adminError}
          gateways={editorDevices.filter((device) => device.is_gateway && device.device_id !== editingDevice.device_id)}
          onChange={setEditingDevice}
          onClose={() => setEditingDevice(null)}
          onDelete={() => void deleteEditedDevice()}
          onSave={() => void saveEditedDevice()}
          onUnauthorized={onUnauthorized}
          profiles={deviceProfiles}
          token={null}
          working={adminWorking}
        />
      )}
    </main>
  );
}

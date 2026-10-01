"use client";

import { type FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  BffApiError,
  acceptResourceInvitation,
  cancelResourceInvitation,
  claimDevice,
  createAsset,
  getAssetLiveView,
  getAssetTelemetry,
  getDeviceLiveView,
  getDeviceTelemetry,
  listAlerts,
  listAssets,
  listDevices,
  listResourceProfiles,
  listResourceInvitations,
  listUserCapabilities,
  sendDeviceCommandAndWait,
  updateDevice,
  type Alert,
  type Asset,
  type CommandLifecycle,
  type CommandMode,
  type Device,
  type LiveView,
  type ResourceInvitation,
  type TelemetryPoint,
  type TimeRange,
} from "../lib/browser-api";
import { powerProfilePresentation } from "../lib/power-profiles";
import { CommandPanel } from "./command-panel";
import { DeviceControlPanel } from "./device-control-panel";
import { LiveTelemetryCharts } from "./live-telemetry-charts";
import { PowerTelemetryTable } from "./power-telemetry-table";
import { PowerMonitorTree } from "./powermonitor-tree";
import { ResourceEditDrawer } from "./resource-edit-drawer";
import { TimeRangeControl } from "./time-range-control";

type PowerMonitorDashboardProps = {
  initialAssetId?: string;
  initialDeviceId?: string;
};

type BarcodeDetectorLike = {
  detect(source: ImageBitmapSource): Promise<Array<{ rawValue?: string }>>;
};

type BarcodeDetectorConstructor = new (options: { formats: string[] }) => BarcodeDetectorLike;

function parsePairingQr(value: string): { serialNumber: string; code: string } {
  const uri = new URL(value);
  if (uri.protocol !== "iotnano:" || uri.hostname !== "claim") {
    throw new Error("This QR code is not an IoT Nano pairing code.");
  }
  const serialNumber = uri.searchParams.get("serial_number")?.trim() ?? "";
  const code = uri.searchParams.get("code")?.trim() ?? "";
  if (serialNumber.length === 0 || code.length === 0) {
    throw new Error("The pairing QR code is incomplete.");
  }
  return { serialNumber, code };
}

export function PowerMonitorDashboard({
  initialAssetId,
  initialDeviceId,
}: PowerMonitorDashboardProps) {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [assetProfileNames, setAssetProfileNames] = useState<Record<string, string>>({});
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [assetBusy, setAssetBusy] = useState(false);
  const [assetName, setAssetName] = useState("");
  const [assetOpen, setAssetOpen] = useState(false);
  const [assetParentId, setAssetParentId] = useState("");
  const [command, setCommand] = useState<CommandLifecycle | null>(null);
  const [commandBusy, setCommandBusy] = useState(false);
  const [claimBusy, setClaimBusy] = useState(false);
  const [claimCode, setClaimCode] = useState("");
  const [claimSerialNumber, setClaimSerialNumber] = useState("");
  const [claimOpen, setClaimOpen] = useState(false);
  const [capabilities, setCapabilities] = useState<string[]>([]);
  const [devices, setDevices] = useState<Device[]>([]);
  const [deviceProfileNames, setDeviceProfileNames] = useState<Record<string, string>>({});
  const [deviceAssignments, setDeviceAssignments] = useState<Record<string, string>>({});
  const [deviceAssignmentBusyId, setDeviceAssignmentBusyId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [invitationBusyId, setInvitationBusyId] = useState<string | null>(null);
  const [invitations, setInvitations] = useState<ResourceInvitation[]>([]);
  const [invitationsOpen, setInvitationsOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [liveView, setLiveView] = useState<LiveView>({ charts: [], profile: null });
  const [liveViewLoading, setLiveViewLoading] = useState(false);
  const [editingResource, setEditingResource] = useState<{
    asset_id?: string | null;
    id: string;
    kind: "asset" | "device";
    name?: string;
    parent_id?: string | null;
    permission?: Device["permission"] | Asset["permission"];
  } | null>(null);
  const [range, setRange] = useState<TimeRange>("1h");
  const [selectedAssetId, setSelectedAssetId] = useState<string | null>(initialAssetId ?? null);
  const [selectedDeviceId, setSelectedDeviceId] = useState<string | null>(initialDeviceId ?? null);
  const [sidebarTab, setSidebarTab] = useState<"assets" | "devices">("assets");
  const [telemetry, setTelemetry] = useState<TelemetryPoint[]>([]);
  const [telemetryLoading, setTelemetryLoading] = useState(false);
  const selectedAssetIdRef = useRef<string | null>(initialAssetId ?? null);
  const selectedDeviceIdRef = useRef<string | null>(initialDeviceId ?? null);
  const selectionGenerationRef = useRef(0);
  const telemetryRequestIdRef = useRef(0);
  const liveViewRequestIdRef = useRef(0);
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
  const selectedProfileName = selectedDevice?.device_profile_id
    ? deviceProfileNames[selectedDevice.device_profile_id]
    : selectedAsset?.asset_profile_id
      ? assetProfileNames[selectedAsset.asset_profile_id]
      : undefined;
  const onlineCount = devices.filter((device) => device.online).length;
  const selectedResource = selectedAsset !== null
    ? {
      id: selectedAsset.id,
      kind: "asset" as const,
      name: selectedAsset.name,
      parent_id: selectedAsset.parent_id,
      permission: selectedAsset.permission,
      asset_profile_id: selectedAsset.asset_profile_id,
      profile_name: selectedProfileName,
    }
    : selectedDevice !== null
      ? {
        asset_id: selectedDevice.asset_id,
        id: selectedDevice.id,
        kind: "device" as const,
        name: selectedDevice.name,
        permission: selectedDevice.permission,
        device_profile_id: selectedDevice.device_profile_id,
        profile_name: selectedProfileName,
      }
      : null;
  const canManageSelectedResource = selectedResource?.permission === "manager"
    || selectedResource?.permission === "owner";
  const canCreateAssets = capabilities.includes("create_assets");
  const canClaimDevices = capabilities.includes("claim_devices");
  const canAssignDevicesToAssets = capabilities.includes("assign_devices_to_assets");
  const unassignedDevices = useMemo(
    () => devices.filter((device) => device.asset_id === null || device.asset_id === undefined),
    [devices],
  );
  const assignableAssets = useMemo(
    () => assets.filter((asset) => asset.permission === "manager" || asset.permission === "owner"),
    [assets],
  );
  const resourcePath = useMemo(
    () => getResourcePath(assets, selectedAsset, selectedDevice),
    [assets, selectedAsset, selectedDevice],
  );
  const profilePresentation = powerProfilePresentation(
    selectedDevice !== null ? "device" : "asset",
    selectedProfileName,
  );
  const effectiveLiveView = liveView.profile !== null || profilePresentation === null
    ? liveView
    : {
      charts: profilePresentation.charts,
      profile: {
        id: selectedDevice?.device_profile_id ?? selectedAsset?.asset_profile_id ?? profilePresentation.label,
        name: profilePresentation.label,
      },
    };

  const refresh = useCallback(async (): Promise<boolean> => {
    const requestId = workspaceRequestIdRef.current + 1;
    const selectionGeneration = selectionGenerationRef.current;
    workspaceRequestIdRef.current = requestId;
    setLoading(true);
    setError(null);
    try {
      const [[nextDevices, nextAssets, nextAlerts], [nextDeviceProfiles, nextAssetProfiles], invitationRead, nextCapabilities] = await Promise.all([
        Promise.all([listDevices(), listAssets(), listAlerts()]),
        Promise.all([listResourceProfiles("device").catch(() => []), listResourceProfiles("asset").catch(() => [])]),
        readResourceInvitations(),
        readUserCapabilities(),
      ]);
      if (
        workspaceRequestIdRef.current !== requestId
        || selectionGenerationRef.current !== selectionGeneration
      ) {
        return false;
      }
      setDevices(nextDevices);
      setAssets(nextAssets);
      setDeviceProfileNames(Object.fromEntries(nextDeviceProfiles.map((profile) => [profile.id, profile.name])));
      setAssetProfileNames(Object.fromEntries(nextAssetProfiles.map((profile) => [profile.id, profile.name])));
      setAlerts(nextAlerts);
      setCapabilities(nextCapabilities);
      setInvitations(invitationRead.items);
      if (invitationRead.warning !== null) {
        setError(invitationRead.warning);
      }
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

  const refreshLiveView = useCallback(async (): Promise<void> => {
    const assetId = selectedAssetIdRef.current;
    const deviceId = selectedDeviceIdRef.current;
    const target = assetId !== null
      ? { id: assetId, type: "asset" as const }
      : deviceId !== null
        ? { id: deviceId, type: "device" as const }
        : null;
    const requestId = liveViewRequestIdRef.current + 1;
    liveViewRequestIdRef.current = requestId;
    if (target === null) {
      setLiveView({ charts: [], profile: null });
      setLiveViewLoading(false);
      return;
    }
    setLiveViewLoading(true);
    setLiveView({ charts: [], profile: null });
    try {
      const view = target.type === "asset"
        ? await getAssetLiveView(target.id)
        : await getDeviceLiveView(target.id);
      if (liveViewRequestIdRef.current === requestId) {
        setLiveView(view);
      }
    } catch (reason) {
      if (liveViewRequestIdRef.current === requestId) {
        setError(errorMessage(reason));
      }
    } finally {
      if (liveViewRequestIdRef.current === requestId) {
        setLiveViewLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refreshLiveView();
  }, [refreshLiveView, selectedAssetId, selectedDeviceId]);

  useEffect(() => {
    const refreshVisibleTelemetry = () => {
      if (!document.hidden) {
        void refreshTelemetry();
      }
    };
    const interval = window.setInterval(refreshVisibleTelemetry, 5_000);
    document.addEventListener("visibilitychange", refreshVisibleTelemetry);
    return () => {
      window.clearInterval(interval);
      document.removeEventListener("visibilitychange", refreshVisibleTelemetry);
    };
  }, [refreshTelemetry]);

  const refreshWorkspaceAndTelemetry = useCallback(async (): Promise<void> => {
    if (await refresh()) {
      await refreshTelemetry();
    }
  }, [refresh, refreshTelemetry]);

  const submitAsset = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const name = assetName.trim();
    if (name.length === 0) {
      return;
    }
    setAssetBusy(true);
    setError(null);
    try {
      const asset = await createAsset({
        name,
        parent_asset_id: assetParentId === "" ? null : assetParentId,
      });
      setAssetName("");
      setAssetParentId("");
      setAssetOpen(false);
      selectedAssetIdRef.current = asset.id;
      selectedDeviceIdRef.current = null;
      setSelectedAssetId(asset.id);
      setSelectedDeviceId(null);
      await refreshWorkspaceAndTelemetry();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setAssetBusy(false);
    }
  };

  const submitDeviceClaim = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const serialNumber = claimSerialNumber.trim();
    const code = claimCode.trim();
    if (serialNumber.length === 0 || code.length === 0) {
      return;
    }
    setClaimBusy(true);
    setError(null);
    try {
      const device = await claimDevice(serialNumber, code);
      setClaimCode("");
      setClaimSerialNumber("");
      setClaimOpen(false);
      selectedAssetIdRef.current = null;
      selectedDeviceIdRef.current = device.id;
      setSelectedAssetId(null);
      setSelectedDeviceId(device.id);
      await refreshWorkspaceAndTelemetry();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setClaimBusy(false);
    }
  };

  const assignUnassignedDevice = async (device: Device) => {
    const assetId = deviceAssignments[device.id];
    if (
      assetId === undefined
      || !canAssignDevicesToAssets
      || (device.permission !== "manager" && device.permission !== "owner")
      || !assignableAssets.some((asset) => asset.id === assetId)
    ) {
      return;
    }
    setDeviceAssignmentBusyId(device.id);
    setError(null);
    try {
      await updateDevice(device.id, { asset_id: assetId });
      setDeviceAssignments((current) => {
        const next = { ...current };
        delete next[device.id];
        return next;
      });
      await refreshWorkspaceAndTelemetry();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setDeviceAssignmentBusyId(null);
    }
  };

  const scanClaimQr = async (file: File) => {
    const BarcodeDetector = (window as Window & { BarcodeDetector?: BarcodeDetectorConstructor }).BarcodeDetector;
    if (!BarcodeDetector) {
      setError("QR scanning is unavailable in this browser. Enter the serial number and pairing code manually.");
      return;
    }
    setClaimBusy(true);
    setError(null);
    try {
      const bitmap = await createImageBitmap(file);
      try {
        const code = (await new BarcodeDetector({ formats: ["qr_code"] }).detect(bitmap))[0]?.rawValue;
        if (!code) throw new Error("No QR code was found in the selected image.");
        const pairing = parsePairingQr(code);
        setClaimSerialNumber(pairing.serialNumber);
        setClaimCode(pairing.code);
      } finally {
        bitmap.close();
      }
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setClaimBusy(false);
    }
  };

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

  const respondToInvitation = async (invitationId: string, action: "accept" | "cancel") => {
    setInvitationBusyId(invitationId);
    setError(null);
    try {
      if (action === "accept") {
        await acceptResourceInvitation(invitationId);
      } else {
        await cancelResourceInvitation(invitationId);
      }
      setInvitations((current) => current.filter((invitation) => invitation.id !== invitationId));
      if (action === "accept") {
        await refreshWorkspaceAndTelemetry();
      } else {
        await refresh();
      }
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setInvitationBusyId(null);
    }
  };

  const title = selectedDevice?.name ?? selectedDevice?.id ?? selectedAsset?.name ?? "Fleet overview";
  return (
    <main className="powermonitor-shell">
      <aside className="explorer">
        <header className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">P</span>
          <strong>Power Monitor</strong>
        </header>
        <div className="explorer-heading">
          <strong>Assets</strong>
          <span>{loading ? "Loading" : assets.length + " assets"}</span>
          <span>{onlineCount} online</span>
        </div>
        <PowerMonitorTree
          assets={assets}
          devices={devices}
          onSelectAsset={(assetId) => {
            selectionGenerationRef.current += 1;
            setEditingResource(null);
            selectedAssetIdRef.current = assetId;
            selectedDeviceIdRef.current = null;
            setSelectedAssetId(assetId);
            setSelectedDeviceId(null);
          }}
          onSelectDevice={(deviceId) => {
            selectionGenerationRef.current += 1;
            setEditingResource(null);
            selectedAssetIdRef.current = null;
            selectedDeviceIdRef.current = deviceId;
            setSelectedDeviceId(deviceId);
            setSelectedAssetId(null);
          }}
          selectedAssetId={selectedAssetId}
          selectedDeviceId={selectedDeviceId}
        />
        <div aria-label="Workspace views" className="explorer-tabs" role="tablist">
          <button
            aria-selected={sidebarTab === "assets"}
            onClick={() => setSidebarTab("assets")}
            role="tab"
            type="button"
          >
            Assets
          </button>
          <button
            aria-selected={sidebarTab === "devices"}
            onClick={() => setSidebarTab("devices")}
            role="tab"
            type="button"
          >
            Devices{unassignedDevices.length > 0 ? ` (${unassignedDevices.length})` : ""}
          </button>
        </div>
      </aside>

      <section className="workspace">
        <header className="workspace-header">
          <div>
            <nav aria-label="Resource path" className="resource-path">
              <ol>
                {resourcePath.map((entry, index) => <li key={entry + index}>{entry}</li>)}
              </ol>
            </nav>
            <h1>{title}</h1>
            <div className="resource-meta">
              {selectedDevice !== null && (
                <span className={selectedDevice.online ? "resource-status online" : "resource-status"}>
                  {selectedDevice.online ? "Online" : "Offline"}
                </span>
              )}
              {selectedProfileName !== undefined && <span>{selectedProfileName}</span>}
              <span>{selectedDevice?.id ?? selectedAsset?.id ?? "All accessible resources"}</span>
            </div>
          </div>
          <div className="workspace-actions">
            <button
              aria-controls="resource-invitations"
              aria-expanded={invitationsOpen}
              onClick={() => setInvitationsOpen((open) => !open)}
              type="button"
            >
              {invitations.length === 0 ? "Invitations" : "Invitations (" + invitations.length + ")"}
            </button>
            {canClaimDevices && (
              <button aria-expanded={claimOpen} onClick={() => setClaimOpen((open) => !open)} type="button">
                Add device
              </button>
            )}
            {canCreateAssets && (
              <button aria-expanded={assetOpen} onClick={() => setAssetOpen((open) => !open)} type="button">
                Add asset
              </button>
            )}
            {selectedResource !== null && canManageSelectedResource && (
              <button onClick={() => setEditingResource(selectedResource)} type="button">
                Edit {selectedResource.kind}
              </button>
            )}
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
            <form action="/api/auth/logout" method="post">
              <button type="submit">Sign out</button>
            </form>
          </div>
        </header>

        {sidebarTab === "devices" && (
          <section aria-label="Unassigned devices" className="unassigned-device-panel">
            <header className="section-heading">
              <div>
                <span className="eyebrow">Devices</span>
                <h2>Unassigned devices</h2>
              </div>
              <span>{unassignedDevices.length} awaiting an Asset</span>
            </header>
            {unassignedDevices.length === 0 ? (
              <p className="empty-state">Every visible device is assigned to an Asset.</p>
            ) : (
              <ul className="unassigned-device-list">
                {unassignedDevices.map((device) => {
                  const canAssign = canAssignDevicesToAssets
                    && (device.permission === "manager" || device.permission === "owner")
                    && assignableAssets.length > 0;
                  const selectedAssignment = deviceAssignments[device.id] ?? "";
                  const busy = deviceAssignmentBusyId === device.id;
                  return (
                    <li key={device.id}>
                      <div>
                        <strong>{device.name ?? device.id}</strong>
                        <span>{device.serial_number ?? device.id}</span>
                      </div>
                      {canAssign ? (
                        <div className="unassigned-device-actions">
                          <select
                            aria-label={`Assign ${device.name ?? device.id} to asset`}
                            disabled={busy}
                            onChange={(event) => setDeviceAssignments((current) => ({
                              ...current,
                              [device.id]: event.target.value,
                            }))}
                            value={selectedAssignment}
                          >
                            <option value="">Select an Asset</option>
                            {assignableAssets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}
                          </select>
                          <button
                            disabled={busy || selectedAssignment === ""}
                            onClick={() => void assignUnassignedDevice(device)}
                            type="button"
                          >
                            {busy ? "Assigning" : "Assign to asset"}
                          </button>
                        </div>
                      ) : (
                        <span className="permission-note">You need manager access and Assign devices to assets to assign this device.</span>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
        )}

        {invitationsOpen && (
          <section aria-label="Pending invitations" className="invitation-panel" id="resource-invitations">
            <header className="section-heading">
              <div>
                <span className="eyebrow">Resource access</span>
                <h2>Invitations</h2>
              </div>
            </header>
            {invitations.length === 0 ? (
              <p className="empty-state">No pending invitations.</p>
            ) : (
              <ul className="invitation-list">
                {invitations.map((invitation) => (
                  <li key={invitation.id}>
                    <div>
                      <strong>{invitation.resource_name}</strong>
                      <span>{invitation.sender_username} shared {invitation.permission} access to this {invitation.resource_kind}.</span>
                    </div>
                    <div className="invitation-actions">
                      <button
                        aria-label={"Accept invitation for " + invitation.resource_name}
                        disabled={invitationBusyId === invitation.id}
                        onClick={() => void respondToInvitation(invitation.id, "accept")}
                        type="button"
                      >
                        Accept
                      </button>
                      <button
                        aria-label={"Decline invitation for " + invitation.resource_name}
                        disabled={invitationBusyId === invitation.id}
                        onClick={() => void respondToInvitation(invitation.id, "cancel")}
                        type="button"
                      >
                        Decline
                      </button>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>
        )}

        {claimOpen && (
          <section aria-label="Add device" className="claim-device-panel">
            <header className="section-heading"><h2>Add device</h2></header>
            <form onSubmit={(event) => void submitDeviceClaim(event)}>
              <label>
                <span>Serial number</span>
                <input autoComplete="off" disabled={claimBusy} onChange={(event) => setClaimSerialNumber(event.target.value)} value={claimSerialNumber} />
              </label>
              <label>
                <span>Pairing code</span>
                <input autoComplete="one-time-code" disabled={claimBusy} onChange={(event) => setClaimCode(event.target.value)} value={claimCode} />
              </label>
              <label>
                <span>Scan pairing QR</span>
                <input
                  accept="image/*"
                  capture="environment"
                  disabled={claimBusy}
                  onChange={(event) => {
                    const file = event.currentTarget.files?.[0];
                    event.currentTarget.value = "";
                    if (file) void scanClaimQr(file);
                  }}
                  type="file"
                />
              </label>
              <div className="invitation-actions">
                <button disabled={claimBusy || claimSerialNumber.trim().length === 0 || claimCode.trim().length === 0} type="submit">Add device</button>
                <button disabled={claimBusy} onClick={() => setClaimOpen(false)} type="button">Cancel</button>
              </div>
            </form>
          </section>
        )}

        {assetOpen && (
          <section aria-label="Add asset" className="claim-device-panel">
            <header className="section-heading"><h2>Add asset</h2></header>
            <form onSubmit={(event) => void submitAsset(event)}>
              <label>
                <span>Asset name</span>
                <input autoComplete="off" disabled={assetBusy} onChange={(event) => setAssetName(event.target.value)} required value={assetName} />
              </label>
              <label>
                <span>Parent asset</span>
                <select aria-label="Parent asset" disabled={assetBusy} onChange={(event) => setAssetParentId(event.target.value)} value={assetParentId}>
                  <option value="">No parent</option>
                  {assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}
                </select>
              </label>
              <div className="invitation-actions">
                <button disabled={assetBusy || assetName.trim().length === 0} type="submit">{assetBusy ? "Creating" : "Create asset"}</button>
                <button disabled={assetBusy} onClick={() => setAssetOpen(false)} type="button">Cancel</button>
              </div>
            </form>
          </section>
        )}

        {error !== null && (
          <div className="error-banner" role="alert">
            <span>{error}</span>
            {(error === "Sign in is required." || error === invitationScopeMessage) && (
              <a href="/api/auth/login">Sign in again</a>
            )}
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
              {liveViewLoading ? (
                <p className="empty-state">Loading configured live view...</p>
              ) : effectiveLiveView.profile === null ? (
                <PowerTelemetryTable points={telemetry} />
              ) : effectiveLiveView.charts.length === 0 ? (
                <p className="empty-state">This profile has no live widgets configured.</p>
              ) : (
                <LiveTelemetryCharts
                  charts={effectiveLiveView.charts}
                  isAsset={selectedAsset !== null}
                  points={telemetry}
                  range={range}
                />
              )}
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
                  <CommandPanel
                    busy={commandBusy}
                    onSubmit={sendCommand}
                    response={command?.response}
                    state={command?.state}
                  />
                </>
              )}
            </>
          )}
        </section>
      </section>
      {editingResource !== null && (
        <ResourceEditDrawer
          assets={assets}
          onClose={() => setEditingResource(null)}
          onSaved={async () => {
            await refreshWorkspaceAndTelemetry();
            await refreshLiveView();
          }}
          resource={editingResource}
        />
      )}
    </main>
  );
}

function errorMessage(reason: unknown): string {
  if (reason instanceof BffApiError && reason.status === 401) {
    return "Sign in is required.";
  }
  return reason instanceof Error ? reason.message : "PowerMonitor request failed.";
}

const invitationScopeMessage = "Sign in again to use resource invitations.";

type InvitationRead = {
  items: ResourceInvitation[];
  warning: string | null;
};

async function readResourceInvitations(): Promise<InvitationRead> {
  try {
    return { items: await listResourceInvitations(), warning: null };
  } catch (reason) {
    if (reason instanceof BffApiError && reason.status === 403) {
      return { items: [], warning: invitationScopeMessage };
    }
    throw reason;
  }
}

async function readUserCapabilities(): Promise<string[]> {
  try {
    return await listUserCapabilities();
  } catch {
    return [];
  }
}

function getResourcePath(
  assets: Asset[],
  selectedAsset: Asset | null,
  selectedDevice: Device | null,
): string[] {
  const assetsById = new Map(assets.map((asset) => [asset.id, asset]));
  const path: string[] = [];
  const visited = new Set<string>();
  let assetId = selectedAsset?.id ?? selectedDevice?.asset_id ?? null;

  while (assetId !== null && !visited.has(assetId)) {
    visited.add(assetId);
    const asset = assetsById.get(assetId);
    if (asset === undefined) {
      break;
    }
    path.unshift(asset.name);
    assetId = asset.parent_id ?? null;
  }

  if (selectedDevice !== null) {
    path.push(selectedDevice.name ?? selectedDevice.id);
  }

  return path.length > 0 ? path : ["All resources"];
}

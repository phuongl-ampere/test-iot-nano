"use client";

import { Copy, Pencil, Plus, Save, Trash2, X } from "lucide-react";
import Link from "next/link";
import { FormEvent, useCallback, useEffect, useState } from "react";

import {
  type ApiClient,
  type ManagementAsset,
  type ManagementAssetProfile,
  type ManagementDevice,
  type ManagementDeviceProfile,
  type ManagementUser,
  UnauthorizedApiError,
  createManagementAsset,
  createManagementAssetProfile,
  createManagementDeviceProfile,
  deleteManagementAsset,
  deleteManagementAssetProfile,
  deleteManagementDevice,
  deleteManagementDeviceProfile,
  fetchManagementAssetProfiles,
  fetchManagementAssets,
  fetchManagementDeviceProfiles,
  fetchManagementDevices,
  fetchManagementUsers,
  provisionManagementDevice,
  updateManagementDevice,
  updateManagementAsset,
  updateManagementAssetProfile,
  updateManagementDeviceProfile,
  updateManagementUser,
} from "../lib/api";
import { browserDateTime } from "../lib/time";
import { DeviceTokenPanel } from "./device-token-panel";
import type { ManagementSection } from "./management-app";

type ManagementPanelProps = {
  client: ApiClient;
  onUnauthorized(): void;
  section: ManagementSection;
};

type PanelProps = Omit<ManagementPanelProps, "section">;
export const managementDeviceRefreshMilliseconds = 5_000;

function useManagementError(onUnauthorized: () => void) {
  const [error, setError] = useState<string | null>(null);
  const handle = useCallback((reason: unknown) => {
    if (reason instanceof UnauthorizedApiError) {
      onUnauthorized();
      return;
    }
    setError(reason instanceof Error ? reason.message : "Management request failed.");
  }, [onUnauthorized]);
  return { error, setError, handle };
}

function jsonObject(value: string, label: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(value);
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
    throw new Error(`${label} must be a JSON object.`);
  }
  return parsed as Record<string, unknown>;
}

type Attributes = Record<string, unknown>;

function telemetryTimestamp(value: string | null): string {
  return browserDateTime(value, "No telemetry");
}

export function AttributeEditor({
  label,
  onChange,
  value,
}: {
  label: string;
  onChange(value: Attributes): void;
  value: Attributes;
}) {
  const entries = Object.entries(value);
  const [keyError, setKeyError] = useState<string | null>(null);
  const rename = (key: string, nextKey: string) => {
    const normalizedKey = nextKey.trim();
    if (normalizedKey !== "" && normalizedKey !== key && Object.hasOwn(value, normalizedKey)) {
      setKeyError(`Attribute key "${normalizedKey}" already exists.`);
      return;
    }
    const next = { ...value };
    delete next[key];
    if (normalizedKey !== "") {
      next[normalizedKey] = value[key];
    }
    setKeyError(null);
    onChange(next);
  };
  const updateValue = (key: string, rawValue: string) => {
    let nextValue: unknown = rawValue;
    try {
      nextValue = JSON.parse(rawValue);
    } catch {
      // Bare text remains a string; JSON literals preserve number, boolean, null, arrays and objects.
    }
    setKeyError(null);
    onChange({ ...value, [key]: nextValue });
  };
  const add = () => {
    let index = 1;
    while (Object.hasOwn(value, `attribute_${index}`)) {
      index += 1;
    }
    setKeyError(null);
    onChange({ ...value, [`attribute_${index}`]: "" });
  };
  return (
    <fieldset className="attribute-editor">
      <legend>{label}</legend>
      {entries.map(([key, entryValue]) => (
        <div className="attribute-row" key={key}>
          <input aria-label={`${label} key ${key}`} onChange={(event) => rename(key, event.target.value)} value={key} />
          <input aria-label={`${label} value ${key}`} onChange={(event) => updateValue(key, event.target.value)} value={JSON.stringify(entryValue) ?? "null"} />
          <button aria-label={`Delete ${label} ${key}`} className="icon-button destructive-icon" onClick={() => rename(key, "")} title={`Delete ${label} ${key}`} type="button"><Trash2 aria-hidden="true" size={15} /></button>
        </div>
      ))}
      {keyError !== null && <p className="attribute-error" role="alert">{keyError}</p>}
      <button aria-label={`Add ${label}`} className="icon-button" onClick={add} title={`Add ${label}`} type="button"><Plus aria-hidden="true" size={15} /></button>
    </fieldset>
  );
}

export function DeviceDrawer({
  assets,
  client,
  device,
  error = null,
  onChange,
  onClose,
  onDelete,
  onSave,
  onUnauthorized,
  profiles,
  token,
  working,
  gateways,
}: {
  assets: ManagementAsset[];
  client: ApiClient;
  device: ManagementDevice;
  error?: string | null;
  onChange(device: ManagementDevice): void;
  onClose(): void;
  onDelete(): void;
  onSave(): void;
  onUnauthorized(): void;
  profiles: ManagementDeviceProfile[];
  token: string | null;
  working: boolean;
  gateways: ManagementDevice[];
}) {
  const status = device.child_status ?? device.gateway_status ?? (device.online ? "online" : "offline");
  return (
    <div className="device-drawer-backdrop" onMouseDown={onClose}>
      <aside
        aria-label="Device details"
        aria-modal="true"
        className="device-drawer"
        onMouseDown={(event) => event.stopPropagation()}
        role="dialog"
      >
        <header className="device-drawer-header">
          <div>
            <span className="eyebrow">Device</span>
            <h2>{device.display_name ?? device.device_id}</h2>
            <p>{device.device_id}</p>
            <dl className="device-drawer-telemetry">
              <div><dt>Status</dt><dd className={device.online ? "value-online" : "value-offline"}>{status}</dd></div>
              <div><dt>Last telemetry</dt><dd>{telemetryTimestamp(device.last_seen_at)}</dd></div>
            </dl>
          </div>
          <button aria-label="Close device details" className="icon-button" onClick={onClose} title="Close device details" type="button">
            <X aria-hidden="true" size={17} />
          </button>
        </header>
        <form className="device-drawer-form" onSubmit={(event) => { event.preventDefault(); onSave(); }}>
          <label><span>Display name</span><input aria-label="Drawer device name" onChange={(event) => onChange({ ...device, display_name: event.target.value })} value={device.display_name ?? ""} /></label>
          <label><span>Asset</span><select aria-label="Drawer device asset" onChange={(event) => onChange({ ...device, asset_id: event.target.value || null })} value={device.asset_id ?? ""}><option value="">Unassigned</option>{assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
          <label><span>Device profile</span><select aria-label="Drawer device profile" onChange={(event) => onChange({ ...device, device_profile_id: event.target.value || null })} value={device.device_profile_id ?? ""}><option value="">Unassigned</option>{profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
          <label className="system-toggle"><input aria-label="Gateway device" checked={device.is_gateway ?? false} onChange={(event) => onChange({ ...device, is_gateway: event.target.checked, gateway_device_id: null })} type="checkbox" /><span>Gateway device</span></label>
          {!device.is_gateway && <label><span>Gateway</span><select aria-label="Gateway" onChange={(event) => onChange({ ...device, gateway_device_id: event.target.value || null })} value={device.gateway_device_id ?? ""}><option value="">Direct connection</option>{gateways.map((gateway) => <option key={gateway.device_id} value={gateway.device_id}>{gateway.display_name ?? gateway.device_id}</option>)}</select></label>}
          {token !== null && <div className="device-token-secret"><input aria-label="New device token" readOnly value={token} /><button aria-label="Copy new device token" className="icon-button" onClick={() => void navigator.clipboard?.writeText(token)} title="Copy new device token" type="button"><Copy aria-hidden="true" size={16} /></button></div>}
          {device.gateway_device_id == null && <DeviceTokenPanel allowProvisioning={false} client={client} deviceId={device.device_id} onUnauthorized={onUnauthorized} />}
          <AttributeEditor label="Device attributes" onChange={(attributes) => onChange({ ...device, attributes })} value={device.attributes} />
          {error !== null && <p className="system-error" role="alert">{error}</p>}
          <footer className="device-drawer-actions">
            <button aria-label="Delete device" className="icon-button destructive-icon" disabled={working} onClick={onDelete} title="Delete device" type="button"><Trash2 aria-hidden="true" size={16} /></button>
            <button className="system-save" disabled={working} type="submit"><Save aria-hidden="true" size={16} />Save device</button>
          </footer>
        </form>
      </aside>
    </div>
  );
}

export function ManagementDrawer({
  children,
  eyebrow,
  onClose,
  subtitle,
  title,
}: {
  children: React.ReactNode;
  eyebrow: string;
  onClose(): void;
  subtitle: string;
  title: string;
}) {
  return (
    <div className="device-drawer-backdrop" onMouseDown={onClose}>
      <aside
        aria-label={`${eyebrow} details`}
        aria-modal="true"
        className="device-drawer"
        onMouseDown={(event) => event.stopPropagation()}
        role="dialog"
      >
        <header className="device-drawer-header">
          <div>
            <span className="eyebrow">{eyebrow}</span>
            <h2>{title}</h2>
            <p>{subtitle}</p>
          </div>
          <button aria-label={`Close ${eyebrow} details`} className="icon-button" onClick={onClose} title={`Close ${eyebrow} details`} type="button">
            <X aria-hidden="true" size={17} />
          </button>
        </header>
        {children}
      </aside>
    </div>
  );
}

function Overview({ client, onUnauthorized }: PanelProps) {
  const [devices, setDevices] = useState<ManagementDevice[]>([]);
  const [assets, setAssets] = useState<ManagementAsset[]>([]);
  const { error, handle } = useManagementError(onUnauthorized);
  useEffect(() => {
    void Promise.all([fetchManagementDevices(client), fetchManagementAssets(client)])
      .then(([nextDevices, nextAssets]) => {
        setDevices(nextDevices);
        setAssets(nextAssets);
      })
      .catch(handle);
  }, [client, handle]);
  return (
    <section className="management-section">
      {error !== null && <p className="system-error" role="alert">{error}</p>}
      <dl className="reading-strip">
        <div><dt>Devices</dt><dd>{devices.length}</dd></div>
        <div><dt>Online</dt><dd>{devices.filter((device) => device.online).length}</dd></div>
        <div><dt>Assets</dt><dd>{assets.length}</dd></div>
      </dl>
    </section>
  );
}

function Devices({ client, onUnauthorized }: PanelProps) {
  const [devices, setDevices] = useState<ManagementDevice[]>([]);
  const [assets, setAssets] = useState<ManagementAsset[]>([]);
  const [profiles, setProfiles] = useState<ManagementDeviceProfile[]>([]);
  const [selectedDeviceId, setSelectedDeviceId] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [name, setName] = useState("");
  const [token, setToken] = useState<string | null>(null);
  const [working, setWorking] = useState(false);
  const { error, setError, handle } = useManagementError(onUnauthorized);
  const refresh = useCallback(() => {
    void Promise.all([
      fetchManagementDevices(client),
      fetchManagementAssets(client),
      fetchManagementDeviceProfiles(client),
    ]).then(([nextDevices, nextAssets, nextProfiles]) => {
      setDevices(nextDevices);
      setAssets(nextAssets);
      setProfiles(nextProfiles);
      setSelectedDeviceId((current) => (
        nextDevices.some((device) => device.device_id === current)
          ? current
          : nextDevices[0]?.device_id ?? ""
      ));
    }).catch(handle);
  }, [client, handle]);
  useEffect(() => {
    refresh();
    const intervalId = window.setInterval(refresh, managementDeviceRefreshMilliseconds);
    return () => window.clearInterval(intervalId);
  }, [refresh]);
  const selected = devices.find((device) => device.device_id === selectedDeviceId) ?? null;
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (name.trim() === "") return;
    setWorking(true);
    setError(null);
    try {
      const created = await provisionManagementDevice(client, name.trim());
      setToken(created.token ?? null);
      setSelectedDeviceId(created.device_id);
      setDrawerOpen(true);
      setName("");
      refresh();
    } catch (reason) {
      handle(reason);
    } finally {
      setWorking(false);
    }
  };
  const saveSelected = async () => {
    if (selected === null) return;
    setWorking(true);
    try {
      await updateManagementDevice(client, selected.device_id, {
        display_name: selected.display_name ?? selected.device_id,
        asset_id: selected.asset_id,
        device_profile_id: selected.device_profile_id,
        attributes: selected.attributes,
        topology: {
          is_gateway: selected.is_gateway ?? false,
          gateway_device_id: selected.gateway_device_id ?? null,
        },
      });
      refresh();
      setDrawerOpen(false);
    } catch (reason) {
      handle(reason);
    } finally {
      setWorking(false);
    }
  };
  const remove = async (target = selected) => {
    if (target === null || !window.confirm(`Delete ${target.display_name ?? target.device_id}?`)) return;
    setWorking(true);
    try {
      await deleteManagementDevice(client, target.device_id);
      setToken(null);
      setSelectedDeviceId("");
      setDrawerOpen(false);
      refresh();
    } catch (reason) {
      handle(reason);
    } finally {
      setWorking(false);
    }
  };
  return (
    <section className="management-section">
      <form className="management-create" onSubmit={(event) => void submit(event)}>
        <label><span>Device name</span><input aria-label="Device name" onChange={(event) => setName(event.target.value)} value={name} /></label>
        <button className="system-save" disabled={working} type="submit"><Plus aria-hidden="true" size={16} />Provision device</button>
      </form>
      {error !== null && <p className="system-error" role="alert">{error}</p>}
      <ManagementTable headings={["Device", "Asset", "Profile", "Status", "Actions"]}>
        {devices.map((device) => <tr key={device.device_id}><td><button className="management-row-open" onClick={() => { setToken(null); setSelectedDeviceId(device.device_id); setDrawerOpen(true); }} type="button">{device.display_name ?? device.device_id}<small>{device.device_id}</small></button></td><td>{device.asset_id ?? "--"}</td><td>{device.device_profile_id ?? "--"}</td><td>{device.child_status ?? device.gateway_status ?? (device.online ? "Online" : "Offline")}</td><td className="management-row-actions"><button aria-label={`Edit ${device.device_id}`} className="icon-button" onClick={() => { setToken(null); setSelectedDeviceId(device.device_id); setDrawerOpen(true); }} title="Edit device" type="button"><Pencil aria-hidden="true" size={15} /></button><button aria-label={`Delete ${device.device_id}`} className="icon-button destructive-icon" onClick={() => void remove(device)} title="Delete device" type="button"><Trash2 aria-hidden="true" size={15} /></button></td></tr>)}
      </ManagementTable>
      {drawerOpen && selected !== null && (
        <DeviceDrawer
          assets={assets}
          client={client}
          device={selected}
          gateways={devices.filter((device) => device.is_gateway && device.device_id !== selected.device_id)}
          onChange={(device) => setDevices((current) => current.map((item) => item.device_id === device.device_id ? device : item))}
          onClose={() => { setDrawerOpen(false); setToken(null); }}
          onDelete={() => void remove()}
          onSave={() => void saveSelected()}
          onUnauthorized={onUnauthorized}
          profiles={profiles}
          token={token}
          working={working}
        />
      )}
    </section>
  );
}

function Assets({ client, onUnauthorized }: PanelProps) {
  const [assets, setAssets] = useState<ManagementAsset[]>([]);
  const [profiles, setProfiles] = useState<ManagementAssetProfile[]>([]);
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [name, setName] = useState("");
  const [parentAssetId, setParentAssetId] = useState("");
  const [profileId, setProfileId] = useState("");
  const [attributes, setAttributes] = useState<Attributes>({});
  const { error, setError, handle } = useManagementError(onUnauthorized);
  const refresh = useCallback(() => {
    void Promise.all([fetchManagementAssets(client), fetchManagementAssetProfiles(client)])
      .then(([nextAssets, nextProfiles]) => {
        setAssets(nextAssets);
        setProfiles(nextProfiles);
        setSelectedAssetId((current) => (
          nextAssets.some((asset) => asset.id === current)
            ? current
            : nextAssets[0]?.id ?? ""
        ));
      })
      .catch(handle);
  }, [client, handle]);
  useEffect(refresh, [refresh]);
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (name.trim() === "") return;
    try {
      setError(null);
      await createManagementAsset(client, {
        name: name.trim(),
        asset_profile_id: profileId || null,
        parent_asset_id: parentAssetId || null,
        metadata: {},
        attributes,
      });
      setName("");
      setParentAssetId("");
      setProfileId("");
      setAttributes({});
      refresh();
    } catch (reason) {
      handle(reason);
    }
  };
  const selected = assets.find((asset) => asset.id === selectedAssetId) ?? null;
  const save = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (selected === null || selected.name.trim() === "") return;
    try {
      await updateManagementAsset(client, selected.id, {
        name: selected.name.trim(),
        asset_profile_id: selected.asset_profile_id,
        parent_asset_id: selected.parent_asset_id,
        metadata: selected.metadata,
        attributes: selected.attributes,
      });
      refresh();
      setDrawerOpen(false);
    } catch (reason) {
      handle(reason);
    }
  };
  const remove = async (target = selected) => {
    if (target === null || !window.confirm(`Delete ${target.name}?`)) return;
    try {
      await deleteManagementAsset(client, target.id);
      setSelectedAssetId("");
      setDrawerOpen(false);
      refresh();
    } catch (reason) {
      handle(reason);
    }
  };
  return (
    <section className="management-section">
      <form className="management-create management-create-wide" onSubmit={(event) => void submit(event)}>
        <label><span>Asset name</span><input aria-label="Asset name" onChange={(event) => setName(event.target.value)} value={name} /></label>
        <label><span>Parent asset</span><select aria-label="Parent asset" onChange={(event) => setParentAssetId(event.target.value)} value={parentAssetId}><option value="">None</option>{assets.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
        <label><span>Asset profile</span><select aria-label="Asset profile" onChange={(event) => setProfileId(event.target.value)} value={profileId}><option value="">Unassigned</option>{profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
        <AttributeEditor label="Asset attributes" onChange={setAttributes} value={attributes} />
        <button className="system-save" type="submit"><Plus aria-hidden="true" size={16} />Create asset</button>
      </form>
      {error !== null && <p className="system-error" role="alert">{error}</p>}
      <ManagementTable headings={["Asset", "Parent", "Profile", "Actions"]}>{assets.map((asset) => <tr key={asset.id}><td><button className="management-row-open" onClick={() => { setSelectedAssetId(asset.id); setDrawerOpen(true); }} type="button">{asset.name}<small>{asset.id}</small></button></td><td>{asset.parent_asset_id ?? "--"}</td><td>{asset.asset_profile_id ?? "--"}</td><td className="management-row-actions"><button aria-label={`Edit ${asset.name}`} className="icon-button" onClick={() => { setSelectedAssetId(asset.id); setDrawerOpen(true); }} title="Edit asset" type="button"><Pencil aria-hidden="true" size={15} /></button><button aria-label={`Delete ${asset.name}`} className="icon-button destructive-icon" onClick={() => void remove(asset)} title="Delete asset" type="button"><Trash2 aria-hidden="true" size={15} /></button></td></tr>)}</ManagementTable>
      {drawerOpen && selected !== null && (
        <ManagementDrawer
          eyebrow="Asset"
          onClose={() => setDrawerOpen(false)}
          subtitle={selected.id}
          title={selected.name}
        >
          <form className="device-drawer-form" onSubmit={(event) => void save(event)}>
            <label><span>Name</span><input aria-label="Drawer asset name" onChange={(event) => setAssets((current) => current.map((asset) => asset.id === selected.id ? { ...asset, name: event.target.value } : asset))} value={selected.name} /></label>
            <label><span>Parent asset</span><select aria-label="Drawer asset parent" onChange={(event) => setAssets((current) => current.map((asset) => asset.id === selected.id ? { ...asset, parent_asset_id: event.target.value || null } : asset))} value={selected.parent_asset_id ?? ""}><option value="">None</option>{assets.filter((asset) => asset.id !== selected.id).map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}</select></label>
            <label><span>Asset profile</span><select aria-label="Drawer asset profile" onChange={(event) => setAssets((current) => current.map((asset) => asset.id === selected.id ? { ...asset, asset_profile_id: event.target.value || null } : asset))} value={selected.asset_profile_id ?? ""}><option value="">Unassigned</option>{profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
            <AttributeEditor label="Asset attributes" onChange={(attributes) => setAssets((current) => current.map((asset) => asset.id === selected.id ? { ...asset, attributes, metadata: attributes } : asset))} value={selected.attributes} />
            <footer className="device-drawer-actions"><button aria-label="Delete asset" className="icon-button destructive-icon" onClick={() => void remove()} title="Delete asset" type="button"><Trash2 aria-hidden="true" size={16} /></button><button className="system-save" type="submit"><Save aria-hidden="true" size={16} />Save asset</button></footer>
          </form>
        </ManagementDrawer>
      )}
    </section>
  );
}

function Profiles({ client, onUnauthorized, kind }: PanelProps & { kind: "device" | "asset" }) {
  const [name, setName] = useState("");
  const [firstJson, setFirstJson] = useState("{}");
  const [secondJson, setSecondJson] = useState("{}");
  const [thirdJson, setThirdJson] = useState("{}");
  const [deviceProfiles, setDeviceProfiles] = useState<ManagementDeviceProfile[]>([]);
  const [assetProfiles, setAssetProfiles] = useState<ManagementAssetProfile[]>([]);
  const [selectedProfileId, setSelectedProfileId] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editName, setEditName] = useState("");
  const [editFirstJson, setEditFirstJson] = useState("{}");
  const [editSecondJson, setEditSecondJson] = useState("{}");
  const [editThirdJson, setEditThirdJson] = useState("{}");
  const { error, setError, handle } = useManagementError(onUnauthorized);
  const profiles = kind === "device" ? deviceProfiles : assetProfiles;
  const refresh = useCallback(() => {
    const request = kind === "device" ? fetchManagementDeviceProfiles(client) : fetchManagementAssetProfiles(client);
    void request.then((profiles) => kind === "device" ? setDeviceProfiles(profiles as ManagementDeviceProfile[]) : setAssetProfiles(profiles as ManagementAssetProfile[])).catch(handle);
  }, [client, handle, kind]);
  useEffect(refresh, [refresh]);
  useEffect(() => {
    setSelectedProfileId((current) => (
      profiles.some((profile) => profile.id === current)
        ? current
        : profiles[0]?.id ?? ""
    ));
  }, [profiles]);
  const selected = profiles.find((profile) => profile.id === selectedProfileId) ?? null;
  useEffect(() => {
    if (selected === null) return;
    setEditName(selected.name);
    if (kind === "device") {
      const profile = selected as ManagementDeviceProfile;
      setEditFirstJson(JSON.stringify(profile.telemetry_schema, null, 2));
      setEditSecondJson(JSON.stringify(profile.metric_mapping, null, 2));
      setEditThirdJson(JSON.stringify(profile.reporting_settings, null, 2));
    } else {
      const profile = selected as ManagementAssetProfile;
      setEditFirstJson(JSON.stringify(profile.fields, null, 2));
      setEditSecondJson(JSON.stringify(profile.dashboard_defaults, null, 2));
      setEditThirdJson("{}");
    }
  }, [kind, selected]);
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (name.trim() === "") return;
    try {
      setError(null);
      if (kind === "device") {
        await createManagementDeviceProfile(client, {
          name: name.trim(),
          telemetry_schema: jsonObject(firstJson, "Telemetry schema"),
          metric_mapping: jsonObject(secondJson, "Metric mapping"),
          reporting_settings: jsonObject(thirdJson, "Reporting settings"),
        });
      } else {
        await createManagementAssetProfile(client, {
          name: name.trim(),
          fields: jsonObject(firstJson, "Fields"),
          dashboard_defaults: jsonObject(secondJson, "Dashboard defaults"),
        });
      }
      setName("");
      setFirstJson("{}");
      setSecondJson("{}");
      setThirdJson("{}");
      refresh();
    } catch (reason) {
      handle(reason);
    }
  };
  const saveEdit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (selected === null || editName.trim() === "") return;
    try {
      if (kind === "device") {
        await updateManagementDeviceProfile(client, selected.id, {
          name: editName.trim(),
          telemetry_schema: jsonObject(editFirstJson, "Telemetry schema"),
          metric_mapping: jsonObject(editSecondJson, "Metric mapping"),
          reporting_settings: jsonObject(editThirdJson, "Reporting settings"),
        });
      } else {
        await updateManagementAssetProfile(client, selected.id, {
          name: editName.trim(),
          fields: jsonObject(editFirstJson, "Fields"),
          dashboard_defaults: jsonObject(editSecondJson, "Dashboard defaults"),
        });
      }
      refresh();
      setDrawerOpen(false);
    } catch (reason) {
      handle(reason);
    }
  };
  const remove = async (target = selected) => {
    if (target === null || !window.confirm(`Delete ${target.name}?`)) return;
    try {
      if (kind === "device") {
        await deleteManagementDeviceProfile(client, target.id);
      } else {
        await deleteManagementAssetProfile(client, target.id);
      }
      setSelectedProfileId("");
      setDrawerOpen(false);
      refresh();
    } catch (reason) {
      handle(reason);
    }
  };
  return (
    <section className="management-section">
      <form className="management-profile-form" onSubmit={(event) => void submit(event)}>
        <label><span>{kind === "device" ? "Device" : "Asset"} profile name</span><input aria-label={`${kind} profile name`} onChange={(event) => setName(event.target.value)} value={name} /></label>
        <label><span>{kind === "device" ? "Telemetry schema JSON" : "Fields JSON"}</span><textarea aria-label={kind === "device" ? "Telemetry schema JSON" : "Fields JSON"} onChange={(event) => setFirstJson(event.target.value)} value={firstJson} /></label>
        <label><span>{kind === "device" ? "Metric mapping JSON" : "Dashboard defaults JSON"}</span><textarea aria-label={kind === "device" ? "Metric mapping JSON" : "Dashboard defaults JSON"} onChange={(event) => setSecondJson(event.target.value)} value={secondJson} /></label>
        {kind === "device" && <label><span>Reporting settings JSON</span><textarea aria-label="Reporting settings JSON" onChange={(event) => setThirdJson(event.target.value)} value={thirdJson} /></label>}
        <button className="system-save" type="submit"><Plus aria-hidden="true" size={16} />Create profile</button>
      </form>
      {error !== null && <p className="system-error" role="alert">{error}</p>}
      <ManagementTable headings={["Profile", "ID", "Actions"]}>{profiles.map((profile) => <tr key={profile.id}><td><button className="management-row-open" onClick={() => { setSelectedProfileId(profile.id); setDrawerOpen(true); }} type="button">{profile.name}</button></td><td>{profile.id}</td><td className="management-row-actions"><button aria-label={`Edit ${profile.name}`} className="icon-button" onClick={() => { setSelectedProfileId(profile.id); setDrawerOpen(true); }} title="Edit profile" type="button"><Pencil aria-hidden="true" size={15} /></button><button aria-label={`Delete ${profile.name}`} className="icon-button destructive-icon" onClick={() => void remove(profile)} title="Delete profile" type="button"><Trash2 aria-hidden="true" size={15} /></button></td></tr>)}</ManagementTable>
      {drawerOpen && selected !== null && (
        <ManagementDrawer
          eyebrow={kind === "device" ? "Device profile" : "Asset profile"}
          onClose={() => setDrawerOpen(false)}
          subtitle={selected.id}
          title={selected.name}
        >
          <form className="device-drawer-form" onSubmit={(event) => void saveEdit(event)}>
            <label><span>Name</span><input aria-label="Drawer profile name" onChange={(event) => setEditName(event.target.value)} value={editName} /></label>
            <label><span>{kind === "device" ? "Telemetry schema JSON" : "Fields JSON"}</span><textarea aria-label={`Drawer ${kind} profile first JSON`} onChange={(event) => setEditFirstJson(event.target.value)} value={editFirstJson} /></label>
            <label><span>{kind === "device" ? "Metric mapping JSON" : "Dashboard defaults JSON"}</span><textarea aria-label={`Drawer ${kind} profile second JSON`} onChange={(event) => setEditSecondJson(event.target.value)} value={editSecondJson} /></label>
            {kind === "device" && <label><span>Reporting settings JSON</span><textarea aria-label="Drawer device profile reporting JSON" onChange={(event) => setEditThirdJson(event.target.value)} value={editThirdJson} /></label>}
            <footer className="device-drawer-actions"><button aria-label="Delete profile" className="icon-button destructive-icon" onClick={() => void remove()} title="Delete profile" type="button"><Trash2 aria-hidden="true" size={16} /></button><button className="system-save" type="submit"><Save aria-hidden="true" size={16} />Save profile</button></footer>
          </form>
        </ManagementDrawer>
      )}
    </section>
  );
}

function Users({ client, onUnauthorized }: PanelProps) {
  const [users, setUsers] = useState<ManagementUser[]>([]);
  const [selectedUsername, setSelectedUsername] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const { error, handle } = useManagementError(onUnauthorized);
  const refresh = useCallback(() => void fetchManagementUsers(client).then((nextUsers) => {
    setUsers(nextUsers);
    setSelectedUsername((current) => (
      nextUsers.some((user) => user.username === current)
        ? current
        : nextUsers[0]?.username ?? ""
    ));
  }).catch(handle), [client, handle]);
  useEffect(refresh, [refresh]);
  const selected = users.find((user) => user.username === selectedUsername) ?? null;
  const save = async (user: ManagementUser) => {
    try {
      await updateManagementUser(client, user.username, { default_app: user.default_app, granted_apps: user.granted_apps });
      refresh();
      setDrawerOpen(false);
    } catch (reason) {
      handle(reason);
    }
  };
  return (
    <section className="management-section">
      {error !== null && <p className="system-error" role="alert">{error}</p>}
      <ManagementTable headings={["Username", "Role", "Default app", "App grants", "Actions"]}>
        {users.map((user) => (
          <tr key={user.id}>
            <td><button className="management-row-open" onClick={() => { setSelectedUsername(user.username); setDrawerOpen(true); }} type="button">{user.username}</button></td>
            <td>{user.role}</td>
            <td>{user.default_app}</td>
            <td>{user.granted_apps.join(", ")}</td>
            <td className="management-row-actions"><button aria-label={`Edit ${user.username}`} className="icon-button" onClick={() => { setSelectedUsername(user.username); setDrawerOpen(true); }} title="Edit user access" type="button"><Pencil aria-hidden="true" size={15} /></button></td>
          </tr>
        ))}
      </ManagementTable>
      {drawerOpen && selected !== null && (
        <ManagementDrawer
          eyebrow="User"
          onClose={() => setDrawerOpen(false)}
          subtitle={selected.role}
          title={selected.username}
        >
          <form className="device-drawer-form" onSubmit={(event) => { event.preventDefault(); void save(selected); }}>
            <label><span>Default app</span><input aria-label="Drawer user default app" onChange={(event) => setUsers((current) => current.map((user) => user.id === selected.id ? { ...user, default_app: event.target.value } : user))} value={selected.default_app} /></label>
            <label><span>App grants</span><input aria-label="Drawer user app grants" onChange={(event) => setUsers((current) => current.map((user) => user.id === selected.id ? { ...user, granted_apps: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) } : user))} value={selected.granted_apps.join(", ")} /></label>
            <footer className="device-drawer-actions"><span /><button className="system-save" type="submit"><Save aria-hidden="true" size={16} />Save user</button></footer>
          </form>
        </ManagementDrawer>
      )}
    </section>
  );
}

function Apps() {
  return (
    <section className="management-section">
      <ManagementTable headings={["App", "Route", "Grant"]}>
        <tr>
          <td>Power Monitor</td>
          <td><Link href="/apps/powermonitor">/apps/powermonitor</Link></td>
          <td>powermonitor</td>
        </tr>
      </ManagementTable>
    </section>
  );
}

function ManagementTable({ children, headings }: { children: React.ReactNode; headings: string[] }) {
  return <div className="readings-table-wrap management-table"><table><thead><tr>{headings.map((heading) => <th key={heading}>{heading}</th>)}</tr></thead><tbody>{children}</tbody></table></div>;
}

export function ManagementPanel({ client, onUnauthorized, section }: ManagementPanelProps) {
  if (section === "overview") return <Overview client={client} onUnauthorized={onUnauthorized} />;
  if (section === "devices") return <Devices client={client} onUnauthorized={onUnauthorized} />;
  if (section === "assets") return <Assets client={client} onUnauthorized={onUnauthorized} />;
  if (section === "apps") return <Apps />;
  if (section === "users") return <Users client={client} onUnauthorized={onUnauthorized} />;
  return <Profiles client={client} kind={section === "device-profiles" ? "device" : "asset"} onUnauthorized={onUnauthorized} />;
}

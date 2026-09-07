"use client";

import { ChevronDown, ChevronRight, Cpu, FolderTree, Pencil, Radio } from "lucide-react";
import { useMemo, useState } from "react";

import type { PowerAsset, PowerDevice } from "../lib/api";

type PowerMonitorTreeProps = {
  assets: PowerAsset[];
  devices: PowerDevice[];
  onEditAsset?(assetId: string): void;
  onEditDevice?(deviceId: string): void;
  onSelectAsset(assetId: string): void;
  onSelectDevice(deviceId: string): void;
  search: string;
  selectedAssetId: string | null;
  selectedDeviceId: string | null;
};

export function PowerMonitorTree({
  assets,
  devices,
  onEditAsset,
  onEditDevice,
  onSelectAsset,
  onSelectDevice,
  search,
  selectedAssetId,
  selectedDeviceId,
}: PowerMonitorTreeProps) {
  const [collapsedAssets, setCollapsedAssets] = useState<Set<string>>(() => new Set());
  const query = search.trim().toLowerCase();
  const { assetsById, childrenByParent, devicesByAsset, rootAssets, unassignedDevices } = useMemo(() => {
    const nextAssetsById = new Map(assets.map((asset) => [asset.id, asset]));
    const nextChildren = new Map<string, PowerAsset[]>();
    const roots: PowerAsset[] = [];
    for (const asset of assets) {
      if (asset.parent_asset_id === null || !nextAssetsById.has(asset.parent_asset_id)) {
        roots.push(asset);
        continue;
      }
      const children = nextChildren.get(asset.parent_asset_id) ?? [];
      children.push(asset);
      nextChildren.set(asset.parent_asset_id, children);
    }
    const nextDevicesByAsset = new Map<string, PowerDevice[]>();
    const unassigned: PowerDevice[] = [];
    for (const device of devices) {
      if (device.asset_id === null || !nextAssetsById.has(device.asset_id)) {
        unassigned.push(device);
        continue;
      }
      const assigned = nextDevicesByAsset.get(device.asset_id) ?? [];
      assigned.push(device);
      nextDevicesByAsset.set(device.asset_id, assigned);
    }
    return {
      assetsById: nextAssetsById,
      childrenByParent: nextChildren,
      devicesByAsset: nextDevicesByAsset,
      rootAssets: roots.sort((left, right) => left.name.localeCompare(right.name)),
      unassignedDevices: unassigned.sort(deviceSort),
    };
  }, [assets, devices]);

  const matchesDevice = (device: PowerDevice) => query === ""
    || `${device.display_name ?? ""} ${device.device_id}`.toLowerCase().includes(query);
  const assetVisible = (asset: PowerAsset, visited = new Set<string>()): boolean => {
    if (visited.has(asset.id)) {
      return false;
    }
    const nextVisited = new Set(visited).add(asset.id);
    if (query === "" || asset.name.toLowerCase().includes(query)) {
      return true;
    }
    if ((devicesByAsset.get(asset.id) ?? []).some(matchesDevice)) {
      return true;
    }
    return (childrenByParent.get(asset.id) ?? []).some((child) => assetVisible(child, nextVisited));
  };
  const toggle = (assetId: string) => {
    setCollapsedAssets((current) => {
      const next = new Set(current);
      if (next.has(assetId)) {
        next.delete(assetId);
      } else {
        next.add(assetId);
      }
      return next;
    });
  };

  const renderDevice = (device: PowerDevice, depth: number) => (
    <div
      className={`powermonitor-tree-row powermonitor-tree-device ${selectedDeviceId === device.device_id ? "is-selected" : ""}`}
      key={device.device_id}
    >
      <button
        className="tree-device-button"
        onClick={() => onSelectDevice(device.device_id)}
        style={{ paddingInlineStart: 14 + depth * 18 }}
        type="button"
      >
        <span className={device.online ? "tree-status is-online" : "tree-status is-offline"} />
        <Cpu aria-hidden="true" size={15} />
        <span className="tree-label">{device.display_name ?? device.device_id}</span>
        <span className="tree-kind">{deviceKind(device)}</span>
      </button>
      {onEditDevice !== undefined && (
        <button
          aria-label={`Edit meter ${device.display_name ?? device.device_id}`}
          className="tree-row-edit"
          onClick={() => onEditDevice(device.device_id)}
          title={`Edit meter ${device.display_name ?? device.device_id}`}
          type="button"
        >
          <Pencil aria-hidden="true" size={14} />
        </button>
      )}
    </div>
  );

  const renderAsset = (asset: PowerAsset, depth: number, visited = new Set<string>()): React.ReactNode => {
    if (!assetVisible(asset, visited)) {
      return null;
    }
    const nextVisited = new Set(visited).add(asset.id);
    const children = (childrenByParent.get(asset.id) ?? [])
      .filter((child) => !nextVisited.has(child.id))
      .sort((left, right) => left.name.localeCompare(right.name));
    const attachedDevices = (devicesByAsset.get(asset.id) ?? []).filter(matchesDevice).sort(deviceSort);
    const hasChildren = children.length > 0 || attachedDevices.length > 0;
    const expanded = !collapsedAssets.has(asset.id);
    return (
      <div className="powermonitor-tree-branch" key={asset.id}>
        <div className={`powermonitor-tree-row powermonitor-tree-asset ${selectedAssetId === asset.id ? "is-selected" : ""}`} style={{ paddingInlineStart: 8 + depth * 18 }}>
          <button
            aria-label={`${expanded ? "Collapse" : "Expand"} ${asset.name}`}
            className="tree-toggle"
            disabled={!hasChildren}
            onClick={() => toggle(asset.id)}
            type="button"
          >
            {hasChildren && (expanded ? <ChevronDown aria-hidden="true" size={15} /> : <ChevronRight aria-hidden="true" size={15} />)}
          </button>
          <button className="tree-asset-button" onClick={() => onSelectAsset(asset.id)} type="button">
            <FolderTree aria-hidden="true" size={15} />
            <span className="tree-label">{asset.name}</span>
            <span className={asset.asset_profile_id === null ? "tree-kind is-missing" : "tree-kind"}>
              {asset.asset_profile_id === null ? "No asset profile" : "Asset"}
            </span>
          </button>
          {onEditAsset !== undefined && (
            <button
              aria-label={`Edit location ${asset.name}`}
              className="tree-row-edit"
              onClick={() => onEditAsset(asset.id)}
              title={`Edit location ${asset.name}`}
              type="button"
            >
              <Pencil aria-hidden="true" size={14} />
            </button>
          )}
        </div>
        {expanded && (
          <div>
            {children.map((child) => renderAsset(child, depth + 1, nextVisited))}
            {attachedDevices.map((device) => renderDevice(device, depth + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <nav aria-label="Power Monitor asset tree" className="powermonitor-tree">
      {rootAssets.map((asset) => renderAsset(asset, 0))}
      {unassignedDevices.filter(matchesDevice).length > 0 && (
        <section className="powermonitor-tree-unassigned">
          <div className="powermonitor-tree-row tree-unassigned-heading">
            <Radio aria-hidden="true" size={15} />
            <span>Unassigned devices</span>
          </div>
          {unassignedDevices.filter(matchesDevice).map((device) => renderDevice(device, 1))}
        </section>
      )}
      {rootAssets.length === 0 && unassignedDevices.filter(matchesDevice).length === 0 && (
        <p className="directory-empty">No assets or devices matched this view.</p>
      )}
    </nav>
  );
}

function deviceSort(left: PowerDevice, right: PowerDevice): number {
  return (left.display_name ?? left.device_id).localeCompare(right.display_name ?? right.device_id);
}

function deviceKind(device: PowerDevice): string {
  if (device.is_gateway) {
    return `Gateway · ${device.gateway_status ?? (device.online ? "online" : "offline")}`;
  }
  if (device.gateway_device_id !== null && device.gateway_device_id !== undefined) {
    return `Child · ${device.child_status ?? "unavailable"}`;
  }
  return "Direct";
}

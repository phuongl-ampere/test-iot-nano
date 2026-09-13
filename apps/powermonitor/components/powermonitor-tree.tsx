import type { Asset, Device } from "../lib/browser-api";

type PowerMonitorTreeProps = {
  assets: Asset[];
  devices: Device[];
  onSelectAsset(assetId: string): void;
  onSelectDevice(deviceId: string): void;
  selectedAssetId: string | null;
  selectedDeviceId: string | null;
};

export function PowerMonitorTree({
  assets,
  devices,
  onSelectAsset,
  onSelectDevice,
  selectedAssetId,
  selectedDeviceId,
}: PowerMonitorTreeProps) {
  const childrenByAsset = new Map<string, Asset[]>();
  const roots: Asset[] = [];
  for (const asset of assets) {
    if (asset.parent_id === undefined || asset.parent_id === null) {
      roots.push(asset);
      continue;
    }
    const children = childrenByAsset.get(asset.parent_id) ?? [];
    children.push(asset);
    childrenByAsset.set(asset.parent_id, children);
  }
  const devicesByAsset = new Map<string, Device[]>();
  const unassigned: Device[] = [];
  for (const device of devices) {
    if (device.asset_id === undefined || device.asset_id === null) {
      unassigned.push(device);
      continue;
    }
    const assigned = devicesByAsset.get(device.asset_id) ?? [];
    assigned.push(device);
    devicesByAsset.set(device.asset_id, assigned);
  }

  const renderDevice = (device: Device, depth: number) => {
    const name = device.name ?? device.id;
    return (
      <li className="tree-device" key={device.id} style={{ paddingInlineStart: depth * 16 }}>
        <button
          aria-current={selectedDeviceId === device.id ? "true" : undefined}
          aria-label={"Select " + name}
          className="tree-row"
          onClick={() => onSelectDevice(device.id)}
          type="button"
        >
          <span aria-hidden="true" className={device.online ? "status-dot online" : "status-dot"} />
          <span>{name}</span>
        </button>
      </li>
    );
  };

  const renderAsset = (asset: Asset, depth: number, seen: Set<string>): React.ReactNode => {
    if (seen.has(asset.id)) {
      return null;
    }
    const nextSeen = new Set(seen);
    nextSeen.add(asset.id);
    const children = (childrenByAsset.get(asset.id) ?? []).sort(assetName);
    const assignedDevices = (devicesByAsset.get(asset.id) ?? []).sort(deviceName);
    return (
      <li className="tree-asset" key={asset.id}>
        <button
          aria-current={selectedAssetId === asset.id ? "true" : undefined}
          aria-label={"Select " + asset.name}
          className="tree-row"
          onClick={() => onSelectAsset(asset.id)}
          style={{ paddingInlineStart: depth * 16 }}
          type="button"
        >
          <span aria-hidden="true" className="asset-mark" />
          <span>{asset.name}</span>
        </button>
        {(children.length > 0 || assignedDevices.length > 0) && (
          <ul>
            {children.map((child) => renderAsset(child, depth + 1, nextSeen))}
            {assignedDevices.map((device) => renderDevice(device, depth + 1))}
          </ul>
        )}
      </li>
    );
  };

  return (
    <nav aria-label="Asset explorer" className="powermonitor-tree">
      <ul>{roots.sort(assetName).map((asset) => renderAsset(asset, 0, new Set()))}</ul>
      {unassigned.length > 0 && (
        <section className="tree-unassigned">
          <h3>Unassigned devices</h3>
          <ul>{unassigned.sort(deviceName).map((device) => renderDevice(device, 0))}</ul>
        </section>
      )}
      {assets.length === 0 && devices.length === 0 && (
        <p className="empty-state">No devices or assets are available.</p>
      )}
    </nav>
  );
}

function assetName(left: Asset, right: Asset): number {
  return left.name.localeCompare(right.name);
}

function deviceName(left: Device, right: Device): number {
  return (left.name ?? left.id).localeCompare(right.name ?? right.id);
}

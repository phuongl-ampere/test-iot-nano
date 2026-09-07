"use client";

import { ChevronRight, Wifi, WifiOff } from "lucide-react";

import type { DeviceSummary } from "../lib/api";

interface DeviceTableProps {
  devices: DeviceSummary[];
  selectedDeviceId: string | null;
  onSelect(deviceId: string): void;
}

export function DeviceTable({ devices, selectedDeviceId, onSelect }: DeviceTableProps) {
  if (devices.length === 0) {
    return <div className="directory-empty">No devices matched this view.</div>;
  }

  return (
    <div className="device-table" role="list">
      {devices.map((device) => {
        const selected = device.device_id === selectedDeviceId;
        const label = device.display_name ?? device.device_id;

        return (
          <div key={device.device_id} role="listitem">
            <button
              className={`device-row ${selected ? "is-selected" : ""}`}
              onClick={() => onSelect(device.device_id)}
              type="button"
              aria-pressed={selected}
            >
              <span className={`signal-rail ${device.online ? "is-online" : "is-offline"}`} />
              <span className="device-row-main">
                <span className="device-row-name">{label}</span>
                <span className="device-row-id">{device.device_id}</span>
              </span>
              <span className={`device-presence ${device.online ? "is-online" : "is-offline"}`}>
                {device.online ? <Wifi aria-hidden="true" size={15} /> : <WifiOff aria-hidden="true" size={15} />}
                <span>{device.online ? "Online" : "Offline"}</span>
              </span>
              <ChevronRight aria-hidden="true" className="row-chevron" size={16} />
            </button>
          </div>
        );
      })}
    </div>
  );
}

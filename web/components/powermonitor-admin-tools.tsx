"use client";

import { FolderPlus, Link2, PlusCircle } from "lucide-react";

export function PowerMonitorAdminTools({
  onAddAsset,
  onAddDevice,
  onAssignDevice,
  canManage,
  selectedDevice,
}: {
  onAddAsset(): void;
  onAddDevice(): void;
  onAssignDevice(): void;
  canManage: boolean;
  selectedDevice: boolean;
}) {
  if (!canManage) {
    return null;
  }
  return (
    <div aria-label="Power Monitor setup actions" className="powermonitor-admin-tools">
      <button className="powermonitor-admin-action is-primary" onClick={onAddAsset} title="Add location" type="button">
        <FolderPlus aria-hidden="true" size={16} />
        <span>Add location</span>
      </button>
      <button className="powermonitor-admin-action" onClick={onAddDevice} title="Add meter" type="button">
        <PlusCircle aria-hidden="true" size={16} />
        <span>Add meter</span>
      </button>
      <button
        className="powermonitor-admin-action"
        disabled={!selectedDevice}
        onClick={onAssignDevice}
        title={selectedDevice ? "Assign selected meter" : "Select a meter to assign"}
        type="button"
      >
        <Link2 aria-hidden="true" size={16} />
        <span>Assign meter</span>
      </button>
    </div>
  );
}

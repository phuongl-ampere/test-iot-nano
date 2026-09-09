"use client";

import { Power, RotateCw } from "lucide-react";

import type { DeviceCommandMethod, ResourcePermission, RpcMode } from "../lib/api";

export function PowerSwitcherControl({
  busy,
  commandState,
  onCommand,
  permission,
  switchState,
}: {
  busy: boolean;
  commandState: string | null;
  onCommand(command: Extract<DeviceCommandMethod, "switch_on" | "switch_off">, mode: RpcMode): void;
  permission: ResourcePermission;
  switchState: boolean | null | undefined;
}) {
  const stateLabel = switchState === true ? "On" : switchState === false ? "Off" : "Unknown";
  const canControl = permission !== "viewer";

  return (
    <section aria-label="Power switch control" className="power-switcher-control">
      <header>
          <div>
            <span className="eyebrow">PowerSwitcher</span>
          <h2>Relay control</h2>
        </div>
        <span className={`switch-state switch-state-${stateLabel.toLowerCase()}`}>
          <Power aria-hidden="true" size={15} />
          {stateLabel}
        </span>
      </header>
      {canControl && (
        <div className="power-switcher-actions">
          <div className="two-way-rpc-state">
            <RotateCw aria-hidden="true" size={14} />
            Two-way confirmation
          </div>
          <div className="switch-command-actions">
            <button
              className="switch-command-on"
              disabled={busy || switchState === true}
              onClick={() => onCommand("switch_on", "two_way")}
              type="button"
            >
              Relay on
            </button>
            <button
              className="switch-command-off"
              disabled={busy || switchState === false}
              onClick={() => onCommand("switch_off", "two_way")}
              type="button"
            >
              Relay off
            </button>
          </div>
        </div>
      )}
      {commandState !== null && <p className="switch-command-status">Command {commandState}</p>}
    </section>
  );
}

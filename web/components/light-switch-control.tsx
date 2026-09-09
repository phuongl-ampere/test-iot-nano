"use client";

import { Lightbulb, RotateCw } from "lucide-react";
import { useEffect, useState } from "react";

import type { DeviceCommandMethod, ResourcePermission, RpcMode } from "../lib/api";

export function LightSwitchControl({
  brightnessPct,
  busy,
  commandState,
  onCommand,
  permission,
  switchState,
}: {
  brightnessPct: number | null | undefined;
  busy: boolean;
  commandState: string | null;
  onCommand(
    command: Extract<DeviceCommandMethod, "switch_on" | "switch_off" | "set_brightness">,
    params: Record<string, number>,
    mode: RpcMode,
  ): void;
  permission: ResourcePermission;
  switchState: boolean | null | undefined;
}) {
  const initialBrightness = Math.round(Math.min(100, Math.max(0, brightnessPct ?? 0)));
  const [brightness, setBrightness] = useState(initialBrightness);
  const stateLabel = switchState === true ? "On" : switchState === false ? "Off" : "Unknown";
  const canControl = permission !== "viewer";

  useEffect(() => setBrightness(initialBrightness), [initialBrightness]);

  return (
    <section aria-label="Light switch control" className="power-switcher-control light-switch-control">
      <header>
        <div>
          <span className="eyebrow">LightSwitch</span>
          <h2>Lighting control</h2>
        </div>
        <span className={`switch-state switch-state-${stateLabel.toLowerCase()}`}>
          <Lightbulb aria-hidden="true" size={15} />
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
              onClick={() => onCommand("switch_on", {}, "two_way")}
              type="button"
            >
              Light on
            </button>
            <button
              className="switch-command-off"
              disabled={busy || switchState === false}
              onClick={() => onCommand("switch_off", {}, "two_way")}
              type="button"
            >
              Light off
            </button>
          </div>
          <div className="light-brightness-control">
            <label htmlFor="light-brightness">Brightness <strong>{brightness}%</strong></label>
            <input
              aria-label="Brightness"
              disabled={busy}
              id="light-brightness"
              max="100"
              min="0"
              onChange={(event) => setBrightness(Number(event.target.value))}
              step="1"
              type="range"
              value={brightness}
            />
            <button
              disabled={busy}
              onClick={() => onCommand("set_brightness", { brightness_pct: brightness }, "two_way")}
              type="button"
            >
              Apply brightness
            </button>
          </div>
        </div>
      )}
      {commandState !== null && <p className="switch-command-status">Command {commandState}</p>}
    </section>
  );
}

"use client";

import { useEffect, useState } from "react";

import type { CommandMode } from "../lib/browser-api";

type DeviceControlPanelProps = {
  brightnessPct?: number | null;
  busy: boolean;
  canControl?: boolean;
  capabilities?: string[];
  commandState: string | null;
  onCommand(method: "switch_on" | "switch_off" | "set_brightness", params: Record<string, number>, mode: CommandMode): void;
  switchState?: boolean | null;
};

export function DeviceControlPanel({
  brightnessPct,
  busy,
  canControl = true,
  capabilities = [],
  commandState,
  onCommand,
  switchState,
}: DeviceControlPanelProps) {
  const initialBrightness = clampBrightness(brightnessPct ?? 0);
  const [brightness, setBrightness] = useState(initialBrightness);
  const canSwitch = capabilities.includes("switch");
  const canSetBrightness = capabilities.includes("brightness");

  useEffect(() => {
    setBrightness(initialBrightness);
  }, [initialBrightness]);

  if (!canSwitch && !canSetBrightness) {
    return null;
  }

  return (
    <section aria-label="Device controls" className="device-control-panel">
      <header>
        <div>
          <span className="eyebrow">Device controls</span>
          <h2>Relay and brightness</h2>
        </div>
        {canSwitch && <span className="switch-state">{switchState === true ? "On" : switchState === false ? "Off" : "Unknown"}</span>}
      </header>
      {canControl && (
        <div className="device-control-actions">
          {canSwitch && (
            <div className="relay-actions">
              <button
                disabled={busy || switchState === true}
                onClick={() => onCommand("switch_on", {}, "two_way")}
                type="button"
              >
                Relay on
              </button>
              <button
                disabled={busy || switchState === false}
                onClick={() => onCommand("switch_off", {}, "two_way")}
                type="button"
              >
                Relay off
              </button>
            </div>
          )}
          {canSetBrightness && (
            <div className="brightness-control">
              <label htmlFor="brightness-range">Brightness</label>
              <input
                aria-label="Brightness"
                disabled={busy}
                id="brightness-range"
                max="100"
                min="0"
                onChange={(event) => setBrightness(clampBrightness(Number(event.target.value)))}
                step="1"
                type="range"
                value={brightness}
              />
              <input
                aria-label="Brightness value"
                disabled={busy}
                max="100"
                min="0"
                onChange={(event) => setBrightness(clampBrightness(Number(event.target.value)))}
                step="1"
                type="number"
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
          )}
        </div>
      )}
      {commandState !== null && <p className="command-state">Command state: {commandState}</p>}
    </section>
  );
}

function clampBrightness(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.round(Math.min(100, Math.max(0, value)));
}

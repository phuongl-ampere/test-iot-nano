"use client";

import { useState } from "react";

import type { CommandMode } from "../lib/browser-api";

type CommandPanelProps = {
  busy: boolean;
  disabled?: boolean;
  onSubmit(method: string, params: Record<string, unknown>, mode: CommandMode): void;
  response?: Record<string, unknown> | null;
  state?: string | null;
};

export function CommandPanel({
  busy,
  disabled = false,
  onSubmit,
  response = null,
  state = null,
}: CommandPanelProps) {
  const [method, setMethod] = useState("sample_now");
  const [mode, setMode] = useState<CommandMode>("one_way");

  return (
    <section aria-label="Send command" className="command-panel">
      <div>
        <span className="eyebrow">Device command</span>
        <h2>Send command</h2>
      </div>
      <div className="command-controls">
        <label>
          <span>Command</span>
          <select aria-label="Command" disabled={busy || disabled} onChange={(event) => setMethod(event.target.value)} value={method}>
            <option value="sample_now">Sample now</option>
            <option value="reboot">Reboot</option>
            <option value="switch_on">Switch on</option>
            <option value="switch_off">Switch off</option>
          </select>
        </label>
        <div aria-label="Command delivery mode" className="command-mode" role="group">
          <button
            aria-pressed={mode === "one_way"}
            disabled={busy || disabled}
            onClick={() => setMode("one_way")}
            type="button"
          >
            One-way
          </button>
          <button
            aria-pressed={mode === "two_way"}
            disabled={busy || disabled}
            onClick={() => setMode("two_way")}
            type="button"
          >
            Two-way
          </button>
        </div>
        <button disabled={busy || disabled} onClick={() => onSubmit(method, {}, mode)} type="button">
          {busy ? "Sending" : "Send command"}
        </button>
      </div>
      {state !== null && <p className="command-state">Command state: {state}</p>}
      {response !== null && (
        <pre aria-label="Command response" className="command-response">
          {JSON.stringify(response, null, 2)}
        </pre>
      )}
    </section>
  );
}

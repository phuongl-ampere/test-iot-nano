"use client";

import { useState } from "react";

type CommandPanelProps = {
  busy: boolean;
  disabled?: boolean;
  onSubmit(method: string, params: Record<string, unknown>): void;
  state?: string | null;
};

export function CommandPanel({ busy, disabled = false, onSubmit, state = null }: CommandPanelProps) {
  const [method, setMethod] = useState("sample_now");

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
        <button disabled={busy || disabled} onClick={() => onSubmit(method, {})} type="button">
          {busy ? "Sending" : "Send command"}
        </button>
      </div>
      {state !== null && <p className="command-state">Command state: {state}</p>}
    </section>
  );
}
